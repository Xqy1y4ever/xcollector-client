"""源库读取器：对着一个**按 nt_msg_db_util 输出结构造**的库跑一遍。

    .\\.venv\\Scripts\\python.exe -m tests.check_source_db

## 为什么先测这一层

客户端的输入就是这一个库。它的表名、列名、时间单位（**秒**）、`content` 的
JSON 形状全都来自别的项目（`QQBackup/nt_msg_db_util`），我们**没有**改它的能力，
只能照着它的文档适配。所以这一层的错法全都很难看：读错列 = 一条都读不出来、
时间单位搞错 = 所有通知的截止时间差 1000 倍、`content` 解析错 = 附件全丢。

这个文件不联网、不碰后端，用临时目录里的库跑。
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
from pathlib import Path

from app.mirror import Mirror
from app.source.attachments import AttachmentResolver, guess_content_type
from app.source.ntmsg import (
    SourceDatabase,
    SourceDatabaseError,
    attachments_from_content,
)

# 临时目录放在仓库里（`.tmp-test/`，已 gitignore），**不用系统 temp**：
# 系统 temp 在文件沙箱下会在清理阶段被拒绝 chmod（WinError 5），
# 那会让一个全通过的测试以一个看不懂的 PermissionError 收场。
SCRATCH = Path(__file__).resolve().parent.parent / ".tmp-test"

fails: list[str] = []
total = 0


def check(name: str, got, want) -> None:
    global total
    total += 1
    if got == want:
        print(f"ok    {name}")
    else:
        fails.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f"  {detail}" if detail else ""), bool(cond), True)


# ---------------------------------------------------------------------------
# 造一个 fixture 库
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE group_messages (
  msg_id       TEXT PRIMARY KEY,
  timestamp    INTEGER,
  direction    INTEGER,
  sender_uid   TEXT,
  sender_qq    TEXT,
  group_id     TEXT,
  group_qq     TEXT,
  msg_type     INTEGER,
  subtype      INTEGER,
  content_type INTEGER,
  text         TEXT,
  parse_status TEXT,
  content      TEXT
);
"""


def image_content(name: str, url: str, md5: str = "d41d8cd98f00b204e9800998ecf8427e") -> str:
    return json.dumps(
        {"type": "image", "filename": name, "width": 100, "height": 50,
         "filesize": 2048, "md5_hex": md5, "cdn_url": url},
        ensure_ascii=False,
    )


def build_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.executemany(
            "INSERT INTO group_messages (msg_id, timestamp, direction, sender_uid, sender_qq,"
            " group_id, group_qq, msg_type, subtype, content_type, text, parse_status, content)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def rows_fixture() -> list[tuple]:
    return [
        # (msg_id, ts, direction, uid, qq, group, group_qq, mtype, subtype, ctype, text, status, content)
        ("1001", 1757692800, 0, "u_a", "10001", "123456789", 123456789, 2, 0, 1,
         "大家下周三前把军训心得交到班长那里，不少于800字。", "typed",
         json.dumps({"type": "text", "text": "大家下周三前把军训心得交到班长那里，不少于800字。"}, ensure_ascii=False)),
        # 同一秒里的第二条：只按秒比会漏掉它 —— 这正是要测的
        ("1002", 1757692800, 0, "u_a", "10001", "123456789", 123456789, 2, 0, 1,
         "收到", "typed", json.dumps({"type": "text", "text": "收到"}, ensure_ascii=False)),
        ("1003", 1757692860, 0, "u_b", "10002", "123456789", 123456789, 2, 0, 2,
         "本周五19:00在教三201开班会", "typed",
         json.dumps({"type": "mixed", "segments": [
             {"type": "text", "text": "本周五19:00在教三201开班会"},
             {"type": "image", "filename": "通知.png", "cdn_url": "https://cdn.example/notice.png",
              "md5_hex": "abc123", "filesize": 4096},
         ]}, ensure_ascii=False)),
        ("1004", 1757692920, 0, "u_c", "10003", "987654321", 987654321, 2, 0, 3,
         "[文件] 实验报告模板", "typed",
         json.dumps({"type": "file", "filename": "模板.docx", "filesize": 10240,
                     "md5_hex": "def456", "ext": ".docx"}, ensure_ascii=False)),
        # parse_status=wire_fallback 且 content 是空的：上游没解析出来
        ("1005", 1757692980, 0, "u_c", "10003", "987654321", 987654321, 2, 0, 0,
         "", "wire_fallback", None),
    ]


def main() -> int:  # noqa: C901
    if SCRATCH.exists():
        # 上一次的残留：尽力清掉，清不掉也无所谓（测试自己会重建表）
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    if True:
        tmp_path = SCRATCH

        # ------------------------------------------------------------------
        print("--- 1. 自检（inspect）---")
        db_file = tmp_path / "nt_msg_export.db"
        build_db(db_file, rows_fixture())
        db = SourceDatabase(db_file)
        info = db.inspect()
        check("行数", info["rows"], 5)
        check("表名", info["table"], "group_messages")
        check("没有缺列", info["absent"], [])
        check_true("列里有 sender_qq", "sender_qq" in info["columns"], str(info["columns"]))

        # ------------------------------------------------------------------
        print("\n--- 2. 时间单位是**秒**（差 1000 倍是最容易犯的错）---")
        first = db.fetch_since(0, "", limit=10)[0]
        check("source 的 timestamp 原样是秒", first.timestamp, 1757692800)
        check("转成毫秒给后端（×1000）", first.ts_ms, 1757692800000)
        check_true("毫秒值量级像个 2025 年的时间戳", 1.7e12 < first.ts_ms < 1.8e12, str(first.ts_ms))

        # ------------------------------------------------------------------
        print("\n--- 3. 增量读：同一秒里的多条不能漏 ---")
        all_rows = db.fetch_since(0, "", limit=10)
        check("全取 5 条", len(all_rows), 5)
        check("按时间正序", [r.msg_id for r in all_rows], ["1001", "1002", "1003", "1004", "1005"])

        # 取到 1001 之后：同一秒的 1002 必须还在
        nxt = db.fetch_since(1757692800, "1001", limit=10)
        check("从 (1757692800, 1001) 之后读 → 不含 1001", [r.msg_id for r in nxt][0], "1002")
        check("同一秒的 1002 没被跳过（这正是用元组比较的原因）", "1002" in [r.msg_id for r in nxt], True)

        # 取到 1002 之后：同秒的没有了，下一条是 1003
        nxt2 = db.fetch_since(1757692800, "1002", limit=10)
        check("从 (1757692800, 1002) 之后读 → 下一条是 1003", [r.msg_id for r in nxt2][0], "1003")

        # 幂等：用最后一条的位置再读一次，应该是空的
        check("从最后一条之后读 → 空", db.fetch_since(1757692980, "1005", limit=10), [])

        # ------------------------------------------------------------------
        print("\n--- 4. 分块迭代（iter_since）---")
        got = [m.msg_id for m in db.iter_since(0, "", limit=3, chunk=2)]
        check("limit=3 只取 3 条", got, ["1001", "1002", "1003"])
        got_all = [m.msg_id for m in db.iter_since(0, "", limit=100, chunk=2)]
        check("limit 大于总数时取全部", got_all, ["1001", "1002", "1003", "1004", "1005"])

        # ------------------------------------------------------------------
        print("\n--- 4b. 按「读没读过」扫描（主路径）---")
        # 判据是镜像里的已读标记，不是时间：镜像里没有的都要读，不管它多老。
        mirror_db = tmp_path / "mirror.db"
        if mirror_db.exists():
            mirror_db.unlink()
        mirror = Mirror(mirror_db)

        check("镜像不存在时全部算没读过", db.count_unread(mirror_db), 5)
        unread = [m.msg_id for m in db.iter_unread(mirror_db, limit=10)]
        check("顺序是从最老的开始（补充关系的原文必须先于补充）", unread, ["1001", "1002", "1003", "1004", "1005"])
        check("一处也没有重复", len(unread), len(set(unread)))

        # 标记两条已读（内容指纹随便给，这里只测"读没读过"）
        for msg_id in ("1001", "1002"):
            mirror.claim(db.fetch_by_ids([msg_id])[msg_id])
            mirror.finish(msg_id, state="done")
        check("读过的就不再出现", db.count_unread(mirror_db), 3)
        check(
            "剩下的三条按时间正序",
            [m.msg_id for m in db.iter_unread(mirror_db, limit=10)],
            ["1003", "1004", "1005"],
        )
        check("limit 生效（分多轮读）", [m.msg_id for m in db.iter_unread(mirror_db, limit=2)], ["1003", "1004"])

        # 分页不能因为"处理时会把镜像写进去"而重复：这里刻意**不写镜像**，
        # 模拟 --dry-run（dry-run 是不记账的，靠副作用翻页就会反复读同一批）。
        paged = [m.msg_id for m in db.iter_unread(mirror_db, limit=3, chunk=1)]
        check("一块一条地翻页也不重复", paged, ["1003", "1004", "1005"])

        # 回看模式：已读的也要能重看（发现"内容被编辑"就靠它），且是最新优先
        rechecked = [
            m.msg_id
            for m in db.iter_unread(mirror_db, limit=10, recheck_since=1757692800)
        ]
        check("回看模式会把已读的也带回来", rechecked, ["1005", "1004", "1003", "1002", "1001"])
        check(
            "回看模式是最新优先（预算不够时先看最新的）",
            [m.msg_id for m in db.iter_unread(mirror_db, limit=2, recheck_since=1757692800)],
            ["1005", "1004"],
        )
        check(
            "回看窗口只决定「读过的」要不要重看，不缩小没读过的范围",
            [m.msg_id for m in db.iter_unread(mirror_db, limit=10, recheck_since=1757692900)],
            ["1005", "1004", "1003"],
        )

        # 排除这一轮已经处理过的（回看窗口与「没读过」是并集，不排就会处理两遍）。
        check(
            "回看可以排除这一轮已经看过的",
            [
                m.msg_id
                for m in db.iter_unread(
                    mirror_db, limit=10, recheck_since=1757692800, exclude={"1005"}
                )
            ],
            ["1004", "1003", "1002", "1001"],
        )
        check(
            "排除名单跟着分页走（chunk=1 也不能漏）",
            [
                m.msg_id
                for m in db.iter_unread(
                    mirror_db,
                    limit=10,
                    chunk=1,
                    recheck_since=1757692800,
                    exclude={"1005", "1004"},
                )
            ],
            ["1003", "1002", "1001"],
        )
        check(
            "未读模式也能排除（排除掉的那条不该占位置）",
            [m.msg_id for m in db.iter_unread(mirror_db, limit=10, exclude={"1003"})],
            ["1004", "1005"],
        )

        # msg_id 类型：镜像里是 TEXT，导出表里可能是 INTEGER —— 不转类型的话
        # 反连接会"全部命中"，等于每次把整个库读一遍。
        int_ids = tmp_path / "int_ids.db"
        if int_ids.exists():
            int_ids.unlink()
        conn = sqlite3.connect(int_ids)
        try:
            conn.executescript(
                SCHEMA.replace("TEXT PRIMARY KEY", "INTEGER PRIMARY KEY")
            )
            conn.executemany(
                "INSERT INTO group_messages (msg_id, timestamp, direction, sender_uid, sender_qq,"
                " group_id, group_qq, msg_type, content_type, text, parse_status, content)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [(7001, 1757693000, 0, "u_a", "10001", "123456789", 123456789, 2, 1, "整数 id", "typed", None)],
            )
            conn.commit()
        finally:
            conn.close()
        int_db = SourceDatabase(int_ids)
        int_db.inspect()
        check("整数 msg_id 的库：镜像空 → 1 条没读过", int_db.count_unread(mirror_db), 1)
        mirror_int = Mirror(tmp_path / "mirror-int.db")
        mirror_int.claim(int_db.fetch_by_ids(["7001"])["7001"])
        mirror_int.finish("7001", state="done")
        check("整数 msg_id 也能被反连接排除（类型转换对了）", int_db.count_unread(tmp_path / "mirror-int.db"), 0)

        # ------------------------------------------------------------------
        print("\n--- 5. 正文与附件解析 ---")
        check("纯文本消息的正文", all_rows[0].text, "大家下周三前把军训心得交到班长那里，不少于800字。")
        check("纯文本消息没有附件", all_rows[0].attachments, [])
        check("发送者取自 sender_qq", all_rows[0].sender_id, "10001")
        check("群号取自 group_id", all_rows[0].group_id, "123456789")

        mixed = all_rows[2]
        check("mixed 的正文", mixed.text, "本周五19:00在教三201开班会")
        check("mixed 里解析出 1 个附件", len(mixed.attachments), 1)
        check("附件的名字", mixed.attachments[0].get("name"), "通知.png")
        check("附件的远程地址", mixed.attachments[0].get("url"), "https://cdn.example/notice.png")
        check("附件的 md5", mixed.attachments[0].get("md5"), "abc123")

        doc = all_rows[3]
        check("文件类型附件", doc.attachments[0].get("type"), "file")
        check("文件名", doc.attachments[0].get("name"), "模板.docx")

        check("空 content 不会炸", all_rows[4].attachments, [])
        check("空 content 的 raw_content", all_rows[4].raw_content, None)
        check("parse_status 保留下来了", all_rows[4].parse_status, "wire_fallback")

        # ------------------------------------------------------------------
        print("\n--- 6. content 解析的边界 ---")
        check("None → 空", attachments_from_content(None), [])
        check("不是 dict → 空", attachments_from_content({"type": "text", "text": "x"}), [])
        # 既没 url 也没名字也没 md5 的附件：记下来只会显示一个打不开的空条目
        check(
            "没地址没名字的附件不记（免得前端显示空条目）",
            attachments_from_content({"type": "image", "width": 3}),
            [],
        )
        check(
            "嵌套在 reply/forward 里的图片也能找到",
            len(attachments_from_content({"type": "reply", "content": {"type": "image", "filename": "r.png"}})),
            1,
        )

        # ------------------------------------------------------------------
        print("\n--- 7. 上游换版本：缺列要降级，不要崩 ---")
        lean = tmp_path / "lean.db"
        conn = sqlite3.connect(lean)
        conn.executescript(
            "CREATE TABLE group_messages (msg_id TEXT PRIMARY KEY, timestamp INTEGER,"
            " sender_qq TEXT, group_id TEXT);"
        )
        conn.execute("INSERT INTO group_messages VALUES ('1', 1757692800, '10001', '123456789')")
        conn.commit()
        conn.close()
        db_lean = SourceDatabase(lean)
        info_lean = db_lean.inspect()
        check_true("缺列被报出来了", len(info_lean["absent"]) > 0, str(info_lean["absent"]))
        check_true("缺 text/content 也能标出来", "text" in info_lean["absent"], str(info_lean["absent"]))
        got_lean = db_lean.fetch_since(0, "", limit=5)
        check("缺列时仍然读得出这一条", len(got_lean), 1)
        check("正文降级成空串（不编内容）", got_lean[0].text, "")
        check("附件降级成空列表", got_lean[0].attachments, [])
        check("群号和发送者还在", (got_lean[0].group_id, got_lean[0].sender_id), ("123456789", "10001"))

        # ------------------------------------------------------------------
        print("\n--- 8. 拿错库 / 文件不在：报错要能指导下一步 ---")
        plain = tmp_path / "nt_msg_plain.db"
        conn = sqlite3.connect(plain)
        conn.executescript("CREATE TABLE group_msg_table (\"40001\" TEXT);")
        conn.commit()
        conn.close()
        try:
            SourceDatabase(plain).inspect()
            check_true("拿 nt_msg_plain.db 来用 → 报错", False, "居然没报错")
        except SourceDatabaseError as exc:
            text = str(exc)
            check_true("说清应该用 nt_msg_export.db", "nt_msg_export.db" in text, text[:160])
            check_true("点名了是 3.export.py", "3.export.py" in text, text[:160])

        try:
            SourceDatabase(tmp_path / "nope.db").inspect()
            check_true("文件不存在 → 报错", False, "居然没报错")
        except SourceDatabaseError as exc:
            check_true("说清了要先用导出工具", "3.export.py" in str(exc), str(exc)[:160])

        lean2 = tmp_path / "lean2.db"
        conn = sqlite3.connect(lean2)
        conn.executescript("CREATE TABLE group_messages (msg_id TEXT);")
        conn.commit()
        conn.close()
        try:
            SourceDatabase(lean2).inspect()
            check_true("缺必需列（timestamp）→ 报错", False, "居然没报错")
        except SourceDatabaseError as exc:
            check_true("点名了缺哪一列", "timestamp" in str(exc), str(exc)[:160])

        # ------------------------------------------------------------------
        print("\n--- 9. 附件字节解析器 ---")
        root = tmp_path / "attachments"
        root.mkdir()
        # 一个以 md5 命名的文件（NTQQ 缓存常见形态）
        md5 = "d41d8cd98f00b204e9800998ecf8427e"
        (root / md5).write_bytes(b"PNGDATA")
        (root / "模板.docx").write_bytes(b"DOCXDATA")

        resolver = AttachmentResolver(root)
        found = resolver.resolve({"type": "image", "name": "x.png", "md5": md5})
        check("按 md5 找到字节", found.content, b"PNGDATA")
        check("找到了就说 found", found.found, True)
        check("没有 reason（找到了不需要解释）", found.reason, None)
        by_name = resolver.resolve({"type": "file", "name": "模板.docx"})
        check("按文件名找到字节", by_name.content, b"DOCXDATA")
        check("MIME 按后缀判", by_name.content_type, guess_content_type("模板.docx"))

        missing = resolver.resolve({"type": "image", "name": "nothere.png", "md5": "ffff"})
        check("找不到时 content 是 None", missing.content, None)
        check_true("并且给出原因（不静默）", bool(missing.reason), str(missing.reason))
        check_true("原因里带了找过的目录", str(root) in str(missing.reason), str(missing.reason))

        disabled = AttachmentResolver(None)
        r = disabled.resolve({"type": "image", "name": "x.png"})
        check("没配根目录时也是 None + 原因", (r.content, bool(r.reason)), (None, True))

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
