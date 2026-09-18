"""`app/ntmsg_db/` 的自检：剥头、解密、导出，以及和读取器的对接。

**离线**跑，不需要后端。

这里用的是**真的库**，不是自己造的假格式：

* 加密库用 `sqlcipher3` 按上游 `1.decrypt.py` 的同一套 PRAGMA 现场造出来，
  再手动加上 1024 字节的 QQ 头 —— 所以"剥头 + 解密"这条链路是真跑的；
* `40800` 里的消息体用**上游 `msgdb` 里的 `_pb2`** 构造（`MsgBody`/`MsgContent`），
  所以导出这一层面对的是真实的 protobuf 结构，而不是手拼的字节。

这样一来，上游改了字段名、表结构或参数，这里会立刻红。

    python -m tests.check_ntmsg_db
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import sqlcipher3.dbapi2 as sc  # noqa: E402

from app.ntmsg_db import (  # noqa: E402
    REPLY_SEQ_COLUMN,
    SEQ_COLUMN,
    DecryptError,
    ExportError,
    decrypt_database,
    export_database,
    open_encrypted,
    sqlcipher_available,
    strip_header,
    watermark,
)
from app.ntmsg_db.prepare import prepare_databases, read_key, resolve_paths  # noqa: E402
from app.ntmsg_db.sqlite_uri import sqlite_uri  # noqa: E402
from app.source.ntmsg import (  # noqa: E402
    SourceDatabase,
    attachments_from_content,
    quote_ref_from_content,
)
from msgdb.proto import c2c_40800_pb2 as pb  # noqa: E402
from msgdb.proto.c2c_40800_parser import parse_40800  # noqa: E402

PASS = 0
FAIL = 0
FAILURES: list[str] = []

(BASE_DIR / ".tmp-test").mkdir(exist_ok=True)
WORK = Path(tempfile.mkdtemp(prefix="xc-ntmsg-", dir=str(BASE_DIR / ".tmp-test")))

KEY = "xI,;bYGeR`i,}H[P"  # 和上游同样形状：16 个 ASCII 字符，带引号/反引号/逗号
HEADER = 1024

# QQ 的表结构（列名就是这些数字；40001 是 INTEGER PRIMARY KEY = rowid 别名）
GROUP_DDL = """CREATE TABLE group_msg_table(
    [40001] INTEGER PRIMARY KEY, [40003] INTEGER, [40011] INTEGER, [40012] INTEGER,
    [40013] INTEGER, [40020] TEXT, [40021] TEXT, [40027] INTEGER, [40033] INTEGER,
    [40050] INTEGER, [40800] BLOB, [40850] INTEGER, [40851] INTEGER, [40030] INTEGER)"""

C2C_DDL = """CREATE TABLE c2c_msg_table(
    [40001] INTEGER PRIMARY KEY, [40003] INTEGER, [40011] INTEGER, [40013] INTEGER,
    [40020] TEXT, [40021] TEXT, [40027] INTEGER, [40030] INTEGER, [40033] INTEGER,
    [40050] INTEGER, [40800] BLOB, [40850] INTEGER)"""

# 没有 INTEGER 主键的表（用来测"不能续跑就得重拷"那条分支）
PLAIN_DDL = "CREATE TABLE ark_to_markdown_config_table([40001] INTEGER, [40002] TEXT)"


def check(condition: bool, label: str) -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(label)
        print(f"  ✗ {label}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def expect_raises(exc_type, fn, label: str, *, contains: str = "") -> Exception | None:
    try:
        fn()
    except exc_type as exc:  # noqa: PERF203
        if contains and contains not in str(exc):
            check(False, f"{label}（异常消息里没有 {contains!r}：{exc}）")
            return exc
        check(True, label)
        return exc
    except Exception as exc:  # noqa: BLE001
        check(False, f"{label}（抛的是 {type(exc).__name__}: {exc}）")
        return exc
    check(False, f"{label}（没有抛异常）")
    return None


# ---------------------------------------------------------------------------
# 造夹具：真的 SQLCipher 库 + 真的 protobuf 消息体
# ---------------------------------------------------------------------------


def body(*segments: pb.MsgContent) -> bytes:
    """`MsgBody{ repeated content = 40800 }` 的序列化结果。"""
    msg = pb.MsgBody()
    for segment in segments:
        msg.content.append(segment)
    return msg.SerializeToString()


def sqlcipher3_connect(path: Path):
    """按上游的 PRAGMA 打开一个 SQLCipher 库（测试里用来读页号等元信息）。"""
    conn = sc.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA cipher_page_size = 4096;")
    conn.execute(f"PRAGMA key = '{KEY.replace(chr(39), chr(39) * 2)}';")
    conn.execute("PRAGMA kdf_iter = 4000;")
    conn.execute("PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
    conn.execute("PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    return conn


def text_segment(text: str, msg_id: int = 1) -> pb.MsgContent:
    return pb.MsgContent(msg_id=msg_id, content_type=1, text=text)


def image_segment(msg_id: int = 2) -> pb.MsgContent:
    return pb.MsgContent(
        msg_id=msg_id,
        content_type=2,
        filename="photo.png",
        filesize=204800,
        md5_raw=bytes.fromhex("deadbeefdeadbeefdeadbeefdeadbeef"),
        img_width=1080,
        img_height=720,
        cdn_url_1="http://cdn.example/x.png",
    )


def reply_segment(seq: int, text: str | None = None, msg_id: int = 3) -> pb.MsgContent:
    segment = pb.MsgContent(msg_id=msg_id, content_type=7, reply_msg_seq=seq,
                            reply_msg_time_alt=1_700_000_000, reply_summary="原通知")
    if text:
        segment.text = text
    return segment


class Fixture:
    """一个「加密的 nt_msg.db」+ 一份对应的期望值。"""

    def __init__(self, root: Path, rows: int = 6, with_header: bool = True):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.clear = root / "nt_msg_clear.db"
        self.plain = root / "nt_msg_plain.db"
        self.export = root / "nt_msg_export.db"
        self.encrypted = root / ("nt_msg.db" if with_header else "nt_msg_clear.db")
        self.with_header = with_header
        self.rows = rows
        self.expected: list[dict] = []
        self._build()

    # -- 造一个 SQLCipher 库（PRAGMA 顺序与上游 1.decrypt.py 一致）--
    def _build(self) -> None:
        base = self.root / "_base.db"
        if base.exists():
            base.unlink()
        conn = sc.connect(str(base), isolation_level=None)
        conn.execute("PRAGMA cipher_page_size = 4096;")
        conn.execute(f"PRAGMA key = '{KEY.replace(chr(39), chr(39) * 2)}';")
        conn.execute("PRAGMA kdf_iter = 4000;")
        conn.execute("PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
        conn.execute("PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
        conn.execute(GROUP_DDL)
        conn.execute(C2C_DDL)
        conn.execute(PLAIN_DDL)
        conn.execute("CREATE INDEX group_msg_table_idx40027_40003 ON group_msg_table([40027],[40003])")

        ts = 1_700_000_000
        for i in range(self.rows):
            msg_id = 1000 + i
            seq = i + 1
            if i == 0:
                blob = body(text_segment("【教务处】下周三前提交开题报告", msg_id))
                row = (msg_id, seq, 2, 0, 0, "u_a", "894880656", 7, 20001, ts + i, blob, 0, 0, 894880656)
                self.expected.append(
                    {"msg_id": msg_id, "seq": seq, "text": "【教务处】下周三前提交开题报告",
                     "status": "typed", "reply_seq": None}
                )
            elif i == 1:
                blob = body(image_segment(msg_id))
                row = (msg_id, seq, 2, 0, 0, "u_a", "894880656", 7, 20001, ts + i, blob, 0, 0, 894880656)
                self.expected.append({"msg_id": msg_id, "seq": seq, "text": None,
                                      "status": "typed", "reply_seq": None})
            elif i == 2:
                # 引用第 1 条（40850 = 1 = 第 1 条的 40003）
                blob = body(reply_segment(1, "时间改到下周五了", msg_id))
                row = (msg_id, seq, 2, 0, 0, "u_b", "894880656", 7, 20002, ts + i, blob, 1, ts, 894880656)
                self.expected.append({"msg_id": msg_id, "seq": seq, "text": "时间改到下周五了",
                                      "status": "typed", "reply_seq": 1})
            elif i == 3:
                blob = b"\x0a\x7fzz"  # 读不通
                row = (msg_id, seq, 2, 0, 0, "u_b", "894880656", 7, 20002, ts + i, blob, 0, 0, 894880656)
                self.expected.append({"msg_id": msg_id, "seq": seq, "text": None,
                                      "status": "invalid", "reply_seq": None})
            elif i == 4:
                row = (msg_id, seq, 2, 0, 0, "u_c", "894880656", 7, 20003, ts + i, None, 0, 0, 894880656)
                self.expected.append({"msg_id": msg_id, "seq": seq, "text": None,
                                      "status": "null", "reply_seq": None})
            else:
                # 序号重复：两条消息共用一个 40003（真实数据里存在）
                blob = body(text_segment(f"第 {i} 条", msg_id))
                row = (msg_id, 1, 2, 0, 0, "u_c", "894880656", 7, 20003, ts + i, blob, 1, ts, 894880656)
                self.expected.append({"msg_id": msg_id, "seq": 1, "text": f"第 {i} 条",
                                      "status": "typed", "reply_seq": 1})
            conn.execute(
                'INSERT INTO group_msg_table VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row
            )
            conn.execute(
                "INSERT INTO c2c_msg_table VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (msg_id, seq, 2, 0, "u_a", "u_peer", 7, 30001, 20001, ts + i,
                 body(text_segment(f"私聊 {i}", msg_id)), 0),
            )
        conn.execute("INSERT INTO ark_to_markdown_config_table VALUES (1, '{}')")
        conn.execute("PRAGMA journal_mode = DELETE;")
        conn.close()

        if self.with_header:
            # 前面加 1024 字节的"QQ 自定义头"，模拟真实的 nt_msg.db
            raw = base.read_bytes()
            self.encrypted.write_bytes(b"\x5a" * HEADER + raw)
            base.unlink()
        else:
            base.replace(self.encrypted)


# ---------------------------------------------------------------------------
# 1. 剥头
# ---------------------------------------------------------------------------


def test_strip_header() -> None:
    section("剥头：nt_msg.db → nt_msg_clear.db")
    fixture = Fixture(WORK / "strip")
    source_size = fixture.encrypted.stat().st_size

    wrote = strip_header(fixture.encrypted, fixture.clear, HEADER)
    check(wrote, "第一次会真的拷")
    check(
        fixture.clear.stat().st_size == source_size - HEADER,
        f"大小 = 原文件 - {HEADER}（实际 {fixture.clear.stat().st_size}）",
    )
    check(
        fixture.clear.read_bytes()[:16] == fixture.encrypted.read_bytes()[HEADER : HEADER + 16],
        "内容就是原文件跳过头部之后的字节",
    )
    check(not strip_header(fixture.encrypted, fixture.clear, HEADER), "大小一致时跳过（幂等）")

    # 大小不对（比如上次跑到一半）→ 必须重拷，不能将就
    with open(fixture.clear, "r+b") as handle:
        handle.truncate(fixture.clear.stat().st_size - 100)
    check(strip_header(fixture.encrypted, fixture.clear, HEADER), "大小不对就重拷")
    check(
        fixture.clear.stat().st_size == source_size - HEADER, "重拷后大小又对了"
    )

    expect_raises(
        DecryptError,
        lambda: strip_header(WORK / "strip" / "missing.db", fixture.clear, HEADER),
        "源文件不存在 → 明确报错",
        contains="nt_msg.db",
    )
    expect_raises(
        DecryptError,
        lambda: strip_header(fixture.encrypted, fixture.clear, source_size + 10),
        "文件头比文件还大 → 明确报错",
    )


# ---------------------------------------------------------------------------
# 2. 打开加密库（PRAGMA + 密钥）
# ---------------------------------------------------------------------------


def test_open_encrypted() -> None:
    section("解密：PRAGMA 与密钥")
    check(sqlcipher_available(), "sqlcipher3 可用（requirements.txt 里按平台装的就是它）")
    fixture = Fixture(WORK / "open")
    strip_header(fixture.encrypted, fixture.clear, HEADER)

    conn = open_encrypted(fixture.clear, KEY)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    check({"group_msg_table", "c2c_msg_table"} <= tables, f"能读到表：{sorted(tables)}")

    exc = expect_raises(
        DecryptError,
        lambda: open_encrypted(fixture.clear, "wrong-key-000000"),
        "密钥错 → DecryptError",
    )
    if exc is not None:
        check("密钥" in str(exc), "错误里点名密钥")
        check("CLIENT_NT_MSG_KEY" in str(exc) or "nt_msg.db" in str(exc), "错误里给出下一步")
    expect_raises(DecryptError, lambda: open_encrypted(fixture.clear, ""), "空密钥 → 明确报错")

    # 密钥里可能有单引号（QQ 的密钥是随机可见字符），拼 `PRAGMA key = '...'` 时必须转义。
    # 上游用"单引号翻倍"，这里用一个真带单引号的 16 字符密钥验证那条路径。
    tricky = "abc'def'ghijklmn"
    check(len(tricky) == 16 and "'" in tricky, "这个密钥 16 个字符且带单引号")
    tricky_root = WORK / "tricky"
    tricky_root.mkdir(parents=True, exist_ok=True)
    tricky_db = tricky_root / "nt_msg_clear.db"
    if tricky_db.exists():
        tricky_db.unlink()
    conn = sc.connect(str(tricky_db), isolation_level=None)
    conn.execute("PRAGMA cipher_page_size = 4096;")
    conn.execute(f"PRAGMA key = '{tricky.replace(chr(39), chr(39) * 2)}';")
    conn.execute("PRAGMA kdf_iter = 4000;")
    conn.execute("PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
    conn.execute("PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    conn.execute("CREATE TABLE t(a)")
    conn.execute("PRAGMA journal_mode = DELETE;")
    conn.close()
    opened = open_encrypted(tricky_db, tricky)
    tables = {r[0] for r in opened.execute("SELECT name FROM sqlite_master")}
    opened.close()
    check("t" in tables, "带单引号的密钥也能正确打开（转义对了）")
    expect_raises(
        DecryptError,
        lambda: open_encrypted(tricky_db, "abcdefghijklmnop"),
        "换一个密钥就打不开（说明上面不是恰好蒙对）",
    )


# ---------------------------------------------------------------------------
# 3. 解密：拷贝、续跑、索引、坏页
# ---------------------------------------------------------------------------


def test_decrypt() -> None:
    section("解密：逐表拷贝")
    fixture = Fixture(WORK / "dec")
    report = decrypt_database(
        fixture.encrypted,
        fixture.clear,
        fixture.plain,
        KEY,
        tables=["group_msg_table", "c2c_msg_table", "ark_to_markdown_config_table"],
    )
    check(report.ok, f"报告 ok（自检={report.integrity}，跳过={report.total_skipped}）")
    check(report.integrity == "ok", "明文库 quick_check 通过")
    check(report.total_rows == fixture.rows * 2 + 1, f"总行数 {report.total_rows}")
    check(report.indexes_copied >= 1, f"索引复制了 {report.indexes_copied} 个")
    check(
        [item.table for item in report.per_table]
        == ["group_msg_table", "c2c_msg_table", "ark_to_markdown_config_table"],
        "报告按表列出",
    )

    with closing(sqlite3.connect(sqlite_uri(fixture.plain, "ro"), uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM group_msg_table ORDER BY "40001"').fetchall()
        check(len(rows) == fixture.rows, f"group 表 {len(rows)} 行")
        check(rows[0]["40003"] == 1 and rows[0]["40050"] == 1_700_000_000, "序号/时间戳原样搬过来")
        check(
            isinstance(rows[0]["40800"], (bytes, bytearray)) and len(rows[0]["40800"]) > 0,
            f"40800 的字节原样搬过来（{len(rows[0]['40800'] or b'')} 字节）",
        )
        check(
            parse_40800(bytes(rows[0]["40800"])).contents[0].text
            == "【教务处】下周三前提交开题报告",
            "搬过来的 40800 还能被上游的解析器解出同样的正文",
        )
        indexes = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        check("group_msg_table_idx40027_40003" in indexes, f"索引搬过来了：{sorted(indexes)}")

    section("解密：断点续跑")
    # 模拟上一次跑到一半：明文库里已经有前两行
    with closing(sqlite3.connect(str(fixture.plain))) as conn:
        conn.execute('DELETE FROM group_msg_table WHERE "40001" > 1001')
        conn.commit()
    again = decrypt_database(
        fixture.encrypted, fixture.clear, fixture.plain, KEY, tables=["group_msg_table"]
    )
    resumed = [item for item in again.per_table if item.table == "group_msg_table"][0]
    check(resumed.resumed_from == 1001, f"从 max(rowid)=1001 续跑（实际 {resumed.resumed_from}）")
    check(resumed.copied_rows == fixture.rows - 2, "只补了缺的那些行")
    with closing(sqlite3.connect(sqlite_uri(fixture.plain, "ro"), uri=True)) as conn:
        count = conn.execute('SELECT count(*) FROM group_msg_table').fetchone()[0]
    check(count == fixture.rows, f"最终 {count} 行，没有重复")

    section("解密：没有 INTEGER 主键的表要重拷而不是续跑")
    plain2 = WORK / "dec2" / "nt_msg_plain.db"
    plain2.parent.mkdir(parents=True, exist_ok=True)
    first = decrypt_database(
        fixture.encrypted,
        WORK / "dec2" / "clear.db",
        plain2,
        KEY,
        tables=["ark_to_markdown_config_table"],
    )
    check(first.total_rows == 1, "先拷一次")
    second = decrypt_database(
        fixture.encrypted,
        WORK / "dec2" / "clear.db",
        plain2,
        KEY,
        tables=["ark_to_markdown_config_table"],
    )
    item = second.per_table[0]
    check(item.recopied, "第二次识别出「没有主键」并清空重拷")
    with closing(sqlite3.connect(sqlite_uri(plain2, "ro"), uri=True)) as conn:
        count = conn.execute("SELECT count(*) FROM ark_to_markdown_config_table").fetchone()[0]
    check(count == 1, f"重拷后还是 1 行（不会插重复）：{count}")

    section("解密：坏行要跳过并计数（rowid+1，上游的逻辑）")
    # 用一个"游标停在 1003 时就报 malformed"的假连接精确制造"某一行读不出来"。
    # 为什么不用真文件：小夹具里 6 条消息只占一页，那一页坏了就是**整张表**读不出来
    # （另一种情况，见下一节）；而"单行坏"要靠控制游标才能稳定复现。
    # batch_size=2 是为了让坏行落到单独一批上（一批 5000 会把 6 行一次读完）。
    import app.ntmsg_db.decrypt as decrypt_mod

    real_open = decrypt_mod.open_encrypted

    def _flaky_open(path, key, **kwargs):
        conn = real_open(path, key, **kwargs)

        class Proxy:
            def __getattr__(self, name):
                return getattr(conn, name)

            def execute(self, sql, *args, **kw):
                params = args[0] if args and isinstance(args[0], (tuple, list)) else args
                if "SELECT rowid, *" in sql and params and params[0] == 1003:
                    raise sqlite3.DatabaseError("database disk image is malformed")
                return conn.execute(sql, *args, **kw)

        return Proxy()

    flaky = WORK / "flaky"
    flaky.mkdir(parents=True, exist_ok=True)
    decrypt_mod.open_encrypted = _flaky_open
    try:
        report = decrypt_database(
            fixture.encrypted, flaky / "clear.db", flaky / "nt_msg_plain.db", KEY,
            tables=["group_msg_table"], integrity="off", batch_size=2,
        )
    finally:
        decrypt_mod.open_encrypted = real_open
    item = report.per_table[0]
    check(item.skipped_rows == 1, f"跳过了 1 行（实际 {item.skipped_rows}）")
    check(len(item.skipped_rowids) == 1, f"跳过哪个 rowid 被记下来：{item.skipped_rowids}")
    check(item.copied_rows == fixture.rows - 1, f"其余 {item.copied_rows} 行照常拷进来")
    check(item.source_rows == fixture.rows, "源表总数是数得出来的（这条不是整块坏）")
    check(not report.ok, "带着跳过的报告 ok=False（不能被当成「完美」）")
    decrypt_mod.open_encrypted = _flaky_open
    try:
        expect_raises(
            DecryptError,
            lambda: decrypt_database(
                fixture.encrypted, flaky / "clear2.db", flaky / "out2.db", KEY,
                tables=["group_msg_table"], max_skips=0, integrity="off", batch_size=2,
            ),
            "max_skips=0 时宁可停下（CLIENT_DECRYPT_MAX_SKIPS）",
            contains="坏页",
        )
    finally:
        decrypt_mod.open_encrypted = real_open

    section("解密：整块读不下去时必须停下来（不能死循环）")
    # 把 group_msg_table 的**根页**搅乱：这张表的每次访问都会失败，
    # 于是"跳一行再试"永远走不到尽头 —— 必须有上限，且要说清是哪种坏。
    damaged = WORK / "damaged"
    damaged.mkdir(parents=True, exist_ok=True)
    broken = damaged / "nt_msg_clear.db"
    shutil.copyfile(fixture.clear, broken)
    with closing(sqlcipher3_connect(broken)) as conn:
        root = conn.execute(
            "SELECT rootpage FROM sqlite_master WHERE name='group_msg_table'"
        ).fetchone()[0]
    raw = bytearray(broken.read_bytes())
    raw[(root - 1) * 4096 + 8] ^= 0xFF
    broken.write_bytes(bytes(raw))
    started = time.time()
    exc = expect_raises(
        DecryptError,
        lambda: decrypt_database(
            broken, damaged / "ignored_clear.db", damaged / "nt_msg_plain.db", KEY,
            header_size=0, tables=["group_msg_table"], integrity="off",
        ),
        "整块坏 → 停下来并说清楚",
        contains="整块读不下去",
    )
    check(time.time() - started < 60, f"而且是**有限时间**内停下来的（{time.time() - started:.1f}s）")
    if exc is not None:
        check("CLIENT_DECRYPT_TABLES" in str(exc), "给出可选的处理办法")


# ---------------------------------------------------------------------------
# 4. 导出：上游表结构 + 我们的两列
# ---------------------------------------------------------------------------


def test_export() -> None:
    section("导出：nt_msg_plain.db → nt_msg_export.db")
    fixture = Fixture(WORK / "exp")
    decrypt_database(
        fixture.encrypted, fixture.clear, fixture.plain, KEY,
        tables=["group_msg_table", "c2c_msg_table"],
    )
    report = export_database(fixture.plain, fixture.export, include_c2c=True, resume=False)
    check(report.written_rows.get("group_messages") == fixture.rows, f"群消息 {report.written_rows}")
    check(report.written_rows.get("c2c_messages") == fixture.rows, f"私聊 {report.written_rows}")
    check(report.failed_rows == 0, f"没有转换失败（{report.last_error}）")
    check(report.seq_added and report.seq_filled == fixture.rows, "序号列补上了")
    check(
        report.parse_status.get("typed", 0) >= fixture.rows - 2,
        f"解析状态分布：{report.parse_status}",
    )

    with closing(sqlite3.connect(sqlite_uri(fixture.export, "ro"), uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        columns = {r[1] for r in conn.execute('PRAGMA table_info("group_messages")')}
        check(
            {"msg_id", "timestamp", "direction", "sender_uid", "sender_qq", "group_id",
             "group_qq", "msg_type", "subtype", "content_type", "text", "parse_status",
             "content", SEQ_COLUMN, REPLY_SEQ_COLUMN} <= columns,
            f"列齐（上游的 + 我们补的两列）：{sorted(columns)}",
        )
        check(
            {"c2c_messages", "c2c_messages_fts", "group_messages_fts"} <= {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master").fetchall()
            },
            "上游的 FTS 表也建了（3.export.py 的行为）",
        )
        row = conn.execute('SELECT * FROM group_messages WHERE msg_id=1000').fetchone()
        check(row["text"] == "【教务处】下周三前提交开题报告", f"正文：{row['text']!r}")
        check(row["timestamp"] == 1_700_000_000, "时间戳（秒）")
        check(row["sender_qq"] == 20001 and row["group_id"] == "894880656", "发送者/群号")
        check(row[SEQ_COLUMN] == 1, f"补的 {SEQ_COLUMN} 有值（{row[SEQ_COLUMN]}）")
        content = json.loads(row["content"])
        check(content["type"] == "msg_body", f"content 是上游形状：{content['type']}")
        check(
            content["segments"][0]["text"] == "【教务处】下周三前提交开题报告",
            "segments[0].text 是真正文",
        )
        bad = conn.execute('SELECT * FROM group_messages WHERE msg_id=1003').fetchone()
        check(bad["parse_status"] == "invalid" and bad["text"] is None, "读不通的行照样入库")
        reply = conn.execute('SELECT * FROM group_messages WHERE msg_id=1002').fetchone()
        check(reply[REPLY_SEQ_COLUMN] == 1, f"被回复序号：{reply[REPLY_SEQ_COLUMN]}")

    section("导出：幂等与增量")
    check(watermark(fixture.export) == 1_700_000_000 + fixture.rows - 1, "水位线是最新时间戳")
    full_again = export_database(fixture.plain, fixture.export, resume=False)
    check(full_again.written_rows.get("group_messages") == fixture.rows, "全量重导一遍仍是同样行数")
    with closing(sqlite3.connect(sqlite_uri(fixture.export, "ro"), uri=True)) as conn:
        count = conn.execute("SELECT count(*) FROM group_messages").fetchone()[0]
    check(count == fixture.rows, f"重导后总行数不变（幂等）：{count}")

    with closing(sqlite3.connect(str(fixture.plain))) as conn:
        conn.execute(
            "INSERT INTO group_msg_table VALUES (2000, 99, 2, 0, 0, 'u_z', '894880656', 7, 20009,"
            " 1700000900, NULL, 0, 0, 894880656)"
        )
        conn.commit()
    inc = export_database(fixture.plain, fixture.export, resume=True, overlap_seconds=0)
    # 起点是 `>= 水位线`，所以水位线那一秒的旧行会再读一遍（刻意的：同秒后到的消息不能漏）
    check(inc.written_rows.get("group_messages") == 2, f"增量只处理边界 + 新行：{inc.written_rows}")
    with closing(sqlite3.connect(sqlite_uri(fixture.export, "ro"), uri=True)) as conn:
        count = conn.execute("SELECT count(*) FROM group_messages").fetchone()[0]
        newest = conn.execute("SELECT max(msg_id) FROM group_messages").fetchone()[0]
    check(count == fixture.rows + 1 and newest == 2000, f"新行进来了（共 {count} 行）")

    section("导出：关掉 c2c / 源库不对")
    only_group = export_database(
        fixture.plain, WORK / "exp" / "group_only.db", include_c2c=False, resume=False
    )
    check("c2c_messages" not in only_group.written_rows, f"没有导 c2c：{only_group.written_rows}")
    empty = WORK / "exp" / "empty.db"
    with closing(sqlite3.connect(str(empty))) as conn:
        conn.execute("CREATE TABLE something_else (a)")
    expect_raises(
        ExportError,
        lambda: export_database(empty, WORK / "exp" / "out.db"),
        "源库没有 group_msg_table → 明确报错",
        contains="group_msg_table",
    )


# ---------------------------------------------------------------------------
# 5. 读取器：上游形状的 content 也能读出附件与引用
# ---------------------------------------------------------------------------


def test_reader() -> None:
    section("读取器：读上游形状的 content")
    from google.protobuf.json_format import MessageToDict

    image = MessageToDict(image_segment(), preserving_proto_field_name=True)
    content = {"type": "msg_body", "segments": [image]}
    attachments = attachments_from_content(content)
    check(len(attachments) == 1, f"认出 1 个附件：{attachments}")
    if attachments:
        item = attachments[0]
        check(item["type"] == "image", f"类型：{item.get('type')}")
        check(item.get("name") == "photo.png", f"文件名：{item.get('name')}")
        check(
            item.get("md5") == "deadbeefdeadbeefdeadbeefdeadbeef",
            f"md5_raw（base64）解成了十六进制：{item.get('md5')}",
        )
        check(item.get("url") == "http://cdn.example/x.png", f"cdn_url_1 → url：{item.get('url')}")
        check(item.get("size") == 204800, f"filesize 是字符串也要认（int64 → str）：{item.get('size')}")
        check(item.get("width") == 1080 and item.get("height") == 720, "宽高")

    # int64 在 protobuf JSON 里是字符串 —— 这一条专门盯着那个坑
    content_int64 = {
        "type": "msg_body",
        "segments": [{"content_type": 3, "filename": "a.pdf", "filesize": "737226",
                      "md5_raw": "", "file_ext": "pdf"}],
    }
    got = attachments_from_content(content_int64)
    check(got and got[0].get("size") == 737226, f"字符串形式的 filesize：{got}")
    check(got and got[0].get("ext") == ".pdf", "扩展名补上点")

    # 引用：优先 reply_msg_seq（47402），而不是 reply_msg_id（47401，只是候选）
    reply = MessageToDict(reply_segment(4321), preserving_proto_field_name=True)
    reply["reply_msg_id"] = "999"  # 同时存在时，必须选 47402
    check(
        quote_ref_from_content({"type": "msg_body", "segments": [reply]}) == "4321",
        "reply_msg_seq 优先于 reply_msg_id",
    )
    check(quote_ref_from_content({"type": "msg_body", "segments": [{"content_type": 1, "text": "x"}]}) is None,
          "没有引用关系时返回 None")
    check(quote_ref_from_content({"reply_source_record_id": "47422"}) == "47422",
          "只有 47422 时也能取到（排在最后）")

    section("读取器：零值不能被当成「有值」")
    # 这条踩到过：`SELECT 40850` 没加引号 → 每行都变成 40850 这个数字
    fixture = Fixture(WORK / "reader")
    decrypt_database(fixture.encrypted, fixture.clear, fixture.plain, KEY,
                     tables=["group_msg_table"])
    export_database(fixture.plain, fixture.export, include_c2c=False, resume=False)
    db = SourceDatabase(fixture.export)
    info = db.inspect()
    check(info["rows"] == fixture.rows, f"inspect：{info['rows']} 行")
    check(db.seq_column() == SEQ_COLUMN, f"序号列：{db.seq_column()}")
    messages = db.fetch_since(0, "", limit=50)
    check(len(messages) == fixture.rows, f"取到 {len(messages)} 条")
    by_id = {m.msg_id: m for m in messages}
    plain_one = by_id["1000"]
    check(plain_one.quote_ref is None, f"普通消息没有引用关系（实际 {plain_one.quote_ref!r}）")
    check(plain_one.seq == "1", f"群内序号：{plain_one.seq}")
    check(plain_one.text.startswith("【教务处】"), "正文")
    check(by_id["1002"].quote_ref == "1", f"引用目标：{by_id['1002'].quote_ref}")
    check(by_id["1001"].attachments, f"图片附件：{by_id['1001'].attachments}")
    check(by_id["1001"].attachments[0].get("url") == "http://cdn.example/x.png", "附件 URL")
    check(by_id["1003"].parse_status == "invalid", "读不通的行标了 invalid")

    section("读取器：序号命中多条时不敢认")
    resolved, matches = db.resolve_seq_detail("894880656", "1")
    check(matches > 1, f"夹具里 (群, 序号=1) 对应多条消息（{matches}）")
    check(resolved is None, "**不猜**：命中多条时返回 None")
    check(db.resolve_seq("894880656", "3") == "1002" or db.resolve_seq("894880656", "3") is None,
          "只命中一条时才给出 msg_id")


# ---------------------------------------------------------------------------
# 6. prepare：该跑才跑
# ---------------------------------------------------------------------------


def test_prepare() -> None:
    section("prepare：按文件时间决定要不要重跑")
    from app.config import Settings

    fixture = Fixture(WORK / "prep", rows=4)
    settings = Settings(
        client_nt_msg_db=str(fixture.encrypted),
        client_nt_msg_key=KEY,
        client_db_path="",
        client_mirror_path=str(WORK / "prep" / "mirror.db"),
    )
    source, clear, plain, export = resolve_paths(settings)
    check(source == fixture.encrypted and clear == fixture.encrypted.with_name("nt_msg_clear.db"),
          f"路径推导：{clear.name}")
    check(plain.name == "nt_msg_plain.db" and export.name == "nt_msg_export.db",
          "中间产物与导出库默认都在 nt_msg.db 旁边")
    check(read_key(settings) == KEY, "密钥读出来了")
    keyfile = WORK / "prep" / "key.txt"
    keyfile.write_text(KEY + "\n", encoding="utf-8")
    from_settings = settings.model_copy(update={"client_nt_msg_key_file": str(keyfile)})
    check(read_key(from_settings) == KEY, "从文件读密钥会去掉换行")

    report = prepare_databases(settings)
    check(report.enabled and report.decrypted and report.exported, "第一次：解密 + 导出都跑了")
    check(export.exists() and plain.exists(), "产物都在")
    check("解密" in report.summary() and "导出" in report.summary(), f"摘要：{report.summary()}")

    again = prepare_databases(settings)
    check(not again.decrypted and not again.exported, "没变化时两步都跳过（只 stat 文件）")

    forced = prepare_databases(settings, force=True)
    check(forced.decrypted and forced.exported, "force=True 会重跑")

    stat = fixture.encrypted.stat()
    import os

    os.utime(fixture.encrypted, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000_000))
    third = prepare_databases(settings)
    check(third.decrypted and third.exported, "nt_msg.db 变新 → 重新解密并重新导出")

    section("prepare：配置不对时的表现")
    off = settings.model_copy(update={"client_nt_msg_db": ""})
    report_off = prepare_databases(off)
    check(not report_off.enabled, "没配 nt_msg.db 时是空操作")
    check(report_off.export_path == off.resolved_db_path, "并把源库指回 CLIENT_DB_PATH")
    missing = settings.model_copy(update={"client_nt_msg_db": str(WORK / "nope.db")})
    expect_raises(DecryptError, lambda: prepare_databases(missing, force=True),
                  "源库不存在 → 明确报错", contains="nt_msg.db")
    no_key = settings.model_copy(update={"client_nt_msg_key": "", "client_nt_msg_key_file": ""})
    expect_raises(DecryptError, lambda: prepare_databases(no_key, force=True),
                  "没配密钥 → 明确报错", contains="CLIENT_NT_MSG_KEY")
    wrong = settings.model_copy(update={"client_nt_msg_key": "definitely-wrong"})
    expect_raises(DecryptError, lambda: prepare_databases(wrong, force=True),
                  "密钥错 → 明确报错", contains="密钥")
    bad_tables = settings.model_copy(update={"client_decrypt_tables": "no_such_table"})
    expect_raises(DecryptError, lambda: prepare_databases(bad_tables, force=True),
                  "CLIENT_DECRYPT_TABLES 写了不存在的表 → 明确报错", contains="no_such_table")


def main() -> int:
    # 这套用例故意造坏页、错密钥，日志里会出现 error/warning（那是被测行为的一部分）
    logging.getLogger("app").setLevel(logging.CRITICAL)
    logging.getLogger("msgdb").setLevel(logging.CRITICAL)
    started = time.time()
    try:
        test_strip_header()
        test_open_encrypted()
        test_decrypt()
        test_export()
        test_reader()
        test_prepare()
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print(f"\n{'=' * 60}")
    if FAIL:
        print(f"✗ 失败 {FAIL} 项 / 共 {PASS + FAIL} 项（{time.time() - started:.0f}s）")
        for item in FAILURES:
            print(f"  · {item}")
        return 1
    print(f"✓ 全部通过：{PASS} 项断言（{time.time() - started:.0f}s）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
