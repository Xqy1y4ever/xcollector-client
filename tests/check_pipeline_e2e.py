"""端到端：源库 → 客户端（镜像 + 补充）→ **真后端**。

需要一个**正在运行的后端**：

    $env:API_TOKEN='service-token'; $env:SIGNUP_MODE='invite'; $env:SERVER_PORT='8005'
    .\\xcollector-backend\\.venv\\Scripts\\python.exe -m app.main

    $env:CLIENT_BASE='http://127.0.0.1:8005'; $env:CLIENT_SERVICE_TOKEN='service-token'
    .\\.venv\\Scripts\\python.exe -m tests.check_pipeline_e2e

## 这个文件要守住的东西

1. **只用 UserToken 就能完整入库。** 脚本注册一个真用户、拿他的 UserToken，全程
   只用那个令牌 —— 客户端只要偷偷调了一个服务令牌专属的接口，这里就会 403。
2. **镜像就是增量状态**：处理过的不再重看、白名单外的记终态、内容变了要重新处理。
   判据是"读没读过"，**不是订阅**（订阅只影响 bot）。
3. **补充要改任务，不是新建任务**：新消息引用了一条已读消息时，更新的是
   **被引用那条**（靠后端的 `(user_id, raw_message_id)` 幂等），通知总数不变。
4. **认不出引用关系时不许猜**：如实按独立新消息处理，并把原因打出来
   （源表没有群内序号列时就是这种情况）。
5. **dry-run 一个状态都不写** —— 否则"先试跑一下"会让整个客户端再也不入库。
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import httpx

from app.backend_client import BackendClient
from app.config import Settings
from app.mirror import STATE_DONE, STATE_SKIPPED, Mirror
from app.run import run_cycle, verify_identity
from app.source.attachments import AttachmentResolver
from app.source.ntmsg import SourceDatabase
from tests._hermetic import isolate_settings

BASE = os.environ.get("CLIENT_BASE", "http://127.0.0.1:8005").rstrip("/")
API = BASE + "/api"
SERVICE = os.environ.get("CLIENT_SERVICE_TOKEN", "service-token")

# 环境变量**留着**（后端地址与令牌就是从上面两行读的），但本机那份 `.env` 必须摘掉：
# 它里面是真实的白名单和导出库路径，会让 `make_settings` 没显式传的字段悄悄变成真配置
# —— 第 3 节就是这样被本机白名单挡掉的（报错只是 outcomes.get("extracted") = None）。
isolate_settings(clear_env=False)

RUN = os.environ.get("CLIENT_RUN") or str(int(time.time() * 1000))
QQ = str(500000000 + int(RUN[-7:]) % 40000000)
_SUF = RUN[-7:]
GROUP = "91" + _SUF
SENDER = "92" + _SUF
GROUP2 = "93" + _SUF

SCRATCH = Path(__file__).resolve().parent.parent / ".tmp-test"
H = {"Authorization": f"Bearer {SERVICE}"}

# 源库时间戳基准：**就在不久之前**，不是写死的绝对时间（否则回看窗口一过，
# 整个测试会以"扫了 0 条"失败，而原因和被测逻辑毫无关系）。
BASE_TS = int(time.time()) - 20 * 3600

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
# 造源库
# ---------------------------------------------------------------------------

# `with_seq=True` 时额外带一列群内序号 —— nt_msg_db_util 的字段文档说回复里的
# `47402` 与群内序号匹配，所以有这一列时"这条在回复哪一条"才能确定性解析出来。
# 当前 3.export.py 的 group_messages **没有**这一列，所以两种都要能测。
SOURCE_SCHEMA = """
CREATE TABLE group_messages (
  msg_id TEXT PRIMARY KEY, timestamp INTEGER, direction INTEGER,
  sender_uid TEXT, sender_qq TEXT, group_id TEXT, group_qq TEXT,
  msg_type INTEGER, subtype INTEGER, content_type INTEGER,
  text TEXT, parse_status TEXT, content TEXT
);
"""

SOURCE_SCHEMA_SEQ = """
CREATE TABLE group_messages (
  msg_id TEXT PRIMARY KEY, timestamp INTEGER, direction INTEGER,
  sender_uid TEXT, sender_qq TEXT, group_id TEXT, group_qq TEXT,
  msg_type INTEGER, subtype INTEGER, content_type INTEGER,
  text TEXT, parse_status TEXT, content TEXT, "40003" INTEGER
);
"""


def make_source_db(path: Path, rows: list[dict], *, with_seq: bool = False) -> None:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SOURCE_SCHEMA_SEQ if with_seq else SOURCE_SCHEMA)
        for row in rows:
            content = row.get("content")
            values = (
                row["msg_id"],
                row["ts"],
                0,
                "u_x",
                row.get("sender", SENDER),
                row.get("group", GROUP),
                int(row.get("group", GROUP)),
                2,
                0,
                1,
                row.get("text", ""),
                row.get("status", "typed"),
                json.dumps(content, ensure_ascii=False) if content else None,
            )
            if with_seq:
                conn.execute(
                    "INSERT INTO group_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (*values, row.get("seq")),
                )
            else:
                conn.execute("INSERT INTO group_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
        conn.commit()
    finally:
        conn.close()


def text_of(text: str) -> dict:
    return {"type": "text", "text": text}


def reply_of(text: str, target_seq: int) -> dict:
    """一条「回复某条消息」的内容。

    `47402` 装被回复消息的群内序号（按 nt_msg_db_util 的群字段文档），
    `47413` 是引用摘要。放成 mixed 段，和真实导出库一样。
    """
    return {
        "type": "mixed",
        "segments": [
            {"type": "reply", "47402": target_seq, "47413": "（引用）"},
            {"type": "text", "text": text},
        ],
    }


# ---------------------------------------------------------------------------
# 后端交互
# ---------------------------------------------------------------------------


def sync(token: str, path: str, method: str = "GET", **kwargs) -> httpx.Response:
    with httpx.Client(base_url=BASE, timeout=20, headers={"Authorization": f"Bearer {token}"}) as c:
        return c.request(method, path, **kwargs)


def register_user() -> tuple[str, str]:
    c = httpx.Client(timeout=20)
    try:
        code = c.post(f"{API}/verify/request", json={"qq": QQ}, headers=H).json()["code"]
        invite = c.post(
            f"{API}/invites", json={"note": f"client-{RUN}", "max_uses": 1}, headers=H
        ).json()["code"]
        r = c.post(f"{API}/register", json={"qq": QQ, "code": code, "invite_code": invite})
        assert r.status_code == 200, r.text
        body = r.json()
        return str(body["user"]["id"]), str(body["token"])
    finally:
        c.close()


def notifications(token: str) -> list[dict]:
    return sync(token, "/api/notifications").json()["notifications"]


def make_settings(token: str, db_path: Path, **overrides) -> Settings:
    base = dict(
        backend_base_url=BASE,
        client_token=token,
        backend_timeout=15.0,
        backend_max_retries=1,
        client_db_path=str(db_path),
        client_mirror_path=str(SCRATCH / f"mirror-{RUN}.db"),
        client_extractor="rule",          # 核心链路不联网、不花钱
        client_batch_size=100,
        client_max_messages_per_cycle=100,
        # 白名单显式留空：不写这两行的话，本机 `.env` 里的真白名单会生效，
        # 所有"应该被抽出来"的断言都会以一种看不懂的方式失败。
        client_group_whitelist="",
        client_sender_whitelist="",
        client_poll_seconds=1,
        client_attachment_root="",
        client_missing_attachment="url",
        client_recheck_overlap_hours=2.0,
        client_gap_alert_hours=2.0,
        client_amendment_enabled=True,
        client_amendment_max_age_hours=72.0,
        digest_tz="Asia/Shanghai",
    )
    base.update(overrides)
    settings = Settings(**base)
    # Settings 是 extra="ignore"：关键字写错会被静默丢掉，测试就会拿着默认值跑
    for name in ("client_db_path", "client_token", "client_extractor", "client_mirror_path"):
        assert getattr(settings, name) == base[name], f"make_settings 的 {name} 没生效"
    for name, value in overrides.items():
        assert getattr(settings, name, None) == value, f"覆盖项 {name} 没生效"
    return settings


async def run_all() -> int:  # noqa: C901
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    uid, token = register_user()
    print(f"→ {BASE}\n    用户 {QQ} = {uid}\n")

    db_path = SCRATCH / f"e2e-{RUN}.db"
    mirror_path = SCRATCH / f"mirror-{RUN}.db"

    settings = make_settings(token, db_path, client_mirror_path=str(mirror_path))
    backend = BackendClient(settings)
    mirror = Mirror(mirror_path)
    try:
        # ----------------------------------------------------------------
        print("--- 1. 身份自检：UserToken 就够了 ---")
        who = await verify_identity(backend, settings)
        check("scope 是 user（不是 service）", who.get("scope"), "user")
        check("拿到的是自己", (who.get("user") or {}).get("qq"), QQ)

        # ----------------------------------------------------------------
        print("\n--- 2. 一条订阅都没有 → 照样入库（客户端不看订阅）---")
        # 这一节以前是"还没订阅 → 本轮什么都不做"。订阅是 bot 的过滤条件：
        # 客户端读的是**自己账号**的聊天记录库，它要做的判断只有"这条我处理过没有"
        # （镜像里的已读标记）+ 本地白名单。权限在后端按表分开。
        make_source_db(db_path, [
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得", "content": text_of("大家下周三前交军训心得")},
        ])
        check("这个用户没有任何订阅", sync(token, "/api/subscriptions").json()["subscriptions"], [])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path), mirror=mirror)
        check("扫到了（不再有「没订阅就整轮不干」）", report.scanned, 1)
        check("抽出来了", report.outcomes.get("extracted"), 1)
        check("没有报错", report.errors, [])

        # ----------------------------------------------------------------
        print("\n--- 3. 存量补上，镜像记成 done，原文落在**自己那一层** ---")
        check("后端里有 1 条通知", len(notifications(token)), 1)
        notif = notifications(token)[0]
        check("标题抽对了", notif.get("title"), "大家下周三前交军训心得")
        check_true("截止时间解析出来了", notif.get("due_at") is not None, str(notif.get("due_at")))
        row = mirror.get("1")
        check("镜像里记成 done", row.state, STATE_DONE)
        check_true("而且记下了后端的 raw_id（补充要落回它）", bool(row.raw_id), str(row.raw_id))
        # 关键的一条：客户端写的原文**不在共享层**里。共享层是所有人订阅的群的并集，
        # 客户端能往那儿写就等于能往所有人看到的表里塞东西。
        check(
            "服务令牌按 id 读这条 → 404（它在 A 自己那层，不在共享层）",
            sync(SERVICE, f"/api/messages/{row.raw_id}").status_code,
            404,
        )
        first_raw_id = row.raw_id
        first_notif_id = notif["id"]

        # ----------------------------------------------------------------
        print("\n--- 4. 再跑一轮：内容没变 → 一条都不重抽 ---")
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path), mirror=mirror)
        check("扫到的都判成 unchanged", report.unchanged, 1)
        check("没有处理任何一条", report.processed, 0)
        check("通知还是 1 条", len(notifications(token)), 1)

        # ----------------------------------------------------------------
        print("\n--- 5. 增量：新消息只处理新的 ---")
        make_source_db(db_path, [
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得", "content": text_of("大家下周三前交军训心得")},
            {"msg_id": "2", "ts": BASE_TS + 60, "text": "收到", "content": text_of("收到")},
            {"msg_id": "3", "ts": BASE_TS + 120, "text": "本周五19:00在教三201开班会", "content": text_of("本周五19:00在教三201开班会")},
        ])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path), mirror=mirror)
        check("只处理了 2 条新的（第 1 条没动）", report.processed, 2)
        check("闲聊判成 noise", report.outcomes.get("noise"), 1)
        check("开会那条建成了", report.outcomes.get("extracted"), 1)
        check("通知变成 2 条", len(notifications(token)), 2)
        check("收到那条在镜像里是 skipped（终态，不再重看）", mirror.get("2").state, STATE_SKIPPED)

        # ----------------------------------------------------------------
        print("\n--- 6. 镜像被删了也不重复抽（后端通知集合兜底）---")
        mirror_path.unlink()
        mirror = Mirror(mirror_path)
        check("新镜像里什么都没有", mirror.stats()["total"], 0)
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path), mirror=mirror)
        check("通知还是 2 条（没有重复建）", len(notifications(token)), 2)
        check_true(
            "而且没有重新花模型的钱（走的是「已经建过通知」那条路）",
            report.unchanged >= 2,
            f"unchanged={report.unchanged} outcomes={report.outcomes}",
        )
        check("镜像被重新补起来了（下次就靠它了）", mirror.get("1").state, STATE_DONE)

        # ----------------------------------------------------------------
        print("\n--- 7. **订阅一条都没有**也照样入库（订阅只影响 bot）---")
        # 这一节以前叫"订阅之外的来源：镜像记 skipped"，测的是客户端按后端的订阅
        # 过滤来源。现在客户端不看订阅：它读的是自己账号的聊天记录库，权限由后端
        # 按**表**分开（客户端写 user_raw_message，bot 写共享的 raw_message）。
        # 所以这里反过来验：这个用户**没有任何订阅**，两条不同来源照样入库。
        make_source_db(db_path, [
            {"msg_id": "10", "ts": BASE_TS + 200, "group": GROUP2, "text": "下周一交实验报告", "content": text_of("下周一交实验报告")},
            {"msg_id": "11", "ts": BASE_TS + 240, "sender": "19999", "text": "明天上午交材料", "content": text_of("明天上午交材料")},
        ])
        mirror2_path = SCRATCH / f"mirror2-{RUN}.db"
        mirror2 = Mirror(mirror2_path)
        settings2 = make_settings(token, db_path, client_mirror_path=str(mirror2_path))
        backend2 = BackendClient(settings2)
        try:
            subs = sync(token, "/api/subscriptions").json()["subscriptions"]
            check("先确认这个用户一条订阅都没有", subs, [])
            report = await run_cycle(backend2, settings2, db=SourceDatabase(db_path), mirror=mirror2)
            check("两条都抽了（订阅不是客户端的门槛）", report.outcomes.get("extracted"), 2)
            check("镜像里也都记成 done", (mirror2.get("10").state, mirror2.get("11").state), (STATE_DONE, STATE_DONE))
            check("后端多了 2 条通知", len(notifications(token)), 4)
            check("仍然一条订阅都没有（客户端不会替用户去订阅）", sync(token, "/api/subscriptions").json()["subscriptions"], [])
        finally:
            await backend2.close()

        # ----------------------------------------------------------------
        print("\n--- 8. 内容被改了 → 重新处理，**更新原来那条任务** ---")
        mirror = Mirror(mirror_path)
        before_count = len(notifications(token))
        make_source_db(db_path, [
            # 同一条 msg_id（1），正文多了"改到本周五"
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得，改到本周五", "content": text_of("大家下周三前交军训心得，改到本周五")},
            {"msg_id": "2", "ts": BASE_TS + 60, "text": "收到", "content": text_of("收到")},
            {"msg_id": "3", "ts": BASE_TS + 120, "text": "本周五19:00在教三201开班会", "content": text_of("本周五19:00在教三201开班会")},
        ])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path), mirror=mirror)
        check("识别成内容变了并重新处理", report.processed, 1)
        after = notifications(token)
        check("**通知条数没变**（是更新，不是新建）", len(after), before_count)
        same = [n for n in after if n["id"] == first_notif_id]
        check_true("还是原来那一条", bool(same), f"找的是 {first_notif_id}")
        if same:
            blob = json.dumps(same[0], ensure_ascii=False)
            check_true(
                "正文里的改动进了这条任务（能看到新的说法）",
                "周五" in blob,
                json.dumps({k: same[0].get(k) for k in ("title", "summary", "evidence")}, ensure_ascii=False),
            )
            check("raw_message_id 还是原来那个（没有另起一条）", same[0].get("raw_message_id"), first_raw_id)
        check("镜像里回到 done", mirror.get("1").state, STATE_DONE)

        # ----------------------------------------------------------------
        print("\n--- 9. 补充：新消息引用了已读消息 → 改那条任务，不新建 ---")
        mirror3_path = SCRATCH / f"mirror3-{RUN}.db"
        db_seq = SCRATCH / f"e2e-seq-{RUN}.db"
        mirror3 = Mirror(mirror3_path)
        settings3 = make_settings(token, db_seq, client_mirror_path=str(mirror3_path))
        backend3 = BackendClient(settings3)
        try:
            # 先让镜像里有一条"已读且已建任务"的消息（msg_id=100，群内序号 500）
            make_source_db(db_seq, [
                {"msg_id": "100", "seq": 500, "ts": BASE_TS + 1000,
                 "text": "下周三前把材料交到学工办", "content": text_of("下周三前把材料交到学工办")},
            ], with_seq=True)
            report = await run_cycle(backend3, settings3, db=SourceDatabase(db_seq), mirror=mirror3)
            check("先建出原任务", report.outcomes.get("extracted"), 1)
            original_raw = mirror3.get("100").raw_id
            check_true("镜像里有它的 raw_id", bool(original_raw), str(original_raw))
            base_count = len(notifications(token))
            base_notif = [n for n in notifications(token) if n.get("raw_message_id") == original_raw]
            check_true("后端里能找到这条任务", bool(base_notif), str(original_raw))

            # 再来一条**回复**它的消息
            make_source_db(db_seq, [
                {"msg_id": "100", "seq": 500, "ts": BASE_TS + 1000,
                 "text": "下周三前把材料交到学工办", "content": text_of("下周三前把材料交到学工办")},
                {"msg_id": "101", "seq": 501, "ts": BASE_TS + 1200,
                 "text": "补充：截止时间改到本周五，交到教三201",
                 "content": reply_of("补充：截止时间改到本周五，交到教三201", 500)},
            ], with_seq=True)
            report = await run_cycle(backend3, settings3, db=SourceDatabase(db_seq), mirror=mirror3)
            check("识别成补充并更新了任务", report.outcomes.get("amended"), 1)
            check("amended 计数", report.amended, 1)
            after = notifications(token)
            check("**通知条数没变**（补充改的是原任务）", len(after), base_count)
            merged = [n for n in after if n.get("raw_message_id") == original_raw]
            check_true("原任务还在", bool(merged), str(original_raw))
            if merged:
                blob = json.dumps(merged[0], ensure_ascii=False)
                check_true("补充的内容进了这条任务", "补充" in blob or "教三201" in blob, blob[:300])
                check_true(
                    "截止时间被补充改掉了（改到本周五）",
                    "周五" in str(merged[0].get("due_text") or "") or merged[0].get("due_at") is not None,
                    json.dumps({k: merged[0].get(k) for k in ("due_text", "due_at")}, ensure_ascii=False),
                )
            check("补充那条在镜像里记成 done", mirror3.get("101").state, STATE_DONE)
            check("补充关系记下来了", mirror3.amendment_of("101"), "100")

            # ------------------------------------------------------------
            print("\n--- 9b. 源表没有群内序号列 → 认不出引用，按独立新消息处理（不猜）---")
            mirror4_path = SCRATCH / f"mirror4-{RUN}.db"
            db_noseq = SCRATCH / f"e2e-noseq-{RUN}.db"
            mirror4 = Mirror(mirror4_path)
            settings4 = make_settings(token, db_noseq, client_mirror_path=str(mirror4_path))
            backend4 = BackendClient(settings4)
            try:
                make_source_db(db_noseq, [
                    {"msg_id": "200", "ts": BASE_TS + 2000, "text": "下周三前交实验报告",
                     "content": text_of("下周三前交实验报告")},
                ])
                await run_cycle(backend4, settings4, db=SourceDatabase(db_noseq), mirror=mirror4)
                count_before = len(notifications(token))
                make_source_db(db_noseq, [
                    {"msg_id": "200", "ts": BASE_TS + 2000, "text": "下周三前交实验报告",
                     "content": text_of("下周三前交实验报告")},
                    {"msg_id": "201", "ts": BASE_TS + 2100, "text": "补充：改到周五",
                     "content": reply_of("补充：改到周五", 900)},
                ])
                src = SourceDatabase(db_noseq)
                check("确认这个库没有群内序号列", src.seq_column(), None)
                check("确认引用值被读出来了（只是解析不了）", src.fetch_since(0, "", limit=5)[1].quote_ref, "900")
                report = await run_cycle(backend4, settings4, db=src, mirror=mirror4)
                check_true(
                    "认不出引用 → 按独立新消息处理（不猜）",
                    report.outcomes.get("amended") is None,
                    str(report.outcomes),
                )
                check("于是多了一条通知（这是诚实的降级）", len(notifications(token)), count_before + 1)
            finally:
                await backend4.close()
        finally:
            await backend3.close()

        # ----------------------------------------------------------------
        print("\n--- 10. dry-run：一个写请求都不发，**镜像也一个状态都不写** ---")
        mirror5_path = SCRATCH / f"mirror5-{RUN}.db"
        db_dry = SCRATCH / f"e2e-dry-{RUN}.db"
        mirror5 = Mirror(mirror5_path)
        settings5 = make_settings(
            token, db_dry, client_mirror_path=str(mirror5_path), client_dry_run=True
        )
        backend5 = BackendClient(settings5)
        try:
            make_source_db(db_dry, [
                {"msg_id": "300", "ts": BASE_TS + 3000, "text": "下周三前交材料", "content": text_of("下周三前交材料")},
            ])
            count_before = len(notifications(token))
            report = await run_cycle(backend5, settings5, db=SourceDatabase(db_dry), mirror=mirror5)
            check("dry-run 也看了消息", report.scanned, 1)
            check("通知数没变", len(notifications(token)), count_before)
            check("镜像里一条都没记（否则真跑时会全被跳过）", mirror5.stats()["total"], 0)

            # 紧接着真跑一次，必须**照常入库** —— 上面那条的回归测试
            settings6 = make_settings(token, db_dry, client_mirror_path=str(mirror5_path))
            backend6 = BackendClient(settings6)
            try:
                report = await run_cycle(
                    backend6, settings6, db=SourceDatabase(db_dry), mirror=mirror5
                )
                check("试跑之后再真跑，照常入库", report.outcomes.get("extracted"), 1)
                check("通知数 +1", len(notifications(token)), count_before + 1)
                check("镜像记成 done", mirror5.get("300").state, STATE_DONE)
            finally:
                await backend6.close()
        finally:
            await backend5.close()

        # ----------------------------------------------------------------
        print("\n--- 11. 附件：配了附件目录就能真的上传 ---")
        att_root = SCRATCH / "attachments"
        att_root.mkdir(exist_ok=True)
        (att_root / "ffffffffffffffffffffffffffffffff").write_bytes(b"\x89PNGfake")
        mirror6_path = SCRATCH / f"mirror6-{RUN}.db"
        db_att = SCRATCH / f"e2e-att-{RUN}.db"
        mirror6 = Mirror(mirror6_path)
        settings7 = make_settings(
            token, db_att, client_mirror_path=str(mirror6_path),
            client_attachment_root=str(att_root),
        )
        backend7 = BackendClient(settings7)
        try:
            make_source_db(db_att, [{
                "msg_id": "400", "ts": BASE_TS + 4000, "text": "下周三前把回执表交到学工办",
                "content": {"type": "mixed", "segments": [
                    {"type": "text", "text": "下周三前把回执表交到学工办"},
                    {"type": "image", "filename": "回执.png", "cdn_url": "https://cdn.example.dead/x.png",
                     "md5_hex": "ffffffffffffffffffffffffffffffff", "filesize": 8},
                ]},
            }])
            report = await run_cycle(
                backend7, settings7,
                db=SourceDatabase(db_att),
                resolver=AttachmentResolver(att_root),
                mirror=mirror6,
            )
            check("带附件那条建成了", report.outcomes.get("extracted"), 1)
            found = [n for n in notifications(token) if "回执" in (n.get("title") or "")]
            check_true("找到那条通知", bool(found), "没有标题含「回执」的通知")
            if found:
                atts = found[0].get("attachments") or []
                check("记了 1 个附件", len(atts), 1)
                check_true("是**真的上传过**的（有 id）", bool(atts and atts[0].get("id")), str(atts[:1]))
                check_true(
                    "url 指向后端的附件接口",
                    "/api/attachments/" in str(atts[0].get("url") if atts else ""),
                    str(atts[:1]),
                )
        finally:
            await backend7.close()

        # ----------------------------------------------------------------
        print("\n--- 12. 统计按用户写进去了 ---")
        stats = sync(token, "/api/stats").json()
        check_true("统计里有 extracted", int(stats.get("extracted") or 0) > 0, str(stats))
        check_true("统计里有 ingested", int(stats.get("ingested") or 0) > 0, str(stats))

        # ----------------------------------------------------------------
        print("\n--- 13. 缺口检测：群静默过久 → 按用户产生告警 ---")
        mirror7_path = SCRATCH / f"mirror7-{RUN}.db"
        db_gap = SCRATCH / f"e2e-gap-{RUN}.db"
        mirror7 = Mirror(mirror7_path)
        settings8 = make_settings(token, db_gap, client_mirror_path=str(mirror7_path))
        backend8 = BackendClient(settings8)
        try:
            make_source_db(db_gap, [
                {"msg_id": "500", "ts": BASE_TS + 5000, "text": "下周三交材料", "content": text_of("下周三交材料")},
                {"msg_id": "501", "ts": BASE_TS + 5000 + 16 * 3600, "text": "下周三交材料", "content": text_of("下周三交材料")},
            ])
            report = await run_cycle(backend8, settings8, db=SourceDatabase(db_gap), mirror=mirror7)
            check("两条都处理了", report.outcomes.get("extracted"), 2)
            alerts = sync(token, "/api/gap-alerts").json()["alerts"]
            check_true("产生了缺口告警", len(alerts) >= 1, str(alerts[:1]))
            if alerts:
                check_true("告警写明了间隔", "16.0 小时" in str(alerts[0].get("reason")), str(alerts[0].get("reason")))
        finally:
            await backend8.close()

        # ----------------------------------------------------------------
        print("\n--- 14. 白名单：配了就收窄，改了立刻生效 ---")
        mirror9_path = SCRATCH / f"mirror9-{RUN}.db"
        db_wl = SCRATCH / f"e2e-wl-{RUN}.db"
        mirror9 = Mirror(mirror9_path)
        # 白名单里放一个**别的**群 → 本地收窄把这条挡下
        settings9 = make_settings(
            token, db_wl, client_mirror_path=str(mirror9_path),
            client_group_whitelist="199999999",
        )
        backend9 = BackendClient(settings9)
        try:
            make_source_db(db_wl, [
                {"msg_id": "600", "ts": BASE_TS + 6000, "text": "下周三前交材料",
                 "content": text_of("下周三前交材料")},
            ])
            count_before = len(notifications(token))
            report = await run_cycle(backend9, settings9, db=SourceDatabase(db_wl), mirror=mirror9)
            check("被白名单挡下", report.skipped_whitelist, 1)
            check("一条通知都没建", len(notifications(token)), count_before)
            check("镜像里是 skipped（终态）", mirror9.get("600").state, STATE_SKIPPED)
            check_true(
                "原因带 whitelist: 前缀（改白名单时就是靠它认出来的）",
                str(mirror9.get("600").last_error).startswith("whitelist:"),
                str(mirror9.get("600").last_error),
            )

            # 白名单改成放行这个群 → 之前被挡的那条要**立刻**被重看
            settings10 = make_settings(
                token, db_wl, client_mirror_path=str(mirror9_path),
                client_group_whitelist=GROUP,
            )
            backend10 = BackendClient(settings10)
            try:
                report = await run_cycle(
                    backend10, settings10, db=SourceDatabase(db_wl), mirror=mirror9
                )
                check("白名单改过 → 把之前跳过的那条放回来", report.reopened, 1)
                check("于是它真的入库了", len(notifications(token)), count_before + 1)
                check("镜像里变成 done", mirror9.get("600").state, STATE_DONE)

                # 指纹没变时不该重复放回（否则每轮都重抽一遍）
                report = await run_cycle(
                    backend10, settings10, db=SourceDatabase(db_wl), mirror=mirror9
                )
                check("指纹没变就不放回", report.reopened, 0)
                check("也没重抽（内容没变）", report.processed, 0)
            finally:
                await backend10.close()
        finally:
            await backend9.close()

        # ----------------------------------------------------------------
        print("\n--- 16. 判据是「没读过」，不是时间：很老的消息照样要读 ---")
        # 这条是这一版的**核心回归**：源库里放一条 20 天前的消息（远远超出任何
        # 时间窗口，也超出 CLIENT_RECHECK_OVERLAP_HOURS），而镜像里没有它 ——
        # 它必须被读到。以前按"水位线 - 回看窗口"扫的时候，这条永远不会被读。
        db_old = SCRATCH / f"e2e-old-{RUN}.db"
        mirror_old_path = SCRATCH / f"mirror-old-{RUN}.db"
        old_ts = BASE_TS - 20 * 24 * 3600
        make_source_db(db_old, [
            {"msg_id": "900", "ts": old_ts, "text": "下周三前交材料",
             "content": text_of("下周三前交材料")},
            {"msg_id": "901", "ts": BASE_TS, "text": "下周三前交材料（新的那条）",
             "content": text_of("下周三前交材料（新的那条）")},
        ])
        settings_old = make_settings(token, db_old, client_mirror_path=str(mirror_old_path))
        backend_old = BackendClient(settings_old)
        mirror_old = Mirror(mirror_old_path)
        try:
            before = len(notifications(token))
            report = await run_cycle(
                backend_old, settings_old, db=SourceDatabase(db_old), mirror=mirror_old
            )
            check("报告里说清了还有多少没读过", report.unread_before, 2)
            check("两条都扫到了（20 天前那条也在内）", report.scanned, 2)
            check("20 天前那条被处理了", mirror_old.get("900").state, STATE_DONE)
            check("新的那条也被处理了", mirror_old.get("901").state, STATE_DONE)
            check("于是建了 2 条通知", len(notifications(token)) - before, 2)

            # 第二轮：都读过了 → 不再重复扫（增量靠的就是这个）
            report = await run_cycle(
                backend_old, settings_old, db=SourceDatabase(db_old), mirror=mirror_old
            )
            check("第二轮没有没读过的了", report.unread_before, 0)
            check("只做了回看（新消息在回看窗口内）", report.rechecked, 1)
            check("没有重抽任何一条", report.processed, 0)
        finally:
            await backend_old.close()
    finally:
        await backend.close()

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run_all()))
