"""端到端：源库 → 客户端 → **真后端**。

需要一个**正在运行的后端**：

    $env:API_TOKEN='service-token'; $env:SIGNUP_MODE='invite'; $env:SERVER_PORT='8005'
    .\\xcollector-backend\\.venv\\Scripts\\python.exe -m app.main

    $env:CLIENT_BASE='http://127.0.0.1:8005'; $env:CLIENT_SERVICE_TOKEN='service-token'
    .\\.venv\\Scripts\\python.exe -m tests.check_pipeline_e2e

## 这个文件要守住的东西

1. **只用 UserToken 就能完整入库**。脚本注册一个真用户、拿他的 UserToken，
   全程只用那个令牌。所以只要客户端偷偷调了一个服务令牌专属的接口，
   这里就会以 403 失败 —— 权限边界不是靠 review 守的，是靠跑出来的。
2. **订阅就是过滤条件**：订了的来源才入库，没订的连 raw 都不写、
   更不会去抽取（不为没人要的消息花模型的钱）。
3. **三层防重**：游标 → 已有通知集合 → 后端幂等。第二层是"游标丢了不心疼"
   的关键，所以专门测它。
4. **游标真的在推进**，而且下一轮不会重复处理。
5. **不静默**：没有证据不建条、拿不到附件要留痕、写共享层被拒要能看出是
   "你还没订阅"而不是"后端坏了"。
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
from pathlib import Path

import httpx

from app.backend_client import BackendClient
from app.config import Settings
from app.run import load_subscriptions, run_cycle, verify_identity
from app.source.attachments import AttachmentResolver
from app.source.ntmsg import SourceDatabase

BASE = os.environ.get("CLIENT_BASE", "http://127.0.0.1:8005").rstrip("/")
API = BASE + "/api"
SERVICE = os.environ.get("CLIENT_SERVICE_TOKEN", "service-token")

RUN = os.environ.get("CLIENT_RUN") or str(int(__import__("time").time() * 1000))
QQ = str(500000000 + int(RUN[-7:]) % 40000000)
_SUF = RUN[-7:]
GROUP = "91" + _SUF
SENDER = "92" + _SUF
GROUP2 = "93" + _SUF

SCRATCH = Path(__file__).resolve().parent.parent / ".tmp-test"
H = {"Authorization": f"Bearer {SERVICE}"}

# 源库的时间戳基准：**就在不久之前**，不是写死的绝对时间。
#
# 为什么：客户端默认只回看 CLIENT_INITIAL_LOOKBACK_HOURS（72 小时）。用一个
# 固定的过去时间当基准，等这个测试过一阵子再跑（或者换台时钟不同的机器），
# 所有消息都会落在回看窗口之外 —— 于是"扫了 0 条"，而失败原因和被测逻辑
# 毫无关系。往前放 20 小时是刻意的：后面要造一条 16 小时后的消息来测缺口，
# 那样它仍然落在过去、不会出现"未来时间的消息"这种怪东西。
BASE_TS = int(__import__("time").time()) - 20 * 3600

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

SOURCE_SCHEMA = """
CREATE TABLE group_messages (
  msg_id TEXT PRIMARY KEY, timestamp INTEGER, direction INTEGER,
  sender_uid TEXT, sender_qq TEXT, group_id TEXT, group_qq TEXT,
  msg_type INTEGER, subtype INTEGER, content_type INTEGER,
  text TEXT, parse_status TEXT, content TEXT
);
"""


def make_source_db(path: Path, rows: list[dict]) -> None:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SOURCE_SCHEMA)
        for row in rows:
            conn.execute(
                "INSERT INTO group_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
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
                    json.dumps(row["content"], ensure_ascii=False) if row.get("content") else None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def text_of(text: str) -> dict:
    return {"type": "text", "text": text}


def register_user() -> tuple[str, str]:
    """注册一个真用户，返回 (user_id, UserToken)。"""
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


def make_settings(token: str, db_path: Path, **overrides) -> Settings:
    base = dict(
        backend_base_url=BASE,
        client_token=token,
        backend_timeout=15.0,
        backend_max_retries=1,
        client_db_path=str(db_path),
        client_extractor="rule",          # 核心链路不联网、不花钱
        client_initial_lookback_hours=72,
        client_batch_size=100,
        client_max_messages_per_cycle=100,
        client_poll_seconds=1,
        client_attachment_root="",
        client_missing_attachment="url",
        client_cursor_key=f"e2e-{RUN}",
        client_gap_alert_hours=2.0,
        digest_tz="Asia/Shanghai",
    )
    base.update(overrides)
    settings = Settings(**base)

    # ⚠️ `Settings` 是 `extra="ignore"` 的：关键字名写错的项会被**静默丢掉**，
    # 于是测试拿着默认值往下跑 —— 轻则失败原因和被测逻辑毫无关系，重则还能"通过"。
    # 所以这里逐个核对关键项确实生效了。
    # （对用户宽容、对测试严格：用户 .env 里留着淘汰的键不该让程序起不来。）
    for name in ("client_db_path", "client_token", "client_extractor", "client_cursor_key"):
        assert getattr(settings, name) == base[name], (
            f"make_settings 的 {name} 没生效（关键字名写错了？extra=ignore 会静默丢掉）"
        )
    for name, value in overrides.items():
        assert getattr(settings, name, None) == value, f"覆盖项 {name} 没生效"
    return settings


def sync(token: str, path: str, method: str = "GET", **kwargs) -> httpx.Response:
    """直接用某个令牌问后端（核对客户端写进去的东西）。"""
    with httpx.Client(base_url=BASE, timeout=20, headers={"Authorization": f"Bearer {token}"}) as c:
        return c.request(method, path, **kwargs)


async def run_all() -> int:
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    uid, token = register_user()
    print(f"→ {BASE}\n    用户 {QQ} = {uid}\n")

    db_path = SCRATCH / f"e2e-{RUN}.db"
    raw_ids: dict[str, str] = {}

    # ------------------------------------------------------------------
    print("--- 1. 身份自检：UserToken 就够了 ---")
    settings = make_settings(token, db_path)
    backend = BackendClient(settings)
    try:
        who = await verify_identity(backend, settings)
        check("scope 是 user（不是 service）", who.get("scope"), "user")
        check("拿到的是自己", (who.get("user") or {}).get("qq"), QQ)

        # ------------------------------------------------------------------
        print("\n--- 2. 还没订阅 → 本轮什么都不做，而且**游标也不动** ---")
        make_source_db(db_path, [
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得", "content": text_of("大家下周三前交军训心得")},
        ])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("一条都没扫（扫了也是白扫，而且会烧掉回看窗口）", report.scanned, 0)
        check("没有建任何通知", report.outcomes.get("extracted"), None)
        check_true("报错说清了是订阅的问题", any("订阅" in e for e in report.errors), str(report.errors[:1]))
        check("后端里确实一条通知都没有", len(sync(token, "/api/notifications").json()["notifications"]), 0)
        # 这一条是关键：先启动客户端、再去订阅是很自然的顺序。如果这里把游标
        # 推到底，那 72 小时的存量就被这一次空跑烧掉了，用户订完之后会发现
        # 什么都没补上、而且毫无线索。
        check(
            "游标没有被推进（保住回看窗口，订完之后还能补存量）",
            sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}").status_code,
            404,
        )

        # ------------------------------------------------------------------
        print("\n--- 3. 订上之后：存量直接补上 ---")
        r = sync(token, "/api/subscriptions", "POST", json={"group_id": GROUP, "sender_id": SENDER})
        check("用**用户令牌**订阅 → 200", r.status_code, 200)

        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("订了之后 → extracted", report.outcomes.get("extracted"), 1)
        notifications = sync(token, "/api/notifications").json()["notifications"]
        check("后端里有 1 条通知", len(notifications), 1)
        notif = notifications[0]
        check("标题抽对了", notif.get("title"), "大家下周三前交军训心得")
        check_true("截止时间解析出来了", notif.get("due_at") is not None, str(notif.get("due_at")))
        check("地点为空（原文没写地点，不许编）", notif.get("location"), None)
        check("抽取器是 rule", notif.get("extractor"), "rule")
        check_true("证据非空", bool(notif.get("evidence")), str(notif.get("evidence")))
        raw_ids["1"] = str(notif.get("raw_message_id"))

        # ------------------------------------------------------------------
        print("\n--- 4. 游标推进了：再跑一轮什么都不做 ---")
        cursor = sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}").json()["value"]
        check("游标停在最后一条的位置", (cursor.get("ts"), cursor.get("msg_id")), (BASE_TS, "1"))
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("第二轮没扫到新消息", report.scanned, 0)
        check("通知还是 1 条（没有重复）", len(sync(token, "/api/notifications").json()["notifications"]), 1)

        # ------------------------------------------------------------------
        print("\n--- 5. 增量：新消息进来只处理新的 ---")
        make_source_db(db_path, [
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得", "content": text_of("大家下周三前交军训心得")},
            {"msg_id": "2", "ts": BASE_TS + 60, "text": "收到", "content": text_of("收到")},
            {"msg_id": "3", "ts": BASE_TS + 120, "text": "本周五19:00在教三201开班会", "content": text_of("本周五19:00在教三201开班会")},
        ])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("只扫了 2 条新的", report.scanned, 2)
        check("闲聊判成 noise", report.outcomes.get("noise"), 1)
        check("开会那条建成了", report.outcomes.get("extracted"), 1)
        after = sync(token, "/api/notifications").json()["notifications"]
        check("通知变成 2 条", len(after), 2)
        titles = sorted(n.get("title") or "" for n in after)
        check_true("两条标题都在", any("开班会" in t for t in titles), str(titles))

        # ------------------------------------------------------------------
        print("\n--- 6. 游标丢了也不重复抽（这是最省钱的一层）---")
        # 把游标删掉，模拟"换了机器 / bot_state 被清"：
        sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}", "DELETE")
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("回扫了全部 3 条", report.scanned, 3)
        check("其中 2 条被识别为「已经建过通知」而跳过", report.already_done, 2)
        check_true("没有重复建条", len(sync(token, "/api/notifications").json()["notifications"]) == 2, str(report.outcomes))
        check("游标又被写回去了", sync(
            token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}"
        ).status_code, 200)

        # ------------------------------------------------------------------
        print("\n--- 7. 订阅之外的来源：连 raw 都不写 ---")
        make_source_db(db_path, [
            {"msg_id": "1", "ts": BASE_TS, "text": "大家下周三前交军训心得", "content": text_of("大家下周三前交军训心得")},
            {"msg_id": "2", "ts": BASE_TS + 60, "text": "收到", "content": text_of("收到")},
            {"msg_id": "3", "ts": BASE_TS + 120, "text": "本周五19:00在教三201开班会", "content": text_of("本周五19:00在教三201开班会")},
            # 没订阅的群，以及同群里没订阅的发送者
            {"msg_id": "4", "ts": BASE_TS + 180, "group": GROUP2, "text": "下周一交实验报告", "content": text_of("下周一交实验报告")},
            {"msg_id": "5", "ts": BASE_TS + 240, "sender": "19999", "text": "明天上午交材料", "content": text_of("明天上午交材料")},
        ])
        sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}", "DELETE")
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("5 条里跳过了 2 条订阅外的", report.skipped_unsubscribed, 2)
        check("通知还是 2 条（订阅外的没被建）", len(sync(token, "/api/notifications").json()["notifications"]), 2)

        # ------------------------------------------------------------------
        print("\n--- 8. 附件：拿不到字节要留痕，不静默 ---")
        make_source_db(db_path, [
            {
                "msg_id": "10",
                "ts": BASE_TS + 300,
                "text": "下周三前把回执表交到学工办",
                "content": {
                    "type": "mixed",
                    "segments": [
                        {"type": "text", "text": "下周三前把回执表交到学工办"},
                        {"type": "image", "filename": "回执.png", "cdn_url": "https://cdn.example.dead/x.png",
                         "md5_hex": "ffffffffffffffffffffffffffffffff", "filesize": 1024},
                    ],
                },
            },
        ])
        sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}", "DELETE")
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("带附件那条建成了", report.outcomes.get("extracted"), 1)
        # 没配附件根目录 → 只能留远程地址，而且要标出这是降级的
        found = [
            n for n in sync(token, "/api/notifications").json()["notifications"]
            if "回执" in (n.get("title") or "")
        ]
        check_true("找到那条通知", bool(found), "没有标题含「回执」的通知")
        if found:
            atts = found[0].get("attachments") or []
            check("记了 1 个附件", len(atts), 1)
            check_true("附件标了 degraded（拿不到字节这件事没被吞）", atts[0].get("degraded"), str(atts[:1]))
            check_true("附件里带原因", bool(atts[0].get("degraded_reason")), str(atts[:1]))

        # 配上附件目录、并把字节按 md5 放进去之后，应该能真的上传
        att_root = SCRATCH / "attachments"
        att_root.mkdir(exist_ok=True)
        (att_root / "ffffffffffffffffffffffffffffffff").write_bytes(b"\x89PNGfake")
        settings2 = make_settings(token, db_path, client_attachment_root=str(att_root))
        backend2 = BackendClient(settings2)
        try:
            make_source_db(db_path, [
                {
                    "msg_id": "11",
                    "ts": BASE_TS + 360,
                    "text": "下周三前把回执表交到学工办（第二版）",
                    "content": {
                        "type": "mixed",
                        "segments": [
                            {"type": "text", "text": "下周三前把回执表交到学工办（第二版）"},
                            {"type": "image", "filename": "回执2.png",
                             "cdn_url": "https://cdn.example.dead/y.png",
                             "md5_hex": "ffffffffffffffffffffffffffffffff", "filesize": 8},
                        ],
                    },
                },
            ])
            sync(token, f"/api/state/{settings2.client_cursor_namespace}/{settings2.cursor_name}", "DELETE")
            report = await run_cycle(
                backend2, settings2, db=SourceDatabase(db_path), resolver=AttachmentResolver(att_root)
            )
            check("第二条带附件的也建成了", report.outcomes.get("extracted"), 1)
            found2 = [
                n for n in sync(token, "/api/notifications").json()["notifications"]
                if "第二版" in (n.get("title") or "")
            ]
            if found2:
                atts2 = found2[0].get("attachments") or []
                check("附件记了 1 个", len(atts2), 1)
                check_true(
                    "这次是真的上传了（有 id、不是只留地址）",
                    bool(atts2 and atts2[0].get("id")),
                    str(atts2[:1]),
                )
                check_true(
                    "而且 url 指向后端的附件接口",
                    "/api/attachments/" in str(atts2[0].get("url") if atts2 else ""),
                    str(atts2[:1]),
                )
        finally:
            await backend2.close()

        # ------------------------------------------------------------------
        print("\n--- 9. 统计按用户、写进去了 ---")
        stats = sync(token, "/api/stats").json()
        check_true("统计里有 extracted", int(stats.get("extracted") or 0) > 0, str(stats))
        check_true("统计里有 ingested", int(stats.get("ingested") or 0) > 0, str(stats))

        # ------------------------------------------------------------------
        print("\n--- 10. 没有证据的抽出一律不建条（硬约束）---")
        # rule 抽取器在只有关键词、没有时间时也会给 evidence，所以这里直接构造
        # 一个"抽取结果没证据"的场景：用一条既没关键词也没时间的消息，
        # 它应该被判成 noise（而不是建一条没有证据的条）。
        make_source_db(db_path, [
            {"msg_id": "20", "ts": BASE_TS + 420, "text": "哈哈哈哈", "content": text_of("哈哈哈哈")},
        ])
        sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}", "DELETE")
        before = len(sync(token, "/api/notifications").json()["notifications"])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("纯闲聊判成 noise", report.outcomes.get("noise"), 1)
        check("通知数没变", len(sync(token, "/api/notifications").json()["notifications"]), before)

        # ------------------------------------------------------------------
        print("\n--- 11. 缺口检测：按用户扇出 ---")
        sync(token, f"/api/state/{settings.client_cursor_namespace}/{settings.cursor_name}", "DELETE")
        make_source_db(db_path, [
            {"msg_id": "30", "ts": BASE_TS + 1000, "text": "下周三交材料", "content": text_of("下周三交材料")},
            # 隔 16 小时再来一条 → 应该产生缺口告警
            {"msg_id": "31", "ts": BASE_TS + 1000 + 16 * 3600, "text": "下周三交材料", "content": text_of("下周三交材料")},
        ])
        report = await run_cycle(backend, settings, db=SourceDatabase(db_path))
        check("两条都处理了", report.outcomes.get("extracted"), 2)
        alerts = sync(token, "/api/gap-alerts").json()["alerts"]
        check_true("产生了缺口告警", len(alerts) >= 1, str(alerts[:1]))
        if alerts:
            check_true("告警写明了间隔", "16.0 小时" in str(alerts[0].get("reason")), str(alerts[0].get("reason")))

        # ------------------------------------------------------------------
        print("\n--- 12. dry-run：一个写请求都不发 ---")
        settings_dry = make_settings(token, db_path, client_dry_run=True)
        cursor_key = f"dry-{RUN}"
        settings_dry = settings_dry.model_copy(update={"client_cursor_key": cursor_key})
        backend3 = BackendClient(settings_dry)
        try:
            sync(token, f"/api/state/{settings_dry.client_cursor_namespace}/{cursor_key}", "DELETE")
            before = len(sync(token, "/api/notifications").json()["notifications"])
            report = await run_cycle(backend3, settings_dry, db=SourceDatabase(db_path))
            check_true("dry-run 也扫了消息", report.scanned > 0, str(report.scanned))
            check("通知数没变", len(sync(token, "/api/notifications").json()["notifications"]), before)
            check(
                "dry-run 没有写游标（下次还会重扫，这是刻意的）",
                sync(token, f"/api/state/{settings_dry.client_cursor_namespace}/{cursor_key}").status_code,
                404,
            )
        finally:
            await backend3.close()
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
