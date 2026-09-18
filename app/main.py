"""入口：`python -m app.main`（或用 `.bat` / systemd timer / 计划任务）。

## 三种跑法

    python -m app.main                 # 定期循环（默认；间隔 CLIENT_POLL_SECONDS）
    python -m app.main --once           # 只跑一轮就退出（交给 cron / 计划任务）
    python -m app.main --status         # 只看配置、源库、身份、订阅，不写任何东西
    python -m app.main --dry-run --once # 只组装不写入，先看一眼会抽出什么

**推荐用 `--once` + 计划任务**：这个客户端本来就是批处理的，把调度交给操作系统
比让它常驻更省心（也不会有"进程活着但其实卡住了"这种最难发现的故障）。
`--loop` 只是给不方便配计划任务的人一个选择。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .backend_client import BackendClient, BackendError
from .config import get_settings
from .logging_setup import setup_logging
from .run import (
    load_processed_raw_ids,
    load_subscriptions,
    log_report,
    run_cycle,
    run_loop,
    verify_identity,
)
from .source.attachments import AttachmentResolver
from .source.ntmsg import SourceDatabase, SourceDatabaseError

logger = logging.getLogger("xcollector.client")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.main",
        description="Xcollector 入库客户端：读聊天记录库，抽取通知，写进后端。",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="只跑一轮就退出（推荐配合计划任务）")
    mode.add_argument("--loop", action="store_true", help="常驻，按 CLIENT_POLL_SECONDS 定期跑")
    mode.add_argument("--status", action="store_true", help="只做自检与统计，不写任何东西")
    parser.add_argument("--dry-run", action="store_true", help="只组装不写入")
    parser.add_argument("--since-hours", type=int, default=None, help="忽略游标，从 N 小时前重扫")
    parser.add_argument("--limit", type=int, default=None, help="本轮最多处理多少条")
    parser.add_argument("--log-level", default=None, help="覆盖 CLIENT_LOG_LEVEL")
    return parser.parse_args(argv)


async def show_status(backend: BackendClient, settings, db: SourceDatabase) -> int:
    """自检 + 现状。**一个写请求都不发。**"""
    print("=== 配置 ===")
    print(f"  后端            {settings.backend_base}")
    print(f"  令牌            {'UserToken' if settings.is_user_token else '⚠️ 不是 UserToken（可能是服务令牌）'}")
    print(f"  源库            {settings.resolved_db_path}")
    print(f"  附件根目录      {settings.resolved_attachment_root or '（未配置，附件只能留远程地址）'}")
    print(f"  抽取器          {settings.client_extractor}")
    print(f"  轮询间隔        {settings.client_poll_seconds}s")
    print(f"  游标键          {settings.cursor_name}")

    print("\n=== 源库 ===")
    try:
        info = db.inspect()
    except SourceDatabaseError as exc:
        print(f"  ❌ {exc}")
        return 1
    print(f"  表 {info['table']}，共 {info['rows']} 行")
    if info["absent"]:
        print(f"  ⚠️ 缺列（会降级使用）：{info['absent']}")
    latest = db.latest()
    print(f"  最新一条的时间戳：{latest[0] if latest else '（空库）'}")

    print("\n=== 后端 ===")
    health = await backend.health()
    print(f"  可达：{health.get('reachable')}" + (f"，错误：{health.get('error')}" if health.get("error") else ""))
    if not health.get("reachable"):
        return 1

    who = await verify_identity(backend, settings)
    user = who.get("user") or {}
    subs = await load_subscriptions(backend)
    print(f"  订阅（这就是过滤条件）：{len(subs)} 条")
    for group_id, sender_id in sorted(subs)[:20]:
        print(f"    · 群 {group_id} · 发送者 {sender_id}")
    if not subs:
        print("    ⚠️ 一条都没有：客户端不会入库。先在网页上（或发 /订阅）订一个来源。")

    processed = await load_processed_raw_ids(backend)
    print(f"  已有通知的 raw id：{len(processed)} 个（游标丢失时靠它跳过重复抽取）")

    cursor = await backend.get_cursor(settings.cursor_name)
    print(f"  当前游标：{cursor or '（还没有，下次会按 initial_lookback_hours 回扫）'}")
    print(f"\n  用户：{user.get('qq')}（{user.get('id')}）")
    return 0


async def run_once(backend: BackendClient, settings, *, since_hours: int | None = None) -> int:
    db = SourceDatabase(settings.resolved_db_path)
    try:
        db.inspect()
    except SourceDatabaseError as exc:
        logger.error("%s", exc)
        return 2

    if since_hours is not None:
        # 显式要重扫：把游标设到那个时间点（只影响这一次，因为它紧接着会被覆盖）
        from .utils import now_ms

        cursor_ts = (now_ms() - int(since_hours) * 3600 * 1000) // 1000
        logger.info("按 --since-hours=%s 从 %s 开始重扫（会覆盖游标）", since_hours, cursor_ts)
        await backend.put_cursor(settings.cursor_name, {"ts": cursor_ts, "msg_id": "", "force": True})

    report = await run_cycle(backend, settings, db=db, resolver=AttachmentResolver(settings.resolved_attachment_root))
    log_report(report)
    # 一条都没处理成功、而且有错 → 用非零退出码，让计划任务/监控能发现
    if report.errors and report.processed == 0 and report.scanned > 0:
        return 1
    return 0


async def amain(args: argparse.Namespace) -> int:
    settings = get_settings()
    if args.dry_run:
        settings = settings.model_copy(update={"client_dry_run": True})
    if args.limit is not None:
        settings = settings.model_copy(update={"client_max_messages_per_cycle": int(args.limit)})

    if not settings.backend_base_url:
        logger.error("没有配置 BACKEND_BASE_URL")
        return 2
    if not settings.client_db_path:
        logger.error(
            "没有配置 CLIENT_DB_PATH —— 它要指向 nt_msg_db_util 的 3.export.py 产出的 "
            "nt_msg_export.db（明文 SQLite），不是 nt_msg.db。"
        )
        return 2

    db = SourceDatabase(settings.resolved_db_path)
    backend = BackendClient(settings)
    try:
        if args.status:
            return await show_status(backend, settings, db)
        await verify_identity(backend, settings)
        if args.loop or not args.once:
            await run_loop(backend, settings)
            return 0
        return await run_once(backend, settings, since_hours=args.since_hours)
    except BackendError as exc:
        logger.error("后端调用失败：%s", exc)
        return 1
    except SourceDatabaseError as exc:
        logger.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        logger.info("收到中断，退出")
        return 0
    finally:
        await backend.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
