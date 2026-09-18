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
    from .mirror import Mirror

    print("=== 配置 ===")
    print(f"  后端            {settings.backend_base}")
    print(f"  令牌            {'UserToken' if settings.is_user_token else '⚠️ 不是 UserToken（可能是服务令牌）'}")
    print(f"  源库            {settings.resolved_db_path}")
    print(f"  镜像库          {settings.resolved_mirror_path}")
    print(f"  附件根目录      {settings.resolved_attachment_root or '（未配置，附件只能留远程地址）'}")
    print(f"  抽取器          {settings.client_extractor}")
    print(f"  轮询间隔        {settings.client_poll_seconds}s")
    print(f"  回看窗口        {settings.client_recheck_overlap_hours}h（识别内容改动的范围）")
    print(
        f"  补充关系        {'开' if settings.client_amendment_enabled else '关'}"
        f"（只认 {settings.client_amendment_max_age_hours}h 之内的引用）"
    )

    print("\n=== 源库 ===")
    try:
        info = db.inspect()
    except SourceDatabaseError as exc:
        print(f"  ❌ {exc}")
        return 1
    print(f"  表 {info['table']}，共 {info['rows']} 行")
    if info["absent"]:
        print(f"  ⚠️ 缺列（会降级使用）：{info['absent']}")
    seq_col = db.seq_column()
    print(
        "  群内序号列      "
        + (
            f"{seq_col} → 能确定性地解析「这条在回复哪一条」"
            if seq_col
            else "**没有** → 认不出引用关系，引用了别人的消息会按独立新消息处理"
            "（源表缺 40003/seq 这一类列，见 README）"
        )
    )
    latest = db.latest()
    print(f"  最新一条的时间戳：{latest[0] if latest else '（空库）'}")

    print("\n=== 镜像库（客户端自己的状态）===")
    mirror = Mirror(settings.resolved_mirror_path)
    stats = mirror.stats()
    print(f"  共 {stats['total']} 条，状态 {stats['by_state'] or {}}")
    print(f"  补充关系 {stats['amendments']} 条")
    print(f"  水位线（已处理完的最大时间戳）：{mirror.watermark() or '（还没有）'}")
    unfinished = mirror.unfinished()
    if unfinished:
        print(f"  ⚠️ 有 {len(unfinished)} 条没处理完（下一轮会重试）")
        for row in unfinished[:5]:
            print(f"      · {row.msg_id}  state={row.state}  error={row.last_error or '-'}")

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
    print(f"  后端已有通知的 raw id：{len(processed)} 个（只在镜像不认识某条消息时兜底）")
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
        # `--since-hours N` = 把扫描起点往前推到 N 小时前重看一遍。
        #
        # 镜像里已经有状态的消息**不会被当成新的**（内容没变就跳过），所以它的
        # 真正用途是：**内容被改过**的消息（超出回看窗口的那些）也要重新处理一遍。
        # 实现方式是临时把回看窗口放大到 N 小时，而不是去动镜像 ——
        # 镜像记的是事实（每一条处理过没有），不该被一次调用改写。
        logger.info(
            "按 --since-hours=%s 把回看窗口临时放大到 %s 小时（重新检查这个范围内的内容改动）",
            since_hours,
            since_hours,
        )
        settings = settings.model_copy(
            update={
                "client_recheck_overlap_hours": float(since_hours),
                # 同时关掉"后端已经有这条通知就跳过抽取"的捷径 ——
                # 否则镜像被删过之后，改过的老消息永远更新不了
                "client_force_recheck": True,
            }
        )

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
