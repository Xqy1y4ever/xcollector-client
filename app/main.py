"""入口：`python -m app.main`（或用 `.bat` / systemd timer / 计划任务）。

## 四种跑法

    python -m app.main --ui       # 本机 Web UI：配置 / 跑一轮 / 看日志（推荐第一次用）
    python -m app.main --once     # 跑一轮就退出（**推荐**：交给 cron / 计划任务）
    python -m app.main --loop     # 常驻，按 CLIENT_POLL_SECONDS 定期跑
    python -m app.main --status   # 只看配置、源库、镜像、身份，不写任何东西

**推荐用 `--once` + 计划任务**：这个客户端本来就是批处理的，把调度交给操作系统
比让它常驻更省心（也不会有"进程活着但其实卡住了"这种最难发现的故障）。
`--loop` 只是给不方便配计划任务的人一个选择，`--ui` 是给"想点着用"的人。

（以前还有 `--prepare` / `--dry-run` / `--since-hours` / `--limit` / `--log-level`。
它们都去掉了：前两个是"多看一步"的辅助模式，实际上没人用；后三个是把写死的常量
临时改一下，而"临时改一下"意味着运行结果不可复现。要改就改 `app/config.py`。）
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .backend_client import BackendClient, BackendError
from .config import (
    DECRYPT_INTEGRITY,
    DECRYPT_MAX_SKIPS,
    MAX_MESSAGES_PER_CYCLE,
    NT_MSG_HEADER_SIZE,
    NT_MSG_HMAC_ALGORITHM,
    NT_MSG_KDF_ALGORITHM,
    NT_MSG_KDF_ITER,
    NT_MSG_PAGE_SIZE,
    ConfigError,
    get_settings,
    unknown_env_keys,
)
from .logging_setup import setup_logging
from .run import (
    load_processed_raw_ids,
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
    mode.add_argument(
        "--ui",
        action="store_true",
        help="打开本机 Web UI（配置 / 跑一轮 / 看日志），默认 http://127.0.0.1:8787",
    )
    parser.add_argument("--host", default="127.0.0.1", help="--ui 监听地址（默认只监听本机）")
    parser.add_argument("--port", type=int, default=8787, help="--ui 监听端口")
    parser.add_argument("--no-browser", action="store_true", help="--ui 不要自动开浏览器")
    return parser.parse_args(argv)


async def show_status(backend: BackendClient, settings, db: SourceDatabase) -> int:
    """自检 + 现状。**一个写请求都不发。**"""
    from .mirror import Mirror

    print("=== 配置 ===")
    print(f"  后端            {settings.backend_base}")
    print(f"  令牌            {'UserToken' if settings.is_user_token else '⚠️ 不是 UserToken（可能是服务令牌）'}")
    print(f"  源库            {settings.ntmsg_export_path}")
    print(f"  镜像库          {settings.resolved_mirror_path}")
    print(f"  附件根目录      {settings.resolved_attachment_root or '（未配置，附件只能留远程地址）'}")
    group_wl = settings.client_group_whitelist.strip() or "（不限制）"
    sender_wl = settings.client_sender_whitelist.strip() or "（不限制）"
    print(f"  白名单（只收窄）群={group_wl}")
    print(f"                  发送者={sender_wl}")
    print(f"  抽取器          {settings.client_extractor}")
    print(f"  轮询间隔        {settings.client_poll_seconds}s（用 --once 时不起作用）")
    print(f"  一轮最多处理    {MAX_MESSAGES_PER_CYCLE} 条（写死的常量，见 app/config.py）")

    print("\n=== nt_msg 前置步骤（剥头 + 解密 + 导出）===")
    if not settings.ntmsg_pipeline_enabled:
        print("  未配置 CLIENT_NT_MSG_DB → 直接用 CLIENT_DB_PATH 指向的导出库")
    else:
        print(f"  nt_msg.db       {settings.client_nt_msg_db}")
        if settings.client_nt_msg_key_file.strip():
            print(f"  密钥来源        CLIENT_NT_MSG_KEY_FILE={settings.client_nt_msg_key_file}")
        elif settings.client_nt_msg_key.strip():
            # **只报长度**，绝不打印密钥本身 —— 这一行经常被贴进 issue/聊天里。
            print(
                f"  密钥来源        CLIENT_NT_MSG_KEY（{len(settings.client_nt_msg_key.strip())} 个字符）"
            )
        else:
            print("  ⚠️ 密钥          没配（CLIENT_NT_MSG_KEY / CLIENT_NT_MSG_KEY_FILE 都是空的）")
        print(
            f"  解密参数        header={NT_MSG_HEADER_SIZE} page_size={NT_MSG_PAGE_SIZE} "
            f"kdf_iter={NT_MSG_KDF_ITER} kdf={NT_MSG_KDF_ALGORITHM} hmac={NT_MSG_HMAC_ALGORITHM}"
            "（上游常量，见 app/config.py）"
        )
        print(f"  中间产物        与 nt_msg.db 同目录：nt_msg_clear.db / nt_msg_plain.db")
        print(
            f"  SQLite 自检     {DECRYPT_INTEGRITY}"
            f"（坏页最多跳过 {'不限' if DECRYPT_MAX_SKIPS < 0 else DECRYPT_MAX_SKIPS} 行，"
            "每次跳过都会报 ERROR）"
        )

    print("\n=== 源库 ===")
    try:
        info = db.inspect()
    except SourceDatabaseError as exc:
        if settings.ntmsg_pipeline_enabled and not settings.ntmsg_export_path.exists():
            print(
                f"  还没生成：{settings.ntmsg_export_path}\n"
                "  跑一次 `python -m app.main --once`：它会先做剥头 + 解密 + 导出，再入库。"
            )
            return 1
        print(f"  ❌ {exc}")
        return 1
    print(f"  表 {info['table']}，共 {info['rows']} 行")
    if info["absent"]:
        print(f"  ⚠️ 缺列（会降级使用）：{info['absent']}")
    latest = db.latest()
    print(f"  最新一条的时间戳：{latest[0] if latest else '（空库）'}")

    print("\n=== 镜像库（客户端自己的状态）===")
    mirror = Mirror(settings.resolved_mirror_path)
    stats = mirror.stats()
    print(f"  共 {stats['total']} 条，状态 {stats['by_state'] or {}}")
    print(f"  水位线（已处理完的最大时间戳）：{mirror.watermark() or '（还没有）'}")
    # 读什么由**这个**决定，不由时间决定：源库里没被标记过的都要读，不管多老。
    # 白名单**下推到 SQL**：配了白名单时，这个数只算白名单内的消息
    # （以前它是整个库的行数，看起来像"要读 77 万条"）。
    groups = tuple(settings.group_whitelist_map)
    senders = tuple(settings.sender_whitelist_map)
    try:
        unread = db.count_unread(settings.resolved_mirror_path, groups=groups, senders=senders)
        scope = "（只算白名单内的）" if (groups or senders) else "（没配白名单：源库里所有的都要读）"
        print(
            f"  没读过的消息    {unread} 条" + scope
            + (
                f"；一轮最多读 {MAX_MESSAGES_PER_CYCLE} 条"
                if unread
                else "；下一轮只读新增的"
            )
        )
        if groups or senders:
            print(
                f"                  白名单内共 {db.count_matching(groups=groups, senders=senders)} 条"
                f"（源库共 {info['rows']} 行）"
            )
    except SourceDatabaseError as exc:
        print(f"  没读过的消息    算不出来：{exc}")
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
    # 订阅是 **bot** 的过滤条件，不是客户端的：客户端读的是自己账号的聊天记录库，
    # 收窄只有 CLIENT_GROUP_WHITELIST / CLIENT_SENDER_WHITELIST（留空 = 不限制）。
    if settings.whitelist_active:
        print(
            "  本地白名单      群=%s 发送者=%s（只收窄）"
            % (
                ",".join(sorted(settings.group_whitelist_map)) or "（不限）",
                ",".join(sorted(settings.sender_whitelist_map)) or "（不限）",
            )
        )
    else:
        print("  本地白名单      （没配：源库里所有来源都读）")
    print("  订阅            不适用（订阅只影响 bot 的实时入库）")

    processed = await load_processed_raw_ids(backend)
    print(f"  后端已有通知的 raw id：{len(processed)} 个（只在镜像不认识某条消息时兜底）")
    print(f"\n  用户：{user.get('qq')}（{user.get('id')}）")
    return 0


def _run_one_cycle(settings):
    """跑一轮（含"先把源库准备好"），返回 `CycleReport`。

    抽出来是因为 Web UI 也要用同一套：它在自己的线程里调这个函数，
    所以这里**不能**有 print / 退出码之类的东西，一切进日志和返回值。
    """
    from .ntmsg_db import prepare_databases
    from .ntmsg_db.decrypt import DecryptError
    from .ntmsg_db.export import ExportError

    prepared = None
    if settings.ntmsg_pipeline_enabled:
        prepared = prepare_databases(settings)
    db = SourceDatabase(prepared.export_path if prepared else settings.resolved_db_path)
    db.inspect()   # 库不对就地抛 SourceDatabaseError，由调用方决定怎么说

    backend = BackendClient(settings)
    try:
        return asyncio.run(
            run_cycle(
                backend,
                settings,
                db=db,
                resolver=AttachmentResolver(settings.resolved_attachment_root),
            )
        )
    finally:
        asyncio.run(backend.close())


async def run_once(backend: BackendClient, settings) -> int:
    """`--once`：跑一轮，打完日志就退出。"""
    try:
        report = _run_one_cycle(settings)
    except SourceDatabaseError as exc:
        logger.error("%s", exc)
        return 2
    except (DecryptError, ExportError) as exc:
        logger.error("准备源库失败：%s", exc)
        return 2
    log_report(report)
    # 一条都没处理成功、而且有错 → 用非零退出码，让计划任务/监控能发现
    if report.errors and report.processed == 0 and report.scanned > 0:
        return 1
    return 0


async def amain(args: argparse.Namespace) -> int:
    settings = get_settings()

    # Web UI 放在所有配置校验**之前**：第一次用的人手上什么都没有（.env 还没写、
    # 源库还没配），而那个页面正是用来填这些的。挡住它的检查应该在页面**里面**做。
    if args.ui:
        from .webui import serve

        return await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: serve(args.host, args.port, open_browser=not args.no_browser),
        )

    if not settings.backend_base_url:
        logger.error("没有配置 BACKEND_BASE_URL")
        return 2
    if not settings.client_db_path and not settings.ntmsg_pipeline_enabled:
        logger.error(
            "既没有配置 CLIENT_NT_MSG_DB，也没有配置 CLIENT_DB_PATH。二选一：\n"
            "  · CLIENT_NT_MSG_DB = 加密的 nt_msg.db 路径（+ CLIENT_NT_MSG_KEY），"
            "客户端自己解密并导出；\n"
            "  · CLIENT_DB_PATH  = 现成的 nt_msg_export.db 路径"
            "（上游 nt_msg_db_util 的 3.export.py 产物，或本客户端上次的产物）。"
        )
        return 2

    # **在读源库、连后端之前**就把配置校验掉。放在这里而不是等到循环里：
    # 白名单里写错一个号码的后果是"那个来源永远不进清单"，而它在日志里只是一个
    # "跳过"。越早炸越好，最好是在发出任何请求之前。
    try:
        settings.validate_whitelist()  # 号码形状不对会抛 ConfigError
    except ConfigError as exc:
        logger.error("配置有问题：%s", exc)
        return 2

    # `.env` 里写了但不是配置项的键**必须出声**：`extra="ignore"` 让它们静悄悄地
    # 什么都不做，于是"我配了"和"根本没生效"长得一模一样。配置项只剩十几个之后，
    # 从旧版本升上来的 `.env` 里会有一大批这种键。
    unknown = unknown_env_keys()
    if unknown:
        logger.warning(
            ".env 里有 %d 个键**不是配置项**（不影响启动，但配了等于没配）：%s%s。"
            "可以删掉这些行（配置项只有十几个，见 .env.example）",
            len(unknown),
            ", ".join(unknown[:12]),
            " …" if len(unknown) > 12 else "",
        )

    db = SourceDatabase(settings.ntmsg_export_path)
    backend = BackendClient(settings)
    try:
        if args.status:
            return await show_status(backend, settings, db)
        await verify_identity(backend, settings)
        if args.loop or not args.once:
            await run_loop(backend, settings)
            return 0
        return await run_once(backend, settings)
    except ConfigError as exc:
        # 配置错误**不进重试、也不降级**：它只会让某些来源安静地不入库
        logger.error("配置有问题：%s", exc)
        return 2
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
    setup_logging(get_settings().client_log_level)
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
