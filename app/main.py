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
from .config import ConfigError, get_settings, stale_env_keys
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
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="只做「解密 nt_msg.db + 导出」，强制重跑一遍然后退出",
    )
    parser.add_argument("--dry-run", action="store_true", help="只组装不写入")
    parser.add_argument(
        "--since-hours",
        type=int,
        default=None,
        help="把「内容改动的回看窗口」放大到 N 小时（不影响「哪些没读过」）",
    )
    parser.add_argument("--limit", type=int, default=None, help="本轮最多处理多少条")
    parser.add_argument("--log-level", default=None, help="覆盖 CLIENT_LOG_LEVEL")
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
    print(f"  轮询间隔        {settings.client_poll_seconds}s")
    print(f"  回看窗口        {settings.client_recheck_overlap_hours}h（已读消息重看、识别内容改动的范围）")
    print(
        f"  补充关系        {'开' if settings.client_amendment_enabled else '关'}"
        f"（只认 {settings.client_amendment_max_age_hours}h 之内的引用）"
    )

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
            f"  解密参数        header={settings.client_nt_msg_header_size} "
            f"page_size={settings.client_nt_msg_page_size} "
            f"kdf_iter={settings.client_nt_msg_kdf_iter} "
            f"kdf={settings.client_nt_msg_kdf_algorithm} "
            f"hmac={settings.client_nt_msg_hmac_algorithm}"
        )
        print(
            f"  中间产物        clear={settings.client_nt_msg_clear_path or '(默认：nt_msg.db 旁边)'} "
            f"plain={settings.client_nt_msg_plain_path or '(默认：nt_msg.db 旁边)'}"
        )
        tables = settings.client_decrypt_tables.strip() or "（全部，和上游一样）"
        print(f"  只解密这些表    {tables}")
        skips = settings.client_decrypt_max_skips
        print(
            f"  SQLite 自检     {settings.client_decrypt_integrity}"
            f"（坏页最多跳过 {'不限' if skips < 0 else skips} 行，每次跳过都会报 ERROR）"
        )
        print(
            f"  导出            c2c={'是' if settings.client_export_include_c2c else '否'}"
            f"，补序号列={'是' if settings.client_export_add_seq else '否'}"
        )

    print("\n=== 源库 ===")
    try:
        info = db.inspect()
    except SourceDatabaseError as exc:
        if settings.ntmsg_pipeline_enabled and not settings.ntmsg_export_path.exists():
            print(
                f"  还没生成：{settings.ntmsg_export_path}\n"
                "  先跑一次 `python -m app.main --prepare`（只解密+导出，不入库），"
                "再回来看这一节。"
            )
            return 1
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
    # 读什么由**这个**决定，不由时间决定：源库里没被标记过的都要读，不管多老。
    try:
        unread = db.count_unread(settings.resolved_mirror_path)
        print(
            f"  没读过的消息    {unread} 条"
            + (
                "（下一轮接着读；一轮最多读 CLIENT_MAX_MESSAGES_PER_CYCLE 条）"
                if unread
                else "（都读过了，下一轮只读新增的）"
            )
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


async def run_prepare(settings) -> int:
    """`--prepare`：强制重跑一遍「解密 + 导出」，不做任何入库动作。

    单独留这个入口，是因为第一次配置密钥/参数时最容易出错，而"跑一轮同步"会把
    解密、导出、模型调用混在一起，看不出问题出在哪一步。`--prepare` 只碰本地文件。
    """
    from .ntmsg_db import prepare_databases
    from .ntmsg_db.decrypt import DecryptError
    from .ntmsg_db.export import ExportError

    if not settings.ntmsg_pipeline_enabled:
        logger.error(
            "没有配置 CLIENT_NT_MSG_DB，没有可准备的东西。"
            "（--prepare 是给「只给一个 nt_msg.db 路径」这种用法准备的）"
        )
        return 2
    try:
        report = prepare_databases(settings, force=True)
    except (DecryptError, ExportError) as exc:
        logger.error("准备失败：%s", exc)
        return 2
    print("=== 剥头 + 解密 + 导出 ===")
    print(f"  nt_msg.db       {settings.client_nt_msg_db}")
    print(f"  剥头产物        {report.clear_path}")
    print(f"  明文库          {report.plain_path}")
    print(f"  导出库          {report.export_path}")
    if report.decrypt is not None:
        print(f"  解密            {report.decrypt.summary()}")
        print(f"  SQLite 自检     {report.decrypt.integrity}")
        for item in report.decrypt.per_table:
            flag = "  ⚠️ 跳过 %d 行" % item.skipped_rows if item.skipped_rows else ""
            print(
                f"      {item.table:<24} {item.copied_rows:>9,}/{item.source_rows:<9,} 行{flag}"
            )
        if report.decrypt.skipped_rowids:
            print(f"  ⚠️ 被跳过的 rowid {report.decrypt.skipped_rowids}")
    if report.export is not None:
        print(f"  导出            {report.export.summary()}")
        for table, written in report.export.written_rows.items():
            print(f"      {table:<24} {written:>9,} 行")
    print("\n下一步：python -m app.main --status 看看源库和订阅。")
    return 0


async def run_once(backend: BackendClient, settings, *, since_hours: int | None = None) -> int:
    from .ntmsg_db import prepare_databases
    from .ntmsg_db.decrypt import DecryptError
    from .ntmsg_db.export import ExportError

    prepared = None
    if settings.ntmsg_pipeline_enabled:
        try:
            prepared = prepare_databases(settings)
        except (DecryptError, ExportError) as exc:
            logger.error("准备源库失败：%s", exc)
            return 2
    db = SourceDatabase(prepared.export_path if prepared else settings.resolved_db_path)
    try:
        db.inspect()
    except SourceDatabaseError as exc:
        logger.error("%s", exc)
        return 2

    if since_hours is not None:
        # `--since-hours N` = 把「内容改动的回看窗口」放大到 N 小时。
        #
        # ⚠️ 它**不**决定"哪些没读过"：没读过的消息无论如何都会被读到（判据是镜像里的
        # 已读标记）。它的真正用途是把**已经读过、但内容可能被改过**的消息的重看范围
        # 放大 —— 比如改了抽取规则想重跑、或者怀疑某批老消息被编辑过。
        # 实现方式是临时放大回看窗口，而不是去动镜像 ——
        # 镜像记的是事实（每一条处理过没有），不该被一次调用改写。
        logger.info(
            "按 --since-hours=%s 把「内容改动的回看窗口」临时放大到 %s 小时"
            "（已读消息里，这个范围内的会重新比对内容指纹）",
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
        settings.whitelist_fingerprint  # 解析即校验（号码形状不对会抛）
    except ConfigError as exc:
        logger.error("配置有问题：%s", exc)
        return 2

    # `.env` 里留着已经失效的键时**必须出声**：`extra="ignore"` 让它们静悄悄地
    # 什么都不做，于是"我配了"和"根本没生效"长得一模一样。
    for key, instead in stale_env_keys():
        logger.warning(
            ".env 里的 %s 已经失效（不影响启动，但配了等于没配）：%s。可以直接删掉这一行。",
            key,
            instead,
        )

    db = SourceDatabase(settings.ntmsg_export_path)
    backend = BackendClient(settings)
    try:
        if args.status:
            return await show_status(backend, settings, db)
        if args.prepare:
            return await run_prepare(settings)
        await verify_identity(backend, settings)
        if args.loop or not args.once:
            await run_loop(backend, settings)
            return 0
        return await run_once(backend, settings, since_hours=args.since_hours)
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
    setup_logging(args.log_level)
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
