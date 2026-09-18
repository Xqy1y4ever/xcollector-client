"""一次同步循环：读源库 → 过滤 → 处理 → 推进游标。

## 游标丢了怎么办（这是这一层最需要想清楚的事）

游标存在后端的 `bot_state` 里（按用户隔离，`client_cursor` 命名空间），所以
换机器、重装都不丢。但"游标丢了"仍然必须是个**安全**情况，而不是"把三个月
的历史重新抽一遍、账单翻十倍"：

  - 游标丢了 → 退回 `initial_lookback_hours` 重扫；
  - 重扫时每一条都会先 `POST /api/messages`（幂等），**拿回它已经存在的 raw id**；
  - 循环开始时已经把"我已经有通知的 raw id"整集合拉下来了（`GET /api/notifications`，
    这是用户令牌**允许**读的），所以命中集合的消息**跳过抽取** ——
    一个模型调用都不会多花；
  - 即使集合没命中，`POST /api/notifications` 也按 `(user_id, raw_message_id)`
    幂等，不会产生第二条。

所以这里有三层防线：游标（省事）→ 已有通知集合（省钱）→ 后端幂等（保正确）。
少了中间那层，前一层一丢就是真金白银。

## 为什么每一条都要重写一次 raw

`POST /api/messages` 是幂等的：已存在时返回同一个 id 且 `is_new=false`。
所以"重写"实际上是一次很便宜的查询 —— 而它是**唯一**能在用户令牌下拿到
raw id 的办法（共享层的读是服务令牌专属的）。这个"用幂等写代替读"的技巧是
客户端能只用 UserToken 工作的关键。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .backend_client import BackendClient, BackendError, BackendRejected
from .config import Settings
from .pipeline.process import (
    OUTCOME_ERROR,
    OUTCOME_EXTRACTED,
    OUTCOME_NOT_SUBSCRIBED,
    OUTCOME_SKIPPED,
    Outcome,
    outcome_stats,
    process_message,
)
from .source.attachments import AttachmentResolver
from .source.ntmsg import SourceDatabase, SourceDatabaseError
from .utils import local_day, now_ms, preview

logger = logging.getLogger(__name__)

# 每处理这么多条就把游标落一次盘：崩了最多重做这么多条，而不是整批。
CURSOR_FLUSH_EVERY = 25


@dataclass
class CycleReport:
    scanned: int = 0
    processed: int = 0
    skipped_unsubscribed: int = 0
    already_done: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    cursor_before: dict = field(default_factory=dict)
    cursor_after: dict = field(default_factory=dict)
    latest_in_db: tuple[int, str] | None = None


async def verify_identity(backend: BackendClient, settings: Settings) -> dict:
    """启动自检：令牌对不对、是不是**用户令牌**。

    服务令牌在这里也能通过（后端也返回 200），但那意味着这个客户端能读写
    所有人的数据 —— 与"每人一个客户端"的设计相悖，所以要明确警告，而不是
    让它悄悄跑起来。
    """
    who = await backend.whoami()
    if not isinstance(who, dict) or not who:
        raise RuntimeError("GET /api/me 没有返回内容：后端地址或令牌不对")
    scope = str(who.get("scope") or "")
    user = who.get("user") or {}
    if scope != "user":
        logger.warning(
            "⚠️ 这个令牌的 scope 是 %r，不是 user。用服务令牌跑客户端意味着它能读写"
            "**所有人**的数据，请改用某个用户的 UserToken（xc_ 开头）。",
            scope,
        )
    else:
        logger.info(
            "身份确认：user_id=%s qq=%s（%s）",
            user.get("id"),
            user.get("qq"),
            user.get("display_name") or "无显示名",
        )
    return who


async def load_subscriptions(backend: BackendClient) -> set[tuple[str, str]]:
    """这个用户的订阅 → `{(group_id, sender_id)}`。

    **这就是客户端的过滤条件**，也是和后端那份配置唯一的事实来源：
    网页上（或 `/订阅` 指令里）订了什么，这里就读到什么。
    """
    subs = await backend.list_subscriptions(include_disabled=False)
    pairs = {(str(s.get("group_id") or ""), str(s.get("sender_id") or "")) for s in subs}
    pairs.discard(("", ""))
    return pairs


async def load_processed_raw_ids(backend: BackendClient) -> set[str]:
    """我已经有通知的那些 raw id —— 游标丢失后靠它跳过重复抽取。"""
    try:
        rows = await backend.list_notifications(limit=2000)
    except BackendError as exc:
        logger.warning("拉取已有通知失败（这次不去重，靠后端幂等兜底）：%s", exc)
        return set()
    return {str(r.get("raw_message_id") or "") for r in rows if r.get("raw_message_id")}


def _gap_alert_reason(gap_ms: int) -> str:
    hours = round(gap_ms / 3600000, 1)
    return f"两条消息间隔 {hours} 小时，此期间的通知可能已永久丢失"


async def _maybe_gap(
    backend: BackendClient,
    settings: Settings,
    message,
    previous_last_msg_ts,
    report: CycleReport,
) -> None:
    """群级缺口检测（和 bot 同一套判据）。"""
    try:
        previous = int(previous_last_msg_ts) if previous_last_msg_ts else None
    except (TypeError, ValueError):
        previous = None
    if not previous:
        return
    gap_ms = message.ts_ms - previous
    if gap_ms <= settings.client_gap_alert_hours * 3600 * 1000:
        return
    try:
        await backend.add_gap_alert(
            group_id=message.group_id,
            group_name=None,
            from_ts=previous,
            to_ts=message.ts_ms,
            reason=_gap_alert_reason(gap_ms),
        )
    except BackendError as exc:
        # 缺口告警只是辅助信息，挂了不该让整条消息丢掉
        logger.warning("写缺口告警失败 group=%s：%s", message.group_id, exc)
        report.errors.append(f"缺口告警失败：{exc}")
        return
    logger.warning(
        "群 %s 两条消息间隔 %s 小时，已生成缺口告警", message.group_id, round(gap_ms / 3600000, 1)
    )


async def run_cycle(
    backend: BackendClient,
    settings: Settings,
    *,
    db: SourceDatabase | None = None,
    resolver: AttachmentResolver | None = None,
    now: int | None = None,
) -> CycleReport:
    """跑一次同步。**任何一条消息失败都不会中断整批**（错误进 report）。"""
    report = CycleReport()
    moment = now_ms() if now is None else now
    database = db or SourceDatabase(settings.resolved_db_path)
    attachment_resolver = resolver or AttachmentResolver(settings.resolved_attachment_root)

    subscriptions = await load_subscriptions(backend)
    if not subscriptions:
        # 没有订阅 = 后端会拒绝写共享层（403）。这是配置问题，必须说清楚，
        # 而不是让用户看到一堆没有解释的失败。
        #
        # ⚠️ 而且**一条都不扫、游标也不动**。这是刻意的：先启动客户端、再去网页上
        # 订阅，是很自然的操作顺序；如果这里照常扫完并把游标推到底，那 72 小时的
        # 回看窗口就被这一次"什么都不做"的运行白白烧掉了 —— 用户订完之后会发现
        # 存量一条都没补上，而且没有任何线索指向原因。留着游标，等订阅配好之后
        # 下一轮自然就把存量补上了。
        logger.warning(
            "这个用户还没有订阅任何来源，所以这一轮什么都不做（游标也没动）。"
            "请先在网页上（或给机器人发 /订阅）订一个 (群, 发送者)，"
            "下一轮就会从 CLIENT_INITIAL_LOOKBACK_HOURS 之前开始补。"
        )
        report.cursor_before = {}
        report.cursor_after = {}
        report.errors.append("还没有订阅任何来源，本轮未做任何事（游标未推进）")
        return report

    processed = await load_processed_raw_ids(backend)
    cursor = await backend.get_cursor(settings.cursor_name) or {}
    report.cursor_before = dict(cursor)

    since_ts = int(cursor.get("ts") or 0)
    since_mid = str(cursor.get("msg_id") or "")
    if not since_ts:
        since_ts = (moment - settings.client_initial_lookback_hours * 3600 * 1000) // 1000
        logger.info(
            "没有游标，从 %d 小时前开始扫（source 库的时间戳是**秒**）",
            settings.client_initial_lookback_hours,
        )

    latest = database.latest()
    report.latest_in_db = latest
    if latest is not None and since_ts > latest[0]:
        # 源库被换成了一个更旧的快照（或者游标来自另一个库）。
        # 什么都不做是最糟的结果 —— 用户会以为"已经同步好了"。
        logger.warning(
            "游标(%s)比源库最新消息(%s)还新：源库可能被换成了更旧的快照。"
            "这次会按空批次结束；如果确实换了库，请清掉游标或换一个 CLIENT_CURSOR_KEY。",
            since_ts,
            latest[0],
        )

    stats: dict[str, int] = {}
    since_flush = 0
    batch_limit = min(settings.client_batch_size, settings.client_max_messages_per_cycle)

    for message in database.iter_since(since_ts, since_mid, limit=batch_limit):
        report.scanned += 1
        since_ts, since_mid = message.timestamp, message.msg_id

        pair = (message.group_id, message.sender_id)
        if subscriptions and pair not in subscriptions:
            report.skipped_unsubscribed += 1
            _count(report, OUTCOME_SKIPPED)
            continue

        try:
            outcome = await process_message(
                message,
                backend,
                settings,
                attachment_resolver,
                dry_run=settings.client_dry_run,
                known_raw_ids=processed,
            )
        except Exception as exc:  # 单条炸了不能拖垮整批
            logger.exception("处理消息失败 msg_id=%s", message.msg_id)
            report.errors.append(f"msg_id={message.msg_id}: {type(exc).__name__}: {exc}")
            _count(report, OUTCOME_ERROR)
            continue

        _count(report, outcome.result)
        report.processed += 1
        if outcome.result == OUTCOME_SKIPPED and outcome.reason and "已经" in outcome.reason:
            report.already_done += 1
        if outcome.result == OUTCOME_NOT_SUBSCRIBED:
            # 说清一次就够了，不要每条消息刷一行
            if not report.errors:
                report.errors.append(outcome.reason or "后端拒绝了共享层写入")
        if outcome.result == OUTCOME_ERROR and outcome.reason:
            report.errors.append(outcome.reason)

        # 群状态 + 缺口检测（和 bot 同一套判据：靠"上一条消息的时间"）
        if outcome.result in (OUTCOME_EXTRACTED, OUTCOME_SKIPPED) and not settings.client_dry_run:
            try:
                group_body = await backend.upsert_group(
                    message.group_id, None, message.ts_ms
                )
                await _maybe_gap(
                    backend,
                    settings,
                    message,
                    group_body.get("previous_last_msg_ts"),
                    report,
                )
            except BackendRejected as exc:
                if exc.status_code == 403:
                    logger.warning(
                        "写群状态被拒（这个群还没有订阅记录）：group=%s", message.group_id
                    )
                else:
                    logger.warning("写群状态失败 group=%s：%s", message.group_id, exc)
            except BackendError as exc:
                logger.warning("写群状态失败 group=%s：%s", message.group_id, exc)

        stats = _merge(stats, outcome_stats(
            outcome=outcome.result,
            degraded=outcome.result == "degraded",
            conflict=False,
            tokens=outcome.tokens,
        ))

        since_flush += 1
        if since_flush >= CURSOR_FLUSH_EVERY and not settings.client_dry_run:
            await _save_cursor(backend, settings, since_ts, since_mid)
            since_flush = 0

    report.outcomes = dict(report.outcomes)

    if not settings.client_dry_run:
        await _save_cursor(backend, settings, since_ts, since_mid)
        if stats:
            try:
                await backend.add_stats(local_day(moment, tz=settings.tz), stats)
            except BackendError as exc:
                # 统计写失败不该让这一批算失败（通知已经建好了）
                logger.warning("写统计失败：%s", exc)
                report.errors.append(f"写统计失败：{exc}")

    report.cursor_after = {"ts": since_ts, "msg_id": since_mid, "updated_at": now_ms()}
    return report


async def _save_cursor(
    backend: BackendClient, settings: Settings, ts: int, msg_id: str
) -> None:
    try:
        await backend.put_cursor(
            settings.cursor_name,
            {"ts": int(ts), "msg_id": str(msg_id), "updated_at": now_ms()},
        )
    except BackendError as exc:
        # 游标没存上：下次会重扫这一段，重复由后端幂等挡住，所以只是浪费
        logger.warning("保存游标失败（下次会重扫这一段，不会重复入库）：%s", exc)


def _count(report: CycleReport, result: str) -> None:
    report.outcomes[result] = report.outcomes.get(result, 0) + 1


def _merge(a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    out = dict(a)
    for key, value in b.items():
        out[key] = out.get(key, 0) + int(value)
    return out


def log_report(report: CycleReport) -> None:
    """把一次循环的结果打成一行（+ 出错时的明细）。"""
    logger.info(
        "本轮：扫了 %d 条，处理 %d 条（其中跳过重复 %d、订阅外 %d），结果=%s",
        report.scanned,
        report.processed,
        report.already_done,
        report.skipped_unsubscribed,
        report.outcomes or {},
    )
    for message in report.errors[:5]:
        logger.warning("  · %s", preview(message, 200))
    if len(report.errors) > 5:
        logger.warning("  · …另有 %d 条错误，见上面的日志", len(report.errors) - 5)


async def run_loop(backend: BackendClient, settings: Settings) -> None:
    """定期跑，直到被取消。"""
    logger.info("每 %d 秒同步一次（Ctrl+C 退出）", settings.client_poll_seconds)
    while True:
        try:
            report = await run_cycle(backend, settings)
            log_report(report)
        except SourceDatabaseError as exc:
            # 源库读不了是**可恢复**的（导出工具可能正在写），不该让进程退出
            logger.error("读源库失败，%d 秒后重试：%s", settings.client_poll_seconds, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("本轮同步异常：%s", exc)
        await asyncio.sleep(max(5, int(settings.client_poll_seconds)))
