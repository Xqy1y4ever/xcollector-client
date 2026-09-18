"""一次同步循环：读源库 → 查镜像 → 处理 → 记镜像。

## 状态在哪

**在镜像库里**（`app/mirror.py`），不在后端，也不再有一个"水位线游标"。
每条消息各有自己的状态，于是"增量更新"就是一次镜像查询：

    不在镜像里              → 新消息，处理
    在、已处理（done/skipped）→ 跳过（零成本）
    在、pending / failed    → 重试（这就是崩溃恢复队列）

判据是「读没读过」，**没有时间窗口**：窗口表达不了"这条处理过没有"，比窗口更老、
而镜像里又没有的消息（换过导出库、镜像被删过、白名单刚放开）会永远读不到 ——
而那正是这套系统最怕的静默漏消息。

## 一轮做两件事，顺序有讲究

1. **先把没做完的做完**（`mirror.unfinished()`）。它们可能落在很老的位置，
   靠"扫没读过的"永远扫不到 —— 不先捞它们，失败的消息就永远卡在那儿。
   顺便：`pending` 只可能来自"上一次没跑完就退出了"，所以这一步同时就是崩溃恢复。
2. **再扫没读过的**（按时间正序）。从最老的开始，一条条处理到本轮预算用完为止；
   剩下的下一轮接着读（`--status` 会告诉你还剩多少）。

## 三层防重（从便宜到贵）

    mirror        每条消息的状态 —— 正常情况下这一层就够了
    后端通知集合   只对"镜像不认识的"消息查一次：镜像被删了/换机器了也不会
                   把整段历史重新抽一遍（那是真金白银的模型调用）
    后端幂等键     (user_id, raw_message_id) —— 最后一道，保证不会重复入库
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .backend_client import BackendClient, BackendError, BackendRejected
from .config import GAP_ALERT_HOURS, MAX_MESSAGES_PER_CYCLE, Settings
from .mirror import (
    STATE_DONE,
    STATE_FAILED,
    STATE_PENDING,
    STATE_SKIPPED,
    Mirror,
)
from .ntmsg_db import PrepareReport, prepare_databases
from .ntmsg_db.decrypt import DecryptError
from .ntmsg_db.export import ExportError
from .pipeline.process import (
    OUTCOME_DEGRADED,
    OUTCOME_ERROR,
    OUTCOME_EXTRACTED,
    OUTCOME_NOISE,
    OUTCOME_SKIPPED,
    OUTCOME_UNCHANGED,
    OUTCOME_UNPARSED,
    Outcome,
    outcome_stats,
    process_message,
)
from .source.attachments import AttachmentResolver
from .source.ntmsg import SourceDatabase, SourceDatabaseError, SourceMessage
from .utils import local_day, now_ms, preview

logger = logging.getLogger(__name__)

# 每处理这么多条落一次镜像状态：崩了最多重做这么多条，而不是整批。
# （状态本身是逐条写的，这个常量只用于"多久打一次进度日志"。）
PROGRESS_EVERY = 25

# 一条消息处理完之后，镜像里记成哪个状态
_STATE_BY_OUTCOME = {
    OUTCOME_EXTRACTED: STATE_DONE,
    OUTCOME_NOISE: STATE_SKIPPED,
    OUTCOME_SKIPPED: STATE_SKIPPED,
    OUTCOME_UNPARSED: STATE_DONE,   # 抽了、没证据 → 终态：重抽还是没证据
    OUTCOME_DEGRADED: STATE_DONE,   # 记成 degraded 也是终态（统计里有盲区记录）
    OUTCOME_ERROR: STATE_FAILED,    # 失败 → 下轮重试
}


@dataclass
class CycleReport:
    scanned: int = 0
    processed: int = 0
    skipped_whitelist: int = 0
    reopened: int = 0
    unchanged: int = 0
    recovered: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    # 这一轮的"解密 + 导出"做了什么（没配 CLIENT_NT_MSG_DB 时是 None）
    prepared: str | None = None
    # 轮开始时源库里还有多少条没读过（判据是镜像，不是时间）
    unread_before: int = 0
    mirror_before: dict = field(default_factory=dict)
    mirror_after: dict = field(default_factory=dict)
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


async def load_processed_raw_ids(backend: BackendClient) -> set[str]:
    """我已经有通知的那些 raw id —— **只在镜像不认识某条消息时**用来兜底。

    为什么还留着这一层：镜像被删了/换了机器，如果没有它，那批历史会被整段重新
    抽取（真金白银的模型调用）。有它，那些消息会被直接认成"处理过了"，
    一次模型调用都不花。
    """
    try:
        rows = await backend.list_notifications(limit=2000)
    except BackendError as exc:
        logger.warning("拉取已有通知失败（这次不去重，靠后端幂等兜底）：%s", exc)
        return set()
    return {str(r.get("raw_message_id") or "") for r in rows if r.get("raw_message_id")}


# ---------------------------------------------------------------------------
# 缺口检测
# ---------------------------------------------------------------------------


def _gap_alert_reason(gap_ms: int) -> str:
    hours = round(gap_ms / 3600000, 1)
    return f"两条消息间隔 {hours} 小时，此期间的通知可能已永久丢失"


async def _maybe_gap(
    backend: BackendClient,
    settings: Settings,
    message: SourceMessage,
    previous_ts_ms,
    report: CycleReport,
) -> None:
    """缺口检测：这个群是不是静默太久了（此期间的通知可能已经永久丢失）。

    `previous_ts_ms` 来自**客户端自己的镜像**（`Mirror.group_seen_ts`），
    不是后端的 `group_state` —— 那个表是共享的，两个客户端会互相顶掉对方的时间线。
    和 bot 的判据（间隔多久算缺口）保持一致，只是时间线换成自己看到的。
    """
    try:
        previous = int(previous_ts_ms) if previous_ts_ms else None
    except (TypeError, ValueError):
        previous = None
    if not previous:
        return
    gap_ms = message.ts_ms - previous
    if gap_ms <= GAP_ALERT_HOURS * 3600 * 1000:
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


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------


async def _handle_one(
    message: SourceMessage,
    backend: BackendClient,
    settings: Settings,
    resolver: AttachmentResolver,
    mirror: Mirror,
    known_raw_ids: set[str],
    report: CycleReport,
) -> Outcome:
    """处理一条消息（白名单 → 抽取 → 建通知），并把它记进镜像。

    ## 这里**不看订阅**

    订阅是 **bot** 的东西（它的实时入库看的是订阅）；客户端读的是**自己账号**的
    聊天记录库 —— "这个来源我订阅过吗"在这个语境下既表达不了它的输入，也表达不了
    它的权限。客户端的收窄只有一处：`CLIENT_GROUP_WHITELIST` /
    `CLIENT_SENDER_WHITELIST`（留空 = 全都读）。

    权限在后端那边是**按表**分的：客户端写的原文进 `user_raw_message`（按用户），
    bot 写的进共享的 `raw_message`。所以这里不需要、也不该自己判订阅。
    """
    # ---- 白名单（本地收窄；留空 = 不限制）----
    if not settings.allows(message.group_id, message.sender_id):
        reason = settings.whitelist_reason(message.group_id, message.sender_id)
        # 记成 skipped 并**带上 whitelist: 前缀**：前缀是标记，白名单一变就会被
        # reopen_whitelist_skips() 放回来重看（否则改白名单等于没改）。
        mirror.finish(message.msg_id, state=STATE_SKIPPED, error=reason)
        report.skipped_whitelist += 1
        return Outcome(result=OUTCOME_SKIPPED, reason=reason)

    outcome = await process_message(
        message, backend, settings, resolver, known_raw_ids=known_raw_ids
    )
    # 后端已经有这条通知（镜像被删过/换机器时会走到这里）→ 补记成 done，
    # 于是下一轮连这次查询都省了
    if outcome.result == OUTCOME_SKIPPED and outcome.reason and "已经建过通知" in outcome.reason:
        report.unchanged += 1
        mirror.finish(message.msg_id, state=STATE_DONE, raw_id=outcome.raw_id)
        return outcome
    _finish_from_outcome(mirror, message.msg_id, outcome)
    return outcome


def _finish_from_outcome(mirror: Mirror, msg_id: str, outcome: Outcome) -> None:
    state = _STATE_BY_OUTCOME.get(outcome.result, STATE_FAILED)
    mirror.finish(
        msg_id,
        state=state,
        raw_id=outcome.raw_id or None,
        error=outcome.reason if state == STATE_FAILED else None,
    )


async def run_cycle(
    backend: BackendClient,
    settings: Settings,
    *,
    db: SourceDatabase | None = None,
    resolver: AttachmentResolver | None = None,
    mirror: Mirror | None = None,
    now: int | None = None,
) -> CycleReport:
    """跑一次同步。**任何一条消息失败都不会中断整批**（错误进 report）。

    这里**不查订阅**：订阅是 bot 的过滤条件，客户端读什么由自己的源库 + 本地白名单
    决定（权限那一侧由后端按表保证，见 `app/run.py: _handle_one` 的说明）。
    """
    report = CycleReport()
    moment = now_ms() if now is None else now
    attachment_resolver = resolver or AttachmentResolver(settings.resolved_attachment_root)
    store = mirror or Mirror(settings.resolved_mirror_path)

    # ---- 0) 源库要先准备好（解密 + 导出）----
    #
    # 放在打开源库之前 —— 这一步失败必须让整轮停下来：带着一个没更新成功的旧库
    # 继续跑，界面看起来一切正常，而新通知一条都没进来。
    if db is None and settings.ntmsg_pipeline_enabled:
        try:
            prepared = prepare_databases(settings)
        except (DecryptError, ExportError) as exc:
            logger.error("准备源库失败，本轮不做任何事：%s", exc)
            report.errors.append(f"准备源库失败：{exc}")
            return report
        report.prepared = prepared.summary()
        logger.info("源库准备：%s", report.prepared)
        database = SourceDatabase(prepared.export_path)
    else:
        database = db or SourceDatabase(settings.resolved_db_path)

    known_raw_ids = await load_processed_raw_ids(backend)
    report.mirror_before = store.stats()

    # ---- 0) 白名单改过了？----
    # 指纹与上次不同时，把当时"因为白名单被跳过"的消息放回待处理。
    # 不做这一步的话，用户往白名单里加一个群会发现"什么都没发生" ——
    # 那些消息早就被记成终态 skipped 了，而他在界面上看不到任何解释。
    fingerprint = settings.whitelist_fingerprint  # 顺带校验号码形状（错了就地抛）
    previous = store.get_meta("whitelist_fingerprint")
    if previous is not None and previous != fingerprint:
        reopened = store.reopen_whitelist_skips()
        report.reopened = reopened
        logger.warning(
            "白名单变了，把之前因它跳过的 %d 条消息放回待处理（重新过一遍）", reopened
        )
    store.set_meta("whitelist_fingerprint", fingerprint)

    budget = MAX_MESSAGES_PER_CYCLE
    stats: dict[str, int] = {}

    async def handle_one(message: SourceMessage, *, recovered: bool) -> None:
        """处理一条消息，并在这一轮的报告/统计里记一笔。"""
        nonlocal stats
        report.scanned += 1
        if recovered:
            report.recovered += 1
        store.claim(message)      # 先记账再干活（崩了最多重做一次）
        try:
            outcome = await _handle_one(
                message, backend, settings, attachment_resolver, store,
                known_raw_ids, report,
            )
        except Exception as exc:  # 单条炸了不能拖垮整批
            logger.exception("处理消息失败 msg_id=%s", message.msg_id)
            store.finish(message.msg_id, state=STATE_FAILED, error=f"{type(exc).__name__}: {exc}")
            report.errors.append(f"msg_id={message.msg_id}: {type(exc).__name__}: {exc}")
            _add(stats, OUTCOME_ERROR)
            return

        report.processed += 1
        _accumulate(report, outcome)
        stats = _merge(stats, outcome_stats(
            outcome=outcome.result,
            degraded=outcome.result == OUTCOME_DEGRADED,
            conflict=False,
            tokens=outcome.tokens,
        ))

        # 缺口检测：靠**自己镜像**里这个群的上一条消息时间（不是后端的 group_state）。
        # 与 bot 的判据一致，只是时间线换成自己看到的那条 —— 共享的 group_state 会被
        # 别人的时间线顶掉，缺口就会静默地漏。
        if outcome.result != OUTCOME_SKIPPED:
            previous_ts = store.group_seen_ts(
                message.group_id, exclude_msg_id=str(message.msg_id)
            )
            if previous_ts:
                # 镜像里存的是**秒**（源库的单位），缺口判据是毫秒
                await _maybe_gap(backend, settings, message, int(previous_ts) * 1000, report)

    # ---- 1) 先把没做完的做完（崩溃恢复 + 失败重试）----
    # 它们可能落在很老的位置，靠"扫没读过的"永远扫不到。
    retry_ids = [row.msg_id for row in store.unfinished()][:MAX_MESSAGES_PER_CYCLE]
    if retry_ids:
        logger.info("有 %d 条没处理完的消息，先重试它们", len(retry_ids))
        fetched = database.fetch_by_ids(retry_ids)
        for msg_id in retry_ids:
            message = fetched.get(msg_id)
            if message is None:
                # 源库里已经没有这条了（换了导出库？）→ 记成跳过，不要永远卡着
                store.finish(msg_id, state=STATE_SKIPPED, error="源库里已经查不到这条消息")
                logger.warning("镜像里的 %s 在源库里已经不存在，标记为跳过", msg_id)
                continue
            await handle_one(message, recovered=True)
            budget -= 1
            if budget <= 0:
                break

    # ---- 2) 扫"没读过"的消息（按时间正序，从最老的开始）----
    #
    # 判据是**镜像**（= 已读标记），不是时间：源库里凡是镜像里没有的都要读，
    # 不管它多老。时间窗口表达不了"这条处理过没有"，会让"比任何窗口都老、而镜像里
    # 又没有"的消息（换过导出库、镜像被删过、白名单刚放开、导出曾经漏了一批）永远
    # 读不到 —— 而那正是这套系统最怕的"静默漏掉通知"。
    unread_before = database.count_unread(store.path)
    report.unread_before = unread_before
    if unread_before:
        logger.info(
            "源库里还有 %s 条没读过的消息（本轮最多处理 %d 条）—— 判据是镜像里的"
            "已读标记，不看时间；处理完就记下，下一轮不再重复读。",
            f"{unread_before:,}",
            max(0, budget),
        )

    latest = database.latest()
    report.latest_in_db = latest
    mirror_watermark = store.watermark()
    if latest is not None and mirror_watermark and latest[0] < mirror_watermark:
        logger.warning(
            "源库最新消息(%s)比镜像里已处理过的最新消息(%s)还旧：源库可能被换成了"
            "更旧的快照（或导出倒退了）。如果确实换了库，把镜像文件删掉、或换一个"
            "CLIENT_MIRROR_PATH 重来 —— 否则「哪些处理过」的判断全是错的。",
            latest[0],
            mirror_watermark,
        )

    for message in database.iter_unread(store.path, limit=max(0, budget)):
        await handle_one(message, recovered=False)
        budget -= 1
        if budget <= 0:
            break

    if stats:
        try:
            await backend.add_stats(local_day(moment, tz=settings.tz), stats)
        except BackendError as exc:
            # 统计写失败不该让这一批算失败（通知已经建好了）
            logger.warning("写统计失败：%s", exc)
            report.errors.append(f"写统计失败：{exc}")

    report.mirror_after = store.stats()
    return report


def _accumulate(report: CycleReport, outcome: Outcome) -> None:
    _add(report.outcomes, outcome.result)
    if outcome.result == OUTCOME_ERROR and outcome.reason:
        report.errors.append(outcome.reason)


def _add(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


def _merge(a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    out = dict(a)
    for key, value in b.items():
        out[key] = out.get(key, 0) + int(value)
    return out


def log_report(report: CycleReport) -> None:
    """把一次循环的结果打成一行（+ 出错时的明细）。"""
    logger.info(
        "本轮：扫了 %d 条（其中重试 %d 条），跳过 %d 条已经处理过的、%d 条白名单外的，"
        "结果=%s",
        report.scanned,
        report.recovered,
        report.unchanged,
        report.skipped_whitelist,
        report.outcomes or {},
    )
    if report.unread_before and report.unread_before > report.scanned:
        logger.info(
            "源库里还剩 %s 条没读过（本轮处理了 %d 条，下一轮接着读）—— "
            "第一轮会把库里所有群消息过一遍，之后每轮只读新增的。",
            f"{report.unread_before - report.scanned:,}",
            report.scanned,
        )
    if report.prepared:
        logger.info("源库准备：%s", report.prepared)
    if report.mirror_after:
        logger.info(
            "镜像：共 %d 条（%s）",
            report.mirror_after.get("total", 0),
            report.mirror_after.get("by_state", {}),
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
