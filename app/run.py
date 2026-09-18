"""一次同步循环：读源库 → 查镜像 → 处理 → 记镜像。

## 状态在哪

**在镜像库里**（`app/mirror.py`），不在后端，也不再有一个"水位线游标"。
每条消息各有自己的状态，于是"增量更新"就是一次镜像查询：

    不在镜像里                  → 新消息，处理
    在、内容没变、已处理        → 跳过（零成本）
    在、**内容变了**            → 重新处理（后端幂等 → 更新原来那条任务）
    在、pending / failed        → 重试（这就是崩溃恢复队列）

## 一轮做三件事，顺序有讲究

1. **先把没做完的做完**（`mirror.unfinished()`）。它们可能落在水位线之前，
   靠增量扫描永远扫不到 —— 不先捞它们，失败的消息就永远卡在那儿。
   顺便：`pending` 只可能来自"上一次没跑完就退出了"，所以这一步同时就是崩溃恢复。
2. **再扫增量**：从 `水位线 - 回看窗口` 开始。回看窗口是为了两件事：
   同一秒里后到的消息、以及**已经被编辑过**的旧消息（内容指纹变了要重抽）。
3. **补充关系**（新消息引用了某条已读消息）在扫到那条新消息时处理 ——
   它更新的是**被引用的那条任务**，不是新建一条。

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
from .config import Settings
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
    OUTCOME_AMENDED,
    OUTCOME_DEGRADED,
    OUTCOME_ERROR,
    OUTCOME_EXTRACTED,
    OUTCOME_NOISE,
    OUTCOME_SKIPPED,
    OUTCOME_UNCHANGED,
    OUTCOME_UNPARSED,
    Outcome,
    outcome_stats,
    process_amendment,
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
    OUTCOME_AMENDED: STATE_DONE,
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
    amended: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    # 这一轮的"解密 + 导出"做了什么（没配 CLIENT_NT_MSG_DB 时是 None）
    prepared: str | None = None
    # 轮开始/结束时源库里还有多少条没读过（判据是镜像，不是时间）
    unread_before: int = 0
    # 其中有多少条属于"回看已读消息、检查内容有没有被编辑"的那一趟（2b）
    rechecked: int = 0
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

    为什么还留着这一层：镜像被删了/换了机器，如果没有它，回看窗口里的历史会被
    整段重新抽取（真金白银的模型调用）。有它，那些消息会被直接认成"处理过了"，
    一次模型调用都不花。
    """
    try:
        rows = await backend.list_notifications(limit=2000)
    except BackendError as exc:
        logger.warning("拉取已有通知失败（这次不去重，靠后端幂等兜底）：%s", exc)
        return set()
    return {str(r.get("raw_message_id") or "") for r in rows if r.get("raw_message_id")}


# ---------------------------------------------------------------------------
# 补充关系的解析
# ---------------------------------------------------------------------------


async def resolve_amendment_target(
    message: SourceMessage,
    mirror: Mirror,
    database: SourceDatabase,
    settings: Settings,
) -> SourceMessage | None:
    """这条消息是在补充哪条**已读**消息？解析不出来返回 None（**不猜**）。

    两条路，都是确定性的：

      1. 引用里那个值是**消息 id** → 直接在镜像里查；
      2. 是**群内序号** → 用源表的序号列（`40003` 之类）反查消息 id。

    第 2 条路依赖源表带"群内序号"列：**本客户端自己导出的库带**（`app/ntmsg_db/export.py`
    会额外输出 `"40003"` / `"40850"`），而上游 `nt_msg_db_util` 的 `group_messages`
    没有这一列（字段文档还说回复里的 `47422` 与主表 `40001` 不匹配）。
    所以读上游的库时"补充关系认不出来"是数据源的限制，不是逻辑没写：这里会把原因
    打出来，让人知道该去补哪一块，而不是静默地把补充当成一条独立的新任务。
    """
    ref = (message.quote_ref or "").strip()
    if not ref:
        return None

    target_id: str | None = None
    how = ""
    ambiguous = 0

    row = mirror.get(ref)
    if row is not None:
        target_id, how = ref, "引用里直接是消息 id"
    else:
        resolved, matches = database.resolve_seq_detail(message.group_id, ref)
        if resolved:
            target_id, how = resolved, f"按群内序号 {ref} 反查到 {resolved}"
        elif matches > 1:
            ambiguous = matches

    if not target_id:
        if ambiguous:
            logger.info(
                "引用里的群内序号 %r 在同一群里对上了 %d 条消息（序号被复用过，"
                "实测 769,003 行里有 2.1 万个这样的组合）—— **不敢认**，"
                "按独立的新消息处理（msg_id=%s）。宁可少一次合并，也不能改错任务。",
                ref,
                ambiguous,
                message.msg_id,
            )
        elif not database.seq_column():
            logger.info(
                "这条消息引用了 %r，但源表没有「群内序号」列，无法确定它在补充哪一条 —— "
                "按独立的新消息处理（msg_id=%s）。"
                "用本客户端自己导出的库（CLIENT_NT_MSG_DB）会带上 40003/40850 这两列，"
                "这个关系就能确定下来。",
                ref,
                message.msg_id,
            )
        else:
            logger.info(
                "引用目标 %r 在镜像里对不上任何消息（可能它不在订阅范围内），"
                "按独立的新消息处理（msg_id=%s）",
                ref,
                message.msg_id,
            )
        return None

    target_row = mirror.get(target_id)
    if target_row is None:
        return None
    if target_row.state != STATE_DONE:
        # 被引用的那条还没成功处理过 —— 不是"对已读消息的补充"
        logger.info(
            "引用目标 %s 的状态是 %s（不是 done），按独立的新消息处理",
            target_id,
            target_row.state,
        )
        return None

    # 只接受**最近**的补充：三个月前那条通知的回复，几乎一定是另一件事。
    age_hours = (message.timestamp - target_row.source_ts) / 3600.0
    if age_hours > settings.client_amendment_max_age_hours:
        logger.info(
            "引用目标 %s 是 %.1f 小时前的，超过 %.0f 小时的上限 —— 按新消息处理",
            target_id,
            age_hours,
            settings.client_amendment_max_age_hours,
        )
        return None

    fetched = database.fetch_by_ids([target_id]).get(target_id)
    if fetched is None:
        logger.warning("镜像里有 %s，但源库里已经查不到它了（导出库换过？）", target_id)
        return None
    logger.info(
        "识别为补充（%s）：%s 补充了 %s", how, message.msg_id, target_id
    )
    return fetched


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


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------


async def _handle_one(
    message: SourceMessage,
    backend: BackendClient,
    settings: Settings,
    resolver: AttachmentResolver,
    mirror: Mirror,
    database: SourceDatabase,
    known_raw_ids: set[str],
    report: CycleReport,
    *,
    record: bool = True,
    allow_known_skip: bool = True,
) -> Outcome:
    """处理一条（已经 claim 过的）消息。

    `record=False`（dry-run）时**一个状态都不写进镜像**。这一条是硬要求：
    dry-run 不往后端写任何东西，要是把消息记成 done，下一次真跑就会把它们全部
    跳过 —— 用户"先试跑一下看看"的动作会安静地让整个客户端什么都不入库。

    `allow_known_skip=False` 时**不允许**用"后端已经有这条通知"来跳过抽取。
    `claim` 判成 `changed`（内容变了）时必须关掉它：那正是要**重新抽一遍去更新
    任务**的情况，而"后端已经有这条通知"永远成立（就是那条要更新的）——
    开着它会让补充/编辑永远不生效，而且是静默的。这个洞是 e2e 抓出来的。

    ## 这里**不再**看订阅

    以前这里有两道订阅门槛：整轮"没订阅就什么都不做"，以及逐条"不在订阅范围就
    跳成 skipped"。现在都没有了：订阅是 **bot** 的东西（它的实时入库看的是订阅），
    客户端读的是**自己账号**的聊天记录库 —— "这个来源我订阅过吗"在这个语境下
    既表达不了它的输入，也表达不了它的权限。客户端的收窄只有一处：
    `CLIENT_GROUP_WHITELIST` / `CLIENT_SENDER_WHITELIST`（留空 = 不限制）。

    权限在后端那边是**按表**分的：客户端写的原文进 `user_raw_message`
    （按用户），bot 写的进共享的 `raw_message`。所以这里不需要、也不该自己判订阅。
    """
    # ---- 白名单（本地收窄；留空 = 不限制）----
    if not settings.allows(message.group_id, message.sender_id):
        reason = settings.whitelist_reason(message.group_id, message.sender_id)
        if record:
            # 记成 skipped 并**带上 whitelist: 前缀**：前缀是标记，白名单一变就会
            # 被 reopen_whitelist_skips() 放回来重看（否则改白名单等于没改）。
            mirror.finish(message.msg_id, state=STATE_SKIPPED, error=reason)
        report.skipped_whitelist += 1
        return Outcome(result=OUTCOME_SKIPPED, reason=reason)

    # ---- 补充关系？----
    if settings.client_amendment_enabled:
        target = await resolve_amendment_target(message, mirror, database, settings)
        if target is not None:
            original_raw_id = mirror.raw_id_of(target.msg_id) or ""
            if not original_raw_id:
                # 镜像说有通知但没记 raw id（老数据/异常）→ 老老实实按新消息处理，
                # 而不是往一个猜出来的 id 上写
                logger.warning(
                    "镜像里 %s 没有 raw_id，无法定位它那条任务 —— 这条按新消息处理", target.msg_id
                )
            else:
                outcome = await process_amendment(
                    message, target, backend, settings, original_raw_id=original_raw_id
                )
                if record:
                    mirror.mark_amendment(message.msg_id, target.msg_id)
                if outcome.result == OUTCOME_AMENDED:
                    logger.info(
                        "补充生效：%s → 任务「%s」已更新（截止 %s）",
                        message.msg_id,
                        preview(outcome.title, 40),
                        outcome.due_text or outcome.due_at,
                    )
                if record:
                    _finish_from_outcome(mirror, message.msg_id, outcome)
                return outcome

    outcome = await process_message(
        message,
        backend,
        settings,
        resolver,
        dry_run=settings.client_dry_run,
        known_raw_ids=known_raw_ids if allow_known_skip else set(),
    )
    # 内容没变、后端也已经有这条通知 → 把镜像补记成 done（镜像被删过/换机器时
    # 会走到这里），于是下一轮连这次查询都省了
    if outcome.result == OUTCOME_SKIPPED and outcome.reason and "已经建过通知" in outcome.reason:
        report.unchanged += 1
        if record:
            mirror.finish(message.msg_id, state=STATE_DONE, raw_id=outcome.raw_id)
        return outcome
    if record:
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
    dry = settings.client_dry_run

    # ---- 0) 白名单改过了？----
    # 指纹与上次不同时，把当时"因为白名单被跳过"的消息放回待处理。
    # 不做这一步的话，用户往白名单里加一个群会发现"什么都没发生" ——
    # 那些消息早就被记成终态 skipped 了，而他在界面上看不到任何解释。
    fingerprint = settings.whitelist_fingerprint  # 顺带校验号码形状（错了就地抛）
    if not dry:
        previous = store.get_meta("whitelist_fingerprint")
        if previous is not None and previous != fingerprint:
            reopened = store.reopen_whitelist_skips()
            report.reopened = reopened
            logger.warning(
                "白名单变了，把之前因它跳过的 %d 条消息放回待处理（重新过一遍）", reopened
            )
        store.set_meta("whitelist_fingerprint", fingerprint)

    batch_limit = min(settings.client_batch_size, settings.client_max_messages_per_cycle)
    budget = batch_limit

    # 这一轮**已经看过**的 msg_id。回看（2b）的窗口与"没读过"是并集，刚在 2a 处理过的
    # 消息自然也在窗口里；不排掉就会在一轮里把同一份内容处理两遍（dry-run 下是白花一次
    # 抽取），`scanned` 也会虚高。
    seen_this_cycle: set[str] = set()

    # ---- 1) 先把没做完的做完（崩溃恢复 + 失败重试）----
    # dry-run 下跳过这一步：它的目的是"看看会抽出什么"，不是把积压清掉。
    pending = [] if dry else store.unfinished()
    retry_ids = [row.msg_id for row in pending][:batch_limit]
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
            report.scanned += 1
            report.recovered += 1
            seen_this_cycle.add(str(message.msg_id))
            outcome = await _handle_one(
                message, backend, settings, attachment_resolver, store, database,
                known_raw_ids, report,
            )
            _accumulate(report, outcome)
            budget -= 1
            if budget <= 0:
                break

    # ---- 2) 扫"没读过"的消息 ----
    #
    # 判据是**镜像**（= 已读标记），不是时间：源库里凡是镜像里没有的都要读，
    # 不管它多老。时间窗口表达不了"这条处理过没有"，会让"比窗口更老、而镜像里又
    # 没有"的消息（换过导出库、镜像被删过、白名单刚放开、导出曾经漏了一批）永远
    # 读不到 —— 而那正是这套系统最怕的"静默漏掉通知"。
    #
    # 时间窗口只剩一个用途：**回看最近一段已读消息**，看内容有没有被编辑过（2b）。
    unread_before = database.count_unread(store.path)
    report.unread_before = unread_before
    if unread_before:
        logger.info(
            "源库里还有 %s 条没读过的消息（本轮最多处理 %d 条）—— 判据是镜像里的"
            "已读标记，不看时间；处理完就记下，下一轮不再重复读。",
            f"{unread_before:,}",
            max(0, budget),
        )
    else:
        logger.debug("源库里没有没读过的消息")

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

    stats: dict[str, int] = {}

    async def handle_scanned(message: SourceMessage) -> None:
        """扫到一条消息之后的所有动作（dry-run 的分支也在里面）。

        抽成函数是因为它现在被两个扫描用：2a（没读过的）和 2b（回看已读的）。
        """
        nonlocal stats
        report.scanned += 1
        seen_this_cycle.add(str(message.msg_id))
        # dry-run：不 claim、不记账，只走一遍抽取看看会得到什么。
        # （claim 会往镜像里插 pending 行，虽然无害，但"试跑"不该改动任何状态。）
        if dry:
            try:
                outcome = await _handle_one(
                    message, backend, settings, attachment_resolver, store, database,
                    known_raw_ids, report, record=False,
                )
            except Exception as exc:
                logger.exception("处理消息失败 msg_id=%s", message.msg_id)
                report.errors.append(f"msg_id={message.msg_id}: {type(exc).__name__}: {exc}")
                _add(stats, OUTCOME_ERROR)
                return
            report.processed += 1
            _accumulate(report, outcome)
            return

        row, what = store.claim(message)
        if what == "unchanged":
            report.unchanged += 1
            _add(stats, OUTCOME_UNCHANGED)
            return
        if what == "changed":
            logger.info(
                "内容变了，重新处理 msg_id=%s（后端幂等会把原来那条任务更新掉）", message.msg_id
            )
        try:
            outcome = await _handle_one(
                message, backend, settings, attachment_resolver, store, database,
                known_raw_ids, report,
                # 内容变了就必须真的重抽一遍：这时候"后端已经有这条通知"永远成立
                # （就是那条要更新的），开着这个捷径会让补充/编辑静默地不生效。
                allow_known_skip=(what != "changed") and not settings.client_force_recheck,
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
            degraded=outcome.result == "degraded",
            conflict=False,
            tokens=outcome.tokens,
        ))

        # 缺口检测：靠**自己镜像**里这个群的上一条消息时间（不是后端的 group_state）。
        # 与 bot 的判据一致，只是时间线换成自己看到的那条 —— 共享的 group_state 会被
        # 别人的时间线顶掉，缺口就会静默地漏。
        if not settings.client_dry_run and outcome.result != OUTCOME_SKIPPED:
            previous_ts = store.group_seen_ts(
                message.group_id, exclude_msg_id=str(message.msg_id)
            )
            if previous_ts:
                # 镜像里存的是**秒**（源库的单位），缺口判据是毫秒
                await _maybe_gap(
                    backend, settings, message, int(previous_ts) * 1000, report
                )

    # ---- 2a) 没读过的：按时间正序 ----
    # 从**最老的**未读开始：补充关系里"原文"必须先于"补充"被处理，否则补充找不到
    # 目标、会被当成一条独立的新消息。
    for message in database.iter_unread(store.path, limit=max(0, budget)):
        await handle_scanned(message)
        budget -= 1
        if budget <= 0:
            break

    # ---- 2b) 回看：最近一段**已读**消息的内容有没有变 ----
    # 这是"消息被编辑过"唯一的发现途径（内容指纹在 mirror.claim 里比）。
    # 只回看最近 `CLIENT_RECHECK_OVERLAP_HOURS` 小时，而且是**最新优先**：
    # 窗口比预算大时，先看最新的那一段，否则老消息会把预算吃光、新改动永远排不上。
    # 排除掉这一轮已经看过的那批（`seen_this_cycle`）—— 同一份内容一轮处理两遍没有
    # 任何意义，dry-run 下还要白花一次抽取。
    if budget > 0 and latest is not None and settings.client_recheck_overlap_hours > 0:
        recheck_since = max(0, int(latest[0]) - int(settings.client_recheck_overlap_hours * 3600))
        if recheck_since > 0:
            for message in database.iter_unread(
                store.path,
                limit=budget,
                recheck_since=recheck_since,
                exclude=seen_this_cycle,
            ):
                report.rechecked += 1
                await handle_scanned(message)
                budget -= 1
                if budget <= 0:
                    break


    if not settings.client_dry_run and stats:
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
    if outcome.result == OUTCOME_AMENDED:
        report.amended += 1


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
        "本轮：扫了 %d 条（恢复 %d，其中回看已读的 %d），跳过 %d 条没变的、"
        "%d 条白名单外的，更新了 %d 条任务，结果=%s",
        report.scanned,
        report.recovered,
        report.rechecked,
        report.unchanged,
        report.skipped_whitelist,
        report.amended,
        report.outcomes or {},
    )
    if report.unread_before and report.unread_before > report.scanned:
        logger.info(
            "源库里还剩 %s 条没读过（本轮处理了 %d 条，下一轮接着读）—— "
            "第一轮会把库里所有群消息过一遍，之后每轮只读新的。",
            f"{report.unread_before - report.scanned:,}",
            report.scanned,
        )
    if report.prepared:
        logger.info("源库准备：%s", report.prepared)
    if report.mirror_after:
        logger.info(
            "镜像：共 %d 条（%s），补充关系 %d 条",
            report.mirror_after.get("total", 0),
            report.mirror_after.get("by_state", {}),
            report.mirror_after.get("amendments", 0),
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
