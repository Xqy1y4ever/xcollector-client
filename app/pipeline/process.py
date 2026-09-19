"""一条聊天记录 → 后端里的一条通知。

## 和 bot 的流水线差在哪

bot 的 `process_raw` 顺序是「**先问谁要**（投递名单）→ 白名单 → 抽取一次 →
按订阅者扇出」。客户端这边少了路由和扇出，因为**每个客户端只服务一个用户**：

  - 过滤条件只有**本地的白名单**（客户端的输入是"你自己账号的聊天记录库"），
    不需要问"谁要"，也不需要订阅；
  - 扇出没有意义 —— 只有一个人收，写一条就够；
  - 于是"抽一次"这件事天然成立。

顺序（每一步都对应一个可观测的痕迹）：

    白名单 → 写前日志(raw) → 附件 → 回填 attachments → 抽取 → 建通知 → 统计

## 三处刻意的不静默

1. **拿不到附件字节**：不假装成功。要么只留 CDN 地址（并在日志里说明那很可能是个
   死链），要么干脆不记附件 —— 两种都会在通知上看得出来。
2. **抽取失败**：LLM 挂了就降级到规则，并把这条记成 `degraded`（进统计，运营者和
   用户都能在盲区里看到），而不是安静地少一条。
3. **没有证据**：一律不建条（和 bot、后端同一条硬约束）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..backend_client import BackendClient, BackendError, BackendRejected
from ..config import (
    CROSS_CHECK_ENABLED,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LLM_TIMEOUT,
    SECONDARY_LLM_API_BASE,
    SECONDARY_LLM_API_KEY,
    SECONDARY_LLM_MODEL,
    VLM_ENABLED,
    Settings,
)
from ..source.attachments import AttachmentResolver, ResolvedAttachment
from ..source.ntmsg import SourceMessage
from ..utils import local_day, now_ms, preview
from .extract import (
    fill_due_from_rule,
    finalize,
    merge_rule_disagreement,
    run_llm,
    cross_check,
)
from .rule_extract import is_group_chore, is_noise, rule_extract

logger = logging.getLogger(__name__)

# 一条消息的处理结果。返回值就是日志里的 `结果=`，也是统计的归类依据。
OUTCOME_EXTRACTED = "extracted"
OUTCOME_NOISE = "noise"
OUTCOME_UNPARSED = "unparsed"      # 抽了，但没有证据 / 抽不出标题
OUTCOME_DEGRADED = "degraded"      # 模型失败且规则也没兜住 —— 真盲区
OUTCOME_SKIPPED = "skipped"        # 本地白名单外的来源 / 缺字段，压根不抽
OUTCOME_ERROR = "error"            # 写不进去（后端拒绝/不可达）
# 已经处理过（后端有这条通知）—— 重扫时的**正常**结果，不是异常
OUTCOME_UNCHANGED = "unchanged"


@dataclass
class Outcome:
    result: str
    raw_id: str = ""
    title: str | None = None
    due_at: int | None = None
    due_text: str | None = None
    due_confidence: float = 0.0
    tokens: int = 0
    reason: str | None = None
    attachments: list[dict] = field(default_factory=list)
    # 重抽之后按新判据"不再是通知"→ 把原来那条通知归档了吗（见 _withdraw_notification）
    withdrawn: bool = False


def message_text(message: SourceMessage) -> str:
    """入库用的正文。

    源库的 `content` 是结构化 JSON，`text` 是导出工具合并好的纯文本。
    两者都没有时给空串（**不编**）—— 一条没有正文的消息后面会被判成 noise。
    """
    return (message.text or "").strip()


def message_id_of(message: SourceMessage) -> str:
    """写进后端 `raw_message.message_id` 的值。

    带上来源前缀，因为后端的唯一键是 `(group_id, message_id)`：以后可能还有别的
    入库方（bot 用 OneBot 的 message_id），两套 id 空间混在一起才需要能分辨。
    """
    return f"ntqq:{message.msg_id}"


def build_raw_payload(message: SourceMessage) -> dict:
    """`POST /api/messages` 的请求体（共享层）。

    `raw` 里留一份源库的原始 content —— 证据链要能回到源头，而不是只有我方
    处理过之后的结论。
    """
    return {
        "message_id": message_id_of(message),
        "group_id": message.group_id,
        "group_name": None,          # 源库里没有群名，不编
        "sender_id": message.sender_id,
        "sender_name": message.sender_name,
        "ts": message.ts_ms,
        "content": message_text(message),
        "attachments": [],
        "raw": {
            "source": "ntqq",
            "msg_id": message.msg_id,
            "parse_status": message.parse_status,
            "content": message.raw_content,
        },
    }


def build_notification_payload(raw_id: str, message: SourceMessage, result: dict) -> dict | None:
    """抽取结果 → `POST /api/notifications` 的请求体。证据为空返回 None。"""
    evidence = str(result.get("evidence") or "").strip()
    if not evidence:
        return None
    return {
        "raw_message_id": raw_id,
        "group_id": message.group_id,
        "group_name": None,
        "sender_id": message.sender_id,
        "sender_name": message.sender_name,
        "source_ts": message.ts_ms,
        "title": result.get("title"),
        "summary": result.get("summary"),
        "location": result.get("location"),
        "due_at": result.get("due_at"),
        "due_text": result.get("due_text"),
        "due_confidence": float(result.get("due_confidence") or 0.0),
        "evidence": evidence,
        "conflict": bool(result.get("conflict")),
        "candidates": list(result.get("candidates") or []),
        "extractor": result.get("extractor"),
        "model": result.get("model"),
        "prompt_ver": result.get("prompt_ver"),
    }


async def extract(message: SourceMessage, settings: Settings) -> tuple[dict | None, bool, int]:
    """抽取。返回 `(结果, 是否降级, tokens)`，**不抛异常**。

    与 bot 的 `parse_content` 同一套语义：LLM 那条路整体失败就降级到规则，
    并把这件事记下来（降级 = 盲区，用户必须能看到）。

    ## 确定性的门**先过**，而且对模型也生效

    闲聊/回执（`is_noise`）和群务（`is_group_chore`：改群名片、进群、查收名单…）
    在这里就地判死，**不看模型怎么说**，也不花那一次模型调用。

    真实教训（2026-09-19 线上）：v4 的提示词已经写了"群务不算通知"，模型还是把
    「各位同学：请完成群昵称修改…」判成了「按时报到并查看近期通知合集」——
    而那条 `GROUP_CHORE` 一票否决只长在 `rule_extract` 里，`llm` 模式下模型说了算，
    于是它从旁边绕过去了。这一层是**代码说了算**的部分，不该由模型的发挥决定。
    """
    text = message_text(message)
    chore = is_group_chore(text)
    if chore or is_noise(text):
        logger.info(
            "确定性判据直接不建条：%s msg_id=%s",
            f"群务（{chore}）" if chore else "闲聊/回执",
            message.msg_id,
        )
        return None, False, 0

    rule_result = rule_extract(text, message.ts_ms, at_all=False)

    if settings.client_extractor == "rule":
        # 核心链路在 EXTRACTOR=rule 时**完全不发任何模型请求**
        return rule_result, False, 0

    images: list[str] = []
    if VLM_ENABLED:
        images = [a["url"] for a in message.attachments if a.get("url") and a.get("type") == "image"]

    raw = {"ts": message.ts_ms, "content": text, "group_id": message.group_id,
           "sender_id": message.sender_id, "sender_name": message.sender_name,
           "message_id": message_id_of(message)}

    try:
        parsed, tokens = await run_llm(
            model=settings.llm_model,
            api_base=settings.llm_api_base,
            api_key=settings.llm_api_key,
            raw=raw,
            images=images,
            tz=settings.digest_tz,
            temperature=LLM_TEMPERATURE,
            timeout=LLM_TIMEOUT,
            max_retries=LLM_MAX_RETRIES,
        )
    except Exception as exc:
        # 关键：把**真实原因**记下来。bot 那边这一步因为一个未定义的名字
        # （见 extract.run_llm 的说明）只会留下一个 NameError，真实原因被顶掉。
        logger.error("LLM 抽取失败，降级为规则抽取 msg_id=%s：%s", message.msg_id, exc)
        return rule_result, True, 0

    llm_result = finalize(parsed, raw, settings.llm_model, extractor="llm", tokens=tokens)

    if CROSS_CHECK_ENABLED and SECONDARY_LLM_MODEL:
        try:
            second, more_tokens = await run_llm(
                model=SECONDARY_LLM_MODEL,
                api_base=SECONDARY_LLM_API_BASE or settings.llm_api_base,
                api_key=SECONDARY_LLM_API_KEY or settings.llm_api_key,
                raw=raw,
                images=images,
                tz=settings.digest_tz,
                temperature=LLM_TEMPERATURE,
                timeout=LLM_TIMEOUT,
                max_retries=LLM_MAX_RETRIES,
            )
            tokens += more_tokens
            secondary = finalize(second, raw, SECONDARY_LLM_MODEL, extractor="llm")
            if secondary is not None and llm_result is not None:
                cross_check(llm_result, secondary, SECONDARY_LLM_MODEL)
        except Exception as exc:
            # 交叉验证是加分项，挂了不该让整条通知丢掉；但要如实记一行
            logger.warning("交叉验证失败（只用主模型的结果）msg_id=%s：%s", message.msg_id, exc)

    merged = merge_rule_disagreement(llm_result, rule_result, settings.llm_model)
    # 模型"是通知但没算出时间"时，用确定性的规则引擎补 due_at（「下周三」「这周天」
    # 这类相对时间靠日期运算，模型经常算不出来，而 parse_due 不会算错）。
    merged = fill_due_from_rule(merged, rule_result, source_ts=message.ts_ms)
    if merged is not None and merged.get("tokens") is None:
        merged["tokens"] = tokens
    return merged, False, tokens


def outcome_stats(*, outcome: str, degraded: bool, conflict: bool, tokens: int) -> dict[str, int]:
    """统计字段。**只能出现后端认识的列** —— 未知键会被静默忽略。

    后端的 `STAT_FIELDS` 是固定那几个（ingested/extracted/unparsed/conflicts/
    degraded/llm_tokens），没有"按结果分类"的列。所以别往这里加自定义键：
    加了不会报错，但会白写一次请求，而且看起来像记上了。
    """
    stats: dict[str, int] = {"ingested": 1}
    if outcome == OUTCOME_EXTRACTED:
        stats["extracted"] = 1
    elif outcome == OUTCOME_UNPARSED:
        stats["unparsed"] = 1
    elif outcome == OUTCOME_DEGRADED:
        stats["degraded"] = 1
        stats["unparsed"] = 1
    if conflict:
        stats["conflicts"] = 1
    if degraded and outcome == OUTCOME_EXTRACTED:
        stats["degraded"] = 1
    if tokens:
        stats["llm_tokens"] = tokens
    return stats


async def process_message(
    message: SourceMessage,
    backend: BackendClient,
    settings: Settings,
    resolver: AttachmentResolver,
    *,
    known_raw_ids: set[str] | None = None,
    notification_ids: dict[str, str] | None = None,
    force: bool = False,
) -> Outcome:
    """处理一条群消息。返回 Outcome（调用方据此记统计与日志）。

    `known_raw_ids`：我已经有通知的 raw id 集合。命中就**跳过抽取** ——
    镜像被删掉之后重扫时，这一个参数决定了要不要把模型的钱再花一遍。

    `notification_ids`：`{raw_id: notification_id}`。**只在重抽（`force`）时用**：
    按新判据判定"不再是通知"的消息，要把原来那条通知归档掉，得知道它的 id。

    `force`：**用户点名要求重抽**的那条（镜像里状态是 `reprocess`，见
    `mirror.mark_unread`）。此时那条"已经有通知就跳过"的捷径**不算数** ——
    这正是"标为未读"这个动作的意义所在：不绕过它的话，用户点了"重新处理"，
    日志里一切正常，而实际上一次模型调用都没发生（静默地什么都没做）。

    重抽之后 `POST /api/notifications` 是**幂等 upsert**（后端按
    `(user_id, raw_message_id)` 唯一，只覆盖机器字段、不动人工修正），
    所以结果就地更新到那条通知上，不会多出一条；而**重抽之后不再是通知**的那些，
    由 `_withdraw_notification()` 归档（否则板子上的误报一条都不会少）。
    """
    text = message_text(message)
    if not message.group_id or not message.sender_id:
        # 源库里缺群号/发送者：这条没法归属到任何来源，如实跳过去。
        return Outcome(result=OUTCOME_SKIPPED, reason="源库里缺 group_id 或 sender_id")

    if not text and not message.attachments:
        return Outcome(result=OUTCOME_SKIPPED, reason="既没有正文也没有附件")

    raw_payload = build_raw_payload(message)

    # ---- 写前日志：先把原文保住 ----
    try:
        created = await backend.create_message(raw_payload)
    except BackendRejected as exc:
        # 用户令牌写自己的原文**不需要订阅任何来源**（后端按表分归属），所以
        # 401/403 只可能是令牌本身的问题（过期、被换掉、拿错了令牌）。
        # 这不是"等用户去订阅就好了"的状态，如实归到 error：下一轮重试并留在
        # report 里，而不是让一批消息安静地卡在 pending 上。
        return Outcome(
            result=OUTCOME_ERROR,
            reason=f"后端拒绝写原文（HTTP {exc.status_code}）：{exc}。"
            "检查 CLIENT_TOKEN 是不是这个用户的 UserToken、有没有过期。",
        )
    except BackendError as exc:
        return Outcome(result=OUTCOME_ERROR, reason=f"写前日志失败：{exc}")

    raw_id = str(created.get("id") or "")
    if not raw_id:
        return Outcome(result=OUTCOME_ERROR, reason="后端没有返回原消息 id")

    # 已经有通知了 → 跳过抽取。**这是"镜像丢了也不心疼"的关键一步**：
    # raw 是幂等的，所以这次 POST 顺便当了一次"这条我处理过吗"的查询 ——
    # 客户端没有直接读原文的接口（那会跨用户），用户令牌下这是唯一拿到 raw id
    # 的正当路径。
    #
    # `force`（用户把它标成了未读）时这一步**不做**：他就是来重抽的。
    if known_raw_ids and raw_id in known_raw_ids and not force:
        return Outcome(
            result=OUTCOME_SKIPPED,
            raw_id=raw_id,
            reason="已经建过通知（重扫时跳过，不重复花模型的钱）",
        )

    # ---- 附件（拿不到字节也要留痕）----
    attachments = await _upload_attachments(message, backend, resolver)
    if attachments:
        try:
            await backend.patch_message(raw_id, {"attachments": attachments})
        except BackendError as exc:
            # 附件回填失败不该让通知丢掉：通知里没有图，但任务还在
            logger.warning("回填 attachments 失败 raw=%s：%s", raw_id, exc)

    # ---- 抽取 ----
    result, degraded, tokens = await extract(message, settings)
    if result is None:
        if degraded:
            await _patch_state(backend, raw_id, OUTCOME_DEGRADED, "LLM 失败且规则也无法解析")
            return Outcome(
                result=OUTCOME_DEGRADED,
                raw_id=raw_id,
                tokens=tokens,
                reason="LLM 失败且规则也无法解析",
                attachments=attachments,
            )
        # **重抽时按新判据不再是通知 → 把原来那条通知归档。**
        #
        # 为什么必须有这一步：判据收紧之后，"重新处理"如果只是把 raw 标成 noise，
        # 原来那条误报还留在任务板上 —— 用户点了"重新处理"，板子上一条都不会少，
        # 看起来就是"改了没用"。归档（corrections: status=archived）是用户令牌
        # 做得到的事（删通知只有服务令牌能做），而且不丢数据：原文和修正历史都在。
        archived = await _withdraw_notification(backend, message, raw_id, force, notification_ids)
        await _patch_state(backend, raw_id, OUTCOME_NOISE, "判定为非通知")
        return Outcome(
            result=OUTCOME_NOISE,
            raw_id=raw_id,
            tokens=tokens,
            attachments=attachments,
            withdrawn=archived,
            reason="判定为非通知，已把原来的任务归档" if archived else None,
        )

    payload = build_notification_payload(raw_id, message, result)
    if payload is None:
        await _patch_state(backend, raw_id, OUTCOME_UNPARSED, "抽取结果缺少 evidence，已拒绝建条")
        return Outcome(
            result=OUTCOME_UNPARSED,
            raw_id=raw_id,
            tokens=tokens,
            reason="抽取结果缺少 evidence，已拒绝建条",
            attachments=attachments,
        )

    try:
        await backend.create_notification(payload)
    except BackendRejected as exc:
        await _patch_state(backend, raw_id, OUTCOME_ERROR, f"建通知被拒：{exc}")
        return Outcome(result=OUTCOME_ERROR, raw_id=raw_id, reason=f"建通知被拒：{exc}")
    except BackendError as exc:
        # 暂时性失败：raw 已经进库了，下一次循环会因为游标没推进而重试这条
        logger.error("建通知失败 raw=%s（游标不会推进，下轮重试）：%s", raw_id, exc)
        return Outcome(result=OUTCOME_ERROR, raw_id=raw_id, reason=f"建通知失败：{exc}")

    await _patch_state(backend, raw_id, OUTCOME_EXTRACTED)
    return Outcome(
        result=OUTCOME_EXTRACTED,
        raw_id=raw_id,
        title=payload.get("title"),
        due_at=payload.get("due_at"),
        due_text=payload.get("due_text"),
        due_confidence=float(payload.get("due_confidence") or 0.0),
        tokens=tokens,
        attachments=attachments,
    )


async def _withdraw_notification(
    backend: BackendClient,
    message: SourceMessage,
    raw_id: str,
    force: bool,
    notification_ids: dict[str, str] | None,
) -> bool:
    """重抽之后"不再是通知"→ 把原来那条通知归档。返回是否真的归档了。

    只对**用户点名重抽**（`force`）的做这件事：正常流程里这条消息本来就没建过条，
    没有东西要撤。归档走 corrections（`status=archived`）—— 用户令牌做得到、
    也不删数据（原文与修正历史都留着，前端切到「已归档」还能看到）。

    做不成也不吞：写一行 WARNING 并返回 False，调用方据此在 Outcome.reason 里
    说明"判定为非通知"，这样"板子上那条为什么还在"是查得到的。
    """
    if not force or not notification_ids:
        return False
    notif_id = notification_ids.get(raw_id)
    if not notif_id:
        return False
    try:
        await backend.add_correction(notif_id, field="status", value="archived", actor="client")
    except BackendError as exc:
        logger.warning(
            "重抽判定为非通知，但归档失败 notif=%s msg_id=%s：%s", notif_id, message.msg_id, exc
        )
        return False
    logger.info("重抽判定为非通知，已把原来的任务归档：notif=%s msg_id=%s", notif_id, message.msg_id)
    return True


async def _patch_state(
    backend: BackendClient, raw_id: str, state: str, reason: str | None = None
) -> None:
    try:
        await backend.patch_message(raw_id, {"state": state, "state_reason": reason})
    except BackendError as exc:
        logger.warning("更新 raw 状态失败 raw=%s state=%s：%s", raw_id, state, exc)


# ---------------------------------------------------------------------------
# 附件
# ---------------------------------------------------------------------------


async def _upload_attachments(
    message: SourceMessage,
    backend: BackendClient,
    resolver: AttachmentResolver,
) -> list[dict]:
    """把附件逐个变成后端认的形状。

    两层降级，都要在日志里说清楚：
      1. 找不到本地字节 → 只留 CDN 地址（**明确记下它可能是死链**）；
      2. 上传失败 → 同样处理。
    """
    out: list[dict] = []
    for item in message.attachments:
        resolved: ResolvedAttachment | None = resolver.resolve(item)
        if resolved is None or not resolved.content:
            # 留 URL：**明确记下它可能是死链**（QQ CDN 的图几小时就过期）
            if item.get("url"):
                logger.info(
                    "附件只有远程地址（很可能是已过期的 QQ CDN 链接，前端可能打不开）："
                    "msg_id=%s file=%s",
                    message.msg_id,
                    item.get("name") or "?",
                )
                out.append(
                    {
                        "type": item.get("type") or "image",
                        "url": item["url"],
                        "name": item.get("name"),
                        "degraded": True,
                        "degraded_reason": (resolved.reason if resolved else "没有本地字节"),
                    }
                )
            continue

        uploaded = await backend.upload_attachment(
            resolved.filename,
            resolved.content,
            resolved.content_type,
            item.get("url"),
        )
        if not uploaded:
            logger.warning("附件上传失败，这条降级为只留地址：msg_id=%s", message.msg_id)
            if item.get("url"):
                out.append(
                    {
                        "type": item.get("type") or "image",
                        "url": item["url"],
                        "name": item.get("name"),
                        "degraded": True,
                        "degraded_reason": "上传失败",
                    }
                )
            continue
        out.append(
            {
                "id": uploaded.get("id"),
                "url": uploaded.get("url"),
                "type": item.get("type") or "image",
                "name": item.get("name") or uploaded.get("filename"),
                "size": uploaded.get("size"),
            }
        )
    return out


__all__ = [
    "OUTCOME_DEGRADED",
    "OUTCOME_ERROR",
    "OUTCOME_UNCHANGED",
    "OUTCOME_EXTRACTED",
    "OUTCOME_NOISE",
    "OUTCOME_SKIPPED",
    "OUTCOME_UNPARSED",
    "Outcome",
    "build_notification_payload",
    "build_raw_payload",
    "extract",
    "is_noise",
    "message_id_of",
    "message_text",
    "outcome_stats",
    "process_message",
    "rule_extract",
    "now_ms",
    "local_day",
    "preview",
]
