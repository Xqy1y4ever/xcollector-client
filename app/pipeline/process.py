"""一条聊天记录 → 后端里的一条通知。

## 和 bot 的流水线差在哪

bot 的 `process_raw` 顺序是「**先问谁要**（投递名单）→ 白名单 → 抽取一次 →
按订阅者扇出」。客户端这边少了路由和扇出，因为**每个客户端只服务一个用户**：

  - 过滤条件是**这个用户自己的订阅**（从后端读来的），不需要问"谁要"；
  - 扇出没有意义 —— 只有一个人收，写一条就够；
  - 于是"抽一次"这件事天然成立，而且**没人订阅的来源根本不会被抽取**
    （连请求都不会发出去），这正好保住了"不为没人要的消息花模型的钱"。

顺序（每一步都对应一个可观测的痕迹）：

    订阅过滤 → 写前日志(raw) → 附件 → 回填 attachments → 抽取 → 建通知 → 统计

## 三处刻意的不静默

1. **拿不到附件字节**：不假装成功。按配置要么只留 CDN 地址（并在日志里说明那
   很可能是个死链），要么干脆不记附件 —— 两种都会在通知上看得出来。
2. **抽取失败**：LLM 挂了就降级到规则，并把这条记成 `degraded`（进统计，
   运营者和用户都能在盲区里看到），而不是安静地少一条。
3. **没有证据**：一律不建条（和 bot、后端同一条硬约束）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..backend_client import BackendClient, BackendError, BackendRejected
from ..config import Settings
from ..source.attachments import AttachmentResolver, ResolvedAttachment
from ..source.ntmsg import SourceMessage
from ..utils import local_day, now_ms, preview
from .extract import (
    finalize,
    merge_rule_disagreement,
    run_llm,
    cross_check,
)
from .rule_extract import is_noise, rule_extract

logger = logging.getLogger(__name__)

# 一条消息的处理结果。返回值就是日志里的 `结果=`，也是统计的归类依据。
OUTCOME_EXTRACTED = "extracted"
OUTCOME_NOISE = "noise"
OUTCOME_UNPARSED = "unparsed"      # 抽了，但没有证据 / 抽不出标题
OUTCOME_DEGRADED = "degraded"      # 模型失败且规则也没兜住 —— 真盲区
OUTCOME_SKIPPED = "skipped"        # 本地白名单外的来源 / 缺字段，压根不抽
OUTCOME_ERROR = "error"            # 写不进去（后端拒绝/不可达）
# 内容没变、已经处理过 —— 增量更新里的**正常**结果，不是异常
OUTCOME_UNCHANGED = "unchanged"
# 这条是对某条**已读**消息的补充：更新了那条任务，而不是新建一条
OUTCOME_AMENDED = "amended"


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
    """
    text = message_text(message)
    rule_result = rule_extract(text, message.ts_ms, at_all=False)

    if settings.client_extractor == "rule":
        # 核心链路在 EXTRACTOR=rule 时**完全不发任何模型请求**
        return rule_result, False, 0

    images: list[str] = []
    if settings.client_vlm_enabled:
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
            temperature=settings.llm_temperature,
            timeout=settings.llm_timeout,
            max_retries=settings.llm_max_retries,
        )
    except Exception as exc:
        # 关键：把**真实原因**记下来。bot 那边这一步因为一个未定义的名字
        # （见 extract.run_llm 的说明）只会留下一个 NameError，真实原因被顶掉。
        logger.error("LLM 抽取失败，降级为规则抽取 msg_id=%s：%s", message.msg_id, exc)
        return rule_result, True, 0

    llm_result = finalize(parsed, raw, settings.llm_model, extractor="llm", tokens=tokens)

    if settings.client_cross_check_enabled and settings.llm_secondary_model:
        try:
            second, more_tokens = await run_llm(
                model=settings.llm_secondary_model,
                api_base=settings.llm_secondary_api_base or settings.llm_api_base,
                api_key=settings.llm_secondary_api_key or settings.llm_api_key,
                raw=raw,
                images=images,
                tz=settings.digest_tz,
                temperature=settings.llm_temperature,
                timeout=settings.llm_timeout,
                max_retries=settings.llm_max_retries,
            )
            tokens += more_tokens
            secondary = finalize(second, raw, settings.llm_secondary_model, extractor="llm")
            if secondary is not None and llm_result is not None:
                cross_check(llm_result, secondary, settings.llm_secondary_model)
        except Exception as exc:
            # 交叉验证是加分项，挂了不该让整条通知丢掉；但要如实记一行
            logger.warning("交叉验证失败（只用主模型的结果）msg_id=%s：%s", message.msg_id, exc)

    merged = merge_rule_disagreement(llm_result, rule_result, settings.llm_model)
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
    dry_run: bool = False,
    known_raw_ids: set[str] | None = None,
) -> Outcome:
    """处理一条群消息。返回 Outcome（调用方据此记统计与日志）。

    `known_raw_ids`：我已经有通知的 raw id 集合。命中就**跳过抽取** ——
    游标丢失后重扫时，这一个参数决定了要不要把模型的钱再花一遍
    （见 `app/run.py` 顶部那段说明）。
    """
    text = message_text(message)
    if not message.group_id or not message.sender_id:
        # 源库里缺群号/发送者：这条没法归属到任何订阅上，如实跳过去。
        return Outcome(result=OUTCOME_SKIPPED, reason="源库里缺 group_id 或 sender_id")

    if not text and not message.attachments:
        return Outcome(result=OUTCOME_SKIPPED, reason="既没有正文也没有附件")

    raw_payload = build_raw_payload(message)

    if dry_run:
        # 只组装不写入：连"能不能写进去"都不碰，一个请求都不发。
        result, degraded, tokens = await extract(message, settings)
        return Outcome(
            result=OUTCOME_EXTRACTED if result else OUTCOME_NOISE,
            title=(result or {}).get("title"),
            due_at=(result or {}).get("due_at"),
            tokens=tokens,
            reason="dry-run 未写入" + ("（降级）" if degraded else ""),
            attachments=message.attachments,
        )

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

    # 已经有通知了 → 跳过抽取。**这是"游标丢了也不心疼"的关键一步**：
    # raw 是幂等的，所以这次 POST 顺便当了一次"这条我处理过吗"的查询 ——
    # 客户端没有直接读原文的接口（那会跨用户），用户令牌下这是唯一拿到 raw id
    # 的正当路径。
    if known_raw_ids and raw_id in known_raw_ids:
        return Outcome(
            result=OUTCOME_SKIPPED,
            raw_id=raw_id,
            reason="已经建过通知（重扫时跳过，不重复花模型的钱）",
        )

    # ---- 附件（拿不到字节也要留痕）----
    attachments = await _upload_attachments(message, backend, settings, resolver)
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
        await _patch_state(backend, raw_id, OUTCOME_NOISE, "判定为非通知")
        return Outcome(
            result=OUTCOME_NOISE, raw_id=raw_id, tokens=tokens, attachments=attachments
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


async def _patch_state(
    backend: BackendClient, raw_id: str, state: str, reason: str | None = None
) -> None:
    try:
        await backend.patch_message(raw_id, {"state": state, "state_reason": reason})
    except BackendError as exc:
        logger.warning("更新 raw 状态失败 raw=%s state=%s：%s", raw_id, state, exc)


# ---------------------------------------------------------------------------
# 补充：新消息在补充一条**已读**消息 → 改那条任务，而不是新建一条
# ---------------------------------------------------------------------------

# 合并后的证据/摘要长度上限。超过就截断并说明 —— 无上限地拼会把通知撑爆，
# 而通知是给人看的（前端也按固定字段渲染）。
_MERGED_EVIDENCE_MAX = 600
_MERGED_SUMMARY_MAX = 300


def merge_amendment(original: dict | None, supplement: dict | None) -> dict | None:
    """把「原通知」和「补充」两次抽取的结果合并成一条。

    合并规则刻意简单、可解释：

      - **截止时间以补充为准**：补充说"改到周五"就是周五。这是用户最关心的字段，
        而且"补充"这个动作本身通常就是在改时间。
      - 原文的 due_confidence/candidates/conflict 保留，但把冲突标出来 ——
        两次结果不一致时用户必须能看见，而不是安静地采用后一个。
      - summary 追加一句"补充：…"；evidence **逐段拼接**（每一段仍是原文逐字复制，
        所以"证据必须能回到原文"这条硬约束没有被破坏）。
      - 补充没抽出东西（None）时**不动**原结果：一句闲聊式的回复不该清掉已有的截止时间。

    `original` 为 None 表示原消息当初没抽成通知（比如被判成闲聊）—— 那这次补充
    抽出来的就是**新**的一条任务，调用方要按新建处理，不能往一个不存在的任务上写。
    """
    if original is None:
        return supplement
    if supplement is None:
        return original

    merged = dict(original)
    sup_due = supplement.get("due_at")
    if sup_due is not None:
        if original.get("due_at") is not None and int(original["due_at"]) != int(sup_due):
            merged["conflict"] = True
            merged["due_confidence"] = min(float(supplement.get("due_confidence") or 0.0), 0.5)
            logger.info(
                "补充改了截止时间：%s → %s（已标 conflict，让人看得见这次改动）",
                original.get("due_at"),
                sup_due,
            )
        merged["due_at"] = sup_due
        merged["due_text"] = supplement.get("due_text") or merged.get("due_text")
        merged["due_confidence"] = float(supplement.get("due_confidence") or 0.0)
    if supplement.get("location"):
        merged["location"] = supplement["location"]
    if supplement.get("title") and not merged.get("title"):
        merged["title"] = supplement["title"]

    sup_summary = (supplement.get("summary") or "").strip()
    if sup_summary:
        base = (merged.get("summary") or "").strip()
        merged["summary"] = (f"{base} 补充：{sup_summary}" if base else sup_summary)[:_MERGED_SUMMARY_MAX]

    sup_evidence = (supplement.get("evidence") or "").strip()
    if sup_evidence:
        base = (merged.get("evidence") or "").strip()
        merged["evidence"] = (f"{base}\n补充：{sup_evidence}" if base else sup_evidence)[
            :_MERGED_EVIDENCE_MAX
        ]

    merged["candidates"] = list(original.get("candidates") or []) + list(
        supplement.get("candidates") or []
    )
    merged["tokens"] = int(original.get("tokens") or 0) + int(supplement.get("tokens") or 0)
    # extractor/model 记成"补充"这一次的：读的人要能看出最后这次是谁抽的
    merged["extractor"] = supplement.get("extractor") or merged.get("extractor")
    merged["model"] = supplement.get("model") or merged.get("model")
    merged["amended"] = True
    return merged


async def process_amendment(
    supplement: SourceMessage,
    original: SourceMessage,
    backend: BackendClient,
    settings: Settings,
    *,
    original_raw_id: str,
    known_raw_ids: set[str] | None = None,
) -> Outcome:
    """把「supplement 补充了 original」落到后端：**更新 original 那条任务**。

    做法是两次独立抽取再合并，而不是把两段文字拼起来抽一次 —— 因为相对时间
    （"下周三前"）必须以**各自消息的发送时间**为锚点。拼起来抽的话，原文那句
    "下周三前"会被按补充的时间重新解释，算出来就是一个错的截止时间 ——
    而错的截止时间比没有截止时间危害更大。

    顺序（每一步都对应一条可追溯的痕迹）：

      1. 把补充这条**按它自己的 msg_id** 写进原始层（证据链要留得住）；
      2. 分别抽取原文与补充（各自锚定自己的发送时间）；
      3. 合并（见 `merge_amendment`）；两边都没抽出东西 → 这条补充不改变任务；
      4. `POST /api/notifications` 用**原文的 raw id** → 后端按
         `(user_id, raw_message_id)` 幂等，于是命中原来那行、只覆盖机器字段。
         **人工修正不会被冲掉**（这是后端早就守着的规则）。
    """
    # 1) 补充本身也要进原始层：否则"这条通知是怎么被改的"就查不到了
    supplement_raw_id = ""
    try:
        created = await backend.create_message(build_raw_payload(supplement))
        supplement_raw_id = str(created.get("id") or "")
    except BackendRejected as exc:
        return Outcome(
            result=OUTCOME_ERROR,
            reason=f"后端拒绝写原文（补充，HTTP {exc.status_code}）：{exc}",
        )
    except BackendError as exc:
        return Outcome(result=OUTCOME_ERROR, reason=f"写前日志（补充）失败：{exc}")

    # 2) 两次独立抽取
    orig_result, orig_degraded, orig_tokens = await extract(original, settings)
    sup_result, sup_degraded, sup_tokens = await extract(supplement, settings)
    tokens = orig_tokens + sup_tokens
    degraded = orig_degraded or sup_degraded

    if sup_result is None and orig_result is None:
        # 两边都没抽出东西：这条补充不构成"对任务的修改"。如实记下来，
        # 不往任务上写任何东西。
        return Outcome(
            result=OUTCOME_NOISE,
            raw_id=supplement_raw_id,
            tokens=tokens,
            reason="补充本身没有抽出内容，原消息也没有任务 —— 不改动任何任务",
        )

    merged = merge_amendment(orig_result, sup_result)
    if merged is None:
        return Outcome(result=OUTCOME_NOISE, raw_id=supplement_raw_id, tokens=tokens)

    if orig_result is None:
        # 原来那条根本没有任务（当初判成闲聊）→ 这是一条**新**任务，
        # 而且不能往原文头上写（那会把一条闲聊变成任务，还改错了归属）。
        payload = build_notification_payload(supplement_raw_id, supplement, merged)
        if payload is None:
            return Outcome(
                result=OUTCOME_UNPARSED,
                raw_id=supplement_raw_id,
                tokens=tokens,
                reason="补充抽出来但没有证据，已拒绝建条",
            )
        try:
            await backend.create_notification(payload)
        except BackendError as exc:
            return Outcome(result=OUTCOME_ERROR, raw_id=supplement_raw_id, reason=f"建条失败：{exc}")
        await _patch_state(backend, supplement_raw_id, OUTCOME_EXTRACTED)
        return Outcome(
            result=OUTCOME_EXTRACTED,
            raw_id=supplement_raw_id,
            title=payload.get("title"),
            due_at=payload.get("due_at"),
            due_text=payload.get("due_text"),
            tokens=tokens,
            reason=f"补充（原文当初没抽成任务）：按新任务建条",
        )

    payload = build_notification_payload(original_raw_id, original, merged)
    if payload is None:
        return Outcome(
            result=OUTCOME_UNPARSED,
            raw_id=supplement_raw_id,
            tokens=tokens,
            reason="合并后没有证据，已拒绝更新",
        )
    try:
        await backend.create_notification(payload)
    except BackendError as exc:
        return Outcome(result=OUTCOME_ERROR, raw_id=supplement_raw_id, reason=f"更新任务失败：{exc}")

    return Outcome(
        result=OUTCOME_AMENDED,
        raw_id=original_raw_id,
        title=payload.get("title"),
        due_at=payload.get("due_at"),
        due_text=payload.get("due_text"),
        due_confidence=float(payload.get("due_confidence") or 0.0),
        tokens=tokens,
        reason=f"补充了 {original.msg_id}（已更新那条任务，不是新建）",
    )



async def _upload_attachments(
    message: SourceMessage,
    backend: BackendClient,
    settings: Settings,
    resolver: AttachmentResolver,
) -> list[dict]:
    """把附件逐个变成后端认的形状。

    两层降级，都要在日志里说清楚：
      1. 找不到本地字节 → 按 `missing_attachment` 配置决定是留 CDN 地址还是丢掉；
      2. 上传失败 → 同样处理。
    """
    out: list[dict] = []
    for item in message.attachments:
        resolved: ResolvedAttachment | None = resolver.resolve(item)
        if resolved is None or not resolved.content:
            if settings.client_missing_attachment == "skip":
                logger.info(
                    "附件没有本地字节，按配置跳过：msg_id=%s file=%s（原因：%s）",
                    message.msg_id,
                    item.get("name") or item.get("md5") or "?",
                    (resolved.reason if resolved else "无法解析"),
                )
                continue
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
    "OUTCOME_AMENDED",
    "OUTCOME_ERROR",
    "OUTCOME_UNCHANGED",
    "merge_amendment",
    "process_amendment",
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
