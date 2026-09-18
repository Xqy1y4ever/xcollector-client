"""抽取：规则 + 大模型，并把"没把握"和"有分歧"如实标出来。

## 与 bot 的关系

`SYSTEM_PROMPT`、`PROMPT_VER`、`finalize()`、`cross_check()`、
`_merge_rule_disagreement()` 的语义都是从 `xcollector-bot/app/pipeline/extract.py`
与 `runner.py` 抄过来的，**必须保持一致**：同一条通知不论从实时链路（bot）
还是从聊天记录库（本客户端）进来，抽出来的字段应该一样，否则用户看到两个
不同的截止时间而不知道该信哪个。

与 bot 的两处**刻意不同**：

1. **修掉了 bot 里 `run_llm` 的一个真 bug**：那里失败分支引用了一个不存在的
   名字 `model`，于是"模型调用失败"会变成 `NameError`，把真实原因（超时？
   401？返回了坏 JSON？）从日志里顶掉，而且重试循环一次都不会跑。
   这里用 `target_model` 明确传参。
2. 没有多提供商网关，只有 OpenAI 兼容端点（见 `app/llm.py` 的说明）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from ..llm import LLMError, acompletion
from ..utils import iso_local, parse_iso_to_ms, to_local

logger = logging.getLogger(__name__)

PROMPT_VER = "llm-v2"  # v2: 新增 location 抽取（与 bot 一致）

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

SYSTEM_PROMPT = """你是一个官方通知抽取器。输入是 QQ 群里发布的官方通知原文。你的任务是判断它是不是一条需要人去做事的通知，并把其中的任务和截止时间抽取出来。

【最重要的规则】
1. 你只能依据原文，不得推测、不得补充原文里没有的信息。
2. evidence 字段必须是从原文中**逐字复制**的一段文字，用来支撑你的判断。如果原文里没有时间信息，evidence 就填支撑"这是一条通知"的那句话。evidence 不能为空，也不能是你自己总结的话。
3. 拿不准的时候，宁可把 due_at 填 null、只在 due_text 里保留原文的时间说法，也**不许猜**一个具体时间。猜错的时间比没有时间危害更大，因为用户会直接相信它。
4. 只有闲聊、回执（"收到""好的""谢谢老师"）、纯表情、广告、纯提问，才算 is_notification=false。凡是让人做事、或告知安排的信息，都算 true。
5. 一条消息里如果有多个截止时间，只抽取最主要的那一个，并在 summary 里说明其他安排。
6. location 必须是原文里**明确写出**的地点（教室、办公室、场馆、校区、线上平台等）。原文没写就填 null，**不许根据常识推测**（比如"交到班长那里"不算地点，"在教三201开会"才算）。

【相对时间】
原文里的"下周三""明天""本周五"等相对说法，一律以**消息发送时间**为锚点计算，不要用今天。
消息发送时间：{send_time}（{weekday}，时区 {tz}）

【输出格式】
只输出一个 JSON 对象，不要任何解释文字，不要 markdown 代码块：
{{
  "is_notification": true 或 false,
  "title": "不超过 20 字的动作标题，祈使句，例如「提交军训心得」",
  "summary": "一到两句话说明要求做什么，不超过 100 字",
  "location": "原文里明确写出的地点，例如「教三201」「学工办」；原文没写就填 null",
  "due_at": "ISO8601 时间，必须带时区偏移，例如 2025-09-12T23:59:00+08:00；无法确定时填 null",
  "due_text": "原文里的时间说法，逐字复制，例如「下周三前」；原文没有就填 null",
  "due_confidence": 0.0 到 1.0 之间的数字，表示你对 due_at 的把握，
  "evidence": "从原文逐字复制的一段文字"
}}

只说明日期、不说具体时间时，截止时间按当天 23:59 计算。"""


class LLMNotification(BaseModel):
    is_notification: bool = False
    title: str = ""
    summary: str = ""
    location: str | None = None
    due_at: str | None = None
    due_text: str | None = None
    due_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence: str = ""


def _strip_code_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _extract_json(text: str) -> dict:
    t = _strip_code_fence(text)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    start, end = t.find("{"), t.rfind("}")
    if start >= 0 and end > start:
        return json.loads(t[start : end + 1])
    raise ValueError(f"模型未返回合法 JSON：{t[:200]}")


def build_system_prompt(ts_ms: int, tz: str) -> str:
    local = to_local(ts_ms)
    return SYSTEM_PROMPT.format(
        send_time=local.strftime("%Y-%m-%d %H:%M") if local else "未知",
        weekday=_WEEKDAY_CN[local.weekday()] if local else "未知",
        tz=tz,
    )


def build_user_content(raw: dict, images: list[str] | None = None) -> Any:
    """用户消息。**正文只放原文** —— 源库里没有群名片，所以不假装有。"""
    header = (
        f"群名称：{raw.get('group_name') or raw.get('group_id')}\n"
        f"发送者：{raw.get('sender_name') or raw.get('sender_id')}\n"
        f"发送时间：{iso_local(raw['ts'])}\n"
        f"---\n原文：\n{raw.get('content') or ''}"
    )
    images = images or []
    if not images:
        return header
    parts: list[dict] = [{"type": "text", "text": header}]
    for url in images:
        parts.append({"type": "image_url", "image_url": {"url": url}})
    parts.append(
        {"type": "text", "text": "注意：上面的图片也是通知原文的一部分，其中的时间信息同样要抽取。"}
    )
    return parts


async def _call_model(
    *,
    model: str,
    api_base: str,
    api_key: str,
    raw: dict,
    images: list[str],
    tz: str,
    temperature: float,
    timeout: float,
    json_mode: bool,
) -> tuple[LLMNotification, int]:
    messages = [
        {"role": "system", "content": build_system_prompt(int(raw["ts"]), tz)},
        {"role": "user", "content": build_user_content(raw, images)},
    ]
    completion = await acompletion(
        api_base=api_base,
        api_key=api_key,
        model=model,
        messages=messages,
        temperature=temperature,
        timeout=timeout,
        json_mode=json_mode,
    )
    return LLMNotification(**_extract_json(completion.text)), completion.total_tokens


async def run_llm(
    *,
    model: str,
    api_base: str,
    api_key: str,
    raw: dict,
    images: list[str],
    tz: str,
    temperature: float = 0.0,
    timeout: float = 60.0,
    max_retries: int = 2,
) -> tuple[LLMNotification, int]:
    """带重试的模型调用。

    第一次用 JSON 模式；厂商不支持 `response_format` 时会报错，于是退回普通模式
    再试。**每次失败都把真实原因打出来**并重试 —— 这正是 bot 那边断掉的一环。
    """
    attempts = max(1, int(max_retries) + 1)
    last_error: Exception | None = None

    for i in range(attempts):
        json_mode = i == 0
        try:
            return await asyncio.wait_for(
                _call_model(
                    model=model,
                    api_base=api_base,
                    api_key=api_key,
                    raw=raw,
                    images=images,
                    tz=tz,
                    temperature=temperature,
                    timeout=timeout,
                    json_mode=json_mode,
                ),
                timeout=timeout + 10,
            )
        except ValidationError as exc:
            last_error = exc
            logger.warning("模型输出不符合 schema（第 %d/%d 次，model=%s）：%s", i + 1, attempts, model, exc)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "模型调用失败（第 %d/%d 次，model=%s，json_mode=%s）：%s",
                i + 1,
                attempts,
                model,
                json_mode,
                exc,
            )

    raise LLMError(f"模型 {model} 调用失败（试了 {attempts} 次）：{last_error}")


def _looks_like_parse_failure(due_at: int | None, source_ts: int) -> bool:
    """时间明显不合理 → 判定为解析失败，而不是真实时间（与 bot 一致）。"""
    if due_at is None:
        return False
    if due_at < source_ts - 24 * 3600 * 1000:
        return True
    if due_at > source_ts + 3 * 365 * 24 * 3600 * 1000:
        return True
    return False


def finalize(
    parsed: LLMNotification,
    raw: dict,
    model: str,
    *,
    extractor: str = "llm",
    tokens: int = 0,
) -> dict | None:
    """模型输出 → notification dict。返回 None 表示这条不该建条。"""
    evidence = re.sub(r"\s+", " ", (parsed.evidence or "")).strip()
    if not parsed.is_notification:
        return None
    if not evidence:
        # 硬约束：没有证据就不建条。防的是模型幻觉出一条无据的任务。
        logger.info("evidence 为空，丢弃该抽取结果 msg_id=%s", raw.get("message_id"))
        return None

    title = (parsed.title or "").strip()[:60] or (raw.get("content") or "")[:40]
    if not title:
        return None

    due_at = parse_iso_to_ms(parsed.due_at)
    if _looks_like_parse_failure(due_at, int(raw["ts"])):
        logger.info("due_at 明显不合理(%s)，降级为仅有 due_text", parsed.due_at)
        due_at = None
        due_confidence = 0.0
    else:
        due_confidence = float(parsed.due_confidence or 0.0)
        if due_at is None:
            due_confidence = 0.0

    return {
        "title": title,
        "summary": (parsed.summary or "").strip()[:300] or None,
        "location": (parsed.location or "").strip()[:60] or None,
        "due_at": due_at,
        "due_text": (parsed.due_text or "").strip() or None,
        "due_confidence": round(due_confidence, 3),
        "confidence": 0.8,
        "evidence": evidence,
        "extractor": extractor,
        "model": model,
        "prompt_ver": PROMPT_VER,
        "conflict": False,
        "candidates": [{"model": model, "due_at": due_at, "due_text": parsed.due_text}],
        "tokens": tokens,
    }


def cross_check(primary: dict, secondary: dict, secondary_model: str) -> None:
    """就地把第二个模型的结果写进 primary 并标出分歧（与 bot 一致）。"""
    primary.setdefault("candidates", []).append(
        {
            "model": secondary_model,
            "due_at": secondary.get("due_at"),
            "due_text": secondary.get("due_text"),
        }
    )
    a, b = primary.get("due_at"), secondary.get("due_at")
    if a is None and b is None:
        return
    if a is None or b is None:
        conflict = True
    else:
        conflict = abs(int(a) - int(b)) > 60_000
    if conflict:
        primary["conflict"] = True
        primary["due_confidence"] = min(float(primary.get("due_confidence") or 0), 0.5)
        logger.info("交叉验证冲突：%s=%s vs %s=%s", primary.get("model"), a, secondary_model, b)


def merge_rule_disagreement(llm_result: dict | None, rule_result: dict | None, model: str) -> dict | None:
    """模型说"不是通知"、但规则认为有明确时间 → 保留条目并标冲突（与 bot 一致）。

    方向是刻意的：宁可多推一条让人一键否决，也不能漏掉一条真通知。
    """
    if llm_result is not None:
        return llm_result
    if rule_result is None:
        return None
    merged = dict(rule_result)
    merged["conflict"] = True
    merged["due_confidence"] = min(float(merged.get("due_confidence") or 0), 0.5)
    merged["candidates"] = [
        {"model": "rule-engine", "due_at": merged.get("due_at"), "due_text": merged.get("due_text")},
        {"model": model, "due_at": None, "due_text": None, "note": "模型判定为非通知"},
    ]
    return merged
