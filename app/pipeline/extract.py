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
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from ..llm import LLMError, acompletion
from ..utils import iso_local, parse_iso_to_ms, to_local
from .timeparse import end_of_month

logger = logging.getLogger(__name__)

PROMPT_VER = "llm-v4"  # v4: "有人要做事"改成"该进任务板的官方通知"（群务/个人叮嘱/软邀请一票否决）

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

SYSTEM_PROMPT = """你是一个**官方通知**筛选器。输入是 QQ 群里发布的一条消息（群里有官方通知，也有日常闲聊和群务）。

【第一步：这条消息该不该进「任务板」】
判 true 要**同时**满足三条：
 1. 是**发给全群**的话（不是对某一个人的叮嘱、不是私人转告、不是替谁办事）；
 2. 有一个**具体要做的事**（提交/报名/缴费/领取/参加/填写/确认/集合/退选/登记/激活/购买……），
    或者一个**要记住的安排**（会议、考试、面试、体检、活动、选课时间……）；
 3. **能指出时间或地点**（「9月11日下午在鼓楼大礼堂」「8月31日中午12点前」），
    或者这是一条**正式口径的通知**（含「通知/公告/报名/截止/提交/材料/学籍/注册/缴费/务必」这类词）。
    三条一条都不占的零碎叮嘱判 false（「补证明」「收起来送行政北楼」「填完了要点击提交审核」）。

下面这些**不是**通知（is_notification=false），哪怕里面有「请」「要求」「全体」和时间词：
 1. **群务/群管理**：改群名片或群昵称、进群/退群/拉人、查收名单、看群公告或群文件
    （「请大家按照要求修改群名片」「还没进群的抓紧了」「查收班团干部名单」）
 2. **对某一个人说的话**：以 @某人 开头的叮嘱、私人转告、替谁办事
    （「@张三 请修改群名片」「叫你们高中老师寄到北大楼」）
 3. **口头催促**，没有具体事项和时间：「抓紧了」「别忘了」「尽快」
    —— 但对**全群**的、带具体动作的催办要判 true（「@全体成员 请大家今天完成基本信息填写」）。
 4. **可选参加/软邀请**：只有号召、没写时间地点或截止的（「欢迎没课的同学参加…」「欢迎大家报名参加！」）
    —— 但**写了时间/地点/报名截止的讲座、活动、比赛预告要判 true**（那是一个要记住的安排）：
    「【讲座预告】时间：9月2日19:00 地点：鼓楼校区大礼堂」= true。
 5. **纯提醒/纪律强调**，没有要做的事：「不要打开这类链接」「不能拍摄教官照片」
 6. **转发、链接分享、资料转存**：「下载地址如下」「课件我放群里了」
 7. 回执与附和、提问与追问、闲聊调侃、表情包、纯图片
 8. 已发生事情的回顾或总结：「今天上午的会开完了」「今天是训练第二天，大家继续加油」
 9. 对通知的讨论、猜测或转述：「我下周三可能去不了」「听说要交材料」
10. 非官方内容：广告、拼团、带货、招兼职、拉票、寻物、拼车、二手交易
11. 只有称呼或寒暄：「各位同学：」「大家注意身体」

- 一条消息里既有通知、后面又跟着一串「收到」刷屏时，看**通知那部分**：是通知就是 true，内容只抽通知里的。
- 拿不准就判 false，并在 reason 里写清为什么。**把群务和闲聊做成任务，比漏掉一条真通知更烦人。**
- 反过来，只要消息里有**具体的事 + 时间/地点（或正式通知口径）**，不要因为它写得随意就判 false。

【第二步：抽字段】
1. 只能依据原文，不得推测、不得补充原文里没有的信息。
2. evidence 必须是从原文中**逐字复制**的一段文字（is_notification=true 时不能为空）。
3. due_at 拿不准就填 null、只在 due_text 里保留原文说法，**不许猜**。猜错的时间比没有时间危害更大。
4. 一条消息里有多个截止时间时，只抽最主要的那一个，并在 summary 里提一句其他安排。
5. location 必须是原文里**明确写出**的地点；原文没写就填 null（「交到班长那里」不算地点，「在教三201开会」才算）。
6. title 用**祈使句**概括要做的事（例：「提交军训心得」），不要抄原话片段、不要带称呼，20 字以内。

【相对时间：照下面这张日历换算，不要自己推】
消息发送时间：{send_time}（{weekday}，时区 {tz}）
{calendar}
- 「下周三」= 下周那一行的周三；「下下周三」= 再下周那一行；「这周天/这周日」= 本周那一行的周日。
- 「本周末/这周末/周末」= 本周的周日；「下周末」= 下周的周日（「周末」如果今天是周日，就是今天）。
- 「月底/月末/本月内」= 本月最后一天 23:59；「下个月底」= 下个月最后一天 23:59。
- 「明天/后天/大后天」= 上面那行里的日期。
- 只说日期不说时刻 → 当天 23:59；「下午3点」→ 15:00；「晚上8点」→ 20:00。
- 如果按这张表算出来的日期**早于**消息发送时间（比如今天周五却说「本周三」），due_at 填 null、
  只在 due_text 里保留原话，**不要**自作主张改成下周。

【输出格式】
只输出一个 JSON 对象，不要任何解释文字，不要 markdown 代码块：
{
  "is_notification": true 或 false,
  "reason": "is_notification=false 时必填：为什么不是通知（20 字以内）；是通知时填空字符串",
  "title": "祈使句动作标题，不超过 20 字",
  "summary": "一到两句话说明要求做什么，不超过 100 字",
  "location": "原文里明确写出的地点；没有就填 null",
  "due_at": "ISO8601 时间，必须带时区偏移，例如 2026-09-23T23:59:00+08:00；无法确定时填 null",
  "due_text": "原文里的时间说法，逐字复制，例如「下周三前」；原文没有就填 null",
  "due_confidence": 0.0 到 1.0 之间的数字，表示你对 due_at 的把握,
  "evidence": "从原文逐字复制的一段文字"
}

【三个例子】
例1（消息发送时间 2026-09-16 15:00 周三）原文：「@全体成员 大家下周三前把军训心得交到班长那里」
输出：{"is_notification": true, "reason": "", "title": "提交军训心得", "summary": "下周三前把军训心得交给班长。", "location": null,
      "due_at": "2026-09-23T23:59:00+08:00", "due_text": "下周三前", "due_confidence": 0.85,
      "evidence": "大家下周三前把军训心得交到班长那里"}
例2（同一时间）原文：「欢迎大家入群，请大家按照要求修改群名片，格式是【年级 院系 姓名】」
输出：{"is_notification": false, "reason": "群务（改群名片），不是要进任务板的通知", "title": "", "summary": "", "location": null,
      "due_at": null, "due_text": null, "due_confidence": 0, "evidence": "请大家按照要求修改群名片"}
例3（同一时间）原文：「收到」「好的 谢谢老师」「我下周三可能去不了」
输出：{"is_notification": false, "reason": "回执与个人安排，没有要人做的事", "title": "", "summary": "", "location": null,
      "due_at": null, "due_text": null, "due_confidence": 0, "evidence": "收到"}
"""


class LLMNotification(BaseModel):
    is_notification: bool = False
    # 判 false 的原因（v3 起要求模型写出来）。它**不入库**，只写进日志/报告 ——
    # "为什么这条被跳过"必须能查，否则收紧判据就成了新的静默。
    reason: str = ""
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


def build_calendar(ts_ms: int, tz: str) -> str:
    """给模型一张**算好的日历**（本周/下周/再下周 + 今天/明天/后天 + 月底）。

    为什么要把日历替模型算好：中文相对时间（「下周三」「这周天」「月底」）要跨周/跨月做
    日期运算，模型经常算不出来或者算错 —— 而这件事对代码来说是**纯确定性的**。
    把结果直接摆在提示里（"下周：周一 09-22、周二 09-23……"），模型只需要查表，
    不需要推理；算错的概率就从"经常"降到"几乎不会"。
    """
    local = to_local(ts_ms)
    if local is None:
        return "（发送时间未知：相对时间一律无法换算 → due_at 必须填 null，只在 due_text 保留原话）"
    monday = (local - timedelta(days=local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    def day(dt) -> str:
        return dt.strftime("%m-%d")

    lines = []
    for label, offset in (("本周", 0), ("下周", 7), ("再下周", 14)):
        start = monday + timedelta(days=offset)
        lines.append(
            label + "：" + "、".join(f"{_WEEKDAY_CN[i]} {day(start + timedelta(days=i))}" for i in range(7))
        )
    lines.append(
        f"今天 {day(local)}（{_WEEKDAY_CN[local.weekday()]}）、"
        f"明天 {day(local + timedelta(days=1))}、后天 {day(local + timedelta(days=2))}"
    )
    lines.append(f"本月最后一天 {day(end_of_month(local))}、下月最后一天 {day(end_of_month(local, 1))}")
    return "\n".join(lines)


def render_system_prompt(ts_ms: int, tz: str) -> str:
    """把提示模板里的几个占位符换成真实值。

    用 `str.replace` 而不是 `str.format`：提示里有大段 JSON 示例（一堆花括号），
    走 format 的话每个 `{`/`}` 都要写成 `{{`/`}}` —— 少写一个就是运行时 KeyError，
    而且读起来完全看不出原样。replace 只认我们自己的四个占位符，多出来的花括号与它无关。
    """
    local = to_local(ts_ms)
    return (
        SYSTEM_PROMPT.replace("{send_time}", local.strftime("%Y-%m-%d %H:%M") if local else "未知")
        .replace("{weekday}", _WEEKDAY_CN[local.weekday()] if local else "未知")
        .replace("{tz}", tz)
        .replace("{calendar}", build_calendar(ts_ms, tz))
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
        {"role": "system", "content": render_system_prompt(int(raw["ts"]), tz)},
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
        # v3：模型要求写清"为什么不是通知"。它不入库，但**必须留在日志里** ——
        # 收紧判据之后，"这条被跳过了"得能查得到原因，否则就是换了个地方静默。
        logger.info(
            "模型判定为非通知（%s）msg_id=%s",
            (parsed.reason or "未说明原因").strip()[:60],
            raw.get("message_id"),
        )
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
    """模型说"不是通知"、但规则**看到了通知特征 + 一个明确时间** → 保留条目并标冲突。

    方向是刻意的：宁可多推一条让人一键否决，也不能漏掉一条真通知。

    两道门槛（v3 起）：

    1. 规则侧只有"时间词"没有"通知词"的消息，`rule_extract` 直接返回 None ——
       「@张三 明天」「我下周三可能去不了」不会再变成任务；
    2. 这里**必须有一个解析出来的 due_at** 才反着推。函数名和文档一直写的是
       "规则认为有**明确时间**"，但代码原来只要求"规则有结果"，于是
       「刚刚的会议记录我发群里了」这种（命中"会议"、没有任何时间）的历史消息，
       在模型正确判成闲聊之后又被拉回来当成一条冲突任务。
    """
    if llm_result is not None:
        return llm_result
    if rule_result is None:
        return None
    if rule_result.get("due_at") is None:
        logger.info(
            "模型判为非通知，规则只命中了通知词、没解析出时间 → 尊重模型，不建条（规则证据：%r）",
            str(rule_result.get("evidence") or "")[:60],
        )
        return None
    merged = dict(rule_result)
    merged["conflict"] = True
    merged["due_confidence"] = min(float(merged.get("due_confidence") or 0), 0.5)
    merged["candidates"] = [
        {"model": "rule-engine", "due_at": merged.get("due_at"), "due_text": merged.get("due_text")},
        {"model": model, "due_at": None, "due_text": None, "note": "模型判定为非通知"},
    ]
    return merged


def fill_due_from_rule(
    llm_result: dict | None, rule_result: dict | None, *, source_ts: int
) -> dict | None:
    """模型判了"是通知"、却没给出时间，而规则引擎在同一段原文里算出了时间 → 用规则的。

    ## 为什么要这么分工

    **是不是通知**靠模型（它读得懂语气和上下文），**相对时间的日期换算**靠规则
    （`timeparse.parse_due` 是纯确定性代码：「下周三」「这周天」「月底前」这种跨周跨月的
    运算，模型经常算不出来或算错，而代码不会）。

    以前规则的结果只在"模型说不是通知"时才用得上，于是最典型的一类通知 ——
    「大家下周三前把军训心得交到班长那里」—— 只要模型没算出来，页面上就只剩一个
    due_text（显示成"待确认"），用户的原话是"相对时间模型抽取不出来"。

    诚实边界（都很重要）：

      - 只填 `due_at`，**判定权仍在模型手里**（模型说是通知才走到这里）；
      - `due_confidence` 用规则引擎自己的把握，并封顶 0.8：这是"模型没给、规则补的"，
        不该看起来比模型亲自算的更有把握（前端会按「约」显示）；
      - 规则的值写进 `candidates`，详情抽屉里能看出**这个时间是谁给的**；
      - `extractor` 记成 `llm+rule`，一眼能看出这条被规则补过；
      - 规则算出来的时间如果**早于消息发送时间一天以上**（比如周五的消息里提到"本周三"），
        按解析失败处理、不填 —— 与模型那条路 `_looks_like_parse_failure` 同一个判据。
    """
    if llm_result is None or rule_result is None:
        return llm_result
    if llm_result.get("due_at") is not None:
        return llm_result
    rule_due = rule_result.get("due_at")
    if rule_due is None:
        return llm_result
    if _looks_like_parse_failure(int(rule_due), int(source_ts)):
        logger.info(
            "规则引擎算出的 due_at(%s) 早于消息时间，判定为解析失败，不采用", rule_due
        )
        return llm_result

    filled = dict(llm_result)
    filled["due_at"] = int(rule_due)
    filled["due_text"] = llm_result.get("due_text") or rule_result.get("due_text")
    filled["due_confidence"] = round(
        min(float(rule_result.get("due_confidence") or 0.0), 0.8), 3
    )
    filled["extractor"] = f"{llm_result.get('extractor') or 'llm'}+rule"
    filled["candidates"] = list(llm_result.get("candidates") or []) + [
        {
            "model": "rule-engine",
            "due_at": int(rule_due),
            "due_text": rule_result.get("due_text"),
            "note": "模型没给出时间，由规则引擎按消息发送时间换算",
        }
    ]
    logger.info(
        "模型没给出 due_at，用规则引擎的结果补上：%s（due_text=%r，confidence=%s）",
        filled["due_at"],
        filled["due_text"],
        filled["due_confidence"],
    )
    return filled
