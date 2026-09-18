"""最小的大模型调用层：只支持 OpenAI 兼容的 `/chat/completions`。

## 为什么比 bot 那套简单得多

bot 里有一个完整的网关（多提供商、Google 原生格式、按事件循环缓存客户端、
失败重试、密钥来源追踪……），因为它是**常驻服务**，配置面要宽。

客户端不需要那些：

  - 它是**批处理**的，一次跑一批然后退出（或睡很久），没有长连接要维护；
  - 它只需要"给一段文本，拿回一个 JSON"；
  - 提供商只需要一个：任何 OpenAI 兼容端点（DeepSeek、通义、OpenAI、自建
    vLLM/Ollama 都是这个形状）。

代价说清楚：**不支持 Google 原生格式**。要用 Gemini 就走它的 OpenAI 兼容端点，
或者把这个文件换成 bot 的网关。这是一处刻意的取舍，不是遗漏。

## 和其它模块一样的两条硬线

  - **密钥绝不出现在日志里**（连长度都不打，只打"配没配"）。
  - **失败绝不静默**：抛出去，由上层决定是降级到规则还是把这条标成降级。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass
class Completion:
    text: str
    total_tokens: int
    model: str


class LLMError(RuntimeError):
    """模型调用失败。消息里会带上真实原因（HTTP 状态 + 后端 detail）。"""


def describe_key(api_key: str) -> str:
    """日志里描述密钥配置状态 —— **永远不打密钥本身**。"""
    return "已配置" if (api_key or "").strip() else "**未配置**"


def _endpoint(api_base: str) -> str:
    base = (api_base or "").strip().rstrip("/")
    if not base:
        raise LLMError("没有配置 LLM_API_BASE")
    # 允许用户直接填到 /chat/completions
    if base.endswith("/chat/completions"):
        return base
    return base + "/chat/completions"


async def acompletion(
    *,
    api_base: str,
    api_key: str,
    model: str,
    messages: list[dict],
    temperature: float = 0.0,
    timeout: float = 60.0,
    json_mode: bool = True,
) -> Completion:
    """调一次模型。失败抛 `LLMError`，**不吞**。"""
    if not (api_key or "").strip():
        raise LLMError("没有配置 LLM_API_KEY")
    if not (model or "").strip():
        raise LLMError("没有配置 LLM_MODEL")

    payload: dict = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
    }
    if json_mode:
        # 多数 OpenAI 兼容端点支持它；不支持时上层会用 json_mode=False 再试一次。
        payload["response_format"] = {"type": "json_object"}

    url = _endpoint(api_base)
    # **本机端点不走系统代理**：httpx 默认 `trust_env=True`，会读 Windows 注册表里的
    # 系统代理（装过 Clash/V2Ray 的机器上常留着一条 `127.0.0.1:7890`）。那个代理没开着
    # 时，连本机自己起的模型服务（ollama / one-api 之类）都会连不上。公网厂商照旧走代理
    # —— 很多人正是靠代理才能访问 OpenAI/Google。
    local = (httpx.URL(url).host or "").lower() in ("127.0.0.1", "localhost", "::1", "0.0.0.0")
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=not local) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key.strip()}",
                    "Content-Type": "application/json",
                },
            )
    except Exception as exc:
        # 网络层错误：把类型和原文都带上，否则运维只能看到"调用失败"
        raise LLMError(f"请求 {url} 失败：{type(exc).__name__}: {exc}") from exc

    if resp.status_code >= 400:
        detail = (resp.text or "").strip()[:300]
        raise LLMError(f"模型返回 HTTP {resp.status_code}：{detail}")

    try:
        body = resp.json()
    except Exception as exc:
        raise LLMError(f"模型返回的不是 JSON：{(resp.text or '')[:200]}") from exc

    try:
        choice = (body.get("choices") or [])[0]
        text = str(choice["message"]["content"] or "")
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMError(f"模型响应里没有 choices[0].message.content：{str(body)[:200]}") from exc

    usage = body.get("usage") or {}
    try:
        tokens = int(usage.get("total_tokens") or 0)
    except (TypeError, ValueError):
        tokens = 0
    return Completion(text=text, total_tokens=tokens, model=str(body.get("model") or model))
