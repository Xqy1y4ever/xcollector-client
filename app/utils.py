"""小工具：时间格式化、ID、日志预览。

`iso_local` / `parse_iso_to_ms` / `to_local` 的语义与 bot 的 `app/utils.py`
**必须一致** —— 它们决定了"消息时间"怎么进 prompt、模型返回的 ISO 时间怎么
回到毫秒。不一致的话，同一条通知在两条链路上会得到不同的截止时间。
"""

from __future__ import annotations

import json
import logging
import secrets
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def now_ms() -> int:
    """当前时间（毫秒整数）。契约里所有时间戳都是这个形状。"""
    return int(time.time() * 1000)


def new_id(prefix: str = "") -> str:
    """时间有序的短 ID（前 13 位为毫秒时间戳）。"""
    return f"{prefix}{now_ms():013d}{secrets.token_hex(4)}"


def local_day(ts_ms: int | None = None, *, tz: Any = None) -> str:
    """**服务器本地时区**下的 YYYY-MM-DD（统计按天累加用的 key）。"""
    stamp = now_ms() if ts_ms is None else ts_ms
    return datetime.fromtimestamp(stamp / 1000, tz=tz).strftime("%Y-%m-%d")


def to_local(ts_ms: int | None) -> datetime | None:
    """毫秒时间戳 → 本地时区的 datetime。无法转换时返回 None。"""
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def iso_local(ts_ms: int | None) -> str:
    """毫秒时间戳 → 带时区偏移的 ISO8601（放进 prompt 给模型当锚点）。"""
    if ts_ms is None:
        return "未知"
    dt = to_local(ts_ms)
    if dt is None:
        return "未知"
    return dt.astimezone().isoformat(timespec="seconds")


def parse_iso_to_ms(value: Any) -> int | None:
    """模型返回的 ISO8601 → 毫秒整数。**解析不出来就返回 None，绝不猜。**

    没有时区偏移的字符串按本地时区解释（模型偶尔会漏掉偏移，这时按本地时区
    理解比当成 UTC 更接近原意）。
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("null", "none"):
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        logger.info("due_at 不是合法 ISO8601，已丢弃：%r", text[:60])
        return None
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return int(dt.timestamp() * 1000)


def preview(text: Any, limit: int | None = None) -> str:
    """把文本压成一行、截断，供日志使用（日志里打印整条消息没有意义）。

    `limit` 省略时取 `CLIENT_LOG_PREVIEW_CHARS`。**这个配置必须真的被读** ——
    一个"配了但没人用"的开关比没有更糟：用户以为他调过了。

    这里延迟导入 config：`utils` 被 config 之外的很多模块引用，模块级导入会绕成
    一个环，而延迟导入只多一次字典查找。
    """
    if limit is None:
        try:
            from .config import get_settings

            limit = int(get_settings().client_log_preview_chars or 60)
        except Exception:  # 配置还没就绪（例如被单独 import 做测试）
            limit = 60
    cleaned = " ".join(str(text or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: max(1, limit - 1)] + "…"


def json_text(value: Any, default: Any) -> str:
    try:
        return json.dumps(value if value is not None else default, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(default, ensure_ascii=False)
