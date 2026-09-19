"""后端客户端：**只用 UserToken**，只调用户令牌允许调的那些接口。

## 为什么这份文件里的方法这么少

后端的权限模型是「按层」的（见 `xcollector-backend/docs/api.md` 的「谁能写什么」）：

| 写什么 | UserToken 行不行 |
|---|---|
| 通知 / 统计 / 缺口告警 / 自己的键值 | ✅ 归属被强制成自己 |
| 共享层：原文 / 群状态 | ✅ 但**必须先订阅这个 (群, 发送者)** |
| 附件字节 | ✅ 没有归属可查 |
| 发邀请码/验证码、列用户、改机器字段、删通知、投递名单、digest-log | ❌ 403 |

所以这里**故意没有** `list_users` / `request_verify_code` / `find_subscribers` /
`delete_notification` 这些方法：不是"还没实现"，而是客户端本来就不该有这些能力。
想加方法之前先回去看那张表 —— 加了之后 e2e 会用 403 把它挡回来。

## 绝不带 `user_id`

用户令牌下后端的 `resolve_owner` 会**无视** `user_id` 参数，永远用自己的身份。
所以这里一个 `user_id` 都不传：传了只会让人误以为"这个客户端能替别人写"。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .config import BACKEND_MAX_RETRIES, BACKEND_TIMEOUT, Settings, get_settings

logger = logging.getLogger(__name__)

RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 8.0
# 这些状态码重试有意义（后端刚重启时路由可能还没挂上 / 限流）
RETRYABLE_STATUS = {404, 405, 408, 425, 429, 500, 502, 503, 504}


class BackendError(RuntimeError):
    """调后端失败的基类。"""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class BackendRejected(BackendError):
    """后端明确拒绝（4xx，且重试改变不了结果）。"""


class BackendUnavailable(BackendError):
    """暂时不可达 / 服务端错误。"""


def _detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except Exception:
        return (resp.text or "").strip()[:200]
    if isinstance(body, dict):
        return str(body.get("detail") or body)[:300]
    return str(body)[:300]


def _is_loopback(url: str) -> bool:
    """`BACKEND_BASE_URL` 指的是本机吗（127.0.0.1 / localhost / ::1 / 0.0.0.0）。"""
    try:
        host = (httpx.URL(url).host or "").lower()
    except Exception:  # noqa: BLE001 - 解析不了就当不是本机（照常走系统代理）
        return False
    return host in ("127.0.0.1", "localhost", "::1", "0.0.0.0")


class BackendClient:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        headers: dict[str, str] = {}
        if self.settings.client_token:
            headers["Authorization"] = f"Bearer {self.settings.client_token}"
        # **本机后端不走代理**：httpx 默认 `trust_env=True`，而它会读 Windows 注册表里的
        # 系统代理（装过 Clash / V2Ray 之类工具的机器上常留着一条 `127.0.0.1:7890`）。
        # 那个代理没开着的时候，连 `http://127.0.0.1:8000` 都会被发过去、然后连接被拒 ——
        # 症状是"后端明明在本机跑着，客户端却说连不上"。公网后端照旧走系统代理。
        self._client = httpx.AsyncClient(
            base_url=self.settings.backend_base,
            timeout=BACKEND_TIMEOUT,
            headers=headers,
            trust_env=not _is_loopback(self.settings.backend_base),
        )

    async def close(self) -> None:
        """关掉连接池。**不抛异常**。

        关闭是收尾动作：它失败说明不了"刚才那些写入没成功"，所以不该把一轮已经
        做完的结果说成失败。历史上这里抛过一次 `RuntimeError: Event loop is closed`
        （在另一个事件循环里关连接池），那一次把**跑完的一轮**整个吞掉了 ——
        所以这里如实记一行 WARNING，然后让调用方继续。
        """
        try:
            await self._client.aclose()
        except Exception as exc:  # noqa: BLE001 - 收尾失败不该盖掉业务结果
            logger.warning("关闭后端连接池失败（已经做完的事不受影响）：%s", exc)

    # ------------------------------------------------------------------
    # 底层请求
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        params: Any = None,
        files: Any = None,
        data: Any = None,
        purpose: str = "",
        retries: int = 0,
    ) -> httpx.Response:
        attempts = max(1, int(retries) + 1)
        delay = RETRY_BASE_DELAY
        last_error: BackendError | None = None

        for attempt in range(1, attempts + 1):
            try:
                resp = await self._client.request(
                    method, url, json=json, params=params, files=files, data=data
                )
            except Exception as exc:
                last_error = BackendUnavailable(f"{type(exc).__name__}: {exc}")
            else:
                if 200 <= resp.status_code < 300:
                    return resp
                detail = _detail(resp)
                if resp.status_code not in RETRYABLE_STATUS:
                    raise BackendRejected(f"HTTP {resp.status_code}: {detail}", resp.status_code)
                last_error = BackendUnavailable(f"HTTP {resp.status_code}: {detail}", resp.status_code)

            if attempt < attempts:
                logger.warning(
                    "调用后端失败（%s，第 %d/%d 次）：%s，%.0fs 后重试",
                    purpose or url,
                    attempt,
                    attempts,
                    last_error,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_DELAY)

        assert last_error is not None
        raise last_error

    async def _json(self, method: str, url: str, **kwargs: Any) -> Any:
        resp = await self._request(method, url, **kwargs)
        if not resp.content:
            return {}
        try:
            return resp.json()
        except Exception:
            return {}

    def _write(self, method: str, url: str, **kwargs: Any) -> Any:
        kwargs.setdefault("retries", BACKEND_MAX_RETRIES)
        return self._request(method, url, **kwargs)

    # ------------------------------------------------------------------
    # 身份与订阅
    # ------------------------------------------------------------------

    async def whoami(self) -> dict:
        """GET /api/me。启动自检用：令牌不对这里就会 401。

        同时把 `scope` 检查出来 —— 服务令牌也会返回 200，所以**必须看 scope**，
        不能只看状态码。
        """
        return await self._json("GET", "/api/me", purpose="me", retries=0)

    async def list_notifications(self, *, limit: int = 2000) -> list[dict]:
        """GET /api/notifications —— 自己那份。

        用途只有一个：游标丢了之后重建"这条源消息已经处理过"的集合，
        免得把整段历史重新抽一遍（那是真金白银的模型调用）。
        """
        body = await self._json(
            "GET",
            "/api/notifications",
            params={"status": "all", "limit": int(limit)},
            purpose="notifications",
        )
        if isinstance(body, dict):
            return body.get("notifications") or []
        return body or []

    # ------------------------------------------------------------------
    # 原始层（写进哪一层由**令牌**决定：用户令牌 → 自己那份 user_raw_message，
    # 服务令牌 → 共享的 raw_message。客户端不需要、也做不到写共享层）
    # ------------------------------------------------------------------

    async def create_message(self, payload: dict) -> dict:
        """POST /api/messages —— 写前日志（原文）。

        用户令牌写的原文进**他自己那层**，不需要订阅任何来源。401/403 说明令牌
        本身有问题（比如误用了服务令牌或令牌过期），客户端**不吞这个错**：
        它会被记成这条消息 failed 并进 report，下一轮重试。
        """
        resp = await self._write("POST", "/api/messages", json=payload, purpose="messages")
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def patch_message(self, raw_id: str, payload: dict) -> dict:
        """PATCH /api/messages/{id} —— 只改自己那一层里的行（state / attachments）。"""
        resp = await self._write(
            "PATCH", f"/api/messages/{raw_id}", json=payload, purpose="messages.patch"
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def upload_attachment(
        self, filename: str, content: bytes, content_type: str, source_url: str | None
    ) -> dict | None:
        """POST /api/attachments。

        **失败返回 None**：为了一张图把整条通知丢掉是本末倒置（和 bot 同样的取舍）。
        """
        form: dict[str, str] = {"filename": filename or "file"}
        if source_url:
            form["source_url"] = source_url
        try:
            resp = await self._request(
                "POST",
                "/api/attachments",
                files={"file": (filename or "file", content, content_type)},
                data=form,
                purpose="attachments",
                retries=BACKEND_MAX_RETRIES,
            )
        except BackendError as exc:
            logger.warning("上传附件失败 filename=%s：%s", filename, exc)
            return None
        body = resp.json()
        return body if isinstance(body, dict) else None

    # ------------------------------------------------------------------
    # 按用户的那一层
    # ------------------------------------------------------------------

    async def create_notification(self, payload: dict) -> dict:
        """POST /api/notifications —— 归属由令牌决定，这里不传 user_id。"""
        resp = await self._write(
            "POST", "/api/notifications", json=payload, purpose="notifications"
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def add_correction(
        self, notif_id: str, *, field: str, value: Any, actor: str = "client"
    ) -> dict:
        """POST /api/notifications/{id}/corrections —— 人工修正（只追加）。

        `field` 只允许 `title/summary/location/due_at/due_text/status`（后端会校验）。
        客户端只用它做一件事：**重抽之后判定"不再是通知"的，把原来那条归档**
        （`field="status", value="archived"`）—— 用户令牌删不了通知（那是服务令牌
        专属），但归档做得到，而且不丢数据。
        """
        resp = await self._write(
            "POST",
            f"/api/notifications/{notif_id}/corrections",
            json={"field": field, "value": value, "actor": actor},
            purpose="notifications.corrections",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def add_stats(self, day: str, fields: dict[str, int]) -> dict:
        cleaned = {k: int(v) for k, v in fields.items() if v}
        if not cleaned:
            return {}
        resp = await self._write(
            "POST", "/api/stats", json={"day": day, "fields": cleaned}, purpose="stats"
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def add_gap_alert(
        self, *, group_id: str, group_name: str | None, from_ts: int, to_ts: int, reason: str
    ) -> dict:
        resp = await self._write(
            "POST",
            "/api/gap-alerts",
            json={
                "group_id": str(group_id),
                "group_name": group_name,
                "from_ts": int(from_ts),
                "to_ts": int(to_ts),
                "reason": reason,
            },
            purpose="gap-alerts",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    # ------------------------------------------------------------------
    # 健康
    # ------------------------------------------------------------------

    async def health(self) -> dict:
        """GET /api/health —— **永远不抛异常**。"""
        result: dict[str, Any] = {"reachable": False, "base_url": self.settings.backend_base, "error": None}
        try:
            body = await self._json("GET", "/api/health", purpose="health", retries=0)
        except BackendError as exc:
            result["error"] = str(exc)
            return result
        if not isinstance(body, dict):
            result["error"] = "后端返回了非对象结构"
            return result
        result.update({"reachable": True, "ok": bool(body.get("ok")), "version": body.get("version")})
        return result
