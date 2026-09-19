"""Web UI 的 HTTP 服务：标准库 `http.server` + 一个静态页面，**零新依赖**。

## 接口

    GET  /                 页面（内嵌 CSS/JS 的单个 HTML）
    GET  /api/state        一次拿全：配置摘要、源库、镜像、未读、后端身份、运行状态
    GET  /api/config       当前 `.env` 的值（密钥类抹空）+ 不是配置项的键
    POST /api/config       写 `.env`（只写配置项；密钥留空 = 不改）
    POST /api/config/cleanup  把不是配置项的键注释掉
    POST /api/run          跑一轮（mode=once）或开始自动跑（mode=auto）
    POST /api/stop         停止自动跑（正在跑的那一轮做完了就停）
    POST /api/mark-unread  把镜像里的记录标成未读（下一轮重抽一遍），可顺带跑一轮
    GET  /api/log          最近的日志（内存里留最后 N 条）

页面用轮询（1.5s）而不是 SSE/WebSocket：逻辑少、断了也能自己恢复，
而这个页面的量级本来就不需要推送。

## 线程模型（为什么这么写）

`http.server` 是阻塞式多线程；而 `run_cycle` 是 `asyncio` 的。硬要在请求线程里
`asyncio.run()` 会让"跑一轮"挡住其它请求（页面就转圈、日志也刷不出来）。
所以：**每个请求一个线程**（`ThreadingHTTPServer`），入库跑在**自己的 worker 线程**
（那里面 `asyncio.run(...)`），主线程只读它的状态快照。一把锁保护"有没有在跑"。
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import webbrowser
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from ..backend_client import BackendClient, BackendError
from ..config import MAX_MESSAGES_PER_CYCLE, get_settings, unknown_env_keys
from ..mirror import STATE_REPROCESS, Mirror
from ..run import verify_identity
from ..source.ntmsg import SourceDatabase, SourceDatabaseError
from ..utils import now_ms
from .envfile import (
    SECRET_KEYS,
    comment_out_keys,
    config_values,
    env_override_keys,
    env_path,
    read_env,
    write_env_values,
)

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
LOG_BUFFER = 500
UI_HEADER = "X-XC-UI"
# 请求体的上限。这个页面的请求都是几十字节的配置值，1MB 已经很宽松了。
MAX_BODY_BYTES = 1_000_000


# ---------------------------------------------------------------------------
# 日志环形缓冲（给页面上的"日志"面板用）
# ---------------------------------------------------------------------------


class _RingHandler(logging.Handler):
    def __init__(self, size: int = LOG_BUFFER) -> None:
        super().__init__(level=logging.DEBUG)
        self._lines: list[dict] = []
        self._lock = threading.Lock()
        self._size = size

    def emit(self, record: logging.LogRecord) -> None:
        try:
            text = record.getMessage()
        except Exception:  # noqa: BLE001 - 日志本身不许把程序搞崩
            text = "<格式化失败>"
        if record.exc_info:
            text += " | " + logging.Formatter().formatException(record.exc_info).strip()
        item = {
            "ts": int(record.created * 1000),
            "level": record.levelname,
            "name": record.name,
            "text": text,
        }
        with self._lock:
            self._lines.append(item)
            if len(self._lines) > self._size:
                del self._lines[: len(self._lines) - self._size]

    def tail(self, since_ts: int = 0) -> list[dict]:
        with self._lock:
            return [item for item in self._lines if item["ts"] > since_ts]


def install_log_buffer() -> _RingHandler:
    """把环形缓冲挂到根 logger（只挂一次）。"""
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, _RingHandler):
            return handler
    handler = _RingHandler()
    root.addHandler(handler)
    return handler


# ---------------------------------------------------------------------------
# 运行控制（worker 线程 + 一次一个）
# ---------------------------------------------------------------------------


@dataclass
class Runner:
    """入库的运行状态。**一次只允许一件事在跑**（once 或 auto）。"""

    lock: threading.Lock = field(default_factory=threading.Lock)
    mode: str = "idle"            # idle | once | auto
    started_at: int = 0
    cycles: int = 0
    stop_requested: bool = False
    last_report: dict = field(default_factory=dict)
    last_error: str = ""
    thread: threading.Thread | None = None

    # 用不可变快照跨线程读状态：页面永远看到的是自洽的一组值
    def snapshot(self) -> dict:
        with self.lock:
            return {
                "mode": self.mode,
                "started_at": self.started_at,
                "cycles": self.cycles,
                "stop_requested": self.stop_requested,
                "busy": self.mode != "idle",
                "last_report": dict(self.last_report),
                "last_error": self.last_error,
            }

    def _set(self, **fields: Any) -> None:
        with self.lock:
            for key, value in fields.items():
                setattr(self, key, value)


RUNNER = Runner()


def _report_dict(report) -> dict:
    return {
        "scanned": report.scanned,
        "processed": report.processed,
        "unchanged": report.unchanged,
        "recovered": report.recovered,
        "reprocessed": report.reprocessed,
        "skipped_whitelist": report.skipped_whitelist,
        "dropped_whitelist_rows": report.dropped_whitelist_rows,
        "unread_before": report.unread_before,
        "outcomes": report.outcomes or {},
        "errors": list(report.errors[:10]),
        "prepared": report.prepared,
        "mirror_after": report.mirror_after or {},
        "finished_at": now_ms(),
    }


def _run_cycle_sync(mode: str) -> None:
    """worker 线程里跑（`asyncio.run` 自己的事件循环）。"""
    from ..main import _run_one_cycle  # 延迟导入：避免 main ↔ webui 的环

    settings = get_settings()
    interval = max(5, int(settings.client_poll_seconds))
    try:
        while True:
            try:
                report = _run_one_cycle(settings)
                RUNNER._set(last_report=_report_dict(report), last_error="")
                logging.getLogger("xcollector.webui").info(
                    "跑完一轮：扫了 %d 条，处理 %d 条，错误 %d 条",
                    report.scanned,
                    report.processed,
                    len(report.errors),
                )
            except Exception as exc:  # 单轮失败不能把自动模式打死
                RUNNER._set(last_error=f"{type(exc).__name__}: {exc}")
                logging.getLogger("xcollector.webui").exception("这一轮失败：%s", exc)
            RUNNER._set(cycles=RUNNER.cycles + 1)
            if mode == "once" or RUNNER.stop_requested:
                break
            # 自动模式：按 CLIENT_POLL_SECONDS 等下一轮，期间可以被打断
            deadline = time.monotonic() + interval
            while time.monotonic() < deadline:
                if RUNNER.stop_requested:
                    break
                time.sleep(min(0.5, max(0.05, deadline - time.monotonic())))
            if RUNNER.stop_requested:
                break
    finally:
        RUNNER._set(mode="idle", stop_requested=False, thread=None)


def _invalidate_state_cache() -> None:
    """状态变了就丢掉缓存（不然页面会拿着 3 秒前的快照说"没在跑"）。"""
    _STATE_CACHE.update(at=0, value=None)


def start_run(mode: str) -> tuple[bool, str]:
    """启动一次运行。返回 `(是否启动, 说明)`。"""
    with RUNNER.lock:
        if RUNNER.mode != "idle":
            return False, f"已经在跑（{RUNNER.mode}），先停掉再来"
        RUNNER.mode = mode
        RUNNER.started_at = now_ms()
        RUNNER.stop_requested = False
        RUNNER.last_error = ""
        thread = threading.Thread(target=_run_cycle_sync, args=(mode,), daemon=True)
        RUNNER.thread = thread
    thread.start()
    _invalidate_state_cache()
    return True, f"已开始（{mode}）"


def stop_run() -> str:
    """请求停止。正在跑的那一轮**不会被中断**（它最多 500 条）。"""
    with RUNNER.lock:
        if RUNNER.mode == "idle":
            return "现在没在跑"
        RUNNER.stop_requested = True
        mode = RUNNER.mode
    _invalidate_state_cache()
    return f"已请求停止（{mode}）：当前这一轮跑完就停"


# ---------------------------------------------------------------------------
# 状态快照
# ---------------------------------------------------------------------------


async def _collect_state() -> dict:
    settings = get_settings()
    state: dict[str, Any] = {
        "config": {
            "backend_base": settings.backend_base,
            "token_is_user": settings.is_user_token,
            "source": str(settings.ntmsg_export_path),
            "mirror": str(settings.resolved_mirror_path),
            "extractor": settings.client_extractor,
            "whitelist_active": settings.whitelist_active,
            "groups": sorted(settings.group_whitelist_map),
            "senders": sorted(settings.sender_whitelist_map),
            "poll_seconds": settings.client_poll_seconds,
            "pipeline": settings.ntmsg_pipeline_enabled,
            "attachment_root": str(settings.resolved_attachment_root or ""),
            "max_per_cycle": MAX_MESSAGES_PER_CYCLE,
        },
        "env": {"path": str(env_path()), "exists": env_path().exists()},
        "unknown_keys": unknown_env_keys(),
    }

    db = SourceDatabase(settings.ntmsg_export_path)
    try:
        info = db.inspect()
        state["source"] = {
            "ok": True,
            "path": str(settings.ntmsg_export_path),
            "rows": info["rows"],
            "absent": info["absent"],
            "latest_ts": (db.latest() or [None])[0],
        }
    except SourceDatabaseError as exc:
        state["source"] = {"ok": False, "path": str(settings.ntmsg_export_path), "error": str(exc)}

    mirror = Mirror(settings.resolved_mirror_path)
    try:
        stats = mirror.stats()
        state["mirror"] = {
            "path": str(settings.resolved_mirror_path),
            **stats,
            "unfinished": len(mirror.unfinished()),
            # 单独给一个数：它和"没读过的"是两件事 —— 这些消息在镜像里**有**记录，
            # 只是被（人或上一轮）要求重做。页面上不显示的话，用户点了"标为未读"
            # 之后会看到"没读过 0 条"，以为没生效。
            "reprocess": int(stats["by_state"].get(STATE_REPROCESS, 0)),
            "watermark": mirror.watermark(),
        }
    except Exception as exc:  # noqa: BLE001 - 状态页不该因为镜像坏了就打不开
        state["mirror"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # **正在跑一轮的时候不去读源库。** 那两个计数要开连接读导出库，而导出那一轮
    # 收尾要把它的日志模式切回 DELETE（需要独占）—— 页面每 1.5 秒轮询一次，
    # 正好撞上就报 "database is locked"，整轮白跑（2026-09-19 用户日志里那次）。
    # 页面本来也不需要跑的中间那几秒的数字。空着必须在页面上说明原因（见下）。
    busy = RUNNER.snapshot()["busy"]

    if state["source"].get("ok"):
        groups = tuple(settings.group_whitelist_map)
        senders = tuple(settings.sender_whitelist_map)
        state["config"]["groups"] = sorted(groups)
        state["config"]["senders"] = sorted(senders)
        if busy:
            state["unread"] = None
            state["matching"] = None
            state["scope"] = "whitelist" if (groups or senders) else "all"
        else:
            try:
                # 白名单**下推到 SQL**：这两个数都只算白名单内的消息。
                # 页面上要能同时看到"白名单内共 N 条"和"没读过的 M 条" ——
                # 配了白名单的人不该看到"还有 77 万条没读过"。
                state["matching"] = db.count_matching(groups=groups, senders=senders)
                state["unread"] = db.count_unread(
                    settings.resolved_mirror_path, groups=groups, senders=senders
                )
                state["scope"] = "whitelist" if (groups or senders) else "all"
            except SourceDatabaseError as exc:
                state["unread"] = None
                state["source"]["unread_error"] = str(exc)

    if busy:
        state["counts_skipped"] = "正在跑一轮：这几个数先不统计（免得和导出抢同一个库），跑完自动出来"

    backend = BackendClient(settings)
    try:
        who = await verify_identity(backend, settings)
        state["backend"] = {
            "reachable": True,
            "scope": who.get("scope"),
            "qq": (who.get("user") or {}).get("qq"),
            "user_id": (who.get("user") or {}).get("id"),
        }
    except Exception as exc:  # noqa: BLE001 - 后端不可达是常见状态，页面要照常打开
        state["backend"] = {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        await backend.close()

    state["run"] = RUNNER.snapshot()
    return state


# 状态缓存：页面每 1.5 秒轮询一次，但**没必要每次都去问后端**（那会变成一台机器上
# 每秒几百毫秒的无效请求）。缓存 3 秒，同时保证"刚点完跑一轮"能看到最新结果。
_STATE_CACHE: dict[str, Any] = {"at": 0, "value": None}
_STATE_CACHE_SECONDS = 3.0


def collect_state(*, fresh: bool = False) -> dict:
    now = time.monotonic()
    if not fresh and _STATE_CACHE["value"] is not None and now - _STATE_CACHE["at"] < _STATE_CACHE_SECONDS:
        return _STATE_CACHE["value"]
    value = asyncio.run(_collect_state())
    _STATE_CACHE.update(at=now, value=value)
    return value


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    server_version = "xcollector-client-ui"
    protocol_version = "HTTP/1.1"

    # ---- 基础 ----

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003 - 覆盖父类
        logger.debug("ui %s - %s", self.address_string(), fmt % args)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # 本机工具：不许被别的站点嵌进 iframe，也不给任何 CORS 放行
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cache-Control", "no-store")
        if status >= 400:
            # 出错就关连接：不关的话，"没读完请求体就回错"会让这个 keep-alive
            # 连接上剩下的字节被当成下一个请求的起始行（实测表现为 501）。
            # 请求体我们**已经**读干净了（见 `_read_json`），这里是第二道保险。
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload: Any, status: int = 200) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _error(self, message: str, status: int = 400) -> None:
        self._json({"error": message}, status)

    def _read_body(self) -> bytes:
        """把请求体**读干净**。

        这一步必须在任何提前返回（权限不足、路径不对……）**之前**做：`http.server`
        是 keep-alive 的，留下没读的字节会让下一个请求从这些字节开始解析，
        于是下一个请求变成一个"未知方法" → **501**。这个坑在 Windows 上看不出来
        （连接被回收得早），在 Linux/CI 上必现 —— 是 `check_webui` 在 WSL 里跑出来的。
        """
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        if length > MAX_BODY_BYTES:
            # 太大的请求体：读掉能读的，剩下的靠"出错就关连接"兜住
            self.rfile.read(MAX_BODY_BYTES)
            raise ValueError(f"请求体太大（{length} 字节）")
        return self.rfile.read(length)

    def _read_json(self) -> dict:
        """读请求体。**要求 `X-XC-UI` 头**（跨站请求发不出来，见模块说明）。"""
        raw = self._read_body()
        if self.headers.get(UI_HEADER) != "1":
            raise PermissionError(f"缺少 {UI_HEADER} 头：这个接口只给自带的页面用")
        if not raw:
            return {}
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"请求体不是 JSON：{exc}") from exc
        return body if isinstance(body, dict) else {}

    # ---- 路由 ----

    def do_GET(self) -> None:  # noqa: N802 - 父类约定
        path = self.path.split("?", 1)[0]
        try:
            if path in ("/", "/index.html"):
                return self._static("index.html")
            if path == "/api/state":
                return self._json(collect_state())
            if path == "/api/config":
                return self._json({
                    "values": config_values(),
                    "secrets": list(SECRET_KEYS),
                    "path": str(env_path()),
                    "unknown_keys": unknown_env_keys(),
                    # 进程环境变量优先级**高于** .env：写进文件也不会生效的那些键
                    "env_override": env_override_keys(),
                })
            if path == "/api/log":
                since = 0
                if "?" in self.path:
                    for part in self.path.split("?", 1)[1].split("&"):
                        if part.startswith("since="):
                            since = int(part[6:] or 0)
                return self._json({"lines": LOG_BUFFER_HANDLER.tail(since)})
            return self._error("没有这个接口", HTTPStatus.NOT_FOUND)
        except Exception as exc:  # noqa: BLE001 - 页面要能看到错误，而不是白屏
            logger.exception("UI GET %s 失败", path)
            return self._error(f"{type(exc).__name__}: {exc}", HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802 - 父类约定
        path = self.path.split("?", 1)[0]
        try:
            body = self._read_json()
        except PermissionError as exc:
            return self._error(str(exc), HTTPStatus.FORBIDDEN)
        except ValueError as exc:
            return self._error(str(exc), HTTPStatus.BAD_REQUEST)

        try:
            if path == "/api/config":
                values = {k: v for k, v in (body.get("values") or {}).items()}
                # 密钥留空 = 不改动（页面上永远显示空，不能因此把它清掉）
                for key in SECRET_KEYS:
                    if key in values and not str(values[key] or "").strip():
                        values.pop(key)
                saved = write_env_values(values)
                get_settings.cache_clear()   # 下一次运行就用新配置
                _invalidate_state_cache()
                logger.info("配置已保存：%s", ", ".join(saved) or "（没有变化）")
                return self._json({"saved": saved, "unknown_keys": unknown_env_keys()})
            if path == "/api/config/cleanup":
                touched = comment_out_keys(body.get("keys") or unknown_env_keys())
                logger.info("把不是配置项的键注释掉了：%s", ", ".join(touched) or "（没有）")
                return self._json({"touched": touched, "unknown_keys": unknown_env_keys()})
            if path == "/api/run":
                mode = str(body.get("mode") or "once")
                if mode not in ("once", "auto"):
                    return self._error("mode 只能是 once / auto")
                started, message = start_run(mode)
                return self._json({"started": started, "message": message}, 200 if started else 409)
            if path == "/api/stop":
                return self._json({"message": stop_run()})
            if path == "/api/mark-unread":
                return self._mark_unread(body)
            return self._error("没有这个接口", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return self._error(str(exc), HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            logger.exception("UI POST %s 失败", path)
            return self._error(f"{type(exc).__name__}: {exc}", HTTPStatus.INTERNAL_SERVER_ERROR)

    # ---- 标为未读 ----

    def _mark_unread(self, body: dict) -> None:
        """把镜像里的记录标成未读（= 下一轮重新处理它们）。

        三条刻意的限制：

        * **正在跑的时候拒绝**（409）：一边跑一边标，正在处理的那几条会被这一轮的
          结果（done/skipped）覆盖掉，用户以为标上了、其实只标了一半。
        * **默认不顺手跑一轮**（`run` 得显式给 true）：重抽要花模型的钱，
          这个动作本身只该改状态。页面上那个按钮会显式传 true（它就叫
          "标为未读并重新处理"），并且先弹一个确认框把要花多少钱说清楚。
        * **数要说全**：返回"本次新标记"和"标完之后总共待重抽"两个数。只回一个
          "标了 0 条"会让人以为没生效，而其实是它们早就在队列里了。
        """
        if RUNNER.mode != "idle":
            return self._error(
                "正在跑（%s）：先等这一轮跑完再标 —— 一边跑一边标，"
                "正在处理的那几条会被这一轮的结果覆盖掉，看起来像标上了其实没有。" % RUNNER.mode,
                HTTPStatus.CONFLICT,
            )
        msg_ids = body.get("msg_ids")
        if msg_ids is not None and not isinstance(msg_ids, list):
            return self._error("msg_ids 要么不给（= 全部），要么给一个数组")

        settings = get_settings()
        store = Mirror(settings.resolved_mirror_path)
        try:
            result = store.mark_unread(msg_ids)
        except Exception as exc:  # noqa: BLE001 - 镜像坏了要在页面上看得见
            logger.exception("标为未读失败")
            return self._error(f"{type(exc).__name__}: {exc}", HTTPStatus.INTERNAL_SERVER_ERROR)

        logger.info(
            "标为未读：本次 %d 条，镜像是共 %d 条等着重抽（下一轮会重新调用抽取）",
            result["marked"],
            result["total"],
        )
        _invalidate_state_cache()

        payload: dict[str, Any] = dict(result)
        payload["message"] = (
            f"已标 {result['marked']} 条为未读；镜像里共有 {result['total']} 条等着重新处理"
        )
        if body.get("run"):
            started, message = start_run("once")
            payload["run_started"] = started
            payload["run_message"] = message
            if not started:
                # 上面已经挡了 busy，这里挡的是"同一瞬间被别的东西抢走了"
                payload["message"] += f"（没能开始跑：{message}）"
        return self._json(payload)

    # ---- 静态 ----

    def _static(self, name: str) -> None:
        target = (STATIC_DIR / name).resolve()
        if not str(target).startswith(str(STATIC_DIR)) or not target.exists():
            return self._error("没有这个文件", HTTPStatus.NOT_FOUND)
        suffix = target.suffix.lower()
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
        }.get(suffix, "application/octet-stream")
        self._send(HTTPStatus.OK, target.read_bytes(), ctype)


LOG_BUFFER_HANDLER = install_log_buffer()


def serve(host: str = "127.0.0.1", port: int = 8787, *, open_browser: bool = True) -> int:
    """启动 Web UI（阻塞）。返回进程退出码。"""
    loopback = host in ("127.0.0.1", "localhost", "::1")
    if not loopback:
        logger.warning(
            "⚠️ Web UI 绑在 %s（不是 127.0.0.1）：这个页面能读到你的令牌、改配置、"
            "触发入库，而且**没有鉴权**。除本机以外的任何地址都不该这么用。",
            host,
        )
    if not read_env() and not env_path().exists():
        logger.info("还没有 .env：页面上填完保存就行（会写到 %s）", env_path())

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    url = f"http://{host if loopback else host}:{port}/"
    print(f"Xcollector 客户端 Web UI: {url}")
    print("  配置、跑一轮、看日志都在这个页面里。Ctrl+C 退出。")
    if open_browser and loopback:
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001 - 打不开浏览器不影响服务
            logger.debug("打不开浏览器，手动访问 %s 即可", url)
    try:
        httpd.serve_forever(poll_interval=0.3)
    except KeyboardInterrupt:
        print("\n收到中断，服务退出（正在跑的那一轮会被放弃）")
    finally:
        httpd.server_close()
    return 0
