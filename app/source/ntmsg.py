"""读 `nt_msg_db_util` 导出的结构化库 `nt_msg_export.db`。

## 表结构从哪来的

不是猜的，是从 `nt_msg_db_util` 的 README 与 `db_docs/` 抄下来的（v1 快照）：

    group_messages
      msg_id        TEXT  主键          ← 源字段 40001，消息唯一 ID
      timestamp     INTEGER            ← 40050，**Unix 秒**
      direction     INTEGER            ← 40013
      sender_uid    TEXT               ← 40020
      sender_qq     TEXT               ← 40033
      group_id      TEXT               ← 40021，十进制群号字符串
      group_qq      TEXT
      msg_type / subtype / content_type
      text          TEXT               合并后的纯文本（FTS 来源）
      parse_status  TEXT               null / typed / wire_fallback / invalid
      content       TEXT               40800 解析出来的 JSON，带 type 鉴别字段

`content` 的 `type` 有：text / image / video / file / sticker / contact / reply /
forward / legacy_forward / call / sys / mixed。

## 这个读取器刻意做的几件事

1. **只读打开**。导出库可能正被另一个进程写（导出工具在跑），所以用
   `mode=ro` + `immutable=0` 打开，并在读失败时给出能看懂的错，而不是抛一个
   光秃秃的 `sqlite3.OperationalError: database is locked`。
2. **列名不写死单一版本**。先用 `PRAGMA table_info` 读实际列名，缺列时按"这一列
   没有"处理并记一行 warning —— 上游换版本加/改列时，客户端应该退化而不是崩。
   唯一真正必需的是 `msg_id` 和 `timestamp`。
3. **增量按 (timestamp, msg_id) 走**。不用 rowid（导出库重建后 rowid 会变），
   也不用 `msg_id >`（`msg_id` 是字符串，跨版本不一定单调）。
   `timestamp` 可能有大量并列，所以用元组比较，并**包含边界**（`>=`）——
   宁可重复读一条（后端幂等会挡住），也不能跳过同一秒里的其他消息。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# 真正必需的列。少了它们，这个库就不是我们认识的形状，直接报错比猜好。
REQUIRED_COLUMNS = ("msg_id", "timestamp")

# 其余列缺了就降级使用。
OPTIONAL_COLUMNS = (
    "sender_qq",
    "sender_uid",
    "group_id",
    "group_qq",
    "content_type",
    "text",
    "content",
    "parse_status",
)


class SourceDatabaseError(RuntimeError):
    """源库读不了 / 不是我们认识的形状。消息里会写清"该怎么办"。"""


@dataclass
class SourceMessage:
    """一条群消息（已经规整成入库用的形状）。"""

    msg_id: str
    timestamp: int  # Unix **秒**
    group_id: str
    sender_id: str
    sender_name: str | None
    text: str
    attachments: list[dict] = field(default_factory=list)
    parse_status: str | None = None
    raw_content: dict | None = None

    @property
    def ts_ms(self) -> int:
        return int(self.timestamp) * 1000


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return {str(r[1]) for r in rows}


def _json_or_none(text: Any) -> dict | None:
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def attachments_from_content(content: dict | None) -> list[dict]:
    """从 `content` JSON 里挑出附件。

    形状（`nt_msg_db_util` 的 `content` 是按 type 变的）：

        {"type":"image","filename":"x.jpg","width":1080,"cdn_url":"...","md5_hex":"..."}
        {"type":"file","filename":"a.pdf","filesize":1024,"md5_hex":"...","ext":".pdf"}
        {"type":"mixed","segments":[...]}   ← 一段一段往下找

    **不猜附件地址**：只有 `cdn_url` 时也照样记下来（那可能是死链，所以
    下载失败要有一条明确的日志与降级说明，不能安静地当没有）。
    """
    if not isinstance(content, dict):
        return []

    out: list[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        kind = str(node.get("type") or "")
        if kind in ("image", "video", "file", "sticker"):
            item: dict[str, Any] = {"type": "image" if kind == "sticker" else kind}
            for src, dst in (
                ("filename", "name"),
                ("cdn_url", "url"),
                ("md5_hex", "md5"),
                ("filesize", "size"),
                ("width", "width"),
                ("height", "height"),
                ("ext", "ext"),
            ):
                if node.get(src) is not None:
                    item[dst] = node[src]
            # 一条既没有 url 也没有 filename 的附件是没用的：记下来只会让
            # 前端显示一个打不开的空条目。
            if item.get("url") or item.get("name") or item.get("md5"):
                out.append(item)
            return
        # mixed / reply / forward：往下找
        for key in ("segments", "content", "items", "children"):
            if key in node:
                walk(node[key])

    walk(content)
    return out


class SourceDatabase:
    """`nt_msg_export.db` 的只读读取器。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._columns: set[str] = set()
        self._table = "group_messages"

    # ---------------- 打开与自检 ----------------

    def _connect(self) -> sqlite3.Connection:
        if not self.path.exists():
            raise SourceDatabaseError(
                f"源库不存在：{self.path}。请先用 nt_msg_db_util 的 3.export.py "
                f"导出 nt_msg_export.db，再把 CLIENT_DB_PATH 指向它。"
            )
        try:
            # mode=ro：只读打开，绝不因为手滑改动用户的聊天记录库。
            # immutable=0（默认）：允许它在被别的进程追加时仍然可读。
            uri = f"file:{self.path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        except sqlite3.Error as exc:
            raise SourceDatabaseError(f"打不开源库 {self.path}：{exc}") from exc
        conn.row_factory = sqlite3.Row
        return conn

    # ⚠️ 调用方必须用 `contextlib.closing(self._connect())` 包住连接。
    #
    # 为什么专门写这一句：`sqlite3.Connection` 的 `with` 是**事务**上下文
    # （退出时 commit/rollback），**不是**关闭。写成 `with self._connect() as c`
    # 连接会一直留到被 GC 回收 —— 在 Windows 上表现为"源库文件被占用、
    # 导出工具写不进去"，在长时间 --loop 里表现为文件句柄越积越多。
    # 这个坑是 e2e 测试（重写 fixture 库时 unlink 失败）抓出来的。

    def inspect(self) -> dict:
        """自检：这个库能不能读、有没有我们要的表和列。

        启动时调用一次，把结果打出来 —— 上游换版本时，这行日志就是唯一的线索。
        """
        try:
            with closing(self._connect()) as conn:
                tables = {
                    str(r[0])
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
                    ).fetchall()
                }
                if self._table not in tables:
                    raise SourceDatabaseError(
                        f"源库里没有 {self._table} 表（有：{sorted(tables)[:10]}）。"
                        "这个库看起来不是 nt_msg_db_util 导出的结构化库 —— "
                        "注意要用 3.export.py 产出的 nt_msg_export.db，"
                        "而不是 nt_msg_plain.db（那里面是 Protobuf 原始列）。"
                    )
                self._columns = _column_names(conn, self._table)
                missing = [c for c in REQUIRED_COLUMNS if c not in self._columns]
                if missing:
                    raise SourceDatabaseError(
                        f"{self._table} 缺少必需的列 {missing}；"
                        f"实际有：{sorted(self._columns)}"
                    )
                absent = [c for c in OPTIONAL_COLUMNS if c not in self._columns]
                row = conn.execute(f'SELECT COUNT(*) AS n FROM "{self._table}"').fetchone()
                total = int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise SourceDatabaseError(f"读源库失败 {self.path}：{exc}") from exc

        if absent:
            logger.warning(
                "源库 %s 的 %s 少了这些列：%s（会按「没有」处理，功能可能降级）",
                self.path.name,
                self._table,
                absent,
            )
        return {"table": self._table, "columns": sorted(self._columns), "rows": total, "absent": absent}

    # ---------------- 增量读 ----------------

    def latest(self) -> tuple[int, str] | None:
        """库里最新一条的 (timestamp, msg_id)。空库返回 None。

        用途：判断"游标是否比库还新"（导出库被换成了一个更旧的快照时，
        静默什么都不做是最糟的结果 —— 要能看出来）。
        """
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            row = conn.execute(
                f'SELECT timestamp AS t, msg_id AS m FROM "{self._table}"'
                " ORDER BY timestamp DESC, msg_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return int(row["t"]), str(row["m"])

    def fetch_since(
        self,
        since_ts: int,
        since_msg_id: str = "",
        *,
        limit: int,
    ) -> list[SourceMessage]:
        """取 `(timestamp, msg_id) > (since_ts, since_msg_id)` 的消息，按时间正序。

        元组比较而不是 `timestamp > ?`：同一秒里可能有很多条，只按秒比会漏。
        `since_msg_id` 为空时退化成 `timestamp >= since_ts`（首次运行的路径），
        也就是**会重复读同一秒里的消息** —— 重复由后端幂等挡住，漏一条可没人挡。
        """
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            select = [c for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS) if c in self._columns]
            sql = (
                f'SELECT {", ".join(select)} FROM "{self._table}"'
                " WHERE (timestamp > ?) OR (timestamp = ? AND msg_id > ?)"
                " ORDER BY timestamp ASC, msg_id ASC LIMIT ?"
            )
            try:
                rows = conn.execute(sql, (since_ts, since_ts, since_msg_id, int(limit))).fetchall()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(
                    f"查询源库失败（{self.path.name}）：{exc}。"
                    "如果提示 database is locked，说明导出工具正在写它 —— "
                    "确认导出是「跑完再读」还是「边写边读」，前者要等它写完。"
                ) from exc
        return [self._to_message(row) for row in rows]

    def _to_message(self, row: sqlite3.Row) -> SourceMessage:
        keys = set(row.keys())
        content = _json_or_none(row["content"]) if "content" in keys else None
        text = str(row["text"] or "") if "text" in keys else ""
        sender = ""
        for candidate in ("sender_qq", "sender_uid"):
            if candidate in keys and row[candidate]:
                sender = str(row[candidate])
                break
        group = ""
        for candidate in ("group_id", "group_qq"):
            if candidate in keys and row[candidate]:
                group = str(row[candidate])
                break
        # 正文：text 是导出工具合并好的纯文本，优先用它；
        # 它为空但 content 是纯文本段时，退回 content 里的 text。
        if not text and isinstance(content, dict) and content.get("type") == "text":
            text = str(content.get("text") or "")
        return SourceMessage(
            msg_id=str(row["msg_id"]),
            timestamp=int(row["timestamp"]),
            group_id=group,
            sender_id=sender,
            sender_name=None,  # 导出库里没有群名片/昵称，只有号码
            text=text,
            attachments=attachments_from_content(content),
            parse_status=str(row["parse_status"]) if "parse_status" in keys and row["parse_status"] else None,
            raw_content=content,
        )

    def iter_since(
        self, since_ts: int, since_msg_id: str = "", *, limit: int, chunk: int = 500
    ) -> Iterator[SourceMessage]:
        """分块迭代，直到取满 `limit` 条或没有更多。"""
        taken = 0
        ts, mid = since_ts, since_msg_id
        while taken < limit:
            want = min(chunk, limit - taken)
            batch = self.fetch_since(ts, mid, limit=want)
            if not batch:
                return
            for message in batch:
                yield message
                taken += 1
            ts, mid = batch[-1].timestamp, batch[-1].msg_id
            if len(batch) < want:
                return
