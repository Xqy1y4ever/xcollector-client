"""客户端的**镜像库**：只记「这条源消息我处理过没有」。

## 为什么要有它

后端那个游标（`bot_state` 里的 `client_cursor`）只是**一个水位线**，它有两个
天生的问题：

1. 水位线只能表达"我处理到哪儿了"，表达不了"这一条处理好了没有"。
   中途失败要么丢消息（水位推过去了），要么整段重放；
2. 它是**一条**纪录，所以"这条的内容变了"这种事根本没法表达 ——
   而通知被编辑/补充恰恰是常态。

镜像库把状态降到**每一条消息**上，于是这三件事一起解决了：

    读源库 → 这条在镜像里吗？→ 不在：处理它
                            → 在、内容没变、状态 done：跳过（零成本）
                            → 在、**内容变了**：重新处理 → 后端按
                              (user_id, raw_message_id) 幂等，**更新原来那条任务**
                            → 在、状态 pending/failed：重试（这就是恢复队列）

于是"增量更新"就是一次镜像查询，而不是靠水位线猜。

## 状态是四态而不是一个布尔

你说的是"只存储消息id和是否已读"。这里存的是 `state`，因为一个布尔会逼出
一个坏选择：在读到的当下就标已读 → 处理失败的消息**永久丢失**（而"不漏信息"
是这套系统的底线）；处理成功才标已读 → 那这个布尔就等于 `state='done'`，
只是名字短一点。

所以四态就是那句话的诚实版本：

    pending  读到了、还没处理完（崩溃/重启后就是靠它恢复的）
    done     处理完成（= 你说的"已读"）
    skipped  判定为不需要（订阅之外、闲聊、源库缺字段）—— 也是终态，不再重试
    failed   处理失败，下轮重试

另外存了 `content_hash` 和 `raw_id`：前者是"内容变了"的判据，后者让"补充"
能落到原来那条任务上。除这两样，其余都只是为了让人能看出来**为什么**。
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .utils import now_ms

logger = logging.getLogger(__name__)

# 状态取值（见模块开头）
STATE_PENDING = "pending"
STATE_DONE = "done"
STATE_SKIPPED = "skipped"
STATE_FAILED = "failed"

TERMINAL_STATES = (STATE_DONE, STATE_SKIPPED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS message (
  msg_id        TEXT PRIMARY KEY,
  group_id      TEXT NOT NULL DEFAULT '',
  sender_id     TEXT NOT NULL DEFAULT '',
  source_ts     INTEGER NOT NULL DEFAULT 0,   -- 源库的秒级时间戳
  content_hash  TEXT NOT NULL DEFAULT '',     -- 用于识别"这条的内容变了"
  state         TEXT NOT NULL DEFAULT 'pending',
  raw_id        TEXT,                         -- 后端的原文 id（补充要落回它）
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  first_seen_at INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_message_state_ts ON message(state, source_ts);
CREATE INDEX IF NOT EXISTS ix_message_ts       ON message(source_ts);

-- 「谁补充了谁」。source_msg_id 是**做补充的那条新消息**，
-- target_msg_id 是被它补充的那条**已经处理过**的消息。
-- 有了它，同一条补充消息不会被反复当成新的补充来处理。
CREATE TABLE IF NOT EXISTS amendment (
  source_msg_id TEXT PRIMARY KEY,
  target_msg_id TEXT NOT NULL,
  created_at    INTEGER NOT NULL DEFAULT 0
);
"""


def content_hash(message) -> str:
    """一条源消息的指纹。

    只用**会影响抽取结果**的东西：正文 + 附件清单。刻意不含 `source_ts`
    （时间戳是标识，不是内容）也不含 parse_status（那是解析质量，不是内容）——
    把标识混进指纹会让"内容没变"永远判成"变了"，于是每轮都重新抽一次，
    白花模型的钱。
    """
    parts = [
        str(getattr(message, "group_id", "") or ""),
        str(getattr(message, "sender_id", "") or ""),
        str(getattr(message, "text", "") or ""),
    ]
    for item in sorted(
        (getattr(message, "attachments", None) or []),
        key=lambda a: str(a.get("name") or a.get("url") or a.get("md5") or ""),
    ):
        parts.append("|".join(str(item.get(k) or "") for k in ("type", "name", "url", "md5", "size")))
    raw = "\x1f".join(parts)
    return hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:32]


@dataclass
class MirrorRow:
    msg_id: str
    group_id: str
    sender_id: str
    source_ts: int
    content_hash: str
    state: str
    raw_id: str | None
    attempts: int
    last_error: str | None
    first_seen_at: int = 0
    updated_at: int = 0

    @property
    def finished(self) -> bool:
        return self.state in TERMINAL_STATES


def _row(record) -> MirrorRow:
    """sqlite3.Row → MirrorRow（只取数据类认识的列，多出来的列不会让它炸）。"""
    known = set(MirrorRow.__dataclass_fields__)
    return MirrorRow(**{key: record[key] for key in record.keys() if key in known})


class Mirror:
    """本地镜像库（就是客户端自己的那个 SQLite 文件）。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)

    # ---------------- 基础 ----------------

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 这个库是**我们自己的**，要写；和源库（只读）不是一回事。
        conn = sqlite3.connect(str(self.path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        return conn

    def get(self, msg_id: str) -> MirrorRow | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM message WHERE msg_id=?", (str(msg_id),)
            ).fetchone()
        return _row(row) if row else None

    def get_many(self, msg_ids: Iterable[str]) -> dict[str, MirrorRow]:
        ids = [str(i) for i in msg_ids]
        if not ids:
            return {}
        out: dict[str, MirrorRow] = {}
        with closing(self._connect()) as conn:
            # 分批查，避免 SQLite 的参数上限（默认 999）
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                marks = ",".join("?" * len(chunk))
                for row in conn.execute(
                    f"SELECT * FROM message WHERE msg_id IN ({marks})", chunk
                ).fetchall():
                    out[str(row["msg_id"])] = _row(row)
        return out

    # ---------------- 读写状态 ----------------

    def claim(self, message_row) -> tuple[MirrorRow, str]:
        """把这条消息登记进镜像（如果还没有），返回 `(行, 发生了什么)`。

        `发生了什么` 是给调用方决定要不要重新抽取的：

            new       第一次见 → 处理
            changed   **内容变了** → 重新处理（后端幂等会把原来那条任务更新掉）
            retry     上次没处理完/失败了 → 重试
            unchanged 处理过了、内容也没变 → 跳过

        ⚠️ 登记成 `pending` 而不是 `done`：**先记账再干活**。反过来（干完才记账）
        的话，进程在"已经写进后端、还没记账"之间崩掉就只是重做一次（幂等挡住），
        而"记完账才发现没写成"会**丢掉一条消息**。宁可重做，不可漏。
        """
        stamp = now_ms()
        digest = content_hash(message_row)
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM message WHERE msg_id=?", (str(message_row.msg_id),)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO message (msg_id, group_id, sender_id, source_ts,"
                    " content_hash, state, attempts, first_seen_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,0,?,?)",
                    (
                        str(message_row.msg_id),
                        str(message_row.group_id or ""),
                        str(message_row.sender_id or ""),
                        int(message_row.timestamp or 0),
                        digest,
                        STATE_PENDING,
                        stamp,
                        stamp,
                    ),
                )
                conn.commit()
                what = "new"
            else:
                if str(row["content_hash"]) != digest:
                    # 内容变了：重新处理。后端按 (user_id, raw_message_id) 幂等，
                    # 所以这一条会把**原来那条任务**更新掉，而不是新建一条。
                    conn.execute(
                        "UPDATE message SET content_hash=?, state=?, group_id=?, sender_id=?,"
                        " source_ts=?, updated_at=? WHERE msg_id=?",
                        (
                            digest,
                            STATE_PENDING,
                            str(message_row.group_id or ""),
                            str(message_row.sender_id or ""),
                            int(message_row.timestamp or 0),
                            stamp,
                            str(message_row.msg_id),
                        ),
                    )
                    conn.commit()
                    what = "changed"
                elif str(row["state"]) in TERMINAL_STATES:
                    what = "unchanged"
                else:
                    what = "retry"
            fresh = conn.execute(
                "SELECT * FROM message WHERE msg_id=?", (str(message_row.msg_id),)
            ).fetchone()
        return _row(fresh), what

    def finish(self, msg_id: str, *, state: str, raw_id: str | None = None,
               error: str | None = None) -> None:
        """记下这条的最终状态。`attempts` 只在失败时加。"""
        stamp = now_ms()
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE message SET state=?, raw_id=COALESCE(?, raw_id), last_error=?,"
                " attempts=attempts + ?, updated_at=? WHERE msg_id=?",
                (
                    state,
                    raw_id,
                    error,
                    1 if state == STATE_FAILED else 0,
                    stamp,
                    str(msg_id),
                ),
            )
            conn.commit()

    # ---------------- 增量与水位的辅助查询 ----------------

    def watermark(self) -> int:
        """已经处理完的消息里最大的 `source_ts`。

        用它当增量扫描的起点，**但会往前留一段重叠**（见 `run_cycle`）：
        源库是按时间追加的，可是同一秒里可能后到，而"内容被编辑"更是发生在
        任意更早的位置上。
        """
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT MAX(source_ts) AS m FROM message WHERE state IN (?,?)",
                TERMINAL_STATES,
            ).fetchone()
        return int(row["m"] or 0) if row else 0

    def unfinished(self) -> list[MirrorRow]:
        """还没处理完的（pending/failed）—— 崩溃恢复与失败重试靠它。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM message WHERE state IN (?,?) ORDER BY source_ts ASC",
                (STATE_PENDING, STATE_FAILED),
            ).fetchall()
        return [_row(r) for r in rows]

    def stats(self) -> dict:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM message GROUP BY state"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) AS n FROM message").fetchone()
            amendments = conn.execute("SELECT COUNT(*) AS n FROM amendment").fetchone()
        return {
            "total": int(total["n"] if total else 0),
            "by_state": {str(r["state"]): int(r["n"]) for r in rows},
            "amendments": int(amendments["n"] if amendments else 0),
        }

    # ---------------- 补充关系 ----------------

    def mark_amendment(self, source_msg_id: str, target_msg_id: str) -> None:
        """记下「source 补充了 target」。已经记过就什么都不做。"""
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO amendment (source_msg_id, target_msg_id, created_at)"
                " VALUES (?,?,?)",
                (str(source_msg_id), str(target_msg_id), now_ms()),
            )
            conn.commit()

    def amendment_of(self, source_msg_id: str) -> str | None:
        """这条消息是补充吗？是的话返回它补充的那条。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT target_msg_id FROM amendment WHERE source_msg_id=?",
                (str(source_msg_id),),
            ).fetchone()
        return str(row["target_msg_id"]) if row else None

    def raw_id_of(self, msg_id: str) -> str | None:
        row = self.get(msg_id)
        return row.raw_id if row else None
