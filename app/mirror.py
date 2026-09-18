"""客户端的**镜像库**：只记「这条源消息我处理过没有」。

## 为什么要有它

后端那个游标（`bot_state` 里的 `client_cursor`）只是**一个水位线**，它有两个
天生的问题：

1. 水位线只能表达"我处理到哪儿了"，表达不了"这一条处理好了没有"。
   中途失败要么丢消息（水位推过去了），要么整段重放；
2. 它是**一条**纪录，粒度太粗：同一秒里后到的消息会被它跳过。

镜像库把状态降到**每一条消息**上：

    读源库 → 这条在镜像里吗？→ 不在：处理它
                            → 在、状态 done/skipped：跳过（零成本）
                            → 在、状态 pending/failed：重试（这就是恢复队列）

判据是「**读没读过**」，不是时间窗口 —— 时间窗口表达不了"这一条到底处理过没有"，
比窗口更老、而镜像里又没有的消息会永远读不到（换过导出库、镜像被删过、白名单刚
放开），而那正是这套系统最怕的静默漏消息。

## 状态是四态而不是一个布尔

你说的是"只存储消息id和是否已读"。这里存的是 `state`，因为一个布尔会逼出
一个坏选择：在读到的当下就标已读 → 处理失败的消息**永久丢失**（而"不漏信息"
是这套系统的底线）；处理成功才标已读 → 那这个布尔就等于 `state='done'`，
只是名字短一点。

所以四态就是那句话的诚实版本：

    pending  读到了、还没处理完（崩溃/重启后就是靠它恢复的）
    done     处理完成（= 你说的"已读"）
    skipped  判定为不需要（白名单外、闲聊、源库缺字段）—— 也是终态，不再重试
    failed   处理失败，下轮重试

另外存了 `raw_id`：它让"这条消息对应后端哪条原文"能被查回来（重试、去重都要它）。
其余字段都只是为了让人能看出来**为什么**。
"""

from __future__ import annotations

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
  state         TEXT NOT NULL DEFAULT 'pending',
  raw_id        TEXT,                         -- 后端的原文 id（重试/去重要用它）
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  first_seen_at INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_message_state_ts ON message(state, source_ts);
CREATE INDEX IF NOT EXISTS ix_message_ts       ON message(source_ts);
CREATE INDEX IF NOT EXISTS ix_message_group    ON message(group_id, source_ts);

-- 小配置快照。目前只存一件事：**白名单的指纹**。
--
-- 为什么要存：白名单改了之后，之前"因为它而被跳过"的消息必须重新过一遍。
-- 否则用户往白名单里加一个群，会发现"什么都没发生" —— 那些消息早就被记成
-- skipped 了，而这件事在界面上完全看不出来。这正是本项目最怕的那种静默失败。
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);
"""


@dataclass
class MirrorRow:
    msg_id: str
    group_id: str
    sender_id: str
    source_ts: int
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

    def claim(self, message_row) -> MirrorRow:
        """把这条消息登记进镜像，返回登记后的那一行。

        **已经有了就原样返回**（不覆盖状态）：调用方只会在"这条我还没读过"时喊它，
        所以这里就是一条 `INSERT OR IGNORE`。已经存在的行由 `unfinished()`（重试）
        和 `reopen_whitelist_skips()`（白名单变了）负责改状态，不从这里走。

        ⚠️ 登记成 `pending` 而不是 `done`：**先记账再干活**。反过来（干完才记账）
        的话，进程在"已经写进后端、还没记账"之间崩掉就只是重做一次（幂等挡住），
        而"记完账才发现没写成"会**丢掉一条消息**。宁可重做，不可漏。
        """
        stamp = now_ms()
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO message (msg_id, group_id, sender_id, source_ts,"
                " state, attempts, first_seen_at, updated_at)"
                " VALUES (?,?,?,?,?,0,?,?)",
                (
                    str(message_row.msg_id),
                    str(message_row.group_id or ""),
                    str(message_row.sender_id or ""),
                    int(message_row.timestamp or 0),
                    STATE_PENDING,
                    stamp,
                    stamp,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM message WHERE msg_id=?", (str(message_row.msg_id),)
            ).fetchone()
        return _row(row)

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

    def group_seen_ts(self, group_id: str, *, exclude_msg_id: str = "") -> int | None:
        """这个群里**见过**的、除 `exclude_msg_id` 之外最晚一条消息的时间（秒）。

        用途：缺口检测要的"上一条消息的时间"。以前这个值是后端 `group_state`
        给的，那个表是**共享**的（一个群一行，全站一张表）—— 两个客户端各写各的
        时间线会互相把 previous 顶掉，缺口告警就会静默地漏掉。现在改成从
        **自己的镜像**里取：这个群里我见过什么，只有我知道，也只有我关心。

        用 `MAX`（而不是"比当前这条更早的最近一条"）是刻意的：补处理一条很老的
        消息时，`MAX` 会给出更新的那条，`gap` 算出来是负的 → 不告警。否则每补一条
        历史消息就会凭空冒出一个"缺口"。

        白名单跳过的**不算**"见过"：用户已经说了他不看那个来源，再为它的沉默
        告警是噪音（`reopen_whitelist_skips()` 会把它们放回来，那时再算）。
        """
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT MAX(source_ts) AS m FROM message"
                " WHERE group_id=? AND msg_id != ?"
                "   AND NOT (state=? AND last_error LIKE 'whitelist:%')",
                (str(group_id), str(exclude_msg_id), STATE_SKIPPED),
            ).fetchone()
        value = int(row["m"] or 0) if row else 0
        return value or None

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
        return {
            "total": int(total["n"] if total else 0),
            "by_state": {str(r["state"]): int(r["n"]) for r in rows},
        }

    def raw_id_of(self, msg_id: str) -> str | None:
        row = self.get(msg_id)
        return row.raw_id if row else None

    # ---------------- 小配置快照 ----------------

    def get_meta(self, key: str) -> str | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (str(key),)).fetchone()
        return str(row["value"]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(key), str(value)),
            )
            conn.commit()

    def reopen_whitelist_skips(self) -> int:
        """把"因为白名单被跳过"的消息重新放回待处理，返回放回了几条。

        只在**白名单指纹变了**的时候调用（见 `app/run.py`）。这一步是"改白名单
        之后立刻生效"的全部秘密：那些消息当时被记成终态 skipped，不放回来的话，
        用户加完白名单只会看到什么都没发生。
        """
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "UPDATE message SET state=?, last_error=NULL, updated_at=?"
                " WHERE state=? AND last_error LIKE 'whitelist:%'",
                (STATE_PENDING, now_ms(), STATE_SKIPPED),
            )
            conn.commit()
            return int(cur.rowcount or 0)
