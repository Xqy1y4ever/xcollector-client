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
    skipped  判定为不需要（闲聊、源库缺字段）—— 也是终态，不再重试
    failed   处理失败，下轮重试

## 第五态 `reprocess`：用户把"已读"改回"未读"

"把当前消息标成未读、让它重新处理一遍"是这个客户端需要的一个动作（换了抽取器、
改了提示词、怀疑上一遍抽坏了）。它是**第五个状态**而不是"删掉那几行"，理由是
删掉会丢掉两件必须留住的事：

1. **是谁标的**。删掉之后，那些消息看起来和"镜像被删过/换了机器"完全一样，
   于是 `run_cycle` 里那条"后端已经有通知就跳过抽取"的保命捷径会把它们**静默跳过** ——
   用户以为重新处理了，实际上一次模型调用都没发生（这正是这套系统最怕的静默）。
   记成 `reprocess` 之后，重跑时明确知道"这条是用户要求重抽的"，于是绕过那条捷径。
2. **它还没做完**。`reprocess` 和 pending/failed 一样进 `unfinished()`：进程中途
   退出、这一轮预算用完，下一轮接着做，不会丢。

白名单外的消息不在这里**：白名单下推到 SQL 了（扫描根本不碰它们），所以
"镜像 = 白名单内我处理过的那些"。镜像里只可能剩下旧版本留下的白名单记录，
由 `drop_whitelist_skips()` 每次循环清一次。

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
STATE_REPROCESS = "reprocess"   # 用户标成未读：下一轮**重新抽一次**

TERMINAL_STATES = (STATE_DONE, STATE_SKIPPED)

# 标为未读时写进 `last_error` 的那句话。它是**给人在界面上看的**（`--status` 会
# 把它打出来），不是错误 —— 所以措辞要说清楚"这是谁要求的、接下来会发生什么"。
MARK_UNREAD_REASON = "用户标为未读：下一轮重新抽一次（会重新花模型的钱）"

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
        所以这里就是一条 `INSERT OR IGNORE`。已经存在的行由 `unfinished()` 负责
        （重试没做完的），不从这里走。

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

        白名单跳过的**不算**"见过"（新版本里这种记录根本不会产生，这里只是兼容
        旧镜像里还剩着的那几条：用户说了不看那个来源，为它的沉默告警是噪音）。
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
        """还没做完的：pending / failed（崩溃恢复 + 失败重试）**和 reprocess**。

        `reprocess` 是"用户标成未读、要求重抽"的那些（见 `mark_unread`）。它们必须
        走这里、而不是"扫没读过的"那条路 —— 后者靠"镜像里有没有这一行"判断，
        而它们**在**镜像里（这正是我们要的信息：哪些是用户点名要重做的）。
        """
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM message WHERE state IN (?,?,?) ORDER BY source_ts ASC",
                (STATE_PENDING, STATE_FAILED, STATE_REPROCESS),
            ).fetchall()
        return [_row(r) for r in rows]

    def mark_unread(self, msg_ids: Iterable[str] | None = None) -> dict:
        """把已经处理过的记录标成**未读**，下一轮会**重新处理**它们（真的重抽）。

        参数不给 = 镜像里**全部**记录；给了 `msg_ids` = 只标这些（不存在的 id 不算数，
        也不会凭空建行 —— "未读"必须有源头那条消息，凭空造行只会造出一个永远
        处理不了的东西）。

        返回 `{"marked": 本次新标的, "total": 标完之后总共待重新处理的, "dropped": 清掉的旧垃圾}`。
        **三个数都要给出来**：只报"标了 0 条"而不同时说"本来就有 109 条在排队"，
        会让人以为没生效。

        两个诚实的边界：

        * **别和正在跑的那一轮同时用**。一边标、另一边正好把这条处理完，`finish()`
          会把状态改成 done/skipped，这一条的记号就没了（页面上运行中直接拒绝）。
        * **重新处理会重传附件**：后端附件按 id 存、没有内容去重，所以一条带图的消息
          重抽一次就多一份附件字节。消息带附件不多时无所谓，介意的话就别整库重来。
        """
        ids = None if msg_ids is None else [str(i) for i in msg_ids]
        if ids is not None and not ids:
            return {
                "marked": 0,
                "total": int(self.stats()["by_state"].get(STATE_REPROCESS, 0)),
                "dropped": 0,
            }

        # 旧版本留下的"白名单跳过"记录先清掉：它们不是"处理过的东西"，
        # 标成未读只会让一个已经不看来源的消息被重新抽一遍（纯浪费 + 噪音）。
        dropped = self.drop_whitelist_skips()
        stamp = now_ms()
        marked = 0
        with closing(self._connect()) as conn:
            if ids is None:
                cur = conn.execute(
                    "UPDATE message SET state=?, attempts=0, last_error=?, updated_at=?"
                    " WHERE state != ?",
                    (STATE_REPROCESS, MARK_UNREAD_REASON, stamp, STATE_REPROCESS),
                )
                marked = int(cur.rowcount or 0)
            else:
                for start in range(0, len(ids), 500):   # 避开 SQLite 的参数上限
                    chunk = ids[start : start + 500]
                    marks = ",".join("?" * len(chunk))
                    cur = conn.execute(
                        "UPDATE message SET state=?, attempts=0, last_error=?, updated_at=?"
                        f" WHERE state != ? AND msg_id IN ({marks})",
                        (STATE_REPROCESS, MARK_UNREAD_REASON, stamp, STATE_REPROCESS, *chunk),
                    )
                    marked += int(cur.rowcount or 0)
            conn.commit()
        return {
            "marked": marked,
            "total": int(self.stats()["by_state"].get(STATE_REPROCESS, 0)),
            "dropped": dropped,
        }

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

    def drop_whitelist_skips(self) -> int:
        """删掉"因为白名单被跳过"的那些记录，返回删了几条。

        ## 为什么是**删**，而不是像以前那样"白名单变了就放回待处理"

        白名单现在**下推到 SQL**（见 `app/source/ntmsg.py`）：扫描根本不碰白名单外的
        消息，所以镜像里**不该**有它们的记录。于是"改了白名单要不要重看"这个问题
        自己就没了 —— 新放开的来源本来就不在镜像里，下一轮自然会被读到。

        这个方法只做一件事：把**旧版本**留下的那批记录清掉（那时是"逐条扫、逐条记
        skipped"）。它们留在镜像里的坏处很具体：一个用户的真实库里这个数字是
        9,400 条纯垃圾，而"白名单内共 109 条"才是他真正关心的。
        每次循环调一次，幂等；删掉之后那些消息的"读没读过"重新由白名单说话。
        """
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "DELETE FROM message WHERE state=? AND last_error LIKE 'whitelist:%'",
                (STATE_SKIPPED,),
            )
            conn.commit()
            return int(cur.rowcount or 0)
