"""把明文库 `nt_msg_plain.db` 导成客户端认识的结构化库 `nt_msg_export.db`。

**这一层是 `nt_msg_db_util` 的 `3.export.py` 整合进本项目的版本。**
上游的 `msgdb` 包已经**逐字**搬到本仓库根目录（见 `msgdb/VENDORED.md`），
这里只做两件事：把上游的流程串起来、加上客户端要的进度与报告。

流程与上游完全一致：

    init_db（建表 + FTS + 触发器）
    → 摘掉 FTS 触发器（批量写入时维护索引太慢）
    → 逐行解析 40800 并批量写 c2c_messages / group_messages
    → 建二级索引
    → 重建 FTS 索引

表结构、`content` 的形状（`{"type":"msg_body","segments":[...]}`）、`parse_status`
的取值都由上游的 `msgdb` 决定 —— **这一层不改它的形状**，客户端那边按这个形状读。

## 全量，不做增量

上游每次都全量解析（实测 77 万行约 45 秒）。客户端**也全量**：按时间过滤会让
"时间戳没变、内容变了"的旧消息永远进不了导出库 —— 上层的判断就都建立在旧内容
上了。45 秒换"导出库和源库一致"，这笔账是划算的。
（`since_ts` 参数还留着，是给"我知道自己在干什么"的调用方用的；正常路径不传。）
"""

from __future__ import annotations

import logging
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from msgdb.c2c import parser as c2c_parser
from msgdb.export_schema import (
    create_indexes,
    drop_fts_triggers,
    init_db,
    insert_group_messages_batch,
    insert_messages_batch,
    rebuild_fts,
)
from msgdb.group import exporter as group_exporter

from ..utils import sqlite_uri

logger = logging.getLogger(__name__)

# 进度日志每隔多少行打一次（上游也是 50_000）。
LOG_INTERVAL = 50_000

DEFAULT_BATCH_SIZE = 2000


class ExportError(RuntimeError):
    """导出失败。消息里写清"该怎么办"。"""


# 收尾"切回 DELETE"的重试次数与间隔：给正在读这个库的连接（页面轮询、另一个实例）
# 一点让路的时间。合计 ~5 秒；切不动也不当失败（见 `_switch_back_to_delete`）。
JOURNAL_SWITCH_ATTEMPTS = 5
JOURNAL_SWITCH_DELAY = 1.0


@dataclass
class ExportReport:
    src: Path
    dst: Path
    tables: list[str] = field(default_factory=list)
    source_rows: dict[str, int] = field(default_factory=dict)
    written_rows: dict[str, int] = field(default_factory=dict)
    parse_status: dict[str, int] = field(default_factory=dict)
    failed_rows: int = 0
    last_error: str | None = None
    # 每张表的起点（"我已经导到哪一条了"）。全量时是 0。
    since_msg_id: dict[str, int] = field(default_factory=dict)
    # 因为"源库看起来换了一份"而整表重导的表
    rebuilt: list[str] = field(default_factory=list)
    # 收尾那一步"切回 DELETE"没成功（导出库当时还被别人读着）。数据是好的，
    # 只是这个库暂时停在 WAL 模式上 —— 见 `_switch_back_to_delete()` 的说明。
    journal_mode_locked: bool = False
    watermark: int = 0
    seconds: float = 0.0

    @property
    def total_written(self) -> int:
        return sum(self.written_rows.values())

    @property
    def total_source(self) -> int:
        return sum(self.source_rows.values())

    def summary(self) -> str:
        parts = [f"写入 {self.total_written:,} 行"]
        if self.rebuilt:
            parts.append(f"⚠️ 整表重导：{'、'.join(self.rebuilt)}")
        if self.parse_status:
            parts.append(
                "正文解析：" + "、".join(f"{k} {v:,}" for k, v in sorted(self.parse_status.items()))
            )
        if self.failed_rows:
            parts.append(f"⚠️ {self.failed_rows} 行转换失败（最后一条：{self.last_error}）")
        if self.journal_mode_locked:
            parts.append("⚠️ 导出库还被别的连接读着，没能切回 DELETE（下一轮再试）")
        parts.append(f"耗时 {self.seconds:.1f}s")
        return "；".join(parts)


def _switch_back_to_delete(conn: sqlite3.Connection, report: ExportReport) -> None:
    """收尾把导出库切回 DELETE 日志模式；**切不动也不算失败**。

    ## 为什么这一步会"database is locked"

    `PRAGMA journal_mode=DELETE` 需要**独占**：只要有别的连接正开着这个库，
    SQLite 立刻返回 SQLITE_BUSY —— 而且它**不走 busy_timeout**（这是 journal_mode
    切换的既定行为，所以 `sqlite3.connect(timeout=60)` 在这里帮不上忙）。
    真实撞到过的场景（2026-09-19 用户日志）：

        ERROR xcollector.webui | 这一轮失败：导出失败（nt_msg_plain.db → nt_msg_export.db）：
                                 OperationalError: database is locked

    原因是**本进程的 Web UI 正在轮询状态**（`/api/state` 会读一下导出库算"没读过的"），
    或者还有**另一个客户端实例**开着。两种情况都不是"数据坏了"。

    ## 所以这里怎么办

    1. 重试几次（给读者让路 —— 页面轮询是亚秒级的）；
    2. 仍然切不动就**不抛**：数据已经写完了，把它记成 `report.journal_mode_locked`
       并在日志里说清"常见原因是还有实例开着"，下一轮导出会再试。
       把它当失败会让一整轮白跑，而用户看到的报错（"database is locked"）
       指不到真正的原因。
    3. 万一这个库就一直停在 WAL 模式上也不致命：`SourceDatabase._connect()` 有兜底
       （只读打不开就退化成普通打开，见那里的说明）。
    """
    for attempt in range(1, JOURNAL_SWITCH_ATTEMPTS + 1):
        try:
            row = conn.execute("PRAGMA journal_mode=DELETE;").fetchone()
            mode = str(row[0]).lower() if row else ""
            if mode == "delete":
                return
            logger.warning("导出库切回 DELETE 没成功（现在是 %s），重试 %d/%d",
                           mode or "未知", attempt, JOURNAL_SWITCH_ATTEMPTS)
        except sqlite3.OperationalError as exc:
            logger.warning("导出库正被别的连接占着（%s），重试 %d/%d",
                           exc, attempt, JOURNAL_SWITCH_ATTEMPTS)
        time.sleep(JOURNAL_SWITCH_DELAY)

    report.journal_mode_locked = True
    logger.warning(
        "导出库没能切回 DELETE（还开着这个库的连接没放）：**数据已经写好了**，这次先这样。"
        "常见原因是还有另一个客户端实例在跑（它的页面在轮询状态），或者是本进程的页面"
        "正在刷新 —— 关掉多余的实例即可，下一轮导出会再试一次。"
    )


def _incremental_sql(select_sql: str, since_msg_id: int) -> tuple[str, tuple]:
    """给上游的 SELECT 外面套一层 `msg_id > ?`（`since_msg_id<=0` = 全量）。
    **为什么按 msg_id 而不是时间**：`msg_id`（源表的 `"40001"`）是 `INTEGER PRIMARY
    KEY`，也就是 rowid —— 加这个条件是一条**索引区间扫描**，只读新行；而按
    `timestamp >= ?` 要全表扫一遍（实测 77 万行 5.5 秒），而且还有"同秒后到"的
    漏读风险（时间戳更早的新消息会被跳过）。msg_id 是单调追加的，没有这个问题。

    上游语句以 `ORDER BY "40001"` 结尾，包成子查询后 SQLite 会把条件下推到主键上。
    外层再 `ORDER BY msg_id` 一次：不依赖"子查询的 ORDER BY 会不会被保留"。
    """
    if since_msg_id <= 0:
        return select_sql, ()
    return f"SELECT * FROM ({select_sql}) WHERE msg_id > ? ORDER BY msg_id", (int(since_msg_id),)


def watermark(path: Path | str, table: str = "group_messages") -> int:
    """导出库里已有的**最大 msg_id**（增量导出的起点）。

    `msg_id` 就是源表的 rowid（`"40001"`），所以这个数是"我已经导到哪一条了"，
    比时间戳精确：不会漏掉同秒后到的消息，也不需要"回看窗口"。
    """
    path = Path(path)
    if not path.exists():
        return 0
    try:
        with closing(
            sqlite3.connect(sqlite_uri(path, "ro"), uri=True, timeout=30.0)
        ) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not exists:
                return 0
            row = conn.execute(f'SELECT max("msg_id") FROM "{table}"').fetchone()
    except sqlite3.Error as exc:
        raise ExportError(f"读导出库失败 {path}：{exc}") from exc
    return int(row[0]) if row and row[0] is not None else 0


def source_max_id(src: Path | str, table: str) -> int:
    """明文源库里某张表的最大 rowid（用来判断"有没有新行"）。"""
    path = Path(src)
    if not path.exists():
        return 0
    try:
        with closing(sqlite3.connect(sqlite_uri(path, "ro"), uri=True, timeout=30.0)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not exists:
                return 0
            row = conn.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()
    except sqlite3.Error as exc:
        raise ExportError(f"读明文库失败 {path}：{exc}") from exc
    return int(row[0]) if row and row[0] is not None else 0


def _source_table(label: str) -> str:
    """导出的目标表 → 源表（上游那两张消息表）。"""
    return "c2c_msg_table" if label == "c2c" else "group_msg_table"


def _dest_max_id(conn: sqlite3.Connection, target: str) -> int:
    """导出库里某张表的 `max(msg_id)`（= 已经导到哪一条了）。表不存在时返回 0。"""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (target,)
    ).fetchone()
    if not exists:
        return 0
    row = conn.execute(f'SELECT max(msg_id) FROM "{target}"').fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def export_database(
    src: Path | str,
    dst: Path | str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    include_c2c: bool = True,
    full: bool = False,
    progress: Callable[[int, int, str], None] | None = None,
) -> ExportReport:
    """`nt_msg_plain.db` → `nt_msg_export.db`（**增量**，按 msg_id）。

    增量是"接着导出库里已有的最大 msg_id 往后导"。为什么这样安全：

    * 源表的 `msg_id`（`"40001"`）就是 `INTEGER PRIMARY KEY`，**只增不改** ——
      新消息的 id 永远比旧的大。所以"导到哪一条了"就是一个精确的游标，
      既不会漏（不像时间戳会漏掉同秒后到的），也不需要回看窗口。
    * 导库的写入是幂等的（主键 `msg_id`），中断了下次接着导，最多重导一批。

    什么时候会**整表重导**（`report.rebuilt`）：源库的 `max(msg_id)` 比导出库里的
    还小 —— 那说明换了一份更旧的源库（或者换了账号）。这时候继续增量只会让导出库
    停在旧数据上，所以清空重导。想强制全量传 `full=True`（或直接删掉导出库文件）。
    """
    started = time.monotonic()
    src_path = Path(src)
    dst_path = Path(dst)
    if not src_path.exists():
        raise ExportError(f"找不到解密后的库：{src_path}（先跑解密那一步）")
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    report = ExportReport(src=src_path, dst=dst_path)

    try:
        src_conn = sqlite3.connect(sqlite_uri(src_path, "ro"), uri=True, timeout=60.0)
    except sqlite3.Error as exc:
        raise ExportError(f"打不开明文库 {src_path}：{exc}") from exc

    try:
        src_conn.row_factory = sqlite3.Row
        tables = {
            str(r[0])
            for r in src_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "group_msg_table" not in tables:
            raise ExportError(
                f"{src_path.name} 里没有 group_msg_table（有：{sorted(tables)[:12]}）。"
                "如果这是一份已经导出过的库，请把 CLIENT_DB_PATH 直接指向它、"
                "并把解密/导出这两步关掉。"
            )

        with closing(
            sqlite3.connect(sqlite_uri(dst_path, "rwc"), uri=True, timeout=60.0)
        ) as dst_conn:
            dst_conn.execute("PRAGMA journal_mode=WAL")
            dst_conn.execute("PRAGMA synchronous=NORMAL")
            dst_conn.execute("PRAGMA cache_size=-65536")

            init_db(dst_conn)
            drop_fts_triggers(dst_conn)

            jobs: list[tuple[str, str, object, object]] = []
            if include_c2c:
                jobs.append(("c2c", "c2c_messages", c2c_parser.SELECT_SQL, c2c_parser.parse_row))
            jobs.append(
                ("group", "group_messages", group_exporter.SELECT_SQL, group_exporter.parse_row)
            )

            for label, target, select_sql, parse_row in jobs:
                if label == "c2c" and "c2c_msg_table" not in tables:
                    logger.info("源库里没有 c2c_msg_table，跳过")
                    continue
                count_sql = (
                    c2c_parser.SELECT_COUNT_SQL
                    if label == "c2c"
                    else group_exporter.SELECT_COUNT_SQL
                )
                report.source_rows[target] = int(src_conn.execute(count_sql).fetchone()[0])

                # 起点：导出库里已有的最大 msg_id（每一轮接着上一轮）
                since = 0 if full else _dest_max_id(dst_conn, target)
                source_max = int(
                    src_conn.execute(
                        f'SELECT max(rowid) FROM "{_source_table(label)}"'
                    ).fetchone()[0]
                    or 0
                )
                if since and source_max and source_max < since:
                    logger.warning(
                        "%s：源库最大 msg_id %s 比导出库里的 %s 还小 —— 像是换了一份更旧的"
                        "源库（或换了账号）。整表重导一次。",
                        target,
                        f"{source_max:,}",
                        f"{since:,}",
                    )
                    dst_conn.execute(f'DELETE FROM "{target}"')
                    dst_conn.commit()
                    report.rebuilt.append(target)
                    since = 0
                report.since_msg_id[target] = since

                written = _export_one(
                    src_conn,
                    dst_conn,
                    target=target,
                    select_sql=select_sql,
                    parse_row=parse_row,
                    batch_size=batch_size,
                    since_msg_id=since,
                    report=report,
                    progress=progress,
                )
                report.written_rows[target] = written
                if written:
                    report.tables.append(target)

            logger.info("建立二级索引…")
            create_indexes(dst_conn)

            # 一份新行都没写进去时，**收尾这几步全都不用做**：索引没变、FTS 没变。
            # 这几步里 `rebuild_fts()` 是固定 5.45 秒（77 万行），而客户端每 5 分钟
            # 就跑一轮 —— 绝大多数轮次一条新消息都没有，那 5 秒纯属白花。
            # 有新行时仍然老老实实重建（FTS 是外部内容表，插了行不重建就会漏搜 ——
            # 客户端自己不用 FTS，但用户可能拿导出库跑上游的 nt_msg_search.py）。
            if report.total_written:
                logger.info("重建 FTS 索引（上游的 3.export.py 也做这一步）…")
                rebuild_fts(dst_conn)
                dst_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            else:
                logger.debug("没有新行：跳过重建 FTS 与收尾检查点")

            # 收尾切回 DELETE：客户端是**只读**打开导出库的，而 WAL 模式下没有 -shm
            # 文件就没法只读打开（-shm 是被删过/换过机器之后就没了）。
            _switch_back_to_delete(dst_conn, report)
    except sqlite3.Error as exc:
        raise ExportError(
            f"导出失败（{src_path.name} → {dst_path.name}）：{type(exc).__name__}: {exc}"
        ) from exc
    finally:
        try:
            src_conn.close()
        except Exception:  # noqa: BLE001,S110
            pass

    report.watermark = watermark(dst_path)
    report.seconds = time.monotonic() - started
    if report.failed_rows:
        logger.error(
            "⚠️ 有 %d 行在转换时抛了异常，被跳过（最后一条：%s）。"
            "这些消息不会出现在导出库里，也就不会进清单。",
            report.failed_rows,
            report.last_error,
        )
    logger.info("[完成] %s", report.summary())
    return report


def _export_one(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    *,
    target: str,
    select_sql: str,
    parse_row: Callable,
    batch_size: int,
    since_msg_id: int,
    report: ExportReport,
    progress: Callable[[int, int, str], None] | None,
) -> int:
    """一张表：逐行解析 → 批量写。返回写入行数。"""
    insert = insert_messages_batch if target == "c2c_messages" else insert_group_messages_batch
    sql, params = _incremental_sql(select_sql, since_msg_id)
    total = report.source_rows.get(target, 0)

    batch: list[dict] = []
    written = 0
    seen = 0
    t0 = time.monotonic()

    def flush() -> None:
        nonlocal written
        if not batch:
            return
        with dst_conn:
            insert(dst_conn, batch)
        written += len(batch)
        batch.clear()

    for raw in src_conn.execute(sql, params):
        seen += 1
        try:
            value = parse_row(raw)
            row = value.to_db_row() if hasattr(value, "to_db_row") else value
            batch.append(row)
            status = row.get("parse_status")
            if status:
                report.parse_status[status] = report.parse_status.get(status, 0) + 1
        except Exception as exc:  # noqa: BLE001 - 一行坏掉不该中断整批（上游同样继续）
            report.failed_rows += 1
            report.last_error = f"{type(exc).__name__}: {exc}"
            logger.debug("%s msg_id=%s 转换失败：%s", target, raw["msg_id"], exc)

        if len(batch) >= batch_size:
            flush()
            if progress:
                progress(written, total, target)
            if seen % LOG_INTERVAL == 0:
                logger.info("      %s：已写 %s 行…", target, f"{written:,}")
    flush()

    elapsed = time.monotonic() - t0
    rate = written / elapsed if elapsed else 0
    logger.info(
        "      → %s：%s 行（%.0f 行/s，%.0fs）",
        target,
        f"{written:,}",
        rate,
        elapsed,
    )
    return written
