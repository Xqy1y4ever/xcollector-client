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
    since_ts: int = 0
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
        if self.since_ts:
            parts.append(f"（只处理 {self.since_ts} 之后的行）")
        if self.parse_status:
            parts.append(
                "正文解析：" + "、".join(f"{k} {v:,}" for k, v in sorted(self.parse_status.items()))
            )
        if self.failed_rows:
            parts.append(f"⚠️ {self.failed_rows} 行转换失败（最后一条：{self.last_error}）")
        parts.append(f"耗时 {self.seconds:.1f}s")
        return "；".join(parts)


def _incremental_sql(select_sql: str, since_ts: int) -> tuple[str, tuple]:
    """给上游的 SELECT 外面套一层时间过滤（`since_ts=0` 时原样返回 = 全量）。

    上游的语句以 `ORDER BY "40001"` 结尾，包成子查询后 SQLite 会把 WHERE 条件下推，
    效果和改写原语句一样；这样就不必去动 `msgdb` 里的常量（保持逐字搬运）。
    """
    if since_ts <= 0:
        return select_sql, ()
    return f"SELECT * FROM ({select_sql}) WHERE timestamp >= ?", (since_ts,)


def watermark(path: Path | str, table: str = "group_messages") -> int:
    """导出库里已有的最大时间戳。没有就返回 0（报告里用）。"""
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
            row = conn.execute(f'SELECT max("timestamp") FROM "{table}"').fetchone()
    except sqlite3.Error as exc:
        raise ExportError(f"读导出库失败 {path}：{exc}") from exc
    return int(row[0]) if row and row[0] is not None else 0


def export_database(
    src: Path | str,
    dst: Path | str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    include_c2c: bool = True,
    since_ts: int = 0,
    overlap_seconds: int = 0,
    progress: Callable[[int, int, str], None] | None = None,
) -> ExportReport:
    """`nt_msg_plain.db` → `nt_msg_export.db`（**全量**）。

    `since_ts`/`overlap_seconds` 只在调用方明确要给一个起点时才用（正常路径不传，
    即全量）。按时间过滤的代价见模块开头那段说明。
    """
    started = time.monotonic()
    src_path = Path(src)
    dst_path = Path(dst)
    if not src_path.exists():
        raise ExportError(f"找不到解密后的库：{src_path}（先跑解密那一步）")
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    report = ExportReport(src=src_path, dst=dst_path)
    if since_ts > 0 and overlap_seconds:
        since_ts = max(1, since_ts - max(0, overlap_seconds))
    report.since_ts = since_ts

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
                written = _export_one(
                    src_conn,
                    dst_conn,
                    target=target,
                    select_sql=select_sql,
                    parse_row=parse_row,
                    batch_size=batch_size,
                    since_ts=since_ts,
                    report=report,
                    progress=progress,
                )
                report.written_rows[target] = written
                if written:
                    report.tables.append(target)

            logger.info("建立二级索引…")
            create_indexes(dst_conn)

            logger.info("重建 FTS 索引（上游的 3.export.py 也做这一步）…")
            rebuild_fts(dst_conn)

            dst_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            # 收尾切回 DELETE：客户端是只读打开导出库的，只读连接建不了 -shm
            dst_conn.execute("PRAGMA journal_mode=DELETE;")
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
    since_ts: int,
    report: ExportReport,
    progress: Callable[[int, int, str], None] | None,
) -> int:
    """一张表：逐行解析 → 批量写。返回写入行数。"""
    insert = insert_messages_batch if target == "c2c_messages" else insert_group_messages_batch
    sql, params = _incremental_sql(select_sql, since_ts)
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
