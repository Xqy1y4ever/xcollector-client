"""把明文库 `nt_msg_plain.db` 导成客户端认识的结构化库 `nt_msg_export.db`。

**这一层是 `nt_msg_db_util` 的 `3.export.py` 整合进本项目的版本。**
上游的 `msgdb` 包已经**逐字**搬到本仓库根目录（见 `msgdb/VENDORED.md`），
这里只做三件事：把上游的流程串起来、加上客户端要的进度与报告、补两列。

流程与上游完全一致：

    init_db（建表 + FTS + 触发器）
    → 摘掉 FTS 触发器（批量写入时维护索引太慢）
    → 逐行解析 40800 并批量写 c2c_messages / group_messages
    → 建二级索引
    → 重建 FTS 索引

表结构、`content` 的形状（`{"type":"msg_body","segments":[...]}`）、`parse_status`
的取值都由上游的 `msgdb` 决定 —— **这一层不改它的形状**，客户端那边按这个形状读。

## 客户端加的三件事

1. **增量**（`since_ts`）：上游每次都全量解析 77 万行。客户端是"定期跑"的，
   所以默认只解析水位线之后的行（同一个 SELECT 外面套一层 WHERE），
   没变过的老行不再重复解析。`force`（`--prepare`）时退回全量。
2. **补 `"40003"` / `"40850"` 两列**：群内消息序号、被回复消息的群内序号。
   上游的 `group_messages` 没有它们，于是"这条消息是在补充哪条通知"没法确定性
   反查（客户端只好把它当成一条新消息）。补这两列之后补充关系才能真正落到
   原来那条任务上。列是**后加的**，所以不影响任何按列名读取的消费者。
3. **报告**：写了多少行、解析状态分布、多少行转换失败。上游把失败行记在
   `errors` 计数里就继续跑 —— 这里同样继续跑（一条消息解析失败不该中断整个导出），
   但把计数和最后一例原因报出来。
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

from ..utils import quote_sql, sqlite_uri

logger = logging.getLogger(__name__)

# 进度日志每隔多少行打一次（上游也是 50_000）。
LOG_INTERVAL = 50_000

DEFAULT_BATCH_SIZE = 2000

# 后加的两列：列名和含义都来自上游的字段文档（`db_docs/group_msg_table/`）。
#   "40003" 群内消息序号（每个群各自一套，所以反查时要带 group_id）
#   "40850" 被回复消息的群内序号
SEQ_COLUMN = "40003"
REPLY_SEQ_COLUMN = "40850"


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
    seq_added: bool = False
    seq_filled: int = 0
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
            parts.append(f"（增量：只处理 {self.since_ts} 之后的行）")
        if self.parse_status:
            parts.append(
                "正文解析：" + "、".join(f"{k} {v:,}" for k, v in sorted(self.parse_status.items()))
            )
        if self.failed_rows:
            parts.append(f"⚠️ {self.failed_rows} 行转换失败（最后一条：{self.last_error}）")
        if self.seq_added:
            parts.append(f"补序号列 {self.seq_filled:,} 行")
        parts.append(f"耗时 {self.seconds:.1f}s")
        return "；".join(parts)


def _incremental_sql(select_sql: str, since_ts: int) -> tuple[str, tuple]:
    """给上游的 SELECT 外面套一层增量过滤。

    上游的语句以 `ORDER BY "40001"` 结尾，包成子查询后 SQLite 会把 WHERE 条件下推，
    效果和改写原语句一样；这样就不必去动 `msgdb` 里的常量（保持逐字搬运）。
    """
    if since_ts <= 0:
        return select_sql, ()
    return f"SELECT * FROM ({select_sql}) WHERE timestamp >= ?", (since_ts,)


def _add_seq_columns(dst: sqlite3.Connection, src_path: Path, report: ExportReport) -> None:
    """补 `"40003"` / `"40850"` 两列，并从源库把值填进去。

    为什么是"导完再补"而不是改上游的 INSERT：这样 `msgdb/` 保持与上游逐字一致
    （上游升级时直接覆盖即可），扩展只活在客户端自己的代码里。

    实现上把源库 ATTACH 进来做一条 `UPDATE ... = (SELECT ...)`：源表的 `"40001"`
    是 `INTEGER PRIMARY KEY`（rowid 别名），所以每条都是一次主键查找。
    """
    columns = {str(r[1]) for r in dst.execute('PRAGMA table_info("group_messages")')}
    for column in (SEQ_COLUMN, REPLY_SEQ_COLUMN):
        if column not in columns:
            dst.execute(f'ALTER TABLE group_messages ADD COLUMN "{column}" INTEGER')
    report.seq_added = True

    dst.execute(
        "ATTACH DATABASE " + quote_sql(sqlite_uri(src_path, "ro")) + " AS seqsrc"
    )
    try:
        has = {
            str(r[1])
            for r in dst.execute('PRAGMA seqsrc.table_info("group_msg_table")').fetchall()
        }
        sets: list[str] = []
        if SEQ_COLUMN in has:
            sets.append(
                f'"{SEQ_COLUMN}" = (SELECT s."{SEQ_COLUMN}" FROM seqsrc.group_msg_table s '
                f'WHERE s."40001" = group_messages.msg_id)'
            )
        if REPLY_SEQ_COLUMN in has:
            sets.append(
                f'"{REPLY_SEQ_COLUMN}" = (SELECT s."{REPLY_SEQ_COLUMN}" '
                f'FROM seqsrc.group_msg_table s WHERE s."40001" = group_messages.msg_id)'
            )
        if not sets:
            logger.info("源表里没有 %s/%s 这两列，序号列留空", SEQ_COLUMN, REPLY_SEQ_COLUMN)
            return
        before = dst.execute('SELECT count(*) FROM group_messages WHERE "40003" IS NOT NULL').fetchone()[0]
        dst.execute(f'UPDATE group_messages SET {", ".join(sets)}')
        after = dst.execute('SELECT count(*) FROM group_messages WHERE "40003" IS NOT NULL').fetchone()[0]
        report.seq_filled = int(after) - int(before)
        dst.commit()
    finally:
        dst.execute("DETACH DATABASE seqsrc")


def watermark(path: Path | str, table: str = "group_messages") -> int:
    """导出库里已有的最大时间戳（增量导出的起点）。没有就返回 0。"""
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
    add_seq: bool = True,
    resume: bool = True,
    progress: Callable[[int, int, str], None] | None = None,
) -> ExportReport:
    """`nt_msg_plain.db` → `nt_msg_export.db`。

    `resume=True` 时按导出库里已有的最大时间戳接着导（并往回多看
    `overlap_seconds` 秒）。**默认（`resume=False`）是全量**，因为按时间过滤有个
    后果：时间戳没变、内容被改过的旧消息不会重新进导出库 —— 客户端那一层靠内容
    指纹发现"内容变了"的前提就是导出库里有新内容。全量的代价是每轮重新解析一遍
    源库（实测 77 万行约 45 秒）。
    """
    started = time.monotonic()
    src_path = Path(src)
    dst_path = Path(dst)
    if not src_path.exists():
        raise ExportError(f"找不到解密后的库：{src_path}（先跑解密那一步）")
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    report = ExportReport(src=src_path, dst=dst_path)
    if resume and since_ts <= 0:
        existing = watermark(dst_path)
        if existing > 0:
            since_ts = max(1, existing - max(0, overlap_seconds))
            logger.info(
                "%s 里已经导到 %s，从 %s 接着导（含 %d 秒回看）",
                dst_path.name,
                existing,
                since_ts,
                overlap_seconds,
            )
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

        # uri=True 是必须的：下面补序号列时要用 `ATTACH 'file:...?mode=ro'`，
        # 而 ATTACH 只在这个连接本身按 URI 打开时才认 URI 文件名。
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

            if add_seq:
                logger.info("补 %s / %s 两列（补充关系要用）…", SEQ_COLUMN, REPLY_SEQ_COLUMN)
                _add_seq_columns(dst_conn, src_path, report)

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
