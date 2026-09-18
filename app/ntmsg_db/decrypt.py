"""把 `nt_msg.db` 解密成普通 SQLite 库。

**这一层是 `nt_msg_db_util` 的 `1.decrypt.py` 整合进本项目的版本**（不是另写一套
密码学实现）。流程与它完全一致：

    1. 剥掉 QQ 前面那 1024 字节的自定义头      → nt_msg_clear.db
    2. 用 sqlcipher3 打开（**PRAGMA 顺序是有讲究的**）
    3. 逐表 rowid 游标分页拷到明文库，坏页自动跳过 → nt_msg_plain.db
    4. 复制索引定义

上游那句"PRAGMA 顺序错误会导致解密失败"是真的，顺序照抄：

    PRAGMA cipher_page_size = 4096;   ← 必须在 key 之前
    PRAGMA key = '<16 个 ASCII 字符>';
    PRAGMA kdf_iter = 4000;
    PRAGMA cipher_hmac_algorithm = HMAC_SHA1;
    PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;

## 和上游的三处差别（都是"把话说清楚"，不是改行为）

1. **坏页跳过的行号会记下来并报出来。** 上游只计数（`bad_skips`），我们额外记录
   具体跳过了哪些 rowid 并在结束时打 ERROR 级日志 —— 被跳过的行意味着**那几条消息
   永远进不来**，这属于必须让人看见的事，不能只留一个数字。
2. **拷完以后对一遍账**：源表多少行、拷了多少行、跳了多少行，三者对不上就报 ERROR。
3. **输出库收尾时把 WAL 收干净**（checkpoint 后切回 journal_mode=DELETE）。
   客户端是**只读**打开这个库的（`mode=ro`），只读连接建不了 `-shm`，
   留着 `-wal` 容易在下一个进程里报出莫名其妙的错。上游的产物就有这个残留。

## 一个上游没覆盖的情况

`INSERT OR IGNORE ... VALUES(...)` 这种写法**依赖表的 rowid 就是业务主键**，
所以"从 `max(rowid)` 续跑"只对 `INTEGER PRIMARY KEY` 的表成立（消息表都是，
所以上游没踩到）。对没有 INTEGER 主键的表，续跑会插重复行。这里的处理是：
这类表**不续跑**，如果输出库里已经有行就清空重拷（幂等且正确），并打一行说明。
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence

from .sqlite_uri import sqlite_uri

logger = logging.getLogger(__name__)

# NTQQ 自定义头的长度（固定值，上游同样写死 1024）。
DEFAULT_HEADER_SIZE = 1024

# 每批读多少行。遇到坏页会临时缩小，成功后再逐步放大回来。
DEFAULT_BATCH_SIZE = 5000

# 复制大量块时用的缓冲区（64 MB），避免大文件占满内存。
COPY_CHUNK = 64 << 20

# SQLite 自己的内部表，不拷（上游同样跳过）。
INTERNAL_TABLES = {
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
}

INTEGRITY_MODES = ("quick", "full", "off")

# 坏页跳过的 rowid 最多记这么多个（计数仍然是全量）。
MAX_RECORDED_SKIPS = 50

# 连续跳过多少行之后放弃这张表。
#
# 为什么需要这个上限：坏页本身只需要 `rowid + 1` 跳过就行，但**整块页面损坏**时，
# 任何一次查询都会失败（连"rowid 更大的行"也定位不到），于是"跳过一行再试"会
# **永远循环下去**。上游 `1.decrypt.py` 没有这个上限（它的注释假设"单行坏"），
# 真遇到整块坏掉的表会卡死。这里的取舍是：连续跳过这么多行、中间一行都没读到，
# 就认定这张表读不下去了，停下来把话说清楚。
MAX_CONSECUTIVE_SKIPS = 50


class DecryptError(RuntimeError):
    """解密失败。消息里一定写清"下一步该怎么办"。"""


def sqlcipher_available() -> bool:
    """能不能 import sqlcipher3（解密这一层唯一的必需依赖）。"""
    try:
        import sqlcipher3.dbapi2  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


def _require_sqlcipher():
    try:
        import sqlcipher3.dbapi2 as sc
    except ImportError as exc:  # pragma: no cover - 环境缺依赖时的明确报错
        raise DecryptError(
            "缺少 sqlcipher3，无法解密 nt_msg.db。\n"
            "  Windows ：pip install sqlcipher3（有 cp312~cp314 的 wheel）\n"
            "  Linux   ：pip install sqlcipher3-wheels（同一个模块名，有预编译 wheel）\n"
            "  Docker  ：用本仓库的 Dockerfile 就行，它已经装好了。"
        ) from exc
    return sc


def _pragma_hmac(algorithm: str) -> str:
    return f"HMAC_{algorithm.upper()}"


def _pragma_kdf(algorithm: str) -> str:
    return f"PBKDF2_HMAC_{algorithm.upper()}"


@dataclass
class TableReport:
    table: str
    source_rows: int = 0
    copied_rows: int = 0
    skipped_rows: int = 0
    skipped_rowids: list[int] = field(default_factory=list)
    resumed_from: int = 0
    recopied: bool = False
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        """没有跳过的行就算好。总数未知（-1）时只看"有没有跳过"。"""
        if self.skipped_rows:
            return False
        if self.source_rows < 0:
            return self.copied_rows > 0
        return self.copied_rows >= self.source_rows


@dataclass
class DecryptReport:
    src: Path
    clear_path: Path
    out_path: Path
    header_size: int = DEFAULT_HEADER_SIZE
    header_reused: bool = False
    tables: list[str] = field(default_factory=list)
    per_table: list[TableReport] = field(default_factory=list)
    indexes_copied: int = 0
    integrity: str | None = None
    integrity_mode: str = "quick"
    seconds: float = 0.0

    @property
    def total_rows(self) -> int:
        return sum(item.copied_rows for item in self.per_table)

    @property
    def total_source_rows(self) -> int:
        """源表总行数。有表数不出来（坏页）时返回 -1。"""
        values = [item.source_rows for item in self.per_table]
        if any(value < 0 for value in values):
            return -1
        return sum(values)

    @property
    def total_skipped(self) -> int:
        return sum(item.skipped_rows for item in self.per_table)

    @property
    def skipped_rowids(self) -> list[int]:
        out: list[int] = []
        for item in self.per_table:
            out.extend(item.skipped_rowids)
        return out

    @property
    def ok(self) -> bool:
        return self.total_skipped == 0 and (self.integrity in (None, "ok"))

    def summary(self) -> str:
        parts = [
            f"{len(self.tables)} 张表、{self.total_rows:,} 行",
            f"耗时 {self.seconds:.1f}s",
        ]
        if self.total_skipped:
            parts.append(f"⚠️ 跳过 {self.total_skipped} 行（坏页）")
        if self.indexes_copied:
            parts.append(f"{self.indexes_copied} 个索引")
        if self.integrity_mode != "off":
            parts.append(f"自检={self.integrity}")
        return "；".join(parts)


# ---------------------------------------------------------------------------
# 步骤 1：剥头
# ---------------------------------------------------------------------------


def strip_header(src: Path | str, dst: Path | str, header_size: int = DEFAULT_HEADER_SIZE) -> bool:
    """`nt_msg.db` → `nt_msg_clear.db`：跳过前 `header_size` 字节，其余原样拷。

    返回 True 表示真的拷了；False 表示目标文件已经是对的（大小一致），跳过。
    """
    src_path = Path(src)
    dst_path = Path(dst)
    if not src_path.exists():
        raise DecryptError(f"找不到 nt_msg.db：{src_path}")
    expected = src_path.stat().st_size - header_size
    if expected <= 0:
        raise DecryptError(
            f"{src_path} 只有 {src_path.stat().st_size:,} 字节，比 {header_size} 字节的文件头还小"
        )
    if dst_path.exists() and dst_path.stat().st_size == expected:
        logger.info("[1/3] %s 已存在且大小一致，跳过剥头", dst_path.name)
        return False

    logger.info("[1/3] 剥头：%s → %s", src_path.name, dst_path.name)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with open(src_path, "rb") as fin:
        fin.seek(header_size)
        with open(dst_path, "wb") as fout:
            while True:
                chunk = fin.read(COPY_CHUNK)
                if not chunk:
                    break
                fout.write(chunk)
    actual = dst_path.stat().st_size
    if actual != expected:
        raise DecryptError(
            f"剥头后写出的大小不对：期望 {expected:,} 字节，实际 {actual:,} 字节"
        )
    logger.info("      写出 %s 字节", f"{actual:,}")
    return True


# ---------------------------------------------------------------------------
# 步骤 2：打开加密库
# ---------------------------------------------------------------------------


def open_encrypted(
    path: Path | str,
    key: str,
    *,
    page_size: int = 4096,
    kdf_iter: int = 4000,
    hmac_algorithm: str = "sha1",
    kdf_algorithm: str = "sha512",
):
    """按上游那套 PRAGMA 打开 SQLCipher 库，并**真的读一次**以验证密钥。

    `PRAGMA` 的顺序不能变：`cipher_page_size` 必须在 `key` 之前。
    """
    sc = _require_sqlcipher()
    if not key:
        raise DecryptError(
            "没有配置密钥。密钥要从 NTQQ 进程内存里自己取（见 README「怎么拿到密钥」），"
            "然后配到 CLIENT_NT_MSG_KEY（或用 CLIENT_NT_MSG_KEY_FILE 指向一个只读文件）。"
        )
    path = Path(path)
    if not path.exists():
        raise DecryptError(f"找不到要解密的库：{path}")

    try:
        conn = sc.connect(str(path), isolation_level=None)
    except Exception as exc:  # noqa: BLE001
        raise DecryptError(f"打不开 {path}：{exc}") from exc

    # 上游用"把单引号翻倍"的方式转义 —— 密钥里真会出现引号（QQ 的密钥是随机可见
    # 字符），所以这一步不能省。
    safe_key = key.replace("'", "''")
    try:
        conn.execute(f"PRAGMA cipher_page_size = {int(page_size)};")
        conn.execute(f"PRAGMA key = '{safe_key}';")
        conn.execute(f"PRAGMA kdf_iter = {int(kdf_iter)};")
        conn.execute(f"PRAGMA cipher_hmac_algorithm = {_pragma_hmac(hmac_algorithm)};")
        conn.execute(f"PRAGMA cipher_kdf_algorithm = {_pragma_kdf(kdf_algorithm)};")
        # 这一句是"真的去读一次"：密钥不对时它才会抛，前面的 PRAGMA 都是延迟生效的
        conn.execute("SELECT count(*) FROM sqlite_master;").fetchone()
    except Exception as exc:  # noqa: BLE001
        try:
            conn.close()
        except Exception:  # noqa: BLE001,S110
            pass
        raise DecryptError(
            f"解密失败（密钥或参数不对）：{type(exc).__name__}: {exc}\n"
            "请依次确认：\n"
            "  1) 密钥是不是**这个 QQ 账号**的（换账号/换机器/重装 QQ 都要重取）；\n"
            f"  2) 密钥长度是 {len(key)} 字节（应该是 16 个 ASCII 字符）；\n"
            "  3) 抄进来的密钥有没有多余的空格/换行（用 CLIENT_NT_MSG_KEY_FILE 会自动去掉）；\n"
            "  4) 给的是不是 nt_msg.db（剥头前的原始库）；\n"
            "  5) 参数是不是被改过（默认 page_size=4096 / kdf_iter=4000 / "
            "HMAC_SHA1 / PBKDF2_HMAC_SHA512）。"
        ) from exc
    return conn


def table_names(conn) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]


def _integer_pk(conn, table: str) -> str | None:
    """表的 rowid 别名主键（`INTEGER PRIMARY KEY`）是哪一列；没有就返回 None。

    这个判断决定了"能不能从 max(rowid) 续跑"：只有 rowid 就是业务主键时，
    `INSERT OR IGNORE ... VALUES` 的 rowid 才和源库一致。
    """
    info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    if len(info) == 1 and str(info[0][1]) == "rowid":
        return "rowid"
    for _cid, name, ctype, _notnull, _default, pk in info:
        if pk and str(ctype or "").upper() == "INTEGER":
            return str(name)
    return None


# ---------------------------------------------------------------------------
# 步骤 3：逐表拷贝
# ---------------------------------------------------------------------------


def copy_table(
    enc_path: Path | str,
    plain: sqlite3.Connection,
    table: str,
    key: str,
    *,
    page_size: int = 4096,
    kdf_iter: int = 4000,
    hmac_algorithm: str = "sha1",
    kdf_algorithm: str = "sha512",
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress: Callable[[str, int, int], None] | None = None,
) -> TableReport:
    """把一张表从加密库拷到明文库（含坏页容忍与断点续跑）。

    返回这一张表的报告。**不在这里抛坏页异常** —— 坏页只跳过并计数，
    由调用方决定"跳过多少算不能接受"。
    """
    report = TableReport(table=table)
    started = time.monotonic()
    enc = open_encrypted(
        enc_path,
        key,
        page_size=page_size,
        kdf_iter=kdf_iter,
        hmac_algorithm=hmac_algorithm,
        kdf_algorithm=kdf_algorithm,
    )
    try:
        row = enc.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if row is None or not row[0]:
            raise DecryptError(f"加密库里没有 {table} 表（或它没有建表语句）")
        ddl = re.sub(
            r"^CREATE\s+TABLE\s+", "CREATE TABLE IF NOT EXISTS ", str(row[0]), flags=re.IGNORECASE
        )
        plain.execute(ddl)
        plain.commit()

        ncols = len(enc.execute(f'PRAGMA table_info("{table}")').fetchall())
        placeholders = ",".join("?" * ncols)

        pk = _integer_pk(enc, table)
        try:
            report.source_rows = int(
                enc.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            )
        except Exception as exc:  # noqa: BLE001
            # 表里有坏页时**连 count(*) 都会失败**（它要扫全表）。上游在这一步会直接
            # 崩掉、整轮结束；这里退一步：先把能读的行读出来，坏行照样被跳过并计数，
            # 只是"总共该有多少行"这个数字拿不到了（记成 -1，报告里显示"未知"）。
            logger.error(
                "%s 的行数都数不出来（%s）—— 说明这个表里有坏页。"
                "继续逐行拷贝，坏掉的行会被跳过并计数，但**总数未知**。",
                table,
                exc,
            )
            report.source_rows = -1

        # 已有数据怎么办：
        #   · rowid 就是业务主键 → 从 max(rowid) 续跑（上游的做法）
        #   · 否则 → 续跑会插重复行，只能清空重拷
        existing = plain.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
        if existing and pk is None:
            logger.warning(
                "%s 没有 INTEGER 主键，无法安全续跑 —— 清空输出表重拷（上游在这种情况下会插重复行）",
                table,
            )
            plain.execute(f'DELETE FROM "{table}"')
            plain.commit()
            report.recopied = True
        elif existing:
            report.resumed_from = (
                plain.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()[0] or 0
            )
            if report.resumed_from:
                logger.info(
                    "%s 已经拷过 %s 行，从 rowid %s 之后接着拷",
                    table,
                    f"{existing:,}",
                    f"{report.resumed_from:,}",
                )

        last_rowid = report.resumed_from
        batch = max(1, int(batch_size))
        copied = 0
        skipped = 0
        consecutive_skips = 0
        while True:
            try:
                rows = enc.execute(
                    f'SELECT rowid, * FROM "{table}" WHERE rowid > ? ORDER BY rowid LIMIT ?',
                    (last_rowid, batch),
                ).fetchall()
            except Exception as exc:  # noqa: BLE001 - 坏页：重连 + 缩小批次
                logger.debug("%s 读到坏页（%s），重连并把批次缩到 %d", table, exc, max(1, batch // 4))
                try:
                    enc.close()
                except Exception:  # noqa: BLE001,S110
                    pass
                enc = open_encrypted(
                    enc_path,
                    key,
                    page_size=page_size,
                    kdf_iter=kdf_iter,
                    hmac_algorithm=hmac_algorithm,
                    kdf_algorithm=kdf_algorithm,
                )
                if batch == 1:
                    # 单行也读不出来 → 这个 rowid 是坏的，跳过它继续
                    last_rowid += 1
                    skipped += 1
                    consecutive_skips += 1
                    if len(report.skipped_rowids) < MAX_RECORDED_SKIPS:
                        report.skipped_rowids.append(last_rowid)
                    if consecutive_skips > MAX_CONSECUTIVE_SKIPS:
                        raise DecryptError(
                            f"{table} 连续跳过 {consecutive_skips} 行、中间一行都没读到："
                            f"这张表**整块读不下去**了（最后一条错误：{exc}）。"
                            f"已经拷入 {copied:,} 行、跳过 {skipped:,} 行。\n"
                            "这不是「某一页偶尔坏掉」，靠跳过解决不了。请：\n"
                            "  1) 关掉 QQ，把 nt_msg.db 复制一份再试（可能只是正在写盘）；\n"
                            "  2) 或者用上游的 nt_msg_db_util 跑一遍，看是否同样失败；\n"
                            "  3) 实在只要某几个群的消息，可以把 CLIENT_DECRYPT_TABLES "
                            "收窄到需要的表。"
                        ) from exc
                    batch = min(int(batch_size), 100)
                else:
                    batch = max(1, batch // 4)
                continue

            if not rows:
                break

            consecutive_skips = 0
            last_rowid = int(rows[-1][0])
            plain.executemany(
                f'INSERT OR IGNORE INTO "{table}" VALUES ({placeholders})',
                [r[1:] for r in rows],
            )
            plain.commit()
            copied += len(rows)

            if batch < int(batch_size):
                batch = min(int(batch_size), batch * 2)  # 恢复批次
            if progress and copied and copied % 50_000 == 0:
                progress(table, copied, report.source_rows)

        report.copied_rows = copied
        report.skipped_rows = skipped
    finally:
        try:
            enc.close()
        except Exception:  # noqa: BLE001,S110
            pass

    report.seconds = time.monotonic() - started
    logger.info(
        "      %s：拷入 %s/%s 行%s（%.0fs）",
        table,
        f"{report.copied_rows:,}",
        f"{report.source_rows:,}",
        f"，**跳过 {report.skipped_rows} 行**" if report.skipped_rows else "",
        report.seconds,
    )
    return report


def copy_indexes(enc_path: Path | str, plain: sqlite3.Connection, key: str, **kwargs) -> tuple[int, int]:
    """把加密库里的索引定义搬到明文库。返回 `(成功, 失败)`。

    失败是**正常**的：索引可能依赖没被复制过来的表（或者已经存在）。这里不静默丢弃，
    而是把数量报出来 —— 索引缺失会让查询变慢，但不会让数据出错。
    """
    enc = open_encrypted(enc_path, key, **kwargs)
    try:
        indexes = enc.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
        ).fetchall()
    finally:
        try:
            enc.close()
        except Exception:  # noqa: BLE001,S110
            pass
    copied = 0
    failed: list[str] = []
    for name, ddl in indexes:
        try:
            plain.execute(str(ddl))
            copied += 1
        except Exception:  # noqa: BLE001 - 已存在 / 依赖的表不在
            failed.append(str(name))
    plain.commit()
    if failed:
        logger.debug("有 %d 个索引没能复制（依赖的表不在或已存在）：%s", len(failed), failed[:8])
    return copied, len(failed)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def decrypt_database(
    src: Path | str,
    clear_path: Path | str,
    out_path: Path | str,
    key: str,
    *,
    header_size: int = DEFAULT_HEADER_SIZE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    tables: Sequence[str] | None = None,
    page_size: int = 4096,
    kdf_iter: int = 4000,
    hmac_algorithm: str = "sha1",
    kdf_algorithm: str = "sha512",
    integrity: str = "quick",
    max_skips: int = -1,
    progress: Callable[[str, int, int], None] | None = None,
) -> DecryptReport:
    """`nt_msg.db` → `nt_msg_plain.db`（中间产物 `nt_msg_clear.db`）。

    `tables=None` 表示**所有非内部表**（上游行为）。只关心消息表时用
    `CLIENT_DECRYPT_TABLES=group_msg_table,c2c_msg_table`，能省掉一部分时间。

    `max_skips`：允许多少行因为坏页被跳过。`-1` = 不限制（上游行为）。
    超过就抛 `DecryptError` —— 跳过的行意味着那几条消息永远进不来，
    默认容忍是为了不因为一个坏页卡死整天，但**必须报出来**。
    """
    started = time.monotonic()
    src_path = Path(src)
    clear = Path(clear_path)
    out = Path(out_path)
    if integrity not in INTEGRITY_MODES:
        raise DecryptError(f"integrity 只能是 {list(INTEGRITY_MODES)}，收到 {integrity!r}")

    report = DecryptReport(
        src=src_path,
        clear_path=clear,
        out_path=out,
        header_size=header_size,
        integrity_mode=integrity,
    )

    # 1) 剥头
    report.header_reused = not strip_header(src_path, clear, header_size)

    # 2) 打开验证 + 列表
    enc = open_encrypted(
        clear,
        key,
        page_size=page_size,
        kdf_iter=kdf_iter,
        hmac_algorithm=hmac_algorithm,
        kdf_algorithm=kdf_algorithm,
    )
    try:
        all_tables = table_names(enc)
    finally:
        try:
            enc.close()
        except Exception:  # noqa: BLE001,S110
            pass
    wanted = [t for t in all_tables if t not in INTERNAL_TABLES]
    if tables:
        missing = [t for t in tables if t not in all_tables]
        if missing:
            raise DecryptError(
                f"CLIENT_DECRYPT_TABLES 里的这些表在库里不存在：{missing}。"
                f"库里有：{all_tables}"
            )
        wanted = [t for t in tables]
    report.tables = wanted
    logger.info("[2/3] 密钥正确，共 %d 张表（要拷 %d 张）", len(all_tables), len(wanted))

    # 3) 逐表拷
    out.parent.mkdir(parents=True, exist_ok=True)
    logger.info("[3/3] 导出明文库 → %s", out)
    with closing(sqlite3.connect(str(out))) as plain:
        plain.execute("PRAGMA journal_mode = WAL;")
        plain.execute("PRAGMA synchronous = NORMAL;")
        plain.execute("PRAGMA cache_size = -65536;")
        for table in wanted:
            report.per_table.append(
                copy_table(
                    clear,
                    plain,
                    table,
                    key,
                    page_size=page_size,
                    kdf_iter=kdf_iter,
                    hmac_algorithm=hmac_algorithm,
                    kdf_algorithm=kdf_algorithm,
                    batch_size=batch_size,
                    progress=progress,
                )
            )
        copied, failed_idx = copy_indexes(
            clear,
            plain,
            key,
            page_size=page_size,
            kdf_iter=kdf_iter,
            hmac_algorithm=hmac_algorithm,
            kdf_algorithm=kdf_algorithm,
        )
        report.indexes_copied = copied
        plain.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        # 收尾切回 DELETE：只读消费者（客户端）建不了 -shm，留着 -wal 容易出怪错
        plain.execute("PRAGMA journal_mode = DELETE;")

    # 对账 + 自检
    skipped = report.total_skipped
    if skipped:
        detail = report.skipped_rowids[:MAX_RECORDED_SKIPS]
        logger.error(
            "⚠️ 有 %d 行因为坏页被跳过（跳过的 rowid：%s%s）。"
            "**这些行里的消息不会进清单** —— 如果它们属于要收通知的群，"
            "请用上游的 nt_msg_db_util 再试一次，或先把库备份出来人工看一眼。",
            skipped,
            detail,
            "…" if skipped > len(detail) else "",
        )
    if max_skips >= 0 and skipped > max_skips:
        raise DecryptError(
            f"坏页导致跳过了 {skipped} 行，超过 CLIENT_DECRYPT_MAX_SKIPS={max_skips}。"
            f"输出库留在 {out}（可以人工检查）。确认能接受这些消息丢失后，"
            f"把 CLIENT_DECRYPT_MAX_SKIPS=-1（不限）或调大再跑。"
        )
    if failed_idx:
        logger.info("（有 %d 个索引没能复制，通常是它依赖的表没被复制过来）", failed_idx)

    if integrity != "off":
        report.integrity = check_integrity(out, integrity)
        if report.integrity != "ok":
            logger.error(
                "解密完成，但 SQLite 自检没通过（PRAGMA %s_check）：%s\n"
                "这说明拷出来的库里有 SQLite 读不懂的结构。",
                integrity,
                report.integrity,
            )
        else:
            logger.info("      明文库自检通过（PRAGMA %s_check = ok）", integrity)

    report.seconds = time.monotonic() - started
    logger.info("[完成] %s", report.summary())
    return report


def check_integrity(path: Path | str, mode: str = "quick") -> str:
    """让 SQLite 自己检查一遍明文库。返回 `"ok"` 或第一条错误描述。"""
    if mode == "off":
        return "ok"
    pragma = "integrity_check" if mode == "full" else "quick_check"
    try:
        with closing(
            sqlite3.connect(sqlite_uri(path, "ro"), uri=True, timeout=60.0)
        ) as conn:
            row = conn.execute(f"PRAGMA {pragma}").fetchone()
    except sqlite3.Error as exc:
        return f"{type(exc).__name__}: {exc}"
    if row is None:
        return f"{pragma} 没有返回结果"
    return str(row[0])
