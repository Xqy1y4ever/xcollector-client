"""把"用户只给一个 `nt_msg.db` 路径"这件事串起来。

    nt_msg.db ──剥头──▶ nt_msg_clear.db ──解密──▶ nt_msg_plain.db ──导出──▶ nt_msg_export.db
       ↑ 用户只需要配这个                                                    ↑ 客户端读这个

前两步是 `nt_msg_db_util` 的 `1.decrypt.py`，第三步是它的 `3.export.py`
（都已整合进本项目，见各自模块的说明）。中间产物一律放在 `nt_msg.db` 旁边
（上游的默认命名），不再让用户配路径 —— 两个中间文件配错位置只会让人困惑。

## 什么时候重跑

用**文件时间**判断，不额外存状态：

* `nt_msg.db` 比 `nt_msg_plain.db` 新（或明文库不存在）→ 重新剥头 + 解密；
* `nt_msg_plain.db` 比 `nt_msg_export.db` 新（或导出库不存在）→ 重新导出。

`--loop` 每轮都会调用这里，而判断本身只是两次 `stat()`，几乎不要钱；
真正重的活儿只在新数据到来时才做。

## 失败就是失败

任何一步出错都抛出去，由调用方把这一轮停下来 —— **绝不**"跳过这一步、拿旧的库
继续跑"。那正好会造成最坏的结果：界面看起来一切正常，而新的通知一条都没进来。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from ..config import (
    DECRYPT_BATCH_SIZE,
    DECRYPT_INTEGRITY,
    DECRYPT_MAX_SKIPS,
    EXPORT_BATCH,
    EXPORT_INCLUDE_C2C,
    NT_MSG_HEADER_SIZE,
    NT_MSG_HMAC_ALGORITHM,
    NT_MSG_KDF_ALGORITHM,
    NT_MSG_KDF_ITER,
    NT_MSG_PAGE_SIZE,
)
from .decrypt import DEFAULT_BATCH_SIZE, DEFAULT_HEADER_SIZE, DecryptError, DecryptReport
from .decrypt import decrypt_database, open_encrypted, sqlcipher_available, strip_header
from .export import ExportReport, export_database, source_max_id

logger = logging.getLogger(__name__)

# 判断"有没有新行"时看这两张表（客户端真正会读的消息表）。
# 其余表都是元数据（联系人、会话……），小、而且客户端不读它们的内容，
# 不值得为它们牺牲"跳过整表拷贝"这个机会。
SYNC_TABLES = ("group_msg_table", "c2c_msg_table")


@dataclass
class PrepareReport:
    enabled: bool = False
    export_path: Path | None = None
    plain_path: Path | None = None
    clear_path: Path | None = None
    decrypted: bool = False
    exported: bool = False
    notes: list[str] = field(default_factory=list)
    decrypt: DecryptReport | None = None
    export: ExportReport | None = None

    def summary(self) -> str:
        if not self.enabled:
            return "未配置 CLIENT_NT_MSG_DB，跳过解密/导出"
        parts: list[str] = []
        if self.decrypt is not None:
            parts.append(f"解密：{self.decrypt.summary()}")
        elif not self.decrypted:
            parts.append("解密：跳过（没有新消息）")
        if self.export is not None:
            parts.append(f"导出：{self.export.summary()}")
        parts.extend(self.notes)
        return "；".join(parts)


def _max_rowid(conn, table: str) -> int | None:
    """表的最大 rowid；表不存在返回 None。"""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        return None
    row = conn.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _rows_pending(source: Path, clear: Path, plain: Path, settings) -> int:
    """还有几张表有"没拷进明文库的新行"。>0 表示这一轮要解密。

    为什么按 `max(rowid)` 判断而不是文件时间：文件时间只能说明"QQ 写过这个库"
    （它每隔几秒就会写会话/联系人表），说明不了"有没有新消息"。按 rowid 判断才是
    **数据本身**：数字一致就是没有新行，可以直接跳过整个 SQLCipher 拷贝。

    ⚠️ 判断不出来时一律返回 1（要解密）—— 明文库不存在、没配密钥、装不上
    sqlcipher3、打开失败……**宁可多解一次，也不能因为判断不出来就跳过**：
    跳过的后果是新消息永远进不来，那是最坏的失败。
    """
    if not plain.exists():
        return 1
    try:
        strip_header(source, clear, NT_MSG_HEADER_SIZE)
        key = read_key(settings)
    except DecryptError:
        return 1
    if not key or not sqlcipher_available():
        return 1
    try:
        enc = open_encrypted(
            clear,
            key,
            page_size=NT_MSG_PAGE_SIZE,
            kdf_iter=NT_MSG_KDF_ITER,
            hmac_algorithm=NT_MSG_HMAC_ALGORITHM,
            kdf_algorithm=NT_MSG_KDF_ALGORITHM,
        )
    except Exception as exc:  # noqa: BLE001 - 判断失败就交给解密那一步去报错
        logger.debug("没法判断有没有新行（%s），还是解一次", exc)
        return 1
    try:
        pending = 0
        for table in SYNC_TABLES:
            src_max = _max_rowid(enc, table)
            if src_max is None:
                continue
            dst_max = source_max_id(plain, table)
            if src_max > dst_max:
                logger.info(
                    "%s 多了 %s 行（%s → %s），要解密", table, f"{src_max - dst_max:,}",
                    f"{dst_max:,}", f"{src_max:,}",
                )
                pending += 1
        return pending
    finally:
        try:
            enc.close()
        except Exception:  # noqa: BLE001,S110
            pass


def resolve_paths(settings) -> tuple[Path, Path, Path, Path]:
    """算出 `(nt_msg.db, nt_msg_clear.db, nt_msg_plain.db, nt_msg_export.db)`。

    中间产物固定放在 `nt_msg.db` 旁边（和上游 `1.decrypt.py` 的默认命名一致）：
    少两个配置项，也少一处"配错了位置、于是每次都重新解密"的坑。
    """
    source = Path(settings.client_nt_msg_db).expanduser()
    clear = source.with_name("nt_msg_clear.db")
    plain = source.with_name("nt_msg_plain.db")
    return source, clear, plain, settings.ntmsg_export_path


def read_key(settings) -> str:
    """取密钥：优先 `CLIENT_NT_MSG_KEY_FILE`（避免密钥进环境变量/进程列表/命令历史）。"""
    path_text = (settings.client_nt_msg_key_file or "").strip()
    if path_text:
        path = Path(path_text).expanduser()
        if not path.exists():
            raise DecryptError(f"CLIENT_NT_MSG_KEY_FILE 指向的文件不存在：{path}")
        raw = path.read_text(encoding="utf-8", errors="replace")
        # 密钥文件很容易在结尾多一个换行（echo 写出来的就有）—— 去掉首尾空白，
        # 但不做任何其它加工：密钥就是那 16 个字节。
        return raw.strip()
    return (settings.client_nt_msg_key or "").strip()


def prepare_databases(settings) -> PrepareReport:
    """按需解密 + 导出。返回的 `export_path` 就是这一轮该读的库。

    ## 每一轮只做"真的需要做"的那部分

    两步各自判断，判断依据都是**数据**而不是文件时间：

    * 解密：明文库里的 `max(rowid)` 和加密库里的一致 → **没有新行，跳过**
      （连 SQLCipher 都不打开去做整表拷贝）；不一致 → 接着 rowid 续拷。
    * 导出：按导出库里已有的 `max(msg_id)` 往后导（增量），一条新行都没有时
      连 FTS 都不重建。

    实测（真实 709MB / 77 万行）：没有任何新消息的一轮 ≈ 2 秒；来了几条新消息
    ≈ 8 秒。以前是每轮固定 60 秒左右（全量解密 + 全量导出 + 每次重建 FTS）。

    > 为什么以前是全量：那时客户端会"回看最近一段已读消息"，靠对比内容指纹发现
    > 编辑过的通知 —— 增量导出会让编辑过的旧消息不进导出库。现在**已经不回看**了
    > （见 README「它不做什么」），所以增量不再有任何代价。
    """
    report = PrepareReport()
    if not (settings.client_nt_msg_db or "").strip():
        report.enabled = False
        report.export_path = settings.resolved_db_path
        return report

    source, clear, plain, export = resolve_paths(settings)
    report.enabled = True
    report.clear_path = clear
    report.plain_path = plain
    report.export_path = export

    if not source.exists():
        raise DecryptError(
            f"CLIENT_NT_MSG_DB 指向的 {source} 不存在。这一项要的是**加密的 nt_msg.db**"
            "（QQ 的原始库），不是导出库。"
        )

    pending = _rows_pending(source, clear, plain, settings)
    if pending > 0:
        if not sqlcipher_available():
            raise DecryptError(
                "要解密 nt_msg.db，但装不上 sqlcipher3。\n"
                "  Windows：pip install sqlcipher3\n"
                "  Linux  ：pip install sqlcipher3-wheels（同一模块名的预编译包）\n"
                "（requirements.txt 里按平台写好了，重新 pip install -r requirements.txt 即可）"
            )
        key = read_key(settings)
        logger.info(
            "解密 %s（密钥 %d 字节，参数 page_size=%s kdf_iter=%s hmac=%s kdf=%s）",
            source.name,
            len(key),
            NT_MSG_PAGE_SIZE,
            NT_MSG_KDF_ITER,
            NT_MSG_HMAC_ALGORITHM,
            NT_MSG_KDF_ALGORITHM,
        )
        report.decrypt = decrypt_database(
            source,
            clear,
            plain,
            key,
            header_size=NT_MSG_HEADER_SIZE,
            batch_size=DECRYPT_BATCH_SIZE,
            page_size=NT_MSG_PAGE_SIZE,
            kdf_iter=NT_MSG_KDF_ITER,
            hmac_algorithm=NT_MSG_HMAC_ALGORITHM,
            kdf_algorithm=NT_MSG_KDF_ALGORITHM,
            integrity=DECRYPT_INTEGRITY,
            max_skips=DECRYPT_MAX_SKIPS,
        )
        report.decrypted = True
    else:
        report.notes.append("解密：没有新消息，跳过")
        logger.debug("%s 里没有新行，跳过解密", source.name)

    # 导出**每次都跑**：它是增量的（按 msg_id 接着导），没有新行时只花几十毫秒，
    # 而且比"猜文件时间"可靠 —— 导出一旦中断，下一次自动把它补完。
    report.export = export_database(
        plain,
        export,
        batch_size=EXPORT_BATCH,
        include_c2c=EXPORT_INCLUDE_C2C,
    )
    report.exported = True
    return report


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_HEADER_SIZE",
    "SYNC_TABLES",
    "PrepareReport",
    "prepare_databases",
    "read_key",
    "resolve_paths",
]
