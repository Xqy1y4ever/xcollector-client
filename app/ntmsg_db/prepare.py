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
    EXPORT_OVERLAP_SECONDS,
    NT_MSG_HEADER_SIZE,
    NT_MSG_HMAC_ALGORITHM,
    NT_MSG_KDF_ALGORITHM,
    NT_MSG_KDF_ITER,
    NT_MSG_PAGE_SIZE,
)
from .decrypt import DEFAULT_BATCH_SIZE, DEFAULT_HEADER_SIZE, DecryptError, DecryptReport
from .decrypt import decrypt_database, sqlcipher_available
from .export import ExportReport, export_database

logger = logging.getLogger(__name__)

# 增量导出时往回多看的时间（秒）。导出是幂等的（主键 msg_id），重导一遍只是慢，
# 所以宁可多看一点：同一秒里后到的消息也在窗口内。
DEFAULT_EXPORT_OVERLAP_SECONDS = EXPORT_OVERLAP_SECONDS


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
            parts.append("明文库还是新的，未重新解密")
        if self.export is not None:
            parts.append(f"导出：{self.export.summary()}")
        elif not self.exported:
            parts.append("导出库还是新的，未重新导出")
        parts.extend(self.notes)
        return "；".join(parts)


def _mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return -1


def _is_stale(upstream: Path, downstream: Path) -> bool:
    """上游文件比下游产物新（或下游不存在）→ 需要重跑。"""
    if not downstream.exists():
        return True
    return _mtime_ns(upstream) > _mtime_ns(downstream)


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

    **全量导出**（没有增量模式）：按时间过滤会让"时间戳没变、内容变了"的旧消息
    永远不进导出库，而客户端那边也就永远发现不了它被编辑过。
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

    if _is_stale(source, plain):
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
        logger.debug("%s 比 %s 新，跳过解密", plain.name, source.name)

    if _is_stale(plain, export):
        report.export = export_database(
            plain,
            export,
            batch_size=EXPORT_BATCH,
            include_c2c=EXPORT_INCLUDE_C2C,
            overlap_seconds=EXPORT_OVERLAP_SECONDS,
        )
        report.exported = True
    else:
        logger.debug("%s 比 %s 新，跳过导出", export.name, plain.name)
    return report


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_EXPORT_OVERLAP_SECONDS",
    "DEFAULT_HEADER_SIZE",
    "PrepareReport",
    "prepare_databases",
    "read_key",
    "resolve_paths",
]
