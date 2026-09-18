"""`nt_msg.db` 的取数流水线：剥头 → 解密 → 导出 →（交给 `app/source/` 读）。

用户只需要给一个东西：**加密的 `nt_msg.db` 路径**，以及他自己取到的密钥。
剩下三步由这个包完成：

    nt_msg.db ──▶ nt_msg_clear.db ──▶ nt_msg_plain.db ──▶ nt_msg_export.db
    （SQLCipher 加密，   （剥掉前面 1024   （普通 SQLite，      （字段有名字、
      前面还有 1024 字节   字节的 QQ 头）     还是 QQ 的数字列名）   正文是文本）
      的 QQ 文件头）

## 这套东西是从哪来的

**是 `nt_msg_db_util` 的 `1.decrypt.py` 与 `3.export.py` 整合进本项目的版本**，
不是另写一套实现：

| 模块 | 来源 |
| --- | --- |
| `decrypt` | `nt_msg_db_util/1.decrypt.py`（sqlcipher3 + 同一套 PRAGMA 顺序 + rowid 分页 + 坏页容忍） |
| `export` | `nt_msg_db_util/3.export.py`（同一套流程、同一套表结构与 `content` 形状） |
| `msgdb/`（仓库根目录） | 上游的解析套件，**逐字**搬过来（见 `msgdb/VENDORED.md`） |
| `prepare` | 本项目新增：把两步串起来 + "要不要重跑"的判断 |

为什么不自己实现密码学：`nt_msg.db` 是 SQLCipher 4 的库，解密这件事有现成、
经过验证的库（`sqlcipher3`），自己按文档重写一遍只会多出一个可能出错的地方。

为什么要拆成三个文件：这三步的**失败方式完全不同**（密钥不对 / 页坏了 / 正文
解析不了），分开存就能分开排查，也不会因为导出逻辑写错而毁掉已经解密好的数据。
"""

from .decrypt import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_HEADER_SIZE,
    DecryptError,
    DecryptReport,
    TableReport,
    decrypt_database,
    open_encrypted,
    sqlcipher_available,
    strip_header,
)
from .export import (
    ExportError,
    ExportReport,
    export_database,
    watermark,
)
from .prepare import PrepareReport, prepare_databases, read_key, resolve_paths

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_HEADER_SIZE",
    "DecryptError",
    "DecryptReport",
    "ExportError",
    "ExportReport",
    "PrepareReport",
    "TableReport",
    "decrypt_database",
    "export_database",
    "open_encrypted",
    "prepare_databases",
    "read_key",
    "resolve_paths",
    "sqlcipher_available",
    "strip_header",
    "watermark",
]
