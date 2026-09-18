"""日志：和 bot 同一套风格（一行一条、字段用 `key=value`、便于 grep）。

刻意**不打印聊天正文的全文**：源库里的内容是别人的隐私，日志里只留
`CLIENT_LOG_PREVIEW_CHARS` 个字符的预览。
"""

from __future__ import annotations

import logging
import sys

from .config import get_settings

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
_DATEFMT = "%H:%M:%S"

_configured = False


def setup_logging(level: str | None = None, *, force: bool = False) -> None:
    global _configured
    if _configured and not force:
        return
    settings = get_settings()
    logging.basicConfig(
        level=(level or settings.client_log_level or "INFO").upper(),
        format=_FORMAT,
        datefmt=_DATEFMT,
        stream=sys.stdout,
    )
    _configured = True
