"""SQLite 的 URI 文件名（`file:...?mode=ro`）拼装。

为什么专门写一个：`?` 和 `#` 在 URI 里有特殊含义，路径里真出现这两个字符时
（用户目录名里带问号不是不可能）直接拼出来的 URI 会被解析成"带查询参数"的路径，
于是打开的是另一个文件 —— 那种错很难查。这里把它们百分号编码掉。

顺带一个坑：**`ATTACH DATABASE ?` 用绑定参数时不做 URI 解析**（只有连接本身是
`uri=True` 打开的、并且把 URI 当 SQL 字面量传入时才认）。所以 `export.py` 里
那一步用的是字面量，而不是参数。
"""

from __future__ import annotations

from pathlib import Path


def sqlite_uri(path: Path | str, mode: str | None = None) -> str:
    """把路径拼成 SQLite 的 URI 形式；`mode` 可以是 `ro` / `rw` / `rwc`。"""
    text = Path(path).as_posix()
    for raw, encoded in (("%", "%25"), ("?", "%3F"), ("#", "%23")):
        text = text.replace(raw, encoded)
    uri = f"file:{text}"
    if mode:
        uri += f"?mode={mode}"
    return uri


def quote_sql(text: str) -> str:
    """把字符串塞进 SQL 字面量（单引号翻倍）。"""
    return "'" + text.replace("'", "''") + "'"
