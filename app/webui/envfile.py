"""`.env` 的读写 —— 带注释、可往返、不碰不属于我们的键。

## 为什么单独一个模块

Web UI 要"改配置"，而 `.env` 是**用户的手写文件**：里面可能有注释、有空行、有
我们自己已经不认识的老键。直接 dump 一份新的会把用户的东西冲掉，而"悄悄删掉别人的
一行配置"正是这个项目一直在防的那种失败。

所以这里的规则是：

1. **只改配置项那几行**（`Settings.model_fields` 里的键），其余行原样保留；
2. 键不存在就**追加**到文件末尾（并标一行注释说明是自动加的）；
3. 想删掉不认识的键时走 `comment_out_keys()`：**注释掉而不是删除** ——
   用户看得见我们动了什么，也能自己撤销。

纯函数，没有 IO 之外的副作用，所以 `tests/check_webui.py` 能直接对着它测。
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from ..config import BASE_DIR, Settings

# 这些键**永远不回显给浏览器**（它们能直接冒用这个用户的身份/解开他的聊天记录）。
# 页面里显示成空 + "已设置"标记：留空提交 = 不改动，而不是清空。
SECRET_KEYS = ("CLIENT_TOKEN", "CLIENT_NT_MSG_KEY")

# 我们认的赋值行：`KEY=...`（大小写都认，前后可以有空白）
_ASSIGN_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")


def env_path() -> Path:
    """当前 `.env` 的路径（就是 `Settings` 读的那一个）。

    `Settings.model_config["env_file"]` 可以被测试关掉（`tests/_hermetic.py`），
    那种情况下回退到仓库根目录下的 `.env` —— Web UI 永远只写这一个文件。
    """
    configured = Settings.model_config.get("env_file")
    return Path(str(configured)) if configured else BASE_DIR / ".env"


def managed_keys() -> list[str]:
    """我们能写的键（= 配置项的 ENV 名字），按字段声明顺序。"""
    return [name.upper() for name in Settings.model_fields]


def env_override_keys() -> list[str]:
    """既在**进程环境变量**里、又是配置项的键。

    说出来是因为优先级：`pydantic-settings` 的规则是「环境变量 > `.env` > 默认值」。
    所以这些键写进 `.env` 也不会生效 —— 页面上要提示用户"这是启动时的环境变量在管"，
    否则他会改半天发现没用。
    """
    return sorted(k for k in managed_keys() if k in os.environ)


def read_env(path: Path | str | None = None) -> dict[str, str]:
    """读 `.env` 里的键值（只认配置项；大小写归一化成 ENV 名字）。"""
    target = Path(path) if path is not None else env_path()
    if not target.exists():
        return {}
    fields = {k.upper() for k in managed_keys()}
    out: dict[str, str] = {}
    for line in target.read_text(encoding="utf-8", errors="replace").splitlines():
        m = _ASSIGN_RE.match(line)
        if not m:
            continue
        key = m.group(1).upper()
        if key in fields and key not in out:
            out[key] = _unquote(m.group(2))
    return out


def config_values(redact: bool = True) -> dict[str, str]:
    """当前配置的值（默认把密钥类键抹成空串）。

    值来自 `.env` **而不是** `Settings()`：后者会把进程环境变量也算进来，
    而这里要回答的是"这个文件里写着什么"（Web UI 编辑的就是这个文件）。
    """
    values = {key: "" for key in managed_keys()}
    values.update(read_env())
    if redact:
        for key in SECRET_KEYS:
            values[key] = ""
    return values


def write_env_values(values: dict[str, str], *, path: Path | str | None = None) -> list[str]:
    """把若干配置项写进 `.env`（就地改那几行，其余原样保留）。返回写进去的键。

    * 键不在配置项里 → 直接拒绝（`ValueError`）。Web UI 不该能往 `.env` 里塞任意键。
    * 值是 `None` → 当成"不改动"（密钥输入框留空就是这个意思）。
    * 值里有机密字符（`#`、引号、首尾空白）时加双引号，读回来仍然是原值。
    """
    target = Path(path) if path is not None else env_path()
    fields = {k.upper() for k in managed_keys()}
    unknown = sorted(k.upper() for k in values if k.upper() not in fields)
    if unknown:
        raise ValueError(f"不是配置项，拒绝写入：{unknown}")

    target.parent.mkdir(parents=True, exist_ok=True)
    lines = target.read_text(encoding="utf-8").splitlines() if target.exists() else []
    written: list[str] = []
    appended: list[str] = []

    pending = {k.upper(): v for k, v in values.items() if v is not None}
    for index, line in enumerate(lines):
        m = _ASSIGN_RE.match(line)
        if not m:
            continue
        key = m.group(1).upper()
        if key in pending and key not in written:
            lines[index] = f"{key}={_quote(pending[key])}"
            written.append(key)

    for key, value in pending.items():
        if key not in written:
            appended.append(f"{key}={_quote(value)}")
            written.append(key)

    if appended:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append("# 下面这几行是 Web UI 加的（原来文件里没有这个键）")
        lines.extend(appended)

    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return written


def comment_out_keys(keys: list[str], *, path: Path | str | None = None) -> list[str]:
    """把指定的键**注释掉**（不是删掉），返回真的动过的键。

    用途：`.env` 里留着不是配置项的键（从旧版本升上来的必然情况）——
    它们不会生效，但会让人以为生效了。注释掉之后用户还能自己看回来。
    """
    target = Path(path) if path is not None else env_path()
    if not target.exists():
        return []
    wanted = {k.upper() for k in keys}
    touched: list[str] = []
    out: list[str] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        m = _ASSIGN_RE.match(line)
        key = m.group(1).upper() if m else ""
        if key and key in wanted and not line.lstrip().startswith("#"):
            out.append(f"# {line.lstrip()}   # Web UI: 已不是配置项，不再生效")
            touched.append(key)
        else:
            out.append(line)
    if touched:
        target.write_text("\n".join(out) + "\n", encoding="utf-8")
    return touched


def _quote(value: str) -> str:
    """写进 `.env` 的值。

    ⚠️ **必须真的会加引号**：`.env` 的消费者是 `pydantic-settings`（走 python-dotenv），
    而 dotenv 把 `KEY=值 # 注释` 里的 ` # 注释` 当注释丢掉 —— 于是"页面里存进去一个带
    `#` 的值，读出来少了一半"。带引号时 dotenv 不做这个处理，所以这里按需加双引号，
    `_unquote()` 再原样还原。

    什么时候要加：含 `#`、含引号、首尾有空白、含换行（换行会破坏文件结构）。
    """
    text = str(value).replace("\r", " ").replace("\n", " ")
    if text == "":
        return ""
    needs_quote = (
        "#" in text
        or '"' in text
        or "'" in text
        or text != text.strip()
        or "=" in text
    )
    if not needs_quote:
        return text
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _unquote(raw: str) -> str:
    text = raw.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        quote = text[0]
        inner = text[1:-1]
        if quote == '"':
            # 与 `_quote()` 对应：还原转义。顺序很重要（先 \\\\ 再 \\"）。
            inner = inner.replace('\\\\', '\\').replace('\\"', '"')
        return inner
    return text
