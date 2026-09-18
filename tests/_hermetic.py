"""把「默认值」类测试和这台机器上的真配置隔离开。

用法（在构造 `Settings` 之前调一次）:

    from tests._hermetic import isolate_settings

    isolate_settings()

## 为什么要单独一层

`Settings` 是 `pydantic-settings` 的 `BaseSettings`：除了显式传进来的字段，它还会读
**进程环境变量**和**仓库根目录的 `.env`**（见 `app/config.py` 的 `model_config`）。

而客户端在真机上跑一次就会留下一份 `.env`，里面是真实的令牌、群号白名单、导出库
路径。于是 `check_config` / `check_whitelist` 里那些 `Settings()` 的"默认值"断言会
读到那份真配置：`CLIENT_GROUP_WHITELIST` 明明该是空的、实际有值 —— 断言失败，而
错误信息里一个字的 `.env` 都没提，指向的是代码。

反过来更坏：哪一天本机配置恰好和默认值相同，测试就"过了"，但它什么都没验证。
e2e 也栽在这上面：`make_settings` 显式传了十几个字段，**没传**白名单，于是本机
`.env` 里的白名单生效，第 3 节"订上之后存量要补上"被静默挡掉，报错只是一个
`outcomes.get("extracted") = None`。

所以测试先调一次 `isolate_settings()`：摘掉 `.env`、清掉进程里的 `CLIENT_*`。
**只影响本进程，不动磁盘上那份 `.env`** —— 那是用户的东西。

`clear_env=False` 是给"故意用环境变量配置自己"的测试用的（client 的 e2e 从
`CLIENT_BASE` / `CLIENT_SERVICE_TOKEN` 读后端地址和令牌）。就算这样，`.env` 也必须
摘掉：环境变量是**这一次调用**给的，`.env` 是**这台机器上一直躺着**的。
"""

from __future__ import annotations

import os

from app.config import Settings

ENV_PREFIX = "CLIENT_"


def isolate_settings(*, clear_env: bool = True) -> None:
    """让这个进程里的 `Settings()` 不再读这台机器上的 `.env`。"""
    # None = 不读任何 .env 文件。改的是类属性上那个 dict，对之后所有实例生效。
    Settings.model_config["env_file"] = None
    if not clear_env:
        return
    for key in [k for k in os.environ if k.startswith(ENV_PREFIX)]:
        del os.environ[key]
