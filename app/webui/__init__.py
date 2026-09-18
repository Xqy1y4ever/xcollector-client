"""客户端自带的**本地 Web UI**：配置 + 运行 + 日志，都在浏览器里点。

    python -m app.main --ui            # 打开 http://127.0.0.1:8787

## 为什么是"客户端自带"，而不是塞进 `xcollector-web`

`xcollector-web` 是**后端那边**的通知台（跑在容器里，看的是数据库）。而这个客户
端跑在**你自己的机器**上，能操作的只有本地的东西：`.env`、`nt_msg.db`、镜像库、
以及"现在跑一轮"。让容器里的网页去控制你本机的文件与进程，方向是反的
（客户端没有入站端口，也不该有）。

所以这里是一个**只监听 127.0.0.1** 的小服务：标准库 `http.server` + 一个静态页面，
**零新依赖、零构建步骤**（`pip install -r requirements.txt` 之后就能用）。

## 安全边界（说清楚，别把它当成能对外开的东西）

- **默认只绑 `127.0.0.1`**。绑到别的地址时启动会打一条很响的警告：这个页面能读到
  你的令牌、能改配置、能触发入库 —— 它只适合本机。
- 密钥类字段（`CLIENT_TOKEN` / `CLIENT_NT_MSG_KEY`）**永不回显**：页面上显示空，
  留空提交 = 不改动。
- 所有写操作要求 `Content-Type: application/json` **且**带 `X-XC-UI: 1` 头：
  跨站页面发不出这种请求（会先被 CORS 预检挡下），也就不存在"你打开一个恶意网页，
  它悄悄改你的配置"。
- 服务不带鉴权。本机上任何进程都能调它 —— 这是"本机工具"的固有代价，
  和 Jupyter 之类的工具一样；要更严就得加令牌，而那又会让"打开就能用"没了。
"""

from .envfile import (
    SECRET_KEYS,
    comment_out_keys,
    config_values,
    env_path,
    managed_keys,
    read_env,
    write_env_values,
)
from .server import serve

# 我们能通过 Web UI 写的键（= 配置项），页面和维护脚本都要用
MANAGED_KEYS = managed_keys()

__all__ = [
    "MANAGED_KEYS",
    "SECRET_KEYS",
    "comment_out_keys",
    "config_values",
    "env_path",
    "managed_keys",
    "read_env",
    "serve",
    "write_env_values",
]
