# xcollector-client

Xcollector 的**离线入库客户端**：读一份聊天记录数据库，抽出通知，用**你自己**的
UserToken 写进后端。它 **不连 QQ、不需要 NapCat、不需要服务令牌**。

存在的理由是把「入库」和「bot」解耦 —— 后端的数据不再只依赖那一个活的 OneBot 连接：

```
QQ ──▶ NTQQ（nt_msg.db，SQLCipher 加密，前面还有 1024 字节 QQ 自己的头）
          │
          │  ① 剥头 + 解密      （整合了 nt_msg_db_util 的 1.decrypt.py）
          ▼
     nt_msg_plain.db（普通 SQLite，还是 QQ 的数字列名）
          │
          │  ② 导出            （整合了 nt_msg_db_util 的 3.export.py + msgdb/）
          ▼
     nt_msg_export.db ──▶ 抽取通知 ──UserToken──▶ xcollector-backend
```

**你只需要给一个输入**：加密的 `nt_msg.db` 路径 + 你自己取到的密钥。
① ② 都在客户端里，不需要单独装或跑 `nt_msg_db_util`。

> ⚠️ **同一个 QQ 账号只能开一条入库链路**：bot（实时）或本客户端，二选一。
> 两边都开会让同一条消息变成两条 —— 它们的 `message_id` 格式不同，幂等键拦不住。
> 只用本客户端的话，把 bot 的两个白名单留空（bot 仍然负责 `/注册`、`/订阅`、摘要推送）。

## 部署

### 前置

| | |
|---|---|
| Python | 3.12+ |
| 依赖 | `pip install -r requirements.txt`（含 `sqlcipher3`、`protobuf`） |
| 后端 | 一个跑起来的 `xcollector-backend`，以及**某个用户的 UserToken**（`xc_` 开头） |
| 输入 | 加密的 `nt_msg.db`（A 方案）或现成的 `nt_msg_export.db`（B 方案） |

> 依赖里有一处按平台分叉：`sqlcipher3` 在 PyPI 上**Windows 有 wheel（cp312~cp314）**，
> Linux 只有源码包，所以 Linux 用等价的 `sqlcipher3-wheels`（模块名同样是
> `sqlcipher3`）。`requirements.txt` 按 `sys_platform` 写好了，直接装即可。

### 配置

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows；Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

完整说明见 [`.env.example`](.env.example)（第 3 节讲 A 方案）。最少要配：

```env
# 1) 身份：某个用户的 UserToken（不是服务令牌 API_TOKEN）
CLIENT_TOKEN=xc_...

# 2) 输入，二选一
CLIENT_NT_MSG_DB=/path/to/nt_db/nt_msg.db   # A 方案：加密原始库，客户端自己解密导出
CLIENT_NT_MSG_KEY=<16 个 ASCII 字符的密钥>
# CLIENT_DB_PATH=/path/to/nt_msg_export.db  # B 方案：现成的导出库（此时上面两项留空）

# 3) 后端地址
BACKEND_BASE_URL=http://127.0.0.1:8000
```

配了 `CLIENT_NT_MSG_DB` 就不必再配 `CLIENT_DB_PATH`（导出库默认放 `nt_msg.db` 旁边）；
两个都配了则听 `CLIENT_DB_PATH` 的。密钥也可以放文件里（更稳妥）：
`CLIENT_NT_MSG_KEY_FILE=/path/to/key.txt`，内容首尾空白会被自动去掉。

常用项（其余见 `.env.example`）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `CLIENT_EXTRACTOR` | `rule` | `rule`（不花钱）/ `llm` / `both` |
| `LLM_API_BASE` / `LLM_MODEL` / `LLM_API_KEY` | DeepSeek | `CLIENT_EXTRACTOR` 用 `llm`/`both` 时才需要 |
| `CLIENT_GROUP_WHITELIST` / `CLIENT_SENDER_WHITELIST` | 空 | **只做收窄，不做开关**：留空 = 不额外限制（真正的过滤条件是你在后端配的订阅）。格式与 bot 相同 |
| `CLIENT_POLL_SECONDS` | `300` | `--loop` 的轮询间隔 |
| `CLIENT_INITIAL_LOOKBACK_HOURS` | `72` | 首次运行往回看多少小时 |
| `CLIENT_MAX_MESSAGES_PER_CYCLE` / `CLIENT_BATCH_SIZE` | `500` / `200` | 一轮最多处理 / 一次最多取多少条 |
| `CLIENT_MIRROR_PATH` | 源库旁边 | 客户端自己的状态库（每条消息处理过没有）。**别删**，删了会重新抽一遍 |
| `CLIENT_ATTACHMENT_ROOT` | 空 | NTQQ 附件目录，按 md5/文件名找回真实字节并上传 |
| `CLIENT_DECRYPT_TABLES` | 空 = 全部 | 只解密需要的表（如 `group_msg_table,c2c_msg_table`）会快一些 |
| `CLIENT_DECRYPT_MAX_SKIPS` | `-1` | 允许多少行因坏页被跳过。每次跳过都会打 ERROR 日志 |

### 怎么拿到密钥

密钥是 NTQQ 解密那个库用的 **16 个 ASCII 字符**，只在进程内存里，磁盘上没有。
用 [`QQBackup/qq-win-db-key`](https://github.com/QQBackup/qq-win-db-key)
（或本机 `nt_msg_db_util/getkey.ps1`）附加到 QQ 进程把它读出来。

- **换 QQ 账号、换机器、重装 QQ 都会变**，要重取；
- 它等于「能解开你全部聊天记录的凭据」：别提交进 git、别贴进聊天记录/截图、
  在容器里别做成 build-arg。放 `.env`（已 gitignore）或用 `CLIENT_NT_MSG_KEY_FILE`。

### 跑

```bash
python -m app.main --prepare         # 只做「剥头 + 解密 + 导出」，不连后端（第一次建议先跑）
python -m app.main --status          # 自检：配置、源库、镜像、身份、订阅。**不写任何东西**
python -m app.main --once            # 跑一轮就退出（推荐配合计划任务）
python -m app.main --loop            # 常驻，按 CLIENT_POLL_SECONDS 定期跑
python -m app.main --dry-run --once  # 只组装不写入，先看一眼会抽出什么
python -m app.main --since-hours 72  # 强制重抽 72 小时内的消息
```

`--prepare` 会把每张表拷了多少行、跳过多少行、SQLite 自检结果打印出来。
用 `--once` / `--loop` 时这一步会自动进行（只在新数据到来时才真的跑）。

**推荐 `--once` + 计划任务**：这个客户端本来就是批处理的，把调度交给操作系统
比让它常驻更省心。

```powershell
# Windows：每 5 分钟一次
schtasks /create /tn XcollectorClient /sc minute /mo 5 ^
  /tr "E:\xcollector-client\.venv\Scripts\python.exe -m app.main --once" ^
  /st 00:00
```

```bash
# Linux：cron
*/5 * * * * cd /opt/xcollector-client && .venv/bin/python -m app.main --once
```

退出码：`0` 正常；`1` 后端调用失败；`2` 配置或源库有问题（可以拿来做告警）。

### Docker

镜像 `ghcr.io/xqy1y4ever/xcollector-client:latest`。它是 bot 那个栈里的第二条入库
链路（`./start.sh client`），和 bot 二选一：

```bash
cd xcollector-deploy/bot
# .env 里配：CLIENT_TOKEN、CLIENT_NT_MSG_HOST_DIR（放着 nt_msg.db 的目录）、CLIENT_NT_MSG_KEY
./start.sh client
```

那个目录是**读写**挂进去的（解密产物写在 `nt_msg.db` 旁边）。也可以只挂现成的
`nt_msg_export.db`（B 方案：把文件放进同一个目录，并把 `CLIENT_DB_PATH` 设成
`/data/nt/nt_msg_export.db`）。见
[`xcollector-deploy`](https://github.com/Xqy1y4ever/xcollector-deploy) 的 README。

### 确认在跑

```bash
python -m app.main --status
```

它会打印：后端可达性与身份（是不是 UserToken）、源库路径与行数、镜像库状态、
**你在后端配的订阅**（订阅是真正的过滤条件，一条都没有 = 什么都不会入库）、
以及源表有没有序号列（决定"回复改期"能不能落到原任务上）。

## 已知边界

- **增量导出按时间戳**：老消息被编辑（时间戳不变）时增量不会重看它，
  需要时跑一次 `--prepare` 强制全量。
- **坏行会被跳过**，但会记下 rowid、打 ERROR、报告里标 `ok=False`；
  整张表**整块**读不下去时会直接停下来要求人工处理。
- **附件字节多半拿不到**：导出库里只有文件名、md5 和一段会过期的 CDN 相对路径，
  配好 `CLIENT_ATTACHMENT_ROOT` 才可能找回真字节；找不到会标 `degraded` 而不是静默丢弃。

## 许可

MIT
