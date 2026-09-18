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

**或者用自带的 Web UI 填**（推荐第一次用）：

```bash
python -m app.main --ui           # 然后浏览器打开 http://127.0.0.1:8787
```

页面上能填配置、点"跑一轮"、看实时日志，还会把"令牌不是 UserToken""源库读不了"
".env 里有 N 个键已经失效"这些一眼看不出来的问题直接摆出来。它**只监听 127.0.0.1**，
**零新依赖**（标准库 + 一个静态页，不需要构建）。

```
┌─ Xcollector 客户端 ─────────────────────────────────────────────┐
│ 后端         可达 · 5078xxxxx (user)    没读过的     12 条      │
│ 源库         774,574 行                 镜像库     共 812 条    │
│ 本地白名单   群 3 个 · 发送者 2 个      运行中     空闲         │
├─────────────────────────────────────────────────────────────────┤
│ [▶ 跑一轮]  [🔁 自动跑（每 300 秒）]  [⏹ 停止]                  │
├─────────────────────────────────────────────────────────────────┤
│ BACKEND_BASE_URL  http://127.0.0.1:8000                          │
│ CLIENT_TOKEN      （已设置，留空不改）                           │
│ CLIENT_NT_MSG_DB  C:\Users\me\...\nt_db\nt_msg.db                │
│ ...                                                              │
│                            [保存配置]                            │
├─────────────────────────────────────────────────────────────────┤
│ 12:01:03 INFO  源库准备：解密：… ；导出：写入 774,574 行         │
│ 12:01:52 INFO  本轮：扫了 12 条，…                               │
└─────────────────────────────────────────────────────────────────┘
```

写的是同一个 `.env`（`cp .env.example .env` 那一步可以跳过，页面上保存就会创建）。
写文件时**只改配置项那几行**，你的注释、空行、老键都原样留着；不生效的老键它会
列出来问你要不要注释掉（注释，不是删）。

完整说明见 [`.env.example`](.env.example)（第 2 节讲两种输入方案）。
不想用页面的话，手工最少要配这些：

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

全部配置项（一共 17 个）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `BACKEND_BASE_URL` | `http://127.0.0.1:8000` | 后端地址 |
| `CLIENT_TOKEN` | 空 | 某个用户的 UserToken（`xc_` 开头） |
| `CLIENT_NT_MSG_DB` | 空 | A 方案：加密的 `nt_msg.db` |
| `CLIENT_NT_MSG_KEY` / `CLIENT_NT_MSG_KEY_FILE` | 空 | 上面那个库的密钥（值或文件，推荐文件） |
| `CLIENT_DB_PATH` | 空 | B 方案：现成的导出库 |
| `CLIENT_MIRROR_PATH` | 源库旁边 | 客户端自己的状态库（"哪些读过了"）。**别删**，删了会把整个源库重读一遍 |
| `CLIENT_GROUP_WHITELIST` / `CLIENT_SENDER_WHITELIST` | 空 | **只做收窄，不做开关**：留空 = 全都读（客户端不看后端订阅）。格式与 bot 相同 |
| `CLIENT_ATTACHMENT_ROOT` | 空 | NTQQ 附件目录，按 md5/文件名找回真实字节并上传 |
| `CLIENT_EXTRACTOR` | `rule` | `rule`（不花钱）/ `llm` / `both` |
| `LLM_API_BASE` / `LLM_MODEL` / `LLM_API_KEY` | DeepSeek | `CLIENT_EXTRACTOR` 用 `llm`/`both` 时才需要 |
| `CLIENT_POLL_SECONDS` | `300` | `--loop` / 页面"自动跑"的间隔 |
| `CLIENT_LOG_LEVEL` | `INFO` | 日志级别 |
| `DIGEST_TZ` | `Asia/Shanghai` | **必须和 bot / 后端一致** |

**就这些。** 其余的（一轮读多少、解密参数、重试次数、超时、模型温度……）都写死在
`app/config.py` 里了 —— 那些值要么从来没被改对过，要么改错之后的现象极难查
（例如改 `kdf_iter`，症状是"解出来的库全是乱码"）。想改就改代码。

> `.env` 里留着**不是配置项**的键时会启动警告一次（列出来给你删）。
> `extra="ignore"` 会让它们静默失效 —— 那和"配上了"长得一模一样。

### 怎么拿到密钥

密钥是 NTQQ 解密那个库用的 **16 个 ASCII 字符**，只在进程内存里，磁盘上没有。
用 [`QQBackup/qq-win-db-key`](https://github.com/QQBackup/qq-win-db-key)
（或本机 `nt_msg_db_util/getkey.ps1`）附加到 QQ 进程把它读出来。

- **换 QQ 账号、换机器、重装 QQ 都会变**，要重取；
- 它等于「能解开你全部聊天记录的凭据」：别提交进 git、别贴进聊天记录/截图、
  在容器里别做成 build-arg。放 `.env`（已 gitignore）或用 `CLIENT_NT_MSG_KEY_FILE`。

### 跑

五种跑法：

```bash
python -m app.main --ui           # 本机 Web UI：配置 / 跑一轮 / 看日志（http://127.0.0.1:8787）
python -m app.main --status       # 只看配置、源库、镜像、还有多少没读过、身份。**不写任何东西**
python -m app.main --once         # 跑一轮就退出（推荐配合计划任务）
python -m app.main --loop         # 常驻，按 CLIENT_POLL_SECONDS 定期跑
python -m app.main --mark-unread  # 把已处理的标成未读：下一轮**重新抽一遍**（见下）
```

`--once` / `--loop` 会**先确保源库是最新的**，而且每一步只做"真的需要做"的部分：
剥头看文件时间（1.5 秒），解密看 `max(rowid)`（没新行就整段跳过），导出按
`max(msg_id)` 接着上次导。**什么都没发生的一轮约 2 秒**，来几条新消息约 8 秒
（77 万行的库；以前是每轮固定 60 秒左右）。想看细节就看那一轮的日志
（第一行「源库准备：…」）。

`--ui` 的额外参数：`--host`（默认 `127.0.0.1`，**别改成对外地址**）、`--port`、`--no-browser`。
它和 `--once` 是同一个进程里的两种用法：页面上点"跑一轮"就是后台跑 `--once` 那套逻辑，
点"自动跑"就是 `--loop` 那套逻辑（间隔取 `CLIENT_POLL_SECONDS`）。

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
**白名单内共多少条**、**还有多少条没读过**（只算白名单内；判据是镜像里的已读标记，
不是时间）。

### 让它重新处理一遍（"标为未读"）

想换抽取器、改提示词、或者怀疑上一遍没抽好，可以让客户端把消息**再处理一遍**：

```bash
python -m app.main --mark-unread   # 只动镜像：已读 → 未读，标完就退出
python -m app.main --once          # 下一轮就会重新走一遍流水线（会重新调用模型）
```

页面上是同一个动作，只是合成了一步：**「↻ 标为未读并重新处理」**——先弹一个确认框
（会告诉你一共多少条、这一步会重新花钱），确认后标记并立刻跑一轮。一轮最多处理
`MAX_MESSAGES_PER_CYCLE`（500）条，剩下的下一轮接着做，`--status` 里能看到还剩多少。

几个刻意的取舍，都是为了让这个动作**不会有"看起来做了其实没做"**的版本：

- **不是把镜像删掉。** 删掉之后那些消息和"镜像被删过/换了机器"长得一模一样，
  而客户端有一条保命捷径："后端已经有这条通知就跳过抽取"（免得镜像一丢就把几万条
  历史重新抽一遍、白花模型的钱）。所以"标为未读"是一个**状态**（`reprocess`），
  它明确告诉下一轮"这条是用户点名要求重抽的"，于是绕过那条捷径。重抽的结果由后端的
  幂等键 `(user_id, raw_message_id)` **就地更新**到原来那条通知上，**不会多出一条**。
- **中途退出不会丢。** `reprocess` 和 `pending`/`failed` 一样进"没做完"队列，
  这一轮跑不完、进程被 Ctrl+C，下一轮接着做。
- **正在跑的时候不许标。** 一边跑一边标，正在处理的那几条会被这一轮的结果
  （done/skipped）覆盖掉 —— 那等于"标了一半"。页面会直接拒绝（409），
  `--mark-unread` 也请在没跑的时候用。
- **重抽的是本地导出库里的内容。** 如果一条消息在 QQ 里被**编辑**过，重抽拿到的
  还是当初导出的那份（增量导出按 `msg_id` 只导新行）。要连内容一起更新，得让导出库
  全量重来一次（删掉 `nt_msg_export.db` 再跑）。
- **附件会重传一遍。** 后端的附件是按 id 存的、没有内容去重，所以带图的消息重抽一次
  就多一份附件字节。消息里带附件不多时无所谓。

> **订阅与本客户端无关**：订阅是 bot 的过滤条件（决定它给谁抽），客户端读的是
> **你自己账号**的聊天记录库。收窄只有 `CLIENT_GROUP_WHITELIST` /
> `CLIENT_SENDER_WHITELIST`，留空 = 全都读。

## 一轮做什么

```
① 先把上次没做完的做完   （pending/failed → 重试；reprocess → 用户标了未读，重抽）
② 再扫"没读过"的         （按时间正序：镜像里没有的都要读，不管多老）
   每条：写原文（自己那层）→ 传附件 → 抽取 → 建通知
③ 记镜像 + 缺口告警 + 按天统计
```

**读什么只看一件事：镜像里有没有这条**（= 标没标已读），**没有时间窗口**。
时间窗口表达不了"这条处理过没有"，会让"比任何窗口都老、而镜像里又没有"的消息
（换过导出库、镜像被删过、白名单刚放开、上一版导出漏了一批）永远读不到 ——
而"该看到的没看到"正是这套系统最怕的失败。

**白名单是下推到 SQL 的**：配了 `CLIENT_GROUP_WHITELIST` / `CLIENT_SENDER_WHITELIST`
之后，客户端**只扫白名单内的消息** —— "没读过的"只剩那几条，镜像里也只有那几条，
白名单外的消息扫都不扫（更不会写进镜像）。所以：

```
源库            791,957 行
白名单内             109 条      ← 群 690971751 且发送者 453625637
没读过的             109 条      ← 一轮就能抽完
```

改白名单也是立刻生效的：新放开的来源本来就不在镜像里，下一轮自然会被读到。
（**留空 = 不限制**：那时源库里所有的都要过一遍，几十万条，按一轮
`MAX_MESSAGES_PER_CYCLE`（500）条分多轮读完 —— 想要"只有几条"就把白名单配上。）

### 它不做什么（说清楚，免得你以为有）

- **不识别"回复改期"。** 「回复某条通知说改到周五」会被当成一条**独立的新消息**
  （可能因此多出一条通知）。要改期请在网页上直接改那条任务。
- **不重新读已经读过的消息**（除非你让它重读）。所以老师在群里**编辑**一条已经发过的
  通知（截止时间改到周五），客户端不会自己发现。要重看就用上面那个
  **「标为未读并重新处理」**（或 `--mark-unread` + `--once`）—— 注意它重抽的是本地
  导出库里的内容，QQ 里被编辑过的老消息不会因此变新（见上）。
- **不写共享层。** 原文只进你自己那层；群状态由 bot 写。

## 和后端的关系

客户端用**某个用户的 UserToken**，写的原文进**他自己那一层**
（后端的 `user_raw_message`）；共享层（`raw_message` / `group_state`）只由 bot 写。
所以：

- 客户端**不需要**、也**做不到**订阅任何来源；
- 也读不到别人的东西（后端按 `user_id` 分表，越权在 SQL 层面不可能发生）；
- 缺口检测用的"这个群上一条消息的时间"取自**自己的镜像**，不看后端的共享群状态。

## 已知边界

- **解密与导出都是增量的**：按 `max(rowid)` / `max(msg_id)` 接着上次的位置做，
  没有任何新消息的一轮**约 2 秒**（实测 77 万行：全量解密 34s + 全量导出 54s，
  现在只剩剥头 1.5s 和数据判断）。代价与"不重读已读消息"是同一条：
  **改动过的旧消息不会重新进导出库**，客户端也就不会重新看它。
  想彻底重来就删掉 `nt_msg_plain.db` / `nt_msg_export.db`（会自动全量重建）；
  换了 QQ 账号或换了源库时也应该这么做 —— 不过检测到"源库比导出库还旧"时
  客户端会**自己整表重导一次**。
- **坏行会被跳过**，但会记下 rowid、打 ERROR、报告里标 `ok=False`；
  整张表**整块**读不下去时会直接停下来要求人工处理。
- **附件字节多半拿不到**：导出库里只有文件名、md5 和一段会过期的 CDN 相对路径，
  配好 `CLIENT_ATTACHMENT_ROOT` 才可能找回真字节；找不到会标 `degraded` 而不是静默丢弃。

## 许可

MIT
