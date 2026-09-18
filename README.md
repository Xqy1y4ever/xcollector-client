# xcollector-client

Xcollector 的**入库客户端**：读一份聊天记录数据库，抽出通知，用**你自己**的
UserToken 写进后端。

它 **不连 QQ、不需要 NapCat、不需要服务令牌**。存在的理由是把"入库"和"bot"
解耦 —— 后端的数据不再只依赖那一个活的 OneBot 连接：

```
QQ ──▶ NTQQ（nt_msg.db，加密）
          │
          │  nt_msg_db_util：1.decrypt.py → 3.export.py
          ▼
     nt_msg_export.db（明文 SQLite）
          │
          ▼
   xcollector-client  ──UserToken──▶  xcollector-backend  ◀── xcollector-web
```

> bot 仍然可以做入库（实时性更好），但**不再必需**。

## 它做什么

1. 定期读 `nt_msg_export.db` 里**新增**的群消息（按 `(timestamp, msg_id)` 增量）；
2. 用它读到的**你自己配好的订阅**过滤（订阅只有一份，就在后端里）；
3. 抽取通知：规则 + 可选的大模型，**证据为空一律不建条**；
4. 写进后端：原始消息 → 附件 → 通知 → 统计 →（缺口检测）；
5. 把每条消息的处理结果记进**自己的镜像库**，下一轮据此增量。

## 镜像库：客户端自己的状态

客户端自己维护一份 SQLite（默认放在源库旁边，`<源库>.mirror.db`），
**每条消息一行**：`msg_id` + 已读状态 + 内容指纹 + 后端那条原文的 id。

增量就是一次镜像查询，不再有"水位线游标"那种东西：

| 镜像里 | 含义 | 动作 |
|---|---|---|
| 没有 | 新消息 | 处理 |
| 有、内容没变、已处理 | 处理过了 | **跳过（零成本）** |
| 有、**内容变了** | 消息被编辑/补充过 | **重新处理 → 更新原来那条任务** |
| 有、`pending` / `failed` | 上次没做完 / 失败了 | 重试（这就是恢复队列） |
| 有、`skipped` | 订阅之外 / 闲聊 | 终态，不再重看 |

为什么不是一个布尔：在读到的当下就标已读，处理失败的消息就**永久丢失**；
处理成功才标已读，那这个布尔就等于 `state='done'`。所以状态是四态，而
"已读"就是其中的 `done`。

**先记账再干活**：claim 时记 `pending`，处理完才改 `done`。反过来（干完才记账）
一旦在"已经写进后端、还没记账"之间崩掉就只是重做一次（幂等挡住），
而"记完账才发现没写成"会丢一条消息 —— 宁可重做，不可漏。

## 补充：新消息改了一条已读消息 → 改那条任务，不新建

例：有人回复「下周三前交材料」那条通知，说「补充：改到本周五」。

- **怎么认出来**：靠 QQ 的**引用关系**。回复里带的 `47402` 是**群内消息序号**，
  拿它去源表的序号列（`40003` 一类）反查就能定位到被回复的那条。
  这条路是确定性的，不猜。
- **怎么改**：两次独立抽取（原文一次、补充一次，各自以**自己的发送时间**为锚点
  解析相对时间），合并后 `POST /api/notifications` 用**原文的 raw id** ——
  后端按 `(user_id, raw_message_id)` 幂等，命中的就是原来那行，只覆盖机器字段，
  **人工修正不会被冲掉**。补充那句话本身也会作为一条原始消息入库（证据链）。
- **合并规则**：截止时间以补充为准（"改到周五"就是周五），不一致时标 `conflict`
  让人看得见；summary 追加「补充：…」；evidence 逐段拼接（每段仍是原文逐字复制）。

> ⚠️ **一个现实限制**：`nt_msg_db_util` 的字段文档里写着，回复里的 `47422`
> **不与主表 `40001` 匹配** —— 也就是说拿不到被回复消息的 msg_id，只能走
> "群内序号"这条路。而当前 `3.export.py` 产出的 `group_messages` **没有序号列**。
>
> 所以：**认不出引用关系时，客户端不会猜** —— 它按独立的新消息处理，并在日志里
> 说清原因（`--status` 也会把"源表有没有序号列"直接打出来）。想要补充关系生效，
> 需要导出时带上 `40003`/序号那一列；`CLIENT_QUOTE_KEYS` 可以按你的实际字段名调整。
> 这条产品级缺口我没法在客户端内部解决 —— 它是数据源的限制。


## 前置：先把聊天记录导出成明文库

这个客户端读的是
[`QQBackup/nt_msg_db_util`](https://github.com/QQBackup/nt_msg_db_util) 的
**结构化导出库**，不是 NTQQ 那个加密的 `nt_msg.db`：

```bash
# 在 nt_msg_db_util 里
uv run python 1.decrypt.py     # nt_msg.db      → nt_msg_plain.db
uv run python 3.export.py      # nt_msg_plain.db → nt_msg_export.db   ← 用这个
```

| 文件 | 能直接给客户端用吗 |
|---|---|
| `nt_msg.db` | ❌ SQLCipher 加密的原始库 |
| `nt_msg_plain.db` | ❌ 解了密，但消息正文还是 Protobuf 原始列（`40800`） |
| **`nt_msg_export.db`** | ✅ 有 `group_messages` 表：`text` / `content` 都是能直接读的 |

拿错库时客户端会明确告诉你应该用哪一个，而不是抛一个看不懂的 SQLite 错。

导出库的 `group_messages` 提供了这里需要的一切：`group_id`、`sender_qq`、
`timestamp`（**秒**）、`text`、以及 `content` JSON（图片/文件带 `cdn_url`、`md5_hex`）。

## 安装与配置

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -r requirements.txt

cp .env.example .env
```

至少要改这三项：

| 配置 | 说明 |
|---|---|
| `CLIENT_TOKEN` | **某个用户的 UserToken**（`xc_` 开头）。在 QQ 里给机器人发 `/注册` 拿验证码，到网页上注册后得到 |
| `CLIENT_DB_PATH` | `nt_msg_export.db` 的路径 |
| `CLIENT_EXTRACTOR` | `rule`（不花模型的钱）/ `llm` / `both` |

镜像库默认放在源库旁边（`<源库>.mirror.db`），想放到别处就配 `CLIENT_MIRROR_PATH`。

配完先自检一次（**一个写请求都不发**）：

```bash
python -m app.main --status
```

它会打印配置、源库行数与最新时间、后端可达性、身份（是不是 UserToken）、
镜像库的状态、以及**源表有没有"群内序号"列**（没有的话补充关系认不出来，它会直接说出来）。
它会打印订阅、镜像库状态、以及你的订阅。**订阅是过滤条件**，所以这里会明确告诉你订了几条 ——
一条都没有的话，客户端什么都不会入库。

## 跑

```bash
python -m app.main --once            # 跑一轮就退出（推荐配合计划任务）
python -m app.main                   # 常驻，按 CLIENT_POLL_SECONDS 定期跑
python -m app.main --dry-run --once  # 只组装不写入，先看一眼会抽出什么
python -m app.main --since-hours 72  # 强制重抽 72 小时内的消息（见下）
```

**`--since-hours` 是"强制重抽"**：它把回看窗口放大到 N 小时，并且**关掉**
"后端已经有这条通知就跳过抽取"的捷径。用在两种场景：改了抽取规则想重跑、
或者镜像被删过之后要重新检查内容改动。平时不需要它。

`--dry-run` **一个状态都不写进镜像**。这是硬要求：dry-run 不往后端写东西，
要是把消息记成 done，下一次真跑就会把它们全部跳过 —— "先试跑一下"会安静地
让整个客户端什么都不入库。这条有专门的回归测试。

**推荐 `--once` + 计划任务**（Windows 任务计划 / cron / systemd timer）。
这个客户端本来就是批处理的，把调度交给操作系统比让它常驻更省心 ——
也不会有"进程活着但其实卡住了"这种最难发现的故障。

```powershell
# Windows：每 5 分钟一次
schtasks /create /tn XcollectorClient /sc minute /mo 5 ^
  /tr "E:\xcollector-client\.venv\Scripts\python.exe -m app.main --once" ^
  /st 00:00
```

退出码：`0` 正常；`1` 后端调用失败；`2` 配置或源库有问题（脚本可以参考它做告警）。

## 订阅：不需要在这里配

客户端**不自己配订阅**，它读的是后端里你这个账号已经配好的那几条
（网页上的「订阅管理」，或 QQ 里的 `/订阅`）。这样配置只有一份，不会出现
"网页上订了但客户端不知道"。

这也是后端的一条**权限规则**：往共享层（原始消息 / 群状态）写之前，后端会检查
"这个 `(群, 发送者)` 是你订阅的吗"。没订阅就 403，客户端会把它归类成
`not_subscribed` 并告诉你先去订阅 —— 而不是混在"后端坏了"里。

> 还没有任何订阅时，客户端**一轮什么都不做，镜像也一个状态都不写**。
> 这是刻意的：先启动客户端、再去订阅是很自然的顺序；如果那次空跑把 72 小时的
> 存量都标成"处理过了"，用户订完之后会发现什么都没补上、而且毫无线索。

## 附件：先说清这里的现实

源库里的附件通常**只有 CDN 地址和 md5**，而 QQ 的 CDN 链接几小时就过期。
等客户端跑到那条消息时，链接基本已经死了。所以：

- 想让证据图真的能看，把 `CLIENT_ATTACHMENT_ROOT` 指到 NTQQ 的附件目录
  （或你自己导出的附件目录）。客户端会**按 md5** 找回真实字节并上传给后端。
- 找不到时会**降级但留痕**：按 `CLIENT_MISSING_ATTACHMENT` 决定是只留 CDN
  地址还是干脆不记，并且两种情况都会在附件上标 `degraded` + `degraded_reason`，
  日志里也会写一行。**不会安静地少一张图。**

## 权限：它只用 UserToken

| 能做什么 | 怎么做 |
|---|---|
| 读自己的订阅 / 通知 / 统计 / 缺口 | ✅ UserToken |
| 写自己的通知 / 统计 / 缺口 / 键值 | ✅ 归属被强制成自己 |
| 写共享层（原始消息 / 群状态） | ✅ **前提是订阅了那个来源** |
| 上传附件 | ✅ |
| 列用户、发邀请码、签发验证码、投递名单、改机器字段、删通知 | ❌ 403（本来就不该有） |

`app/backend_client.py` 里**故意没有**那些不允许的方法。想加之前先回去看
`xcollector-backend/docs/api.md` 的「谁能写什么」。这一条不是靠 review 守的：
`tests/check_pipeline_e2e.py` 全程只用 UserToken，偷偷调一个服务令牌专属的接口
就会以 403 失败。

## ⚠️ 和 bot 同时跑会重复入库

同一个 QQ 账号如果 bot 也在入库（OneBot 实时链路），两边的 `message_id`
格式不同（bot 用 OneBot 的，客户端用 `ntqq:<msg_id>`），**现有的幂等键拦不住**，
同一条消息会变成两条原始记录、两条通知。

所以**同一个账号只开一边的入库**。两种干净的用法：

- 只用客户端入库 → 把 bot 的群/发送者白名单留空（它就不处理任何消息），
  bot 只留着做 QQ 交互（`/注册`、`/订阅`、摘要推送）；
- 只用 bot 入库 → 别跑这个客户端。

（客户端启动时不会去检测 bot 是否在跑 —— 那需要服务令牌，与它的权限模型冲突。
这件事靠部署配置保证，也在 `xcollector-deploy` 的 README 里写明了。）

## 自检

```powershell
.\.venv\Scripts\python.exe -m tests.check_source_db     # 源库读取（离线，52 条）
.\.venv\Scripts\python.exe -m tests.check_mirror         # 镜像库状态机（离线，38 条）
.\.venv\Scripts\python.exe -m tests.check_timeparse     # 中文相对时间解析（离线）
.\.venv\Scripts\python.exe -m tests.check_location      # 地点提取（离线）

# 端到端：需要一个正在运行的后端
$env:CLIENT_BASE='http://127.0.0.1:8000'; $env:CLIENT_SERVICE_TOKEN='<API_TOKEN>'
.\.venv\Scripts\python.exe -m tests.check_pipeline_e2e
```

`check_pipeline_e2e` 会自己注册一个真用户、造一个源库、跑完整条链路，
并覆盖：订阅过滤、镜像增量、**内容变了要更新原来那条任务**、
**新消息引用已读消息时改那条任务而不是新建**（以及源表缺序号列时如实说认不出来）、
镜像丢失后靠"已有通知集合"跳过重复抽取、
附件两条降级路径、统计、缺口检测、`--dry-run` 一个写请求都不发。

## 已知边界（都是刻意的取舍）

- **只读 `group_messages`**（群聊）。私聊 `c2c_messages` 没做 —— 官方通知都发在群里。
- **只支持 OpenAI 兼容的模型端点**（DeepSeek / 通义 / OpenAI / 自建 vLLM、Ollama）。
  要 Gemini 就走它的兼容端点，或把 `app/llm.py` 换成 bot 里的那套网关。
- **不推定群名/昵称**：源库里只有号码，客户端不编名字，所以通知上的
  `group_name` / `sender_name` 是空的（网页上显示号码）。
- **不连 QQ**：所以 `/注册`、`/订阅` 指令、每日摘要推送这些仍然需要 bot。

## 为什么有两个文件是从 bot 复制过来的

`app/pipeline/timeparse.py` 和 `app/pipeline/rule_extract.py` 是从
`xcollector-bot/app/pipeline/` **逐字复制**的，连测试
（`tests/check_timeparse.py`、`tests/check_location.py`）也是。

原因：同一条通知不论从实时链路（bot）还是从聊天记录库（客户端）进来，
**相对时间必须解析成同一个绝对时间**，否则用户会看到两个不同的截止时间，
而且没法知道该信哪个。

这是一个真实的技术债：跨仓库共享代码没有干净的办法（bot 没有可发布的包，
而这次的约束是**不改 bot**）。防漂移的手段是"同一个测试文件在两个仓库都能跑"：

```powershell
# 在 bot 仓库跑同样的两个文件，两边都必须是绿的
cd ..\xcollector-bot
.\.venv\Scripts\python.exe -m tests.check_timeparse
.\.venv\Scripts\python.exe -m tests.check_location
```

改了任何一边的同名文件或用例，请把另一边也改一遍。

## 技术栈

| | |
|---|---|
| 语言 | Python 3.12+ |
| 读源库 | 标准库 `sqlite3`（**只读**打开，不改你的聊天记录） |
| 调后端 / 模型 | `httpx` |
| 配置 | `pydantic-settings` |
| 依赖 | 没有厂商 SDK，没有框架 |
