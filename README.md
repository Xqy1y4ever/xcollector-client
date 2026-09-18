# xcollector-client

Xcollector 的**入库客户端**：读一份聊天记录数据库，抽出通知，用**你自己**的
UserToken 写进后端。

它 **不连 QQ、不需要 NapCat、不需要服务令牌**。存在的理由是把"入库"和"bot"
解耦 —— 后端的数据不再只依赖那一个活的 OneBot 连接：

```
QQ ──▶ NTQQ（nt_msg.db，SQLCipher 加密，前面还有 1024 字节 QQ 自己的头）
          │
          │  ① 剥头 + 解密   （nt_msg_db_util 的 1.decrypt.py，已整合进本项目）
          ▼
     nt_msg_plain.db（普通 SQLite，还是 QQ 的数字列名）
          │
          │  ② 导出          （nt_msg_db_util 的 3.export.py + msgdb/，已整合）
          ▼
     nt_msg_export.db（字段有名字、正文是文本）
          │
          ▼
   xcollector-client  ──UserToken──▶  xcollector-backend  ◀── xcollector-web
```

**你只需要给一个输入**：加密的 `nt_msg.db` 路径 + 你自己取到的密钥
（`CLIENT_NT_MSG_DB` + `CLIENT_NT_MSG_KEY`）。① 和 ② 都在客户端里，
不需要单独装/跑 `nt_msg_db_util`。

> bot 仍然可以做入库（实时性更好），但**不再必需**。

## 它做什么

1. 需要的话先**剥头 + 解密 + 导出**源库（只在这一步有新数据时才跑）；
2. 定期读 `nt_msg_export.db` 里**新增**的群消息（按 `(timestamp, msg_id)` 增量）；
3. 用它读到的**你自己配好的订阅**过滤（订阅只有一份，就在后端里）；
4. 抽取通知：规则 + 可选的大模型，**证据为空一律不建条**；
5. 写进后端：原始消息 → 附件 → 通知 → 统计 →（缺口检测）；
6. 把每条消息的处理结果记进**自己的镜像库**，下一轮据此增量。

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

- **怎么认出来**：靠 QQ 的**引用关系**。回复里带的 `47402`（`reply_msg_seq`）是
  **群内消息序号**，拿它去源表的序号列（`40003`）反查就能定位到被回复的那条。
  客户端自己导出的库**带这两列**（见下），所以这条路是确定性的、不猜。
- **怎么改**：两次独立抽取（原文一次、补充一次，各自以**自己的发送时间**为锚点
  解析相对时间），合并后 `POST /api/notifications` 用**原文的 raw id** ——
  后端按 `(user_id, raw_message_id)` 幂等，命中的就是原来那行，只覆盖机器字段，
  **人工修正不会被冲掉**。补充那句话本身也会作为一条原始消息入库（证据链）。
- **合并规则**：截止时间以补充为准（"改到周五"就是周五），不一致时标 `conflict`
  让人看得见；summary 追加「补充：…」；evidence 逐段拼接（每段仍是原文逐字复制）。

> ✅ **这条缺口已经补上**：本客户端自己导出的 `nt_msg_export.db` 会**额外带上
> `"40003"`（群内消息序号）和 `"40850"`（被回复消息的群内序号）两列**，于是
> "这条在回复哪一条"能反查到 `msg_id`，补充关系会落到原来那条任务上。
>
> 读**上游** `nt_msg_db_util` 直接导出的库时没有那两列，客户端会退回从正文里找
> `reply_msg_seq`；两种情况都找不到就按独立的新消息处理，并在日志里说清原因
> （`--status` 也会把"源表有没有序号列"直接打出来）。
>
> ⚠️ 而且**即使能反查也不会硬猜**：序号在同群里会被复用（实测 21,129 个
> `(群, 序号)` 组合对应不止一条消息），所以只有**唯一命中**时才认，
> 命中多条就交回"认不出来" —— 宁可少合并一次，也不能把补充写到别人的任务上。


## 前置：把聊天记录变成可用数据

两条路，任选一条。

### A. 交给客户端（推荐：只给一个 `nt_msg.db` 路径）

```env
CLIENT_NT_MSG_DB=/path/to/nt_db/nt_msg.db   # QQ 那个加密的原始库
CLIENT_NT_MSG_KEY=0123456789abcdef          # 16 个 ASCII 字符，自己取（见下）
```

客户端会按需走完"剥头 → 解密 → 导出"，然后把 `nt_msg_export.db` 当作源库。
产物默认写在 `nt_msg.db` 旁边（`nt_msg_clear.db` / `nt_msg_plain.db` / `nt_msg_export.db`）。

第一次配置建议先只跑这一步，它**不连后端、不花模型的钱**：

```bash
python -m app.main --prepare
```

### B. 自己先用上游工具导出，再告诉客户端读哪个文件

```bash
# 在 nt_msg_db_util 里
uv run python 1.decrypt.py     # nt_msg.db      → nt_msg_plain.db
uv run python 3.export.py      # nt_msg_plain.db → nt_msg_export.db   ← 用这个
```

```env
CLIENT_DB_PATH=/path/to/nt_msg_export.db
```

| 文件 | 能直接给客户端用吗 |
|---|---|
| `nt_msg.db` | ❌ SQLCipher 加密的原始库（**A 方案**就是把它交给客户端） |
| `nt_msg_clear.db` | ❌ 只是砍掉了 QQ 的 1024 字节头（**中间产物，别喂给客户端**） |
| `nt_msg_plain.db` | ❌ 解了密，但消息正文还是 Protobuf 原始列（`40800`） |
| **`nt_msg_export.db`** | ✅ 有 `group_messages` 表：`text` / `content` 都是能直接读的 |

拿错库时客户端会明确告诉你应该用哪一个，而不是抛一个看不懂的 SQLite 错。

导出库的 `group_messages` 提供了这里需要的一切：`group_id`、`sender_qq`、
`timestamp`（**秒**）、`text`、以及 `content` JSON（图片/文件带文件名、md5、CDN 地址）。

## 怎么拿到密钥

密钥是 NTQQ 把它那个 SQLite 库解密用的**16 个 ASCII 字符**，只存在于
NTQQ 进程的内存里 —— 磁盘上没有、QQ 也不会给你。取它的办法是"附加到进程读内存"：

- 上游 [`QQBackup/qq-win-db-key`](https://github.com/QQBackup/qq-win-db-key)
  提供了 Windows（PowerShell + 调试器）和 Android（Frida）的脚本，
  它的 README 里有分步说明（本机 `nt_msg_db_util/getkey.ps1` 也是这套东西）；
- 拿到的是**原文**（它自己会校验"长度正好 16 且都是可打印 ASCII"）。

几条必须知道的事实：

1. **换 QQ 账号、换机器、重装 QQ，密钥都会变**，要重取。
2. 你把密钥配给客户端之后，它就等于"能解开你全部聊天记录的凭据"。
   所以：**别提交进 git**（`.gitignore` 已经忽略 `.env`），别贴进聊天记录/截图，
   在容器里别做成 build-arg。更稳妥的做法是写进一个文件用
   `CLIENT_NT_MSG_KEY_FILE` 指过去。
3. 密钥就是**原文**，照抄即可：上游 `nt_msg_db_util` 传的是
   `PRAGMA key = '<16 个字符>'`（客户端用的是同一套 PRAGMA，顺序也照抄）。
   如果你的工具把密钥打印成了十六进制，先还原成 16 个字符再配。

## 解密 + 导出：这一层就是上游那两个脚本

`app/ntmsg_db/` 不是"另写一套"，而是把 `nt_msg_db_util` 的
**`1.decrypt.py` 与 `3.export.py` 整合进本项目**；上游的解析套件 `msgdb/`
（protobuf 定义、字段 parser、导出表结构）是**逐字**搬到仓库根目录的，
一个字都没改（见 `msgdb/VENDORED.md`）。所以：

- 解密走的是**同一个库**（`sqlcipher3`）、**同一套 PRAGMA**、同一个顺序；
- 导出走的是**同一套流程**（`init_db` → 摘 FTS 触发器 → 批量写 → 建索引 → 重建 FTS）、
  **同一套表结构**、**同一个 `content` 形状**；
- 得到的 `nt_msg_plain.db` / `nt_msg_export.db` 与手工跑上游脚本的产物可以互换。

```
app/ntmsg_db/decrypt.py   ← 1.decrypt.py   （剥头、PRAGMA、rowid 分页、坏页容忍、复制索引）
app/ntmsg_db/export.py    ← 3.export.py    （串起 msgdb 的流程 + 客户端要的报告）
msgdb/**                  ← 上游逐字搬运  （40800 的 protobuf 定义/解析/导出表结构）
app/ntmsg_db/prepare.py   ← 本项目新增    （把两步串起来 + "要不要重跑"的判断）
```

### 客户端在上游基础上加的四件事

1. **增量导出**：上游每次都全量解析 77 万行；客户端是定期跑的，所以默认只解析
   水位线之后的行（同一个 SELECT 外面套一层 `WHERE timestamp >= ?`），
   没变过的老行不再重复解析。`--prepare` 会退回全量。
2. **补 `"40003"` / `"40850"` 两列**（群内序号、被回复消息的群内序号）。上游的
   `group_messages` 没有它们，于是"这条消息是在补充哪条通知"没法确定性反查。
   这两列是导完之后 `ALTER TABLE` 加上并回填的，所以 `msgdb/` 保持一字未改。
3. **坏页/坏行的处理**：上游遇到坏页会"重连 + 缩小批次 + 跳过这个 rowid"，
   客户端照做，但**把跳过的 rowid 记下来、结束时打 ERROR 日志、写进报告** ——
   跳过的行意味着那几条消息永远进不来，这件事必须让人看见。
4. **整块坏掉时停下来**：如果一张表**每次**读取都失败（不是某一页偶尔坏），
   上游的"跳过一行再试"会**永远循环下去**（实测确认过）。客户端加了上限：
   连续跳过 50 行、中间一行都没读到，就停下并把两种情况（密钥错 / 块损坏）
   分开说清楚。

### 验证到什么程度

这份整合**在真实的 709 MB `nt_msg.db` 上跑通过**（本机 `nt_msg_db_util/` 里那份）：

| 步骤 | 结果 |
|---|---|
| 剥头 | 709,156,864 字节（原文件 709,157,888 − 1024） |
| 解密 + 逐表拷贝 | 3 张表 774,638 行（群消息 769,003 + 私聊 5,571 + uid 映射 64），**0 行跳过**，19 秒 |
| `PRAGMA quick_check` | `ok` |
| 导出 | 774,574 行写入，45 秒；`parse_status`：typed 748,136 / null 20,846 / wire_fallback 21 |
| 补序号列 | 769,003 行全部回填 |
| 用客户端读取器读回来 | 769,003 行、无缺列；最新 400 条里 22 条带附件、引用序号能反查到 `msg_id` |

另外 `tests/check_ntmsg_db.py`（105 条断言，离线）覆盖：剥头（含幂等与大小不符）、
密钥错/空密钥/带单引号的密钥、逐表拷贝与断点续跑、没有 INTEGER 主键的表重拷、
坏行跳过与计数、整块坏掉必须停下来、导出表结构与 `content` 形状、增量与幂等、
读取器对上游形状的解析（附件、md5 base64、int64 字符串、引用优先级）。

## 读取器适配：上游的 `content` 形状

导出这一层**保持上游的形状不变**：`content` 是
`{"type":"msg_body","segments":[<protobuf 原生 JSON>]}`。所以客户端的读取器
（`app/source/ntmsg.py`）去适配它，而不是反过来改导出：

- **附件**：按段里的 `content_type` 判类型（2 图片 / 3 文件 / 4 语音 / 5 视频 /
  11 表情），从 `filename`、`cdn_url_1..3`、`md5_raw`、`filesize`、`img_width/height`、
  `file_ext` 里取。两个真实数据的坑都处理了：`md5_raw` 经 protobuf JSON 之后是
  **base64**（要先解码再转十六进制），而 int64 字段（`filesize`）在 JSON 里是
  **字符串**而不是数字。
- **引用关系**：优先用**表列** `"40850"`（被回复消息的群内序号），它比正文里的
  字段可靠；正文里的 `reply_msg_seq`(47402) 排第二，`reply_msg_id`(47401) 只是
  "候选"，`reply_source_record_id`(47422) 已知对不上主表，排在最后。
- **刻意不递归进 `ref_msg`**：那是被引用消息的完整内容，把它里面的图片算成本条
  消息的附件是错的。

> ⚠️ 一个实测出来的坑，值得写下来：`"40003"`/`"40850"` 这种**纯数字列名**在 SQL 里
> **必须加引号**（`SELECT "40850"`）。不加引号时 SQLite 会把它当成**数字字面量**，
> 于是每一行都"有值"（读到的是 40850 这个数字），而不是那一列的内容。
> 这个 bug 让 400 条测试消息全都带上了引用关系，是拿真库跑才看出来的。
> 同一类问题还有：序号 `0` 必须当成"没有"，序号列在**同一群里会被复用**
> （实测 21,129 个 `(群, 序号)` 组合对应不止一条消息）—— 所以按序号反查时
> **只认唯一命中**，命中多条就交回"认不出来"，按新消息处理（宁可少合并一次，
> 也不能把补充写到别人的任务上）。

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
| `CLIENT_NT_MSG_DB` + `CLIENT_NT_MSG_KEY` | 加密的 `nt_msg.db` 路径 + 密钥（推荐，客户端自己剥头/解密/导出） |
| 或 `CLIENT_DB_PATH` | 现成的 `nt_msg_export.db` 的路径（自己用上游工具导出的那种用法） |
| `CLIENT_EXTRACTOR` | `rule`（不花模型的钱）/ `llm` / `both` |

这两组**二选一**：配了 `CLIENT_NT_MSG_DB` 就不必再配 `CLIENT_DB_PATH`
（导出库默认放 `nt_msg.db` 旁边）；两个都配了则听 `CLIENT_DB_PATH` 的。

依赖里有一处按平台分叉，值得知道：解密要的 `sqlcipher3` 在 PyPI 上
**Windows 有官方 wheel（cp312~cp314）**，Linux 却只有源码包；Linux 那边用
`sqlcipher3-wheels`（同一份代码的预编译再发布，**模块名还是 `sqlcipher3`**）。
`requirements.txt` 按 `sys_platform` 写好了，Docker 镜像用 Python 3.13 +
`sqlcipher3-wheels`。

镜像库默认放在源库旁边（`<源库>.mirror.db`），想放到别处就配 `CLIENT_MIRROR_PATH`。
后端地址是 `BACKEND_BASE_URL`（默认 `http://127.0.0.1:8000`）。

### 白名单：**只做收窄，不是开关**

```env
CLIENT_GROUP_WHITELIST=123456789:官方通知群,987654321
CLIENT_SENDER_WHITELIST=10001:张老师
```

格式与 bot **完全一样**（`号码:备注,号码:备注`，备注可省；全角冒号/逗号也认），
可以直接把 bot 那份复制过来。两个是「同时满足」：群在群名单里 **且** 发送者在发送者名单里。

> ⚠️ **语义与 bot 相反，这一点必须看清楚**：
>
> | | 留空 = |
> |---|---|
> | `xcollector-bot` | **谁都不放行**（fail-closed：它是唯一入库方，空名单 = 还没配好） |
> | `xcollector-client` | **不额外限制**（客户端的主过滤条件是**你在后端配的订阅**，白名单只是再收窄一层） |
>
> 照抄 bot 的 fail-closed 会让一个**已经跑通**的客户端在升级后静默停止入库 ——
> 默认值不该让工作正常的部署失效。所以这里刻意选了「默认不改变行为」。
>
> 源库里往往有几百个群、几千个发送者，先按白名单砍一刀能省掉大量无用扫描。

**号码写错会拒绝启动**（退出码 `2`，并指出是哪一项），而不是"跳过那一条"：
写错一个字符的后果是"那个来源永远不进清单"，而它在日志里只会是一个"跳过" ——
属于最难发现的那类故障，所以宁可起不来。

**改了白名单会立刻生效**：之前"因为它而被跳过"的消息会被自动放回待处理重看一遍
（靠镜像库里的白名单指纹）。没有这一步的话，你往白名单里加一个群会发现
"什么都没发生" —— 那些消息早就被记成终态了，而界面上看不到任何解释。

### 想做成「预配好的镜像」？

后端地址与两个白名单可以作为 Docker 的**构建期默认值**：

```bash
docker build \
  --build-arg BACKEND_BASE_URL=https://collector.example.com \
  --build-arg CLIENT_GROUP_WHITELIST=123456789:官方通知群 \
  --build-arg CLIENT_SENDER_WHITELIST=10001:张老师 \
  -t xcollector-client .
```

两条代价说清楚：定进去的值**改不了、除非重新构建**（运行时 `-e` 仍可覆盖）；
**绝不要把 `CLIENT_TOKEN` 做成 build-arg** —— 构建参数留在镜像层里，
任何拿到镜像的人 `docker history` 就能看到，那是这个用户的全部凭据。

配完先自检一次（**一个写请求都不发**）：

```bash
python -m app.main --status
```

它会打印配置、解密/导出的参数与产物路径、源库行数与最新时间、后端可达性、
身份（是不是 UserToken）、镜像库的状态、以及**源表有没有"群内序号"列**
（没有的话补充关系认不出来，它会直接说出来）。
它还会打印订阅 —— **订阅是过滤条件**，所以这里会明确告诉你订了几条，
一条都没有的话，客户端什么都不会入库。

`--status` **不写任何东西**：它不会顺手帮你解密或导出。导出库还不存在时它会直接
告诉你"先跑一次 `--prepare`"。

## 跑

```bash
python -m app.main --prepare         # 只做「解密 + 导出」，不连后端
python -m app.main --once            # 跑一轮就退出（推荐配合计划任务）
python -m app.main                   # 常驻，按 CLIENT_POLL_SECONDS 定期跑
python -m app.main --dry-run --once  # 只组装不写入，先看一眼会抽出什么
python -m app.main --since-hours 72  # 强制重抽 72 小时内的消息（见下）
```

`--prepare` 是给"只给一个 nt_msg.db"那条路用的：它**强制**重跑一遍
剥头 + 解密 + 导出（平时是"源库比产物新才跑"），并按表打印拷了多少行、
跳过了多少行、SQLite 自检结果。第一次配密钥、或者怀疑解密有问题时，先跑它。
用 `--once` / `--loop` 时这一步会自动进行（只在新数据到来时才真的跑）。

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

导出库里的附件有三样东西：**文件名**（`filename`）、**md5**（`md5_raw`）、
以及一段 **CDN 相对路径**（`/download?appid=1407&fileid=...`，不是完整 URL，
而且带 `expire_ts` —— QQ 的链接几小时就过期）。等客户端跑到那条消息时，
链接基本已经死了。所以：

- 想让证据图真的能看，把 `CLIENT_ATTACHMENT_ROOT` 指到 NTQQ 的附件目录
  （或你自己导出的附件目录）。客户端会**按 md5** 找回真实字节并上传给后端。
- 找不到时会**降级但留痕**：按 `CLIENT_MISSING_ATTACHMENT` 决定是只留那个
  CDN 地址还是干脆不记，并且两种情况都会在附件上标 `degraded` + `degraded_reason`，
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
.\.venv\Scripts\python.exe -m tests.check_ntmsg_db      # 剥头/解密/导出（离线，105 条）
.\.venv\Scripts\python.exe -m tests.check_source_db     # 源库读取（离线，52 条）
.\.venv\Scripts\python.exe -m tests.check_mirror        # 镜像库状态机（离线，38 条）
.\.venv\Scripts\python.exe -m tests.check_whitelist     # 白名单（离线，48 条）
.\.venv\Scripts\python.exe -m tests.check_config        # 配置三方对账（离线，34 条）
.\.venv\Scripts\python.exe -m tests.check_timeparse     # 中文相对时间解析（离线，18 条）
.\.venv\Scripts\python.exe -m tests.check_location      # 地点提取（离线，14 条）

# 端到端：需要一个正在运行的后端
$env:CLIENT_BASE='http://127.0.0.1:8000'; $env:CLIENT_SERVICE_TOKEN='<API_TOKEN>'
.\.venv\Scripts\python.exe -m tests.check_pipeline_e2e
```

`check_ntmsg_db` 用**真的库**做夹具：加密库是用 `sqlcipher3` 按上游那套 PRAGMA
现场造的，`40800` 里的消息体是用上游 `msgdb` 的 `_pb2` 构造的 ——
所以上游改了字段名、表结构或参数，这里会立刻红。

`check_pipeline_e2e` 会自己注册一个真用户、造一个源库、跑完整条链路，
并覆盖：订阅过滤、镜像增量、**内容变了要更新原来那条任务**、
**新消息引用已读消息时改那条任务而不是新建**（以及源表缺序号列时如实说认不出来）、
镜像丢失后靠"已有通知集合"跳过重复抽取、
附件两条降级路径、统计、缺口检测、`--dry-run` 一个写请求都不发。

## 已知边界（都是刻意的取舍）

- **只读 `group_messages`**（群聊）。私聊 `c2c_messages` 导了但没入库 —— 官方通知
  都发在群里（订阅模型也是按 `(群, 发送者)` 组织的）。
- **只支持 OpenAI 兼容的模型端点**（DeepSeek / 通义 / OpenAI / 自建 vLLM、Ollama）。
  要 Gemini 就走它的兼容端点，或把 `app/llm.py` 换成 bot 里的那套网关。
- **不推定群名/昵称**：源库里只有号码，客户端不编名字，所以通知上的
  `group_name` / `sender_name` 是空的（网页上显示号码）。
  顺带一提：`nt_uid_mapping_table` 里其实有 uid → QQ 的映射，但那是"编名字"的另一种
  形式，故意没用。
- **不连 QQ**：所以 `/注册`、`/订阅` 指令、每日摘要推送这些仍然需要 bot。
- **坏行会被跳过（默认不限次数）**，但**一定会报出来**：跳过哪些 rowid 进日志和报告，
  报告里的 `ok` 会是 False。整块读不下去时会直接停下来。
- **增量导出按时间戳**：`CLIENT_NT_MSG_DB` 那条路上，导出默认只处理水位线之后的行。
  如果 QQ 里的**老消息被编辑**（时间戳不变），增量导出不会重看它；
  需要的话跑一次 `python -m app.main --prepare`（强制全量）。
- **附件字节仍然多半拿不到**（见「附件」一节）：导出库里的图/文件只有文件名、md5
  和一段 CDN 相对路径（`/download?appid=...&fileid=...`），要配好
  `CLIENT_ATTACHMENT_ROOT` 才可能按 md5/文件名找回真字节。

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
| 语言 | Python 3.12+（本机与 CI 跑 3.14，Docker 镜像用 3.13） |
| 读源库 | 标准库 `sqlite3`（**只读**打开，不改你的聊天记录） |
| 解密 nt_msg.db | `sqlcipher3`（Windows）/ `sqlcipher3-wheels`（Linux），与上游同一个库 |
| protobuf 解析 | `protobuf` + 上游逐字搬来的 `msgdb/`（含 protoc 生成的 `_pb2`） |
| 调后端 / 模型 | `httpx` |
| 配置 | `pydantic-settings` |
| 依赖 | 没有厂商 SDK，没有框架 |

## 和 `nt_msg_db_util` 的关系

| | |
|---|---|
| 用了它的什么 | `1.decrypt.py` 的流程与参数、`3.export.py` 的流程、整个 `msgdb/` 包（逐字） |
| 怎么用的 | 复制进本仓库（`app/ntmsg_db/` + `msgdb/`），不是运行时依赖它、也不是 import 它 |
| 为什么复制而不是依赖 | 它不是一个可安装的包（`uv` 项目、`requires-python >=3.14`），而客户端要能 `pip install` 就跑 |
| 升级怎么办 | 见 `msgdb/VENDORED.md`：覆盖 `msgdb/`，跑 `tests.check_ntmsg_db`，红了就跟着改 |
| 没用的部分 | `2.slim.py`（重建一个"瘦身"库）和 `nt_msg_search.py` 没有搬过来 —— 客户端不搜索，只增量读 |
