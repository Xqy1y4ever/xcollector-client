"""配置：**只有十几个**环境变量，其余参数写死在下面那一块常量里。

**字段名 = 环境变量名的小写形式**（`CLIENT_DB_PATH` → `client_db_path`）。
这不是随便定的：`pydantic-settings` 默认按字段名去找环境变量，名字对不上就会
**静默忽略**那个变量（`extra="ignore"`），于是"我明明配了"和"根本没生效"长得
一模一样 —— 这个坑在 CLI 冒烟时真踩到过一次。所以两边必须严格同名，
和 `xcollector-bot` / `xcollector-backend` 的写法保持一致。

## 为什么砍到十几个

以前这里是 54 个配置项。每一项都"看起来很有用"，但代价是：用户要读 54 行才知道
自己在配什么、配错一个不影响启动只影响行为（最难查的那类故障）、以及**升级时
旧键留在 `.env` 里却不生效**。

现在的分工很清楚：

* **必须由用户给的** → 环境变量（下面 `Settings` 里那十几个）；
* **有默认值就够的** → 写死成常量（`HARDCODED` 那一块），想改就改代码。

`unknown_env_keys()` 会在启动时把"`.env` 里写了、但不是配置项"的键报出来一次 ——
`extra="ignore"` 的另一面就是"配了等于没配"。
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import timedelta, timezone, tzinfo
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

# 时区警告只打一次（见 Settings.tz 的说明）
_warned_tz = False


class ConfigError(RuntimeError):
    """配置本身有问题 —— 这类错误**宁可让进程起不来**，也不能降级跑。

    因为降级的后果全都是"安静地不入库"：白名单里写错一个字符 → 那个来源永远不进
    清单，而日志里只会看到"跳过"，看不到"你配错了"。
    """


def parse_id_name_pairs(raw: str) -> dict[str, str]:
    """解析 `id:备注,id:备注` 形式的配置，返回 `{id: 备注}`。

    格式和 `xcollector-bot` **完全一样**（备注可省略；冒号兼容全角「：」，
    逗号兼容「，」），这样运维可以直接把 bot 里那份复制过来。
    """
    out: dict[str, str] = {}
    for chunk in (raw or "").replace("，", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        id_part, name = chunk, ""
        for sep in (":", "："):
            if sep in chunk:
                id_part, name = chunk.split(sep, 1)
                break
        id_part = id_part.strip()
        if not id_part:
            continue
        out[id_part] = name.strip() or id_part
    return out


# QQ 号 / 群号的形状（与 backend 的 app_user.qq 校验同一套：5~12 位、不以 0 开头）。
_ID_SHAPE = re.compile(r"^[1-9]\d{4,11}$")


def _validate_ids(mapping: dict[str, str], *, field: str) -> None:
    """白名单里的号码形状不对就**直接拒绝启动**。

    这一条是刻意的：写错一个字符的后果是"那个来源永远不进清单"，而它在日志里
    只是一个"跳过" —— 属于最难发现的那类故障。宁可起不来。
    """
    bad = sorted(k for k in mapping if not _ID_SHAPE.match(k))
    if bad:
        raise ConfigError(
            f"{field} 里有不像 QQ 号 / 群号的条目：{bad}。"
            "格式是「号码」或「号码:备注」，多个用逗号分隔"
            "（例：123456789:通知群,987654321:教务处）。"
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---------------- 后端 ----------------
    backend_base_url: str = "http://127.0.0.1:8000"
    # **必须是某个用户的 UserToken（xc_ 开头）**，不是服务令牌。
    # 用服务令牌也能跑，但那意味着这个客户端能读写所有人的数据，与
    # "每人一个客户端"的设计相悖；启动自检会把这件事说清楚。
    client_token: str = ""

    # ---------------- 输入（二选一）----------------
    # A 方案：QQ 的原始加密库。给了它，客户端自己完成"剥头 + 解密 + 导出"。
    client_nt_msg_db: str = ""
    # 16 字节 ASCII 密钥。**别提交进仓库**；优先用下面的文件形式。
    client_nt_msg_key: str = ""
    # 从文件读密钥 —— 比放进环境变量好：环境变量会出现在 `ps`/`/proc`、
    # 容器 inspect、以及 CI 日志里。文件内容首尾空白会被去掉。
    client_nt_msg_key_file: str = ""
    # B 方案：现成的结构化导出库（`nt_msg_db_util` 的 3.export.py 产物）。
    # 两个都配了就以它为准（导出库路径听 CLIENT_DB_PATH）。
    client_db_path: str = ""

    # ---------------- 状态库 ----------------
    # 客户端自己维护的 SQLite，记「这条源消息处理过没有」。增量判据就是它。
    # 留空 = 放在源库旁边（`<源库>.mirror.db`）。**别随便删**：删了会把整个库重读一遍。
    client_mirror_path: str = ""

    # ---------------- 白名单（**只做收窄，不是开关**）----------------
    # 格式与 bot 完全相同（`号码:备注,号码:备注`，备注可省），可以直接复制过来。
    #
    # ⚠️ **语义与 bot 相反**：bot 留空 = 谁都不放行（fail-closed，它是实时入库方）；
    # 客户端留空 = **全都读**（源库里往往有几百个群，想少扫一点就在这里砍一刀）。
    # 照抄 bot 会让一个已经跑通的客户端在升级后静默停止入库，所以这里默认不限制。
    #
    # 两个是「同时满足」（AND）。号码形状不对会**拒绝启动**（见 `_validate_ids`）。
    client_group_whitelist: str = ""
    client_sender_whitelist: str = ""

    # ---------------- 附件 ----------------
    # 附件字节的搜索根目录（可选）。导出库里通常只有 CDN URL 和 md5；把 NTQQ 的
    # 附件目录（或你自己导出的目录）配在这里，客户端就能按 md5/文件名找回真字节。
    client_attachment_root: str = ""

    # ---------------- 抽取 ----------------
    # rule = 只用规则（**完全不发模型请求**，适合先跑通）/ llm = 只信模型
    # （失败降级到规则并记成"盲区"）/ both = 模型为主、规则兜底（推荐）
    client_extractor: Literal["rule", "llm", "both"] = "rule"
    llm_api_base: str = "https://api.deepseek.com/v1"
    llm_api_key: str = ""
    llm_model: str = "deepseek-chat"

    # ---------------- 运行 ----------------
    # 常驻模式的轮询间隔（秒）。用 `--once` + 计划任务时它不起作用。
    client_poll_seconds: int = 300
    client_log_level: str = "INFO"
    # 全系统统一时区。**必须和 bot / 后端用同一个值** —— 否则同一条"下周三前"在两条
    # 链路上会解析到不同的时刻，而用户没法知道该信哪个。
    #
    # ⚠️ 字段名和形状都是照着 bot 抄的：`timeparse.py`（从 bot 逐字复制）读的是
    # `get_settings().tz`，而且拿到的必须是 **tzinfo**，不是字符串。
    digest_tz: str = "Asia/Shanghai"

    # ---------------- 派生 ----------------

    @property
    def backend_base(self) -> str:
        return self.backend_base_url.rstrip("/")

    @property
    def group_whitelist_map(self) -> dict[str, str]:
        mapping = parse_id_name_pairs(self.client_group_whitelist)
        _validate_ids(mapping, field="CLIENT_GROUP_WHITELIST")
        return mapping

    @property
    def sender_whitelist_map(self) -> dict[str, str]:
        mapping = parse_id_name_pairs(self.client_sender_whitelist)
        _validate_ids(mapping, field="CLIENT_SENDER_WHITELIST")
        return mapping

    @property
    def whitelist_active(self) -> bool:
        """有没有配白名单。没配 = 不做这一层收窄。"""
        return bool(self.client_group_whitelist.strip() or self.client_sender_whitelist.strip())

    @property
    def whitelist_fingerprint(self) -> str:
        """白名单的指纹（**顺带做形状校验** —— 号码写错会在这里就地抛 ConfigError）。

        镜像库拿它判断"白名单改过了"。改过就要把之前**因为白名单被跳过**的消息
        重新过一遍 —— 否则用户加上一个群之后会发现"什么都没发生"，而原因
        （那些消息早就被记成 skipped 了）在界面上完全看不出来。

        指纹取**解析并排序之后**的结果，而不是原始字符串：`a,b` 和 `b,a`、
        或者备注改了但号码没改，都不算"白名单变了"，不该触发一次全量重看。
        """
        normalized = json.dumps(
            {
                "groups": sorted(self.group_whitelist_map),
                "senders": sorted(self.sender_whitelist_map),
            },
            ensure_ascii=False,
        )
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    def in_group_whitelist(self, group_id: str | int) -> bool:
        """留空 = 放行（见字段说明：这里是收窄，不是开关）。"""
        mapping = self.group_whitelist_map
        if not mapping:
            return True
        return str(group_id) in mapping

    def in_sender_whitelist(self, sender_id: str | int) -> bool:
        mapping = self.sender_whitelist_map
        if not mapping:
            return True
        return str(sender_id) in mapping

    def allows(self, group_id: str | int, sender_id: str | int) -> bool:
        """两个白名单都要满足（AND），与 bot 的语义一致。"""
        return self.in_group_whitelist(group_id) and self.in_sender_whitelist(sender_id)

    def whitelist_reason(self, group_id: str | int, sender_id: str | int) -> str | None:
        """被白名单挡下时给出**是哪一条**挡的（写进镜像，排查时一眼看到）。"""
        if not self.in_group_whitelist(group_id):
            return f"whitelist:group 群 {group_id} 不在 CLIENT_GROUP_WHITELIST 里"
        if not self.in_sender_whitelist(sender_id):
            return f"whitelist:sender 发送者 {sender_id} 不在 CLIENT_SENDER_WHITELIST 里"
        return None

    @property
    def resolved_mirror_path(self) -> Path:
        """镜像库路径。默认放在**源库**旁边 —— 一个源库对应一份状态，天然不会串。

        用 `ntmsg_export_path` 而不是 `resolved_db_path`：走"只给 nt_msg.db"那条路时
        `CLIENT_DB_PATH` 可以是空的，那时导出库在 nt_msg.db 旁边，镜像也该在那儿（而且
        这样从"手动导出"切到"客户端自己导出"时，只要导出库路径没变，状态就还在）。
        """
        if self.client_mirror_path.strip():
            return Path(self.client_mirror_path).expanduser()
        source = self.ntmsg_export_path
        return source.with_name(source.name + ".mirror.db")

    @property
    def tz(self) -> tzinfo:
        """`DIGEST_TZ` 对应的 tzinfo。**名字和返回类型都必须与 bot 一致** ——
        从 bot 逐字复制的 `timeparse.py` 直接把它喂给 `astimezone()`。

        ⚠️ Windows 上要装 `tzdata`（requirements.txt 里有）：没有它 `zoneinfo`
        找不到时区数据库，会**静默**退回 UTC+8。Asia/Shanghai 恰好全年 UTC+8，
        所以那种"错"是看不出来的 —— 但用户一旦配一个带夏令时的时区就会真的算错。
        所以这条警告**只打一次**（每次都打会被刷屏，刷屏的警告等于没有警告）。
        """
        global _warned_tz
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(self.digest_tz)
        except Exception as exc:
            if not _warned_tz:
                _warned_tz = True
                logger.warning(
                    "时区 %r 无法识别（%s），本次运行按 UTC+8 处理。"
                    "Windows 上通常是因为缺 tzdata：pip install tzdata",
                    self.digest_tz,
                    type(exc).__name__,
                )
            return timezone(timedelta(hours=8))

    @property
    def resolved_db_path(self) -> Path:
        return Path(self.client_db_path).expanduser()

    @property
    def ntmsg_pipeline_enabled(self) -> bool:
        """要不要自己做"剥头 + 解密 + 导出"（配了 `CLIENT_NT_MSG_DB` 才需要）。"""
        return bool((self.client_nt_msg_db or "").strip())

    @property
    def ntmsg_export_path(self) -> Path:
        """这一轮真正要读的库。

        配了 `CLIENT_NT_MSG_DB` 时，导出库**默认放在 nt_msg.db 旁边**
        （`nt_msg_export.db`），而不是要求用户再配一个 `CLIENT_DB_PATH`；
        两者都配了就听 `CLIENT_DB_PATH` 的。
        """
        if not self.ntmsg_pipeline_enabled:
            return self.resolved_db_path
        if self.client_db_path.strip():
            return self.resolved_db_path
        return Path(self.client_nt_msg_db).expanduser().with_name("nt_msg_export.db")

    @property
    def resolved_attachment_root(self) -> Path | None:
        return (
            Path(self.client_attachment_root).expanduser()
            if self.client_attachment_root
            else None
        )

    @property
    def is_user_token(self) -> bool:
        """是不是用户令牌。空令牌（本地开发）按"不校验"处理，也允许。"""
        return not self.client_token or self.client_token.startswith("xc_")


# ==========================================================================
# 写死的参数（以前是 40 多个配置项）
#
# 想改就改这里 —— 它们是**代码的一部分**，改完要重新部署。这样做的理由：这些值
# 要么从来没被改对过、要么改错之后的现象极其难查（例如把 kdf_iter 从 4000 改成
# 别的，症状是"解出来的库全是乱码"）。留在 `.env` 里只会让用户以为改了就生效。
# ==========================================================================

# ---- 一轮读多少 ----
# 一次 SQL 取多少条（分块大小）。
BATCH_SIZE = 200
# 一轮最多处理多少条。第一轮会把导出库里所有群消息过一遍（几十万条），
# 所以积压是分多轮读完的。**它只决定一轮读多少，不决定读哪些** ——
# "读哪些"的判据是镜像里的已读标记（不看时间）。
MAX_MESSAGES_PER_CYCLE = 500

# ---- 附件 ----
# 找不到本地字节时只留 CDN 地址（可能是死链）。另一种做法是干脆不记，但
# "记下来但打不开"比"什么都不留"好排查。

# ---- 缺口告警 ----
# 群静默多久算缺口（小时）。和 bot 的 GAP_ALERT_HOURS 是同一套判据。
GAP_ALERT_HOURS = 2.0

# ---- 后端请求 ----
BACKEND_TIMEOUT = 20.0
# 写接口失败后的重试次数（不含首次），退避 1s/2s/4s
BACKEND_MAX_RETRIES = 3

# ---- 模型 ----
LLM_TIMEOUT = 60.0
LLM_TEMPERATURE = 0.0
LLM_MAX_RETRIES = 2
# 附件字节要不要喂给模型（VLM）。默认关：源库里通常只有 URL，而 QQ CDN 的图片
# 链接几小时就过期，喂进去反而引入"模型在猜图"的风险。想开就把这里改成 True。
VLM_ENABLED = False
# 交叉验证：用第二个模型再抽一次，两个模型对截止时间不一致就标 conflict。
# 这是"LLM 可出错但不可静默出错"里最贵也最有效的一环。要开就填下面两个值
# （api_base 留空 = 用主模型那套地址与密钥）。
CROSS_CHECK_ENABLED = False
SECONDARY_LLM_MODEL = ""
SECONDARY_LLM_API_BASE = ""
SECONDARY_LLM_API_KEY = ""

# ---- 日志 ----
# 日志里预览正文的截断长度（日志里打印整条消息没有意义）。
LOG_PREVIEW_CHARS = 60

# ---- nt_msg.db：剥头 + 解密 + 导出（上游 1.decrypt.py / 3.export.py 的常量）----
# nt_msg.db 前面那段 QQ 自定义头的长度（固定 1024）。
NT_MSG_HEADER_SIZE = 1024
# 下面几个是上游 1.decrypt.py 用的 PRAGMA 值，**不要随便改**：改了就等于换了一套
# 加密参数，只有你自己造过库才需要（改错的症状是"解出来的库全是乱码"）。
NT_MSG_PAGE_SIZE = 4096
NT_MSG_KDF_ITER = 4000
NT_MSG_KDF_ALGORITHM = "sha512"
NT_MSG_HMAC_ALGORITHM = "sha1"
# 每批从加密库读多少行（遇坏页会自动缩小，成功后再放大回来）。
DECRYPT_BATCH_SIZE = 5000
# 解完让 SQLite 自己查一遍明文库（quick / full / off）。只影响报告。
DECRYPT_INTEGRITY = "quick"
# 允许多少行因为坏页被跳过，-1 = 不限（上游行为）。
#
# 坏页真实存在（上游为此专门写了"重连 + 缩小批次 + 跳过坏 rowid"）。容忍是为了
# 不因为一个坏页卡死整天，但**每一次跳过都会打 ERROR 级日志并记进报告** ——
# 被跳过的行意味着那几条消息永远进不来，这必须让人看见。
DECRYPT_MAX_SKIPS = -1
# 导出：每批写多少行。
EXPORT_BATCH = 2000
# 要不要连私聊一起导（和上游 3.export.py 一样两张表都导）。
EXPORT_INCLUDE_C2C = True
# 导出**永远全量**（不做增量）。按时间过滤会让"时间戳没变、内容变了"的旧消息
# 永远不进导出库，那一层一层往上看就都看不见它。全量代价：实测 77 万行约 45 秒。
EXPORT_OVERLAP_SECONDS = 3600


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# 我们自己用的环境变量前缀。`.env` 里这些前缀的键**必须**是上面某个字段，
# 否则就是拼错了或者早就被删了 —— 两种都不该悄悄过去。
ENV_PREFIXES = ("CLIENT_", "LLM_", "BACKEND_")
# 这几个是整份 `.env` 里直接按名字写的（不带前缀）。
ENV_EXACT = ("DIGEST_TZ",)


def unknown_env_keys(env_file: Path | str | None = None) -> list[str]:
    """`.env` 里写了、但**不是配置项**的键。

    `env_file` 不传就用 `Settings` 配的那个（也就是 `BASE_DIR/.env`）；测试会传一个
    临时文件进来。文件不存在、或者 `env_file` 被显式关掉（测试里的隔离）都返回空表。

    为什么必须报出来：`extra="ignore"` 让它们在启动时**静默失效** —— 用户按旧文档
    配了 `CLIENT_XXX`，日志里一切正常，而那个值根本没被读。判断依据就是"它是不是
    一个字段"，所以不需要另外维护一份"已废弃的键"名单（那种名单迟早会和代码漂移）。
    """
    if env_file is None:
        configured = Settings.model_config.get("env_file")
        if not configured:
            return []
        env_file = Path(str(configured))
    path = Path(env_file)
    if not path.exists():
        return []
    fields = {name.upper() for name in Settings.model_fields}
    out: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip().upper()
        if key in fields:
            continue
        # 只报"看起来是我们自己的键"：`.env` 里放别的工具用的东西是合理的，
        # 报出来只会制造噪音（Docker 的宿主目录变量之类）。
        if key in ENV_EXACT or key.startswith(ENV_PREFIXES):
            out.append(key)
    return out
