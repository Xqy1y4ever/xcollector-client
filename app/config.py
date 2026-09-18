"""配置。全部来自环境变量 / `.env`，每一项都有默认值。

**字段名 = 环境变量名的小写形式**（`CLIENT_DB_PATH` → `client_db_path`）。
这不是随便定的：`pydantic-settings` 默认按字段名去找环境变量，名字对不上就会
**静默忽略**那个变量（`extra="ignore"`），于是"我明明配了"和"根本没生效"长得
一模一样 —— 这个坑在 CLI 冒烟时真踩到过一次。所以两边必须严格同名，
和 `xcollector-bot` / `xcollector-backend` 的写法保持一致。
"""

from __future__ import annotations

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
    backend_timeout: float = 20.0
    # 写接口失败后的重试次数（不含首次），退避 1s/2s/4s
    backend_max_retries: int = 3

    # ---------------- 源库 ----------------
    # nt_msg_db_util 的 3.export.py 产出的**结构化导出库**（明文 SQLite）
    client_db_path: str = ""
    # 一次性最多从库里取多少条新消息。别设太大：一批全失败时重试的代价很高。
    client_batch_size: int = 200
    # 首次运行（没有游标）时往回看多少小时。默认 72 小时：
    # 太短会漏掉"导出库不是刚生成的"那种情况，太长会把很久以前的旧通知
    # 一起灌进清单（那些 DDL 早就过期了）。
    client_initial_lookback_hours: int = 72
    # 一轮最多处理多少条（防止第一次跑就一口气吃掉几万条）
    client_max_messages_per_cycle: int = 500
    # 附件字节的搜索根目录（可选）。nt_msg_export.db 里的图片/文件通常只有
    # CDN URL 和 md5，没有本地路径；如果 NTQQ 的附件目录也导出到了某个地方，
    # 把根目录配在这里，客户端就能按 md5/文件名找回真实字节。
    client_attachment_root: str = ""
    # 找不到字节时怎么记：`url`（只留 CDN 地址，可能是死链）/ `skip`（不记附件）
    client_missing_attachment: Literal["url", "skip"] = "url"

    # ---------------- nt_msg.db：剥头 + 解密 + 导出（可选的一整套前置步骤） ----------------
    # 配了 `CLIENT_NT_MSG_DB` 之后，客户端就**只要这一个输入**：
    #
    #   nt_msg.db ─剥头→ nt_msg_clear.db ─解密→ nt_msg_plain.db ─导出→ nt_msg_export.db
    #
    # 这三步就是 `nt_msg_db_util` 的 `1.decrypt.py` 与 `3.export.py`（已整合进本项目，
    # 见 `app/ntmsg_db/`）。不解密的话，用户得自己跑那两个脚本。
    #
    # **留空 = 完全不起作用**：行为和以前一样，直接读 CLIENT_DB_PATH。
    client_nt_msg_db: str = ""
    # 密钥（16 字节 ASCII，从 NTQQ 进程内存里自己取；见 README）。**别提交进仓库。**
    client_nt_msg_key: str = ""
    # 或者从文件读密钥 —— 比放进环境变量好：环境变量会出现在 `ps`/`/proc`、
    # 容器 inspect、以及 CI 日志里。文件内容首尾空白会被去掉。
    client_nt_msg_key_file: str = ""
    # 中间产物放哪。留空 = 放在 nt_msg.db 旁边（上游的默认命名）。
    client_nt_msg_clear_path: str = ""
    client_nt_msg_plain_path: str = ""
    # nt_msg.db 前面那段 QQ 自定义头的长度（固定 1024）。
    client_nt_msg_header_size: int = 1024
    # 下面这几个是上游 1.decrypt.py 用的 PRAGMA 值，**不要随便改**
    # （改了就等于换了一套加密参数，只有你自己造过库才需要）。
    client_nt_msg_page_size: int = 4096
    client_nt_msg_kdf_iter: int = 4000
    client_nt_msg_kdf_algorithm: Literal["sha1", "sha256", "sha512"] = "sha512"
    client_nt_msg_hmac_algorithm: Literal["sha1", "sha256", "sha512"] = "sha1"
    # 解密这一步要不要跑（关掉 = 明文库/导出库你自己维护）。
    client_decrypt_enabled: bool = True
    # 每批从加密库读多少行（遇坏页会自动缩小，成功后再放大回来）。
    client_decrypt_batch_size: int = 5000
    # 只解密哪些表，逗号分隔。留空 = 全部（上游行为）。
    #
    # 客户端只用到 group_msg_table / c2c_msg_table，但其余表都不大，所以默认全拷
    # （好处是得到的 nt_msg_plain.db 和上游一样，能直接给 nt_msg_search.py 用）。
    client_decrypt_tables: str = ""
    # 解完让 SQLite 自己查一遍明文库：quick（默认）/ full / off。只影响报告。
    client_decrypt_integrity: Literal["quick", "full", "off"] = "quick"
    # 允许多少行因为坏页被跳过。-1 = 不限（上游行为）。
    #
    # 坏页是真实存在的（上游为此专门写了"重连 + 缩小批次 + 跳过坏 rowid"）。
    # 默认容忍是为了不因为一个坏页卡死整天，但**每一次跳过都会打 ERROR 级日志并
    # 记进报告** —— 被跳过的行意味着那几条消息永远进不来，这必须让人看见。
    client_decrypt_max_skips: int = -1
    # 导出这一步要不要跑。
    client_export_enabled: bool = True
    # 要不要连私聊消息一起导。默认 true = 和上游 3.export.py 一样两张表都导；
    # 客户端本身只入库群通知（订阅是按 (群, 发送者) 组织的），私聊只是顺带。
    client_export_include_c2c: bool = True
    client_export_batch: int = 2000
    # 给 group_messages 补 "40003"/"40850" 两列（群内序号、被回复消息的序号）。
    # 上游的导出表没有它们，"这条消息在补充哪条通知"就没法确定性反查。
    client_export_add_seq: bool = True
    # 增量导出时往回多看多少秒（导出是幂等的，多看一点只会慢一点）。
    client_export_overlap_seconds: int = 3600

    # ---------------- 镜像库（客户端自己的状态） ----------------
    # 客户端**自己**维护一份 SQLite，只记「这条源消息处理过没有」+ 它的内容指纹。
    # 增量就靠它，不再依赖后端里的游标：
    #   不在镜像里 → 新消息，处理
    #   在、内容没变、已处理 → 跳过（零成本）
    #   在、**内容变了** → 重新处理（后端幂等会把原来那条任务更新掉）
    #   在、状态 pending/failed → 重试（这就是恢复队列）
    # 留空 = 放在源库旁边（`<源库>.mirror.db`）。
    client_mirror_path: str = ""
    # 增量扫描时往回多看的时长（小时）。源库按时间追加，但同一秒里可能后到，
    # 而"消息被编辑/补充"更是发生在任意更早的位置 —— 回看窗口就是这类改动的
    # 识别范围。窗口越大越不容易漏，代价是每轮多读一点。
    client_recheck_overlap_hours: float = 2.0
    # 回复/引用里，"被引用对象"可能装在这些键上（逗号分隔）。默认值来自
    # nt_msg_db_util 的群字段文档（47402 与群内序号匹配）。留空 = 用默认那组。
    client_quote_keys: str = ""
    # 只有**最近这么多小时**内处理过的消息才接受"被补充"。更早的引用按新消息
    # 处理：三个月前那条通知的回复，几乎一定是另一件事。
    client_amendment_max_age_hours: float = 72.0
    # 要不要把"新消息引用了某条已读消息"当成**补充**（更新那条任务而不是新建一条）。
    # 关掉就退化成"每条消息各建一条任务"。
    client_amendment_enabled: bool = True
    # 强制重新抽取（`--since-hours` 会打开它）。
    #
    # 平时会用"后端已经有这条通知"来跳过抽取（省模型的钱）。但那个捷径在一种情况下
    # 是错的：镜像被删过、而你正好**改过**某条老消息的内容 —— 这时快照是新的、
    # 后端有旧内容的任务，捷径一开就永远不更新。强制模式关掉捷径，按内容重抽一遍。
    client_force_recheck: bool = False

    # ---------------- 白名单（**只做收窄，不是开关**） ----------------
    # 格式与 bot 完全相同（`号码:备注,号码:备注`，备注可省），可以直接复制过来：
    #
    #   CLIENT_GROUP_WHITELIST=123456789:官方通知群,987654321
    #   CLIENT_SENDER_WHITELIST=10001:张老师,10002
    #
    # ⚠️ **语义与 bot 相反，这一点必须看清楚**：
    #
    #   bot      ：留空 = 谁都不放行（fail-closed）。它是唯一入库方，空名单意味着
    #              "还没配好"，所以关死。
    #   client   ：留空 = **不额外限制**。客户端的过滤条件是**你在后端配的订阅**，
    #              白名单只是在这个基础上再收窄一层（源库里往往有几百个群，
    #              先按群/发送者砍一刀能省很多无用扫描）。
    #
    # 为什么不做成 fail-closed：照抄 bot 的话，一个已经跑通的客户端在升级后
    # 会因为"白名单还是空的"而**静默停止入库** —— 而它本来工作得好好的。
    # 默认值不该让工作正常的部署失效。
    #
    # 两个都是"同时满足"（AND）：群在群里白名单 **且** 发送者在发送者白名单。
    # 号码形状不对会**拒绝启动**（见 `_validate_ids`）。
    client_group_whitelist: str = ""
    client_sender_whitelist: str = ""

    # ---------------- 抽取 ----------------
    client_extractor: Literal["rule", "llm", "both"] = "rule"
    llm_api_base: str = "https://api.deepseek.com/v1"
    llm_api_key: str = ""
    llm_model: str = "deepseek-chat"
    llm_timeout: float = 60.0
    llm_temperature: float = 0.0
    llm_max_retries: int = 2
    # 交叉验证：用第二个模型再抽一次，两个模型对截止时间不一致就标 conflict。
    # 这是"LLM 可出错但不可静默出错"里最贵也最有效的一环，默认关。
    client_cross_check_enabled: bool = False
    llm_secondary_api_base: str = ""
    llm_secondary_api_key: str = ""
    llm_secondary_model: str = ""
    # 附件字节要不要喂给模型（VLM）。默认关：源库里通常只有 URL，
    # 而 QQ CDN 的图片链接几小时就过期，喂进去反而引入"模型在猜图"的风险。
    client_vlm_enabled: bool = False
    # 群静默多久算缺口（小时）。和 bot 的 GAP_ALERT_HOURS 是同一套判据。
    client_gap_alert_hours: float = 2.0

    # ---------------- 运行 ----------------
    # 轮询间隔（秒）。用 --once + 计划任务时这个值不起作用。
    client_poll_seconds: int = 300
    # 只组装不写入（`--dry-run` 也会设它）。写了日志，但一个请求都不发。
    client_dry_run: bool = False
    # 全系统统一时区。**必须和 bot 用同一个值** —— 否则同一条"下周三前"在两条
    # 链路上会解析到不同的时刻，而用户没法知道该信哪个。
    #
    # ⚠️ 字段名和形状都是**照着 bot 抄的**，不能改：`timeparse.py`（从 bot 逐字
    # 复制）读的是 `get_settings().tz`，而且拿到的必须是 **tzinfo**，
    # 不是字符串。所以配的是 `DIGEST_TZ`，`tz` 是个派生属性。
    digest_tz: str = "Asia/Shanghai"
    client_log_level: str = "INFO"
    client_log_preview_chars: int = 60


    # ---------------- 游标（已废弃） ----------------
    # 这里原本还有一个"把游标存在后端 bot_state 里"的配置（CLIENT_CURSOR_NAMESPACE /
    # CLIENT_CURSOR_KEY）。镜像库出现之后它被删掉了：水位线表达不了"这一条处理好了
    # 没有"，也表达不了"这一条的内容变了"，而这两件事恰恰是这套系统最要紧的。

    @property
    def backend_base(self) -> str:
        return self.backend_base_url.rstrip("/")

    # ---------------- 白名单 ----------------

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
        import hashlib
        import json

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
    def quote_keys(self) -> tuple[str, ...]:
        raw = (self.client_quote_keys or "").replace("，", ",").strip()
        if not raw:
            from .source.ntmsg import DEFAULT_QUOTE_KEYS

            return tuple(DEFAULT_QUOTE_KEYS)
        return tuple(part.strip() for part in raw.split(",") if part.strip())

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
        """要不要自己做"解密 + 导出"（配了 `CLIENT_NT_MSG_DB` 才需要）。"""
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



@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
