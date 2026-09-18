"""配置。全部来自环境变量 / `.env`，每一项都有默认值。

**字段名 = 环境变量名的小写形式**（`CLIENT_DB_PATH` → `client_db_path`）。
这不是随便定的：`pydantic-settings` 默认按字段名去找环境变量，名字对不上就会
**静默忽略**那个变量（`extra="ignore"`），于是"我明明配了"和"根本没生效"长得
一模一样 —— 这个坑在 CLI 冒烟时真踩到过一次。所以两边必须严格同名，
和 `xcollector-bot` / `xcollector-backend` 的写法保持一致。
"""

from __future__ import annotations

import logging
from datetime import timedelta, timezone, tzinfo
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

# 时区警告只打一次（见 Settings.tz 的说明）
_warned_tz = False


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

    @property
    def resolved_mirror_path(self) -> Path:
        """镜像库路径。默认放在源库旁边 —— 一个源库对应一份状态，天然不会串。"""
        if self.client_mirror_path.strip():
            return Path(self.client_mirror_path).expanduser()
        source = self.resolved_db_path
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
