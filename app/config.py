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

    # ---------------- 游标 ----------------
    # 游标存在后端的 bot_state 里（按用户隔离），所以换机器/重装都不会丢。
    # 丢了也不是灾难：会退回到 client_initial_lookback_hours 重扫，而重复的通知
    # 由后端的 (user_id, raw_message_id) 幂等键挡住，不会重复入库。
    client_cursor_namespace: str = "client_cursor"
    # 游标键：默认用导出库的绝对路径。同一个用户读多个库时，每个库各有一条游标。
    client_cursor_key: str = ""

    @property
    def backend_base(self) -> str:
        return self.backend_base_url.rstrip("/")

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

    @property
    def cursor_name(self) -> str:
        """这个源库的游标键。默认用绝对路径 —— 同一个用户读多个库时各有一条。"""
        if self.client_cursor_key.strip():
            return self.client_cursor_key.strip()
        try:
            return str(self.resolved_db_path.resolve())
        except OSError:
            return self.client_db_path or "default"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
