"""配置自检：字段 ↔ 环境变量 ↔ 代码 三方对账。

    .\\venv\\Scripts\\python.exe -m tests.check_config

## 为什么值得单独一个文件

这个客户端的所有配置都来自环境变量，而 `pydantic-settings` 有一条很难发现的规则：
**字段名必须等于环境变量名的小写形式**（`CLIENT_DB_PATH` → `client_db_path`）。
名字对不上时它不报错，而是**静默忽略**那个变量 —— 于是"我明明配了"和
"根本没生效"长得一模一样。

这个坑在开发时真踩到过一次（`CLIENT_TOKEN` 被忽略，客户端以未认证身份跑，
全部 401），所以这里把它钉成三道断言：

  1. `.env.example` 里的每个键都能对应到一个字段（否则用户配了等于白配）；
  2. 每个字段都**真的被代码读过**（否则它是个骗人的开关）；
  3. 白名单的形状校验、时区类型、镜像库默认路径这些"配置派生出来的东西"是对的。

外加一条：已经不存在的旧配置（比如那个被镜像库取代的后端游标）不许再被引用 ——
否则用户按旧文档配了，会以为是生效的。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from app.config import BASE_DIR, ConfigError, Settings

APP_DIR = BASE_DIR / "app"
ENV_EXAMPLE = BASE_DIR / ".env.example"

# 只通过命令行开关设置、**故意不写进 .env.example** 的字段。
# 写进去会误导：这两个是 `--dry-run` / `--since-hours` 的行为开关，
# 放到配置文件里等于鼓励用户长期开着"试跑模式"。
CLI_ONLY_FIELDS = {"client_dry_run", "client_force_recheck"}

# 已经从配置里删掉、且不允许再被引用的键（留着会让人以为它还生效）
REMOVED_KEYS = ("CLIENT_CURSOR_NAMESPACE", "CLIENT_CURSOR_KEY", "WEB_API_TOKEN")

fails: list[str] = []
total = 0


def check(name: str, got, want) -> None:
    global total
    total += 1
    if got == want:
        print(f"ok    {name}")
    else:
        fails.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f"  {detail}" if detail else ""), bool(cond), True)


def env_keys() -> set[str]:
    keys: set[str] = set()
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^([A-Z][A-Z0-9_]*)=(.*)$", line.strip())
        if m:
            keys.add(m.group(1))
    return keys


def app_sources() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in APP_DIR.rglob("*.py")}


def strip_comments(text: str) -> str:
    """去掉整行注释。

    这一步是为了不把**说明性的注释**误判成"还在引用旧配置"：config.py 里就有一段
    注释在解释"这两个键已经删掉了"，那正是我们想要的文档，不是残留引用。
    """
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


def main() -> int:  # noqa: C901
    fields = list(Settings.model_fields)
    field_set = set(fields)

    # ------------------------------------------------------------------
    print("--- 1. .env.example 的每个键都要能对应到字段 ---")
    keys = env_keys()
    check_true(".env.example 不是空的", len(keys) > 20, f"{len(keys)} 个键")
    unknown = sorted(k for k in keys if k.lower() not in field_set)
    check(
        "没有「配了但代码里没有」的键（pydantic 会静默忽略 → 用户以为配上了）",
        unknown,
        [],
    )
    missing = sorted(f for f in fields if f.upper() not in keys and f not in CLI_ONLY_FIELDS)
    check("每个字段都在 .env.example 里有说明", missing, [])

    # ------------------------------------------------------------------
    print("\n--- 2. 每个字段都真的被代码读过 ---")
    sources = app_sources()
    config_src = sources.get("config.py", "")
    unread: list[str] = []
    for name in fields:
        if name in CLI_ONLY_FIELDS:
            continue
        # 三种读法都要认：`settings.<name>`、`get_settings().<name>`、
        # 以及 config 自己那些派生属性里的 `self.<name>`（那也算真的被读了）
        pattern = rf"(?:settings|get_settings\(\)|self)\.{re.escape(name)}\b"
        hits = 0
        for text in sources.values():
            if re.search(pattern, text):
                hits += 1
        if hits == 0:
            unread.append(name)
    check("没有「声明了但没人读」的字段（骗人的开关）", unread, [])

    # ------------------------------------------------------------------
    print("\n--- 3. 已经删掉的旧配置不许再被引用 ---")
    code_text = "\n".join(strip_comments(t) for t in sources.values())
    env_text = ENV_EXAMPLE.read_text(encoding="utf-8")
    for key in REMOVED_KEYS:
        check_true(f"{key} 不在 .env.example 里", key not in env_text, key)
        check_true(f"{key} 不是 Settings 字段", key.lower() not in field_set, key)
        check_true(
            f"{key} 在 app/ 的代码里已无引用",
            key.lower() not in code_text.lower(),
            key,
        )
    check_true("也没有残留的 cursor 配置字段", not any("cursor" in f for f in fields), str(fields))

    # ------------------------------------------------------------------
    print("\n--- 4. 字段名 = 环境变量名的小写（pydantic 的硬规则）---")
    bad_names = [f for f in fields if f != f.lower()]
    check("字段名全是小写", bad_names, [])
    check_true(
        "没有用 env_prefix（否则 .env.example 里的键会对不上）",
        "env_prefix" not in config_src,
    )
    check_true("extra 是 ignore（用户 .env 里留着淘汰的键不该让程序起不来）",
               'extra="ignore"' in config_src)

    # ------------------------------------------------------------------
    print("\n--- 5. 派生出来的配置是对的 ---")
    defaults = Settings()
    check("时区返回的是 tzinfo（timeparse 直接拿它喂 astimezone）",
          type(defaults.tz).__name__, "ZoneInfo" if _has_zoneinfo() else "timezone")
    check(
        "镜像库默认放在源库旁边",
        Settings(client_db_path="/data/nt_msg_export.db").resolved_mirror_path.as_posix(),
        "/data/nt_msg_export.db.mirror.db",
    )
    check(
        "显式配了就用自己的",
        Settings(client_mirror_path="/tmp/m.db").resolved_mirror_path.as_posix(),
        "/tmp/m.db",
    )
    check("后端地址会去掉结尾斜杠", Settings(backend_base_url="http://x:8000/").backend_base, "http://x:8000")
    check("空令牌按「本地开发不校验」处理", Settings().is_user_token, True)
    check("UserToken 认出来了", Settings(client_token="xc_abc").is_user_token, True)
    check("服务令牌会被识别出来（启动时会警告）", Settings(client_token="service-token").is_user_token, False)

    # ---- "只给一个 nt_msg.db" 那条路上的路径推导 ----
    check("没配 CLIENT_NT_MSG_DB → 不起用解密/导出流水线",
          Settings().ntmsg_pipeline_enabled, False)
    check("没配时源库就是 CLIENT_DB_PATH",
          Settings(client_db_path="/data/nt_msg_export.db").ntmsg_export_path.as_posix(),
          "/data/nt_msg_export.db")
    only_source = Settings(client_nt_msg_db="/data/nt_msg.db")
    check("配了 nt_msg.db → 流水线启用", only_source.ntmsg_pipeline_enabled, True)
    check("导出库默认放在 nt_msg.db 旁边（不用再配一个路径）",
          only_source.ntmsg_export_path.as_posix(), "/data/nt_msg_export.db")
    both = Settings(client_nt_msg_db="/data/nt_msg.db", client_db_path="/out/mine.db")
    check("两个都配了 → 听 CLIENT_DB_PATH 的", both.ntmsg_export_path.as_posix(), "/out/mine.db")
    check("镜像库跟着导出库走（换导出库 = 换一份状态，不会串）",
          only_source.resolved_mirror_path.as_posix(), "/data/nt_msg_export.db.mirror.db")

    # ------------------------------------------------------------------
    print("\n--- 6. 白名单配置的派生判定 ---")
    check("留空 → 不限制", Settings().allows("999999999", "88888"), True)
    check(
        "配了 → 只看名单",
        Settings(client_group_whitelist="123456789").allows("999999999", "1"),
        False,
    )
    check(
        "号码写错 → 抛 ConfigError（拒绝启动）",
        _raises_config_error(lambda: Settings(client_sender_whitelist="oops").whitelist_fingerprint),
        True,
    )
    check_true(
        "quotes 键默认有一组（来自 nt_msg_db_util 的文档）",
        len(Settings().quote_keys) >= 3,
        str(Settings().quote_keys),
    )

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


def _has_zoneinfo() -> bool:
    try:
        from zoneinfo import ZoneInfo  # noqa: F401

        ZoneInfo("Asia/Shanghai")
        return True
    except Exception:
        # Windows 上缺 tzdata 时会退化成固定 UTC+8，这是已知且等价的兜底
        return False


def _raises_config_error(fn) -> bool:
    try:
        fn()
    except ConfigError:
        return True
    except Exception:
        return False
    return False


if __name__ == "__main__":
    sys.exit(main())
