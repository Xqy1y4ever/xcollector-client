"""配置自检：字段 ↔ 环境变量 ↔ 代码 三方对账。

    .\\venv\\Scripts\\python.exe -m tests.check_config

## 为什么值得单独一个文件

这个客户端的所有配置都来自环境变量，而 `pydantic-settings` 有一条很难发现的规则：
**字段名必须等于环境变量名的小写形式**（`CLIENT_DB_PATH` → `client_db_path`）。
名字对不上时它不报错，而是**静默忽略**那个变量 —— 于是"我明明配了"和
"根本没生效"长得一模一样。

这个坑在开发时真踩到过一次（`CLIENT_TOKEN` 被忽略，客户端以未认证身份跑，
全部 401），所以这里把它钉成几道断言：

  1. **配置项不超过 20 个**（54 个太多了：用户要读 54 行才知道自己在配什么，
     而且配错一个不影响启动、只影响行为 —— 那是最难查的一类故障）；
  2. `.env.example` 里的每个键都能对应到一个字段（否则用户配了等于白配）；
  3. 每个字段都**真的被代码读过**（否则它是个骗人的开关）；
  4. 砍掉的那些键**不许再回来**（要高级行为就去改 `app/config.py` 的常量）；
  5. 白名单形状校验、时区类型、镜像库默认路径这些"派生出来的东西"是对的。

第 4 条的另一面是 `unknown_env_keys()`：用户 `.env` 里留着非配置项的键时，
启动会警告一次 —— `extra="ignore"` 让它们静默失效，那和"配了"长得一模一样。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from app.config import (
    BATCH_SIZE,
    BASE_DIR,
    CROSS_CHECK_ENABLED,
    EXPORT_BATCH,
    MAX_MESSAGES_PER_CYCLE,
    NT_MSG_HEADER_SIZE,
    NT_MSG_KDF_ITER,
    NT_MSG_PAGE_SIZE,
    VLM_ENABLED,
    ConfigError,
    Settings,
    unknown_env_keys,
)
from tests._hermetic import isolate_settings

# 下面那些 "默认值" 断言要真的从默认值出发 —— 本机那份 .env（真令牌、真白名单、
# 真导出库路径）必须先摘掉，否则测的是这台机器的配置，不是代码。
isolate_settings()

APP_DIR = BASE_DIR / "app"
ENV_EXAMPLE = BASE_DIR / ".env.example"
# 临时文件放仓库里（`.tmp-test/`，已 gitignore），不用系统 temp —— 文件沙箱下
# 系统 temp 在清理阶段会被拒绝 chmod，那会让一个全通过的测试以看不懂的错误收场。
SCRATCH = BASE_DIR / ".tmp-test"

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


def main() -> int:  # noqa: C901
    fields = list(Settings.model_fields)
    field_set = set(fields)

    # ------------------------------------------------------------------
    print("--- 1. 配置项只有十几个，且 .env.example 与字段严格对应 ---")
    keys = env_keys()
    check_true(".env.example 不是空的", len(keys) >= 10, f"{len(keys)} 个键")
    # 这条是这次瘦身的**验收条件**：54 个配置项太多了（用户要读 54 行才知道自己在
    # 配什么、配错一个不影响启动只影响行为）。上限写死在这里，多一个就要有人解释。
    check_true(
        f"配置项不超过 20 个（现在 {len(keys)} 个）",
        len(keys) <= 20,
        str(sorted(keys)),
    )
    check(
        "没有「配了但代码里没有」的键（pydantic 会静默忽略 → 用户以为配上了）",
        sorted(k for k in keys if k.lower() not in field_set),
        [],
    )
    missing = sorted(f for f in fields if f.upper() not in keys)
    check("每个字段都在 .env.example 里有说明", missing, [])

    # ------------------------------------------------------------------
    print("\n--- 2. 每个字段都真的被代码读过 ---")
    sources = app_sources()
    config_src = sources.get("config.py", "")
    unread: list[str] = []
    for name in fields:
        # 三种读法都要认：`settings.<name>`、`get_settings().<name>`、
        # 以及 config 自己那些派生属性里的 `self.<name>`（那也算真的被读了）
        pattern = rf"(?:settings|get_settings\(\)|self)\.{re.escape(name)}\b"
        if not any(re.search(pattern, text) for text in sources.values()):
            unread.append(name)
    check("没有「声明了但没人读」的字段（骗人的开关）", unread, [])

    # ------------------------------------------------------------------
    print("\n--- 3. 砍掉的那些键不许再出现在代码/示例里 ---")
    # 只剩十几个配置项，所以"哪些键被砍了"不再需要一份人工维护的名单 ——
    # 只要它不在字段里、代码里也没有引用，就不该出现在 .env.example 里。
    example_keys = env_keys()
    check(
        ".env.example 里没有不是字段的键",
        sorted(k for k in example_keys if k.lower() not in field_set),
        [],
    )
    # 砍掉的高级项仍然可以**在代码里**出现（写死成了常量），所以这里只查一件事：
    # 它们不再是字段。挑几个最容易"偷偷回来"的。
    for gone in (
        "CLIENT_BATCH_SIZE",
        "CLIENT_MAX_MESSAGES_PER_CYCLE",
        "CLIENT_MISSING_ATTACHMENT",
        "CLIENT_RECHECK_OVERLAP_HOURS",
        "CLIENT_AMENDMENT_ENABLED",
        "CLIENT_DRY_RUN",
        "CLIENT_EXPORT_ADD_SEQ",
        "CLIENT_DECRYPT_ENABLED",
        "CLIENT_INITIAL_LOOKBACK_HOURS",
        "LLM_TEMPERATURE",
        "LLM_SECONDARY_MODEL",
    ):
        check_true(f"{gone} 已经不是配置项（写死成常量了）", gone.lower() not in field_set, gone)
    check_true("也没有残留的 cursor 配置字段", not any("cursor" in f for f in fields), str(fields))

    # ------------------------------------------------------------------
    print("\n--- 3b. 用户 .env 里留着非配置项的键要能看出来 ---")
    # 这一步守的是 `extra="ignore"` 的另一面：这些键不会让程序起不来，但也不会生效
    # —— 于是"我明明配了"和"这个键根本没用"长得一模一样（从旧版本升上来时最明显）。
    env_file = SCRATCH / "unknown.env"
    SCRATCH.mkdir(parents=True, exist_ok=True)
    env_file.write_text(
        "# 注释里的键不算\n"
        "\n"
        "CLIENT_DB_PATH=/data/nt_msg_export.db\n"
        "CLIENT_BATCH_SIZE=200\n"
        "  client_recheck_overlap_hours = 2  \n"
        "POSTGRES_PASSWORD=hunter2\n"
        "TZ=Asia/Shanghai\n"
        "不是键值行\n",
        encoding="utf-8",
    )
    check(
        "只报「我们自己前缀」的键（别的工具用的不报）",
        sorted(unknown_env_keys(env_file)),
        ["CLIENT_BATCH_SIZE", "CLIENT_RECHECK_OVERLAP_HOURS"],
    )
    check("文件不存在 → 空表（不是报错）", unknown_env_keys(SCRATCH / "nope.env"), [])
    check("env_file 被关掉（测试隔离）时不去读磁盘", unknown_env_keys(), [])

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
        _raises_config_error(lambda: Settings(client_sender_whitelist="oops").validate_whitelist()),
        True,
    )

    # ------------------------------------------------------------------
    print("\n--- 7. 写死的常量本身是自洽的 ---")
    # 它们取代了原来的 40 多个配置项，所以这里钉一遍：值还在、类型对、彼此不矛盾。
    check_true("一轮预算是个正整数", isinstance(MAX_MESSAGES_PER_CYCLE, int) and MAX_MESSAGES_PER_CYCLE > 0,
               str(MAX_MESSAGES_PER_CYCLE))
    check_true("一次 SQL 取的量不超过一轮预算", BATCH_SIZE <= MAX_MESSAGES_PER_CYCLE,
               f"{BATCH_SIZE} vs {MAX_MESSAGES_PER_CYCLE}")
    check_true("上游的加密参数没被改坏（page_size/kdf_iter 是固定值）",
               (NT_MSG_PAGE_SIZE, NT_MSG_KDF_ITER) == (4096, 4000),
               f"{NT_MSG_PAGE_SIZE}/{NT_MSG_KDF_ITER}")
    check_true("抽头长度是 1024（上游 1.decrypt.py 的常量）", NT_MSG_HEADER_SIZE == 1024,
               str(NT_MSG_HEADER_SIZE))
    check_true("导出是按 msg_id 增量的（没有时间窗口这种模糊判据）",
               isinstance(EXPORT_BATCH, int) and EXPORT_BATCH > 0, str(EXPORT_BATCH))
    check_true("默认不把图片喂给模型、也不做交叉验证（要开就去改常量）",
               VLM_ENABLED is False and CROSS_CHECK_ENABLED is False, "VLM/CROSS_CHECK")

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
