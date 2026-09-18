"""白名单与「改白名单之后立刻生效」。

    .\\venv\\Scripts\\python.exe -m tests.check_whitelist

## 这个文件守的是什么

客户端这一层的白名单**只做收窄**，留空 = 不限制（和 bot 的 fail-closed 相反）。
所以这里要钉住三件事：

1. **默认不改变行为**：一个号码都不配时，判定恒为放行 —— 这样已经跑通的部署
   升级之后不会突然什么都不入库（那会是一次静默的停摆）。
2. **配了就要真的收窄**，而且群与发送者是「同时满足」（AND），和 bot 一致。
3. **号码写错要拒绝启动**（不是"跳过那条"）。跳过的后果是那个来源永远不进清单，
   而日志里只看到一个"跳过" —— 属于最难发现的那类故障。
4. **改了白名单要立刻生效**：之前因白名单被跳过的消息会被放回待处理。
   没有这一步，用户往白名单里加一个群会发现"什么都没发生"。

不联网、不碰后端。
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from app.config import ConfigError, Settings, parse_id_name_pairs
from app.mirror import STATE_PENDING, STATE_SKIPPED, Mirror
from app.source.ntmsg import SourceMessage
from tests._hermetic import isolate_settings

# "一个号码都不配 → 放行"必须真的从一个空的 Settings 出发：本机那份 .env 里
# 有真实白名单，不清掉的话这一组断言会以看不懂的方式失败。
isolate_settings()

SCRATCH = Path(__file__).resolve().parent.parent / ".tmp-test"

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


def message(msg_id: str, group: str = "123456789", sender: str = "10001", ts: int = 1000) -> SourceMessage:
    return SourceMessage(
        msg_id=msg_id,
        timestamp=ts,
        group_id=group,
        sender_id=sender,
        sender_name=None,
        text="下周三前交材料",
    )


def main() -> int:  # noqa: C901
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    print("--- 1. 解析：格式与 bot 完全一致（能直接复制过来）---")
    parsed = parse_id_name_pairs("123456789:通知群, 223456789 ，323456789:教务处")
    check("全角逗号也能切", len(parsed), 3)
    check("带备注", parsed.get("123456789"), "通知群")
    check("省略备注时用号码本身", parsed.get("223456789"), "223456789")
    check("全角冒号也认", parse_id_name_pairs("123456789：教务处").get("123456789"), "教务处")
    check("空串 → 空", parse_id_name_pairs(""), {})
    check("多余逗号被忽略", len(parse_id_name_pairs("123456789,,,")), 1)

    # ------------------------------------------------------------------
    print("\n--- 2. 留空 = 不限制（这一点和 bot **相反**，是有意的）---")
    empty = Settings()
    check("群不限制", empty.in_group_whitelist("999999999"), True)
    check("发送者不限制", empty.in_sender_whitelist("88888"), True)
    check("整体放行", empty.allows("999999999", "88888"), True)
    check("没配白名单时 whitelist_active 为假", empty.whitelist_active, False)
    check("没被挡时没有原因", empty.whitelist_reason("999999999", "88888"), None)

    # ------------------------------------------------------------------
    print("\n--- 3. 配了就收窄，而且是 AND ---")
    only_group = Settings(client_group_whitelist="123456789:通知群")
    check("名单里的群放行", only_group.allows("123456789", "10001"), True)
    check("名单外的群挡下", only_group.allows("999999999", "10001"), False)
    check("发送者没配 → 不限制发送者", only_group.allows("123456789", "88888"), True)
    check_true(
        "挡下时说明是群的原因",
        "whitelist:group" in str(only_group.whitelist_reason("999999999", "10001")),
        str(only_group.whitelist_reason("999999999", "10001")),
    )

    only_sender = Settings(client_sender_whitelist="10001:张老师")
    check("名单里的发送者放行", only_sender.allows("999999999", "10001"), True)
    check("名单外的发送者挡下（哪怕群没限制）", only_sender.allows("999999999", "88888"), False)
    check_true(
        "挡下时说明是发送者的原因",
        "whitelist:sender" in str(only_sender.whitelist_reason("999999999", "88888")),
        str(only_sender.whitelist_reason("999999999", "88888")),
    )

    both = Settings(client_group_whitelist="123456789", client_sender_whitelist="10001")
    check("两个都在名单里 → 放行", both.allows("123456789", "10001"), True)
    check("群在、发送者不在 → 挡下（AND）", both.allows("123456789", "88888"), False)
    check("发送者在、群不在 → 挡下（AND）", both.allows("999999999", "10001"), False)
    check("whitelist_active 为真", both.whitelist_active, True)

    # ------------------------------------------------------------------
    print("\n--- 4. 号码写错 → 拒绝启动（不是静默跳过）---")
    for bad_value, which in (
        ("abc", "CLIENT_GROUP_WHITELIST"),
        ("123456789:通知群,oops", "CLIENT_GROUP_WHITELIST"),
        ("0123456", "CLIENT_GROUP_WHITELIST"),
        ("123", "CLIENT_SENDER_WHITELIST"),
    ):
        kwargs = {which.lower(): bad_value}
        try:
            Settings(**kwargs).validate_whitelist()
            check_true(f"{which}={bad_value!r} → 应该报错", False, "居然没报错")
        except ConfigError as exc:
            check_true(f"{which}={bad_value!r} → 报错", True, "")
            check_true(f"  报错点出了是哪一项", which in str(exc), str(exc)[:110])
            check_true(f"  报错给了正确格式的例子", "123456789" in str(exc), str(exc)[:110])

    # ------------------------------------------------------------------
    print("\n--- 5. 留空 / 顺序 / 备注：判定只看号码集合 ---")
    a = Settings(client_group_whitelist="123456789:甲,223456789:乙")
    b = Settings(client_group_whitelist="223456789,123456789")
    check("顺序和备注不影响判定", a.allows("223456789", "1"), b.allows("223456789", "1"))
    check("号码不同就不同", a.allows("323456789", "1"), False)
    check("空白名单一律放行", Settings().allows("999999999", "1"), True)
    # 解析出来的集合是我们下推给 SQL 的那一份：界面/日志里显示的和实际扫的必须一致
    check("解析出来的群号集合", sorted(a.group_whitelist_map), ["123456789", "223456789"])
    check("备注留下来了（只用于显示）", a.group_whitelist_map["123456789"], "甲")

    # ------------------------------------------------------------------
    print("\n--- 6. 旧版本留下的「白名单跳过」记录会被清掉 ---")
    # 白名单下推之后，"白名单外的消息"根本不会进镜像（扫都不扫），所以旧版本那批
    # skipped 记录只是垃圾。清掉它们，那些消息的"读没读过"重新由白名单说话：
    # 新放开的来源不在镜像里 → 下一轮自然被读到（不需要"放回待处理"那一步）。
    mirror = Mirror(SCRATCH / "wl-mirror.db")
    mirror.claim(message("1"))
    mirror.finish("1", state=STATE_SKIPPED, error="whitelist:group 群 999999999 不在名单里")
    mirror.claim(message("2"))
    mirror.finish("2", state=STATE_SKIPPED, error="闲聊")
    mirror.claim(message("3"))
    mirror.finish("3", state="done", raw_id="raw-3")

    check("清掉 1 条（只有 whitelist 那种）", mirror.drop_whitelist_skips(), 1)
    check("被白名单挡的那条从镜像里消失了", mirror.get("1"), None)
    check("因为别的原因跳过的**不动**", mirror.get("2").state, STATE_SKIPPED)
    check("已经完成的也不动", mirror.get("3").state, "done")
    check("再清一次是 0 条（幂等）", mirror.drop_whitelist_skips(), 0)

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
