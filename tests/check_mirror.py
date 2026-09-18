"""镜像库：状态机、未读标记、缺口检测用的"上一条"。

    .\\venv\\Scripts\\python.exe -m tests.check_mirror

## 这个文件守的是什么

镜像库是客户端**唯一**的本地状态：`run.py` 判断"这条要不要处理"完全靠它，
判错任何一个状态的后果都是静默的：

  - 把 `done` 当成"还要处理" → 每轮都重新抽一遍，白花模型的钱；
  - 把 `failed` 当成"处理好了" → 失败的消息永远不再试，**漏掉一条通知**；
  - 把"用户标了未读"当成普通的已读 → 用户点了"重新处理"，日志一切正常，
    实际上一次模型调用都没发生（**这就是最坏的那种错**，见第 11 节）。

所以这里逐条把五个状态、标记未读的边界、以及"先记账再干活"的顺序都钉住。
不联网、不碰后端，用临时目录里的库跑。
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from app.mirror import (
    STATE_DONE,
    STATE_FAILED,
    STATE_PENDING,
    STATE_REPROCESS,
    STATE_SKIPPED,
    Mirror,
)
from app.source.ntmsg import SourceMessage

SCRATCH = Path(__file__).resolve().parent.parent / ".tmp-test"
ROOT = Path(__file__).resolve().parent.parent

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


def msg(msg_id: str = "1", text: str = "下周三前交材料", ts: int = 1000, **kw) -> SourceMessage:
    return SourceMessage(
        msg_id=msg_id,
        timestamp=ts,
        group_id=kw.get("group_id", "g1"),
        sender_id=kw.get("sender_id", "10001"),
        sender_name=None,
        text=text,
        attachments=kw.get("attachments", []),
    )


def main() -> int:  # noqa: C901
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    db = SCRATCH / "mirror-test.db"
    mirror = Mirror(db)

    # ------------------------------------------------------------------
    print("--- 1. 第一次见 → 登记成 pending（先记账再干活）---")
    row = mirror.claim(msg("1"))
    check("状态是 pending（不是 done）", row.state, STATE_PENDING)
    check("attempts 从 0 开始", row.attempts, 0)
    check_true("群号/发送者/时间都记下来了",
               (row.group_id, row.sender_id, row.source_ts) == ("g1", "10001", 1000),
               str(row))

    print("\n--- 2. 没处理完就再来一次 → 仍然 pending（这就是崩溃恢复）---")
    again = mirror.claim(msg("1"))
    check("状态没被覆盖成别的", again.state, STATE_PENDING)
    check_true("而且确实被列进未完成队列", any(r.msg_id == "1" for r in mirror.unfinished()))

    print("\n--- 3. 标完成之后 → 不再出现在未完成队列里（下一轮零成本跳过）---")
    mirror.finish("1", state=STATE_DONE, raw_id="raw-1")
    mirror.claim(msg("1"))
    check("状态还是 done（claim 不覆盖已有状态）", mirror.get("1").state, STATE_DONE)
    check("raw_id 记下来了（重试/回溯要用它）", mirror.raw_id_of("1"), "raw-1")
    check_true("不在未完成队列里", not any(r.msg_id == "1" for r in mirror.unfinished()))

    # ------------------------------------------------------------------
    print("\n--- 4. 失败 → failed，而且会被重试 ---")
    mirror.claim(msg("2"))
    mirror.finish("2", state=STATE_FAILED, error="后端 502")
    got = mirror.get("2")
    check("状态是 failed", got.state, STATE_FAILED)
    check("attempts 加了 1", got.attempts, 1)
    check("错误留下了（不是静默失败）", got.last_error, "后端 502")
    check_true("未完成队列里有它", any(r.msg_id == "2" for r in mirror.unfinished()))

    print("\n--- 5. skipped 也是终态（白名单外/闲聊不该每轮重看）---")
    mirror.claim(msg("3"))
    mirror.finish("3", state=STATE_SKIPPED, error="whitelist:group 群 g1 不在名单里")
    check_true("skipped 不在未完成队列里", not any(r.msg_id == "3" for r in mirror.unfinished()))
    mirror.claim(msg("3"))
    check("状态保持 skipped", mirror.get("3").state, STATE_SKIPPED)

    print("\n--- 6. 水位线只算**处理完**的那些 ---")
    mirror.claim(msg("4", ts=5000))
    mirror.finish("4", state=STATE_DONE)
    # 5 号是一条更大的时间戳，但还没处理完
    mirror.claim(msg("5", ts=9000))
    check("水位线取已完成的 5000，而不是有 pending 的 9000", mirror.watermark(), 5000)
    mirror.finish("5", state=STATE_DONE)
    check("5 完成之后水位线跟到 9000", mirror.watermark(), 9000)

    # ------------------------------------------------------------------
    print("\n--- 7. 旧版本留下的「白名单跳过」记录会被清掉 ---")
    # 白名单现在下推成 SQL 了：白名单外的消息根本不进镜像。旧版本是"逐条扫、逐条
    # 记 skipped"，那些行只会让镜像看起来有一堆东西（真实例子里 9,400 条垃圾）。
    check("清掉 1 条", mirror.drop_whitelist_skips(), 1)
    check("它就不在镜像里了", mirror.get("3"), None)
    check("别再清第二次（幂等）", mirror.drop_whitelist_skips(), 0)
    check("别的原因跳过的**不能**动", mirror.get("2").state, STATE_FAILED)
    mirror.claim(msg("3"))          # 复原：后面还要用它统计
    mirror.finish("3", state=STATE_SKIPPED, error="闲聊")

    # ------------------------------------------------------------------
    print("\n--- 8. 统计与批量查询 ---")
    stats = mirror.stats()
    check("总数对得上", stats["total"], 5)
    check_true("按状态分类里有 done", stats["by_state"].get("done", 0) >= 3, str(stats["by_state"]))
    check_true("按状态分类里有 skipped", stats["by_state"].get("skipped", 0) >= 1, str(stats["by_state"]))
    many = mirror.get_many(["1", "2", "nope"])
    check("批量查只返回存在的", sorted(many), ["1", "2"])
    check("批量查空列表返回空", mirror.get_many([]), {})

    print("\n--- 9. 库文件真的落在指定路径（不是内存）---")
    check_true("文件存在", db.exists(), str(db))
    check_true("换个实例读到的状态一样（真的持久化了）", Mirror(db).get("1").state, STATE_DONE)

    # ------------------------------------------------------------------
    print("\n--- 10. 群里「上一条消息」的时间取自镜像（缺口检测用）---")
    # 以前这个是后端的共享 group_state 给的：两个客户端写同一张表会互相把
    # previous 顶掉，缺口告警就静默地漏。现在从自己的镜像里算。
    gap_dir = ROOT / ".tmp-test" / "mirror-gap"
    gap_dir.mkdir(parents=True, exist_ok=True)
    gap_db = gap_dir / "gap.db"
    if gap_db.exists():
        gap_db.unlink()
    gm = Mirror(gap_db)
    check("空镜像 → 没有上一条", gm.group_seen_ts("g1"), None)

    gm.claim(msg("g1-1", ts=1000, group_id="g1"))
    gm.finish("g1-1", state=STATE_DONE)
    gm.claim(msg("g1-2", ts=9000, group_id="g1"))
    gm.finish("g1-2", state=STATE_DONE)
    gm.claim(msg("g2-1", ts=50000, group_id="g2"))
    gm.finish("g2-1", state=STATE_DONE)
    check("取的是**这个群**里最晚的一条", gm.group_seen_ts("g1"), 9000)
    check("别的群不影响", gm.group_seen_ts("g2"), 50000)
    check("没见过的群 → None", gm.group_seen_ts("g3"), None)
    check(
        "正在处理的那条不算「上一条」（否则缺口永远是 0）",
        gm.group_seen_ts("g1", exclude_msg_id="g1-2"),
        1000,
    )
    check("只有它一条时排除掉就没上一条了", gm.group_seen_ts("g2", exclude_msg_id="g2-1"), None)

    # 白名单跳过的**不算见过**：用户说了不看那个来源，再为它的沉默告警是噪音
    gm.claim(msg("g1-3", ts=99000, group_id="g1"))
    gm.finish("g1-3", state=STATE_SKIPPED, error="whitelist:group 群 g1 不在名单里")
    check("白名单跳过的不算见过", gm.group_seen_ts("g1"), 9000)
    gm.claim(msg("g1-4", ts=88000, group_id="g1"))
    gm.finish("g1-4", state=STATE_SKIPPED, error="闲聊")
    check("因为别的原因跳过的算见过（群里确实有消息）", gm.group_seen_ts("g1"), 88000)

    # ------------------------------------------------------------------
    print("\n--- 11. 标为未读：已读 → reprocess（下一轮重新抽一遍）---")
    # 用户要的是"把当前消息标成未读，让它全部重新处理一次"。做成一删了之是不行的：
    # 删掉之后那些消息看起来和"镜像被删过/换机器了"一模一样，`run_cycle` 里那条
    # "后端已经有通知就跳过抽取"的保命捷径会把它们**静默跳过** —— 用户以为重跑了，
    # 其实一次模型调用都没发生。所以这里是一个**状态**，不是一个删除。
    un_dir = ROOT / ".tmp-test" / "mirror-unread"
    un_dir.mkdir(parents=True, exist_ok=True)
    un_db = un_dir / "unread.db"
    if un_db.exists():
        un_db.unlink()
    um = Mirror(un_db)
    um.claim(msg("u1", ts=1000))
    um.finish("u1", state=STATE_DONE, raw_id="raw-u1")
    um.claim(msg("u2", ts=2000))
    um.finish("u2", state=STATE_SKIPPED, error="闲聊")
    um.claim(msg("u3", ts=3000))
    um.finish("u3", state=STATE_FAILED, error="后端 502")
    um.claim(msg("u4", ts=4000))
    um.finish("u4", state=STATE_SKIPPED, error="whitelist:group 群 g1 不在名单里")

    res = um.mark_unread()
    check("本次标了 3 条（旧版本的白名单垃圾先被清掉，不算在内）", res["marked"], 3)
    check("清掉了 1 条白名单跳过记录", res["dropped"], 1)
    check("标完之后共 3 条等着重抽", res["total"], 3)
    check("done → reprocess", um.get("u1").state, STATE_REPROCESS)
    check("skipped（闲聊）→ reprocess：用户说重看就重看", um.get("u2").state, STATE_REPROCESS)
    check("failed → reprocess（它本来就要重试，现在连抽取也重来）", um.get("u3").state, STATE_REPROCESS)
    check("白名单那条是被**删掉**的，不是被标成未读", um.get("u4"), None)
    check("raw_id 留着（重抽时靠它认后端那条原文）", um.get("u1").raw_id, "raw-u1")
    check(
        "reprocess 进未完成队列（否则它永远轮不到）",
        {r.msg_id for r in um.unfinished()},
        {"u1", "u2", "u3"},
    )
    check("标记写下了原因（界面上要能看出这是谁要求的）", "标为未读" in (um.get("u1").last_error or ""), True)

    res2 = um.mark_unread()
    check("再标一次：没有新标上的（幂等，不会把计数越滚越大）", res2["marked"], 0)
    check("但「等着重抽」还是 3 —— 这个数才是用户关心的", res2["total"], 3)

    print("\n--- 12. 只标指定的那几条 / 空列表不等于「全部」---")
    um.finish("u1", state=STATE_DONE, raw_id="raw-u1")   # 模拟 u1 已经被重抽完了
    res3 = um.mark_unread(["u1", "根本没有这条"])
    check("只标点名的那条", res3["marked"], 1)
    check("不存在的 id 不会凭空建行（未读必须有源头那条消息）", um.get("根本没有这条"), None)
    check("没被点名的仍然是 reprocess（状态没被动过）", um.get("u2").state, STATE_REPROCESS)
    check("点名的那条回到 reprocess", um.get("u1").state, STATE_REPROCESS)

    before_empty = um.get("u2").state
    check("空列表 = 什么都不标（不是「全部」）", um.mark_unread([])["marked"], 0)
    check("空列表之后状态没变", um.get("u2").state, before_empty)

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
