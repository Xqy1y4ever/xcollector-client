"""镜像库：状态机、内容指纹、补充关系。

    .\\venv\\Scripts\\python.exe -m tests.check_mirror

## 这个文件守的是什么

镜像库是客户端**唯一**的本地状态，`run.py` 的整个增量逻辑都建在它的四个返回值上
（`new` / `changed` / `retry` / `unchanged`）。这四个判断错任何一个，后果都是静默的：

  - 把 `changed` 判成 `unchanged` → 通知被补充/编辑之后**任务永远不更新**；
  - 把 `unchanged` 判成 `changed` → 每轮都重新抽一遍，白花模型的钱；
  - 把 `retry` 判成 `unchanged` → 失败的消息永远不再试，**漏掉一条通知**。

所以这里逐条把四种判定、指纹的稳定性、以及"先记账再干活"的顺序都钉住。
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
    STATE_SKIPPED,
    Mirror,
    content_hash,
)
from app.source.ntmsg import SourceMessage

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
    print("--- 1. 第一次见 → new，而且记成 pending（先记账再干活）---")
    row, what = mirror.claim(msg("1"))
    check("结果是 new", what, "new")
    check("状态是 pending（不是 done）", row.state, STATE_PENDING)
    check("attempts 从 0 开始", row.attempts, 0)
    check_true("记了内容指纹", bool(row.content_hash), row.content_hash)

    print("\n--- 2. 没处理完就再来一次 → retry（这就是崩溃恢复）---")
    _, what = mirror.claim(msg("1"))
    check("还没标完成 → retry", what, "retry")
    check_true("而且确实被列进未完成队列", any(r.msg_id == "1" for r in mirror.unfinished()))

    print("\n--- 3. 标完成之后、内容没变 → unchanged（零成本跳过）---")
    mirror.finish("1", state=STATE_DONE, raw_id="raw-1")
    _, what = mirror.claim(msg("1"))
    check("→ unchanged", what, "unchanged")
    check("raw_id 记下来了（补充要落回它）", mirror.raw_id_of("1"), "raw-1")

    print("\n--- 4. 内容变了 → changed（要重新处理，后端幂等会更新那条任务）---")
    _, what = mirror.claim(msg("1", text="下周三前交材料（改到周五）"))
    check("正文变了 → changed", what, "changed")
    check("状态回到 pending", mirror.get("1").state, STATE_PENDING)
    check_true("指纹也更新了", mirror.get("1").content_hash != row.content_hash)
    # 复原
    mirror.finish("1", state=STATE_DONE, raw_id="raw-1")

    print("\n--- 5. 指纹只跟**内容**有关，不跟标识有关 ---")
    base = content_hash(msg("1"))
    check("同一条消息算两次一样", content_hash(msg("1")), base)
    check_true("换 msg_id 不影响指纹（标识不是内容）", content_hash(msg("999")) == base, "两者应相同")
    check_true("换时间戳不影响指纹", content_hash(msg("1", ts=99999)) == base)
    check_true("改正文就变了", content_hash(msg("1", text="别的")) != base)
    check_true(
        "改附件就变了",
        content_hash(msg("1", attachments=[{"type": "image", "name": "a.png"}])) != base,
    )
    check_true(
        "附件顺序不影响指纹（否则同一批图每次算出来都不一样）",
        content_hash(msg("1", attachments=[
            {"type": "image", "name": "a.png"}, {"type": "file", "name": "b.pdf"}]
        ))
        == content_hash(msg("1", attachments=[
            {"type": "file", "name": "b.pdf"}, {"type": "image", "name": "a.png"}]
        )),
    )

    # ------------------------------------------------------------------
    print("\n--- 6. 失败 → failed，而且会被重试 ---")
    _, _ = mirror.claim(msg("2"))
    mirror.finish("2", state=STATE_FAILED, error="后端 502")
    got = mirror.get("2")
    check("状态是 failed", got.state, STATE_FAILED)
    check("attempts 加了 1", got.attempts, 1)
    check("错误留下了（不是静默失败）", got.last_error, "后端 502")
    check_true("未完成队列里有它", any(r.msg_id == "2" for r in mirror.unfinished()))
    _, what = mirror.claim(msg("2"))
    check("重试时结果是 retry", what, "retry")

    print("\n--- 7. skipped 也是终态（订阅之外/闲聊不该每轮重看）---")
    _, _ = mirror.claim(msg("3"))
    mirror.finish("3", state=STATE_SKIPPED, error="不在订阅范围内")
    check_true("skipped 不在未完成队列里", not any(r.msg_id == "3" for r in mirror.unfinished()))
    _, what = mirror.claim(msg("3"))
    check("再来一次是 unchanged", what, "unchanged")

    print("\n--- 8. 水位线只算**处理完**的那些 ---")
    mirror.claim(msg("4", ts=5000))
    mirror.finish("4", state=STATE_DONE)
    # 5 号是一条更大的时间戳，但还没处理完
    mirror.claim(msg("5", ts=9000))
    check("水位线取已完成的 5000，而不是有 pending 的 9000", mirror.watermark(), 5000)
    mirror.finish("5", state=STATE_DONE)
    check("5 完成之后水位线跟到 9000", mirror.watermark(), 9000)

    # ------------------------------------------------------------------
    print("\n--- 9. 补充关系 ---")
    mirror.mark_amendment("200", "100")
    check("记下来了", mirror.amendment_of("200"), "100")
    check("没记过的返回 None", mirror.amendment_of("201"), None)
    mirror.mark_amendment("200", "100")  # 重复记
    check("重复记不会炸，也不改变结果", mirror.amendment_of("200"), "100")
    check("统计里能看到补充条数", mirror.stats()["amendments"], 1)
    check("不是补充的消息也返回 None", mirror.amendment_of("1"), None)

    # ------------------------------------------------------------------
    print("\n--- 10. 统计与批量查询 ---")
    stats = mirror.stats()
    check("总数对得上", stats["total"], 5)
    check_true("按状态分类里有 done", stats["by_state"].get("done", 0) >= 3, str(stats["by_state"]))
    check_true("按状态分类里有 skipped", stats["by_state"].get("skipped", 0) >= 1, str(stats["by_state"]))
    many = mirror.get_many(["1", "2", "nope"])
    check("批量查只返回存在的", sorted(many), ["1", "2"])
    check("批量查空列表返回空", mirror.get_many([]), {})

    print("\n--- 11. 库文件真的落在指定路径（不是内存）---")
    check_true("文件存在", db.exists(), str(db))
    check_true("换个实例读到的状态一样（真的持久化了）", Mirror(db).get("1").state, STATE_DONE)

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
