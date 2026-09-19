"""`app.tools.rejudge` 自检：重判历史通知（默认干跑）时到底怎么判、怎么写。

    .\\venv\\Scripts\\python.exe -m tests.check_rejudge

不联网、不调模型：模型那一层用假函数替换掉，后端也是一个几十行的假对象。
要钉住的是三件事：

1. **确定性的门先过**：群务/闲聊直接判死，**不花**那次模型调用；
2. **默认干跑**：不写任何东西；`--apply` 才调 `corrections` 归档；
3. 归档是**归档**（status=archived），不是删除 —— 判据变了也不丢数据。
"""

from __future__ import annotations

import asyncio
import sys

from app.config import Settings
from app.tools import rejudge as rj
from tests._hermetic import isolate_settings

isolate_settings()

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


# ---------------------------------------------------------------------------
# 假后端：只需要 rejudge 用到的那四个方法
# ---------------------------------------------------------------------------


class FakeBackend:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.corrections: list[tuple[str, str, str]] = []
        self.detail_fail: set[str] = set()

    async def list_notifications(self, *, limit: int = 2000) -> list[dict]:
        return self.rows

    async def _json(self, method: str, url: str):  # noqa: ANN201
        notif_id = url.rsplit("/", 1)[-1]
        if notif_id in self.detail_fail:
            from app.backend_client import BackendError

            raise BackendError("取不到原文（假的）")
        row = next(r for r in self.rows if r["id"] == notif_id)
        return {"notification": row, "raw": {"content": row["content"]}}

    async def add_correction(self, notif_id: str, *, field: str, value, actor: str = "client"):  # noqa: ANN001
        self.corrections.append((notif_id, field, value))
        return {"ok": True}


def row(notif_id: str, title: str, content: str) -> dict:
    return {"id": notif_id, "title": title, "content": content, "source_ts": 1758000000000}


def main() -> int:  # noqa: C901
    print("--- 1. decide()：谁该归档 ---")
    check("群务 → 归档（不看模型）", rj.decide("欢迎大家入群，请大家按照要求修改群名片", None),
          (True, "群务（群名片）"))
    check("闲聊/回执 → 归档", rj.decide("收到", None), (True, "闲聊/回执"))
    check("模型说是通知 → 留", rj.decide("大家下周三前交材料", {"is_notification": True})[0], False)
    check("模型说不是 → 归档，并带上它的理由",
          rj.decide("我下周三可能去不了", {"is_notification": False, "reason": "个人安排"}),
          (True, "模型：个人安排"))
    check("模型没给结论（比如调用失败）→ 保守留下",
          rj.decide("大家下周三前交材料", None)[0], False)
    check("群务即使模型说是通知也要归档（代码说了算）",
          rj.decide("请修改群名片", {"is_notification": True})[0], True)

    print("\n--- 2. rejudge()：干跑什么都不写 ---")
    llm_calls: list[str] = []

    async def fake_judge(content: str, ts_ms: int, settings):  # noqa: ANN202
        llm_calls.append(content)
        return {"is_notification": True}

    async def fake_judge_no(content: str, ts_ms: int, settings):  # noqa: ANN202
        llm_calls.append(content)
        return {"is_notification": False, "reason": "闲聊"}

    original = rj._judge
    settings = Settings(client_extractor="llm", llm_model="fake", llm_api_key="k")
    backend = FakeBackend([
        row("n1", "修改群名片", "请大家按照要求修改群名片"),
        row("n2", "提交材料", "大家下周三前把材料交到学工办"),
        row("n3", "闲聊", "我下周三可能去不了"),
    ])
    try:
        rj._judge = fake_judge
        dry = asyncio.run(rj.rejudge(backend, settings, apply=False))
        check("干跑：只有群务那 1 条会归档（模型这次对另外两条都说「是」）", dry.dropped, 1)
        check("干跑：保留 2 条", dry.kept, 2)
        check("干跑：**一个写请求都没发**", backend.corrections, [])
        check("群务那条没花模型调用（只剩 2 条进了模型）", len(llm_calls), 2)
        check("群务那条确实没进模型", "请大家按照要求修改群名片" in llm_calls, False)

        print("\n--- 3. --apply：归档（不是删除） ---")
        llm_calls.clear()
        rj._judge = fake_judge_no          # 这次模型对 n2 也说"不是"
        applied = asyncio.run(rj.rejudge(backend, settings, apply=True))
        check("这次 3 条都会被归档", applied.dropped, 3)
        check("归档写了 3 次 corrections", len(backend.corrections), 3)
        check("写的是 status=archived", {c[1] for c in backend.corrections}, {"status"})
        check("值是 archived", {c[2] for c in backend.corrections}, {"archived"})
        check("归档条数对得上", applied.archived, 3)

        print("\n--- 4. 取不到原文的那些：记成失败，不静默跳过 ---")
        backend.detail_fail.add("n2")
        backend.corrections.clear()
        rj._judge = fake_judge
        partial = asyncio.run(rj.rejudge(backend, settings, apply=True))
        check("失败记下来了", len(partial.failed), 1)
        check_true("失败里带着 id", partial.failed[0].startswith("n2"), partial.failed[0])
        check("其余两条例照常处理", partial.archived + partial.kept, 2)
    finally:
        rj._judge = original

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
