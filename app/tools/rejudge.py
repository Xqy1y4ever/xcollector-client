"""重新判一遍**已经建出来的**通知，把按新判据不成立的归档掉。

    python -m app.tools.rejudge            # 干跑：只统计会归档哪些，不写任何东西
    python -m app.tools.rejudge --apply    # 真做：把判为非通知的 status 改成 archived

## 为什么需要它（"标为未读 + 重新处理"不够用）

"标为未读"只能重抽**镜像里认识**的消息。真实情况（2026-09-19）：某一个用户的板子上
319 条通知里，镜像是只认识其中 61 条 —— 另外 258 条是更早的配置/更早的镜像留下的，
重抽根本碰不到它们。而判据这几轮从"有具体的事要做"收紧到"该进任务板的官方通知"
（v3 → v4），那批历史误报就永远留在板子上了。

所以这个工具换一条路走：**不碰消息，直接按原文重判每一条已建的通知**。

## 它是怎么判的

1. 先过确定性的门：群务（改群名片/进群/查收名单…）、闲聊/回执 —— **直接判死，不调模型**；
2. 剩下的把原文丢给模型（用的是当前提示词，和客户端跑链路时**同一套**），只看
   `is_notification`；
3. 判死的那些**归档**（`corrections: status=archived`）—— 不删数据：原文、修正历史都在，
   前端切到「已归档」还能看到，想恢复也能改回来。

**默认干跑**：先把"会归档多少条、都是些什么"打出来给你看，加 `--apply` 才真的写。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field

from ..backend_client import BackendClient, BackendError
from ..config import LLM_TIMEOUT, Settings, get_settings
from ..llm import acompletion
from ..pipeline.extract import _extract_json, build_user_content, render_system_prompt
from ..pipeline.rule_extract import is_group_chore, is_noise
from ..utils import preview

logger = logging.getLogger("xcollector.rejudge")


@dataclass
class RejudgeReport:
    total: int = 0
    kept: int = 0
    dropped: int = 0
    archived: int = 0
    failed: list[str] = field(default_factory=list)
    by_reason: dict[str, int] = field(default_factory=dict)
    samples: list[tuple[str, str]] = field(default_factory=list)   # (原因, 标题)

    def note(self, reason: str, title: str) -> None:
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
        if len(self.samples) < 40:
            self.samples.append((reason, title))


def decide(content: str, verdict: dict | None) -> tuple[bool, str]:
    """要不要归档这条？返回 `(归档吗, 原因)`。

    `verdict=None` = 确定性判据已经判死（没调模型）。
    """
    chore = is_group_chore(content)
    if chore:
        return True, f"群务（{chore}）"
    if is_noise(content):
        return True, "闲聊/回执"
    if verdict is None:
        return False, "模型没给结论（保留）"
    if verdict.get("is_notification"):
        return False, "模型：是通知"
    return True, f"模型：{str(verdict.get('reason') or '非通知')[:16]}"


async def _judge(content: str, ts_ms: int, settings: Settings) -> dict | None:
    """把原文丢给模型，只问"是不是通知"。失败抛异常（由调用方记成 failed）。"""
    if not settings.llm_api_key:
        raise RuntimeError("没有配置 LLM_API_KEY（这个工具要用模型重判）")
    raw = {"ts": ts_ms, "content": content, "group_id": "rejudge", "group_name": None,
           "sender_id": "?", "sender_name": None, "message_id": "rejudge"}
    completion = await acompletion(
        api_base=settings.llm_api_base,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        messages=[
            {"role": "system", "content": render_system_prompt(ts_ms, settings.digest_tz)},
            {"role": "user", "content": build_user_content(raw, [])},
        ],
        temperature=0.0,
        timeout=LLM_TIMEOUT,
        json_mode=True,
    )
    return _extract_json(completion.text)


async def rejudge(
    backend: BackendClient,
    settings: Settings,
    *,
    apply: bool = False,
    limit: int = 2000,
    progress: int = 25,
) -> RejudgeReport:
    """把已建的通知重判一遍。`apply=False` 只统计。"""
    report = RejudgeReport()
    rows = await backend.list_notifications(limit=limit)
    report.total = len(rows)
    logger.info("拉到 %d 条通知，开始重判（%s）", report.total, "真归档" if apply else "干跑")

    for index, row in enumerate(rows, 1):
        notif_id = str(row.get("id") or "")
        title = str(row.get("title") or "")
        try:
            detail = await backend._json("GET", f"/api/notifications/{notif_id}")
            content = str(((detail.get("raw") or {}).get("content")) or "")
        except BackendError as exc:
            report.failed.append(f"{notif_id}: 取原文失败 {exc}")
            continue
        ts = int(row.get("source_ts") or 0) or 1

        drop, reason = decide(content, None)          # 确定性的门先过
        if not drop:
            try:
                verdict = await _judge(content, ts, settings)
            except Exception as exc:  # noqa: BLE001 - 单条失败不该中断整批
                report.failed.append(f"{notif_id}: {type(exc).__name__}: {exc}")
                continue
            drop, reason = decide(content, verdict)

        if drop:
            report.dropped += 1
            report.note(reason, title)
            if apply:
                try:
                    await backend.add_correction(notif_id, field="status", value="archived",
                                                 actor="rejudge")
                    report.archived += 1
                except BackendError as exc:
                    report.failed.append(f"{notif_id}: 归档失败 {exc}")
        else:
            report.kept += 1
            report.note(reason, title)

        if progress and index % progress == 0:
            logger.info("…%d/%d（会归档 %d，保留 %d）", index, report.total, report.dropped, report.kept)

    return report


def _print_report(report: RejudgeReport, *, apply: bool) -> None:
    print(f"\n共 {report.total} 条通知：")
    print(f"  按新判据**不成立**（{'已归档' if apply else '会归档'}）{report.dropped} 条")
    print(f"  保留                            {report.kept} 条")
    if report.failed:
        print(f"  失败                            {len(report.failed)} 条")
        for line in report.failed[:5]:
            print(f"      · {line}")
    print("\n归档原因分布：")
    for reason, n in sorted(report.by_reason.items(), key=lambda kv: -kv[1]):
        if reason.startswith("模型：是通知") or reason.startswith("模型没给结论"):
            continue
        print(f"  {n:5}  {reason}")
    print("\n被判非通知的例子（前 25 条）：")
    for reason, title in [s for s in report.samples if not s[0].startswith("模型：是通知")][:25]:
        print(f"  [{reason[:20]:20}] {preview(title, 40)}")
    if not apply:
        print("\n这是**干跑**：什么都没改。确认没问题就加 --apply 真归档。")


async def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m app.tools.rejudge",
                                     description="按当前判据重判已建的通知（默认干跑）")
    parser.add_argument("--apply", action="store_true", help="真的把判为非通知的归档")
    parser.add_argument("--limit", type=int, default=2000, help="最多看多少条（默认 2000）")
    args = parser.parse_args()

    settings = get_settings()
    backend = BackendClient(settings)
    try:
        who = await backend.whoami()
        user = who.get("user") or {}
        print(f"身份：{user.get('qq')}（{user.get('id')}）· 模型 {settings.llm_model}")
        report = await rejudge(backend, settings, apply=args.apply, limit=args.limit)
    finally:
        await backend.close()
    _print_report(report, apply=args.apply)
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
