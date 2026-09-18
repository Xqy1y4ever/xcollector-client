"""本地 Web UI 自检：`.env` 读写 + HTTP 接口。**不需要后端、不联网。**

    .\\venv\\Scripts\\python.exe -m tests.check_webui

## 这份文件守的是什么

Web UI 是**唯一会写用户 `.env` 的东西**，所以它的错法都很脏：

1. **把用户的东西冲掉**：`.env` 里有注释、有空行、有从旧版本留下来的键。写配置
   只能改我们认的那几行，其余必须原样保留（想清理也走"注释掉"，不是删）。
2. **把密钥回显出去**：`CLIENT_TOKEN` / `CLIENT_NT_MSG_KEY` 永远不能在响应里出现；
   页面上留空提交 = 不改动（而不是"清空"）。
3. **被别的网站调**：写接口要求 `Content-Type: application/json` + `X-XC-UI` 头，
   跨站请求发不出这种请求。这里专门验一条"不带头的 POST 必须 403"。
4. **"标为未读"不许静默**（第 8 节）：它得真的把镜像改成 `reprocess`、界面上看得见
   条数、正在跑的时候拒绝（一边跑一边标只标了一半）、而且**不**顺手花掉模型的钱
   （`run` 必须显式给）。

HTTP 层是真起一个服务（`ThreadingHTTPServer` 绑 127.0.0.1:0 拿随机端口），
因为"接口能不能通"只有真发一次请求才算数。

> ⚠️ **这一套在 Windows 全绿、在 Linux(CI) 上挂了**，原因值得记下来：被拒的 POST
> 如果**不把请求体读掉**，keep-alive 连接上剩下的字节会被当成下一个请求的起始行，
> 下一个请求就变成 **501**。所以这里有一组"被拒之后紧接着再发一个请求"的断言 ——
> 它们不是凑数，是这个 bug 唯一能被抓到的地方。
> 另外踩过一次"只比对自己那个读取器"：`.env` 的消费者其实是 pydantic/dotenv，
> 值里带 `#` 时它会当注释丢掉（两边一起错就永远看不出来），所以第 3 节用**真的
> Settings** 再读一遍。
"""

from __future__ import annotations

import shutil
import sys
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

import httpx

from app.config import Settings, get_settings
from app.mirror import STATE_DONE, STATE_REPROCESS, Mirror
from app.source.ntmsg import SourceMessage
from app.webui import envfile
from app.webui import server as ui_server
from app.webui.server import Handler
from tests._hermetic import isolate_settings

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


def main() -> int:  # noqa: C901
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    # 隔离：不读本机 `.env`，也不受进程环境变量影响；并且把"要编辑的那个文件"
    # 指到临时目录（**绝不能**让测试碰到仓库里那份真 .env）。
    isolate_settings()
    env_path = SCRATCH / "ui.env"
    Settings.model_config["env_file"] = str(env_path)

    # ------------------------------------------------------------------
    print("--- 1. .env 读写：只改配置项，其余原样保留 ---")
    env_path.write_text(
        "# 用户自己写的注释\n"
        "CLIENT_TOKEN=keep-me\n"
        "\n"
        "CLIENT_BATCH_SIZE=200\n"        # 已经不是配置项了
        "BACKEND_BASE_URL=http://old:8000\n",
        encoding="utf-8",
    )
    values = envfile.read_env()
    check("读到两个配置项，没读那个非配置项",
          sorted(values), ["BACKEND_BASE_URL", "CLIENT_TOKEN"])
    check("值是干净的", values["CLIENT_TOKEN"], "keep-me")

    saved = envfile.write_env_values({
        "BACKEND_BASE_URL": "http://new:8000",
        "CLIENT_EXTRACTOR": "both",
        "CLIENT_TOKEN": None,            # None = 不改动
    })
    check("写进去的键", sorted(saved), ["BACKEND_BASE_URL", "CLIENT_EXTRACTOR"])
    text = env_path.read_text(encoding="utf-8")
    check_true("注释还在", "# 用户自己写的注释" in text, text)
    check_true("非配置项那行没被碰", "CLIENT_BATCH_SIZE=200" in text, text)
    after = envfile.read_env()
    check("改的值生效了", after["BACKEND_BASE_URL"], "http://new:8000")
    check("None 表示不改动", after["CLIENT_TOKEN"], "keep-me")
    check("追加的键也读得回来", after["CLIENT_EXTRACTOR"], "both")

    print("\n--- 2. 不认识的键：注释掉而不是删掉 ---")
    touched = envfile.comment_out_keys(["CLIENT_BATCH_SIZE"])
    check("动了一行", touched, ["CLIENT_BATCH_SIZE"])
    text = env_path.read_text(encoding="utf-8")
    check_true("那一行被注释了（还能看回来）", "# CLIENT_BATCH_SIZE=200" in text, text)
    check_true("说明写清了为什么", "不是配置项" in text, text)
    check("再注释一次是 0 行（幂等）", envfile.comment_out_keys(["CLIENT_BATCH_SIZE"]), [])

    print("\n--- 3. 拒绝不是配置项的键 / 值里的特殊字符 ---")
    try:
        envfile.write_env_values({"NOT_A_FIELD": "x"})
        check_true("写非配置项 → 报错", False, "居然写进去了")
    except ValueError as exc:
        check_true("写非配置项 → 报错", "不是配置项" in str(exc), str(exc))

    # 带 `#` / 空格的值：dotenv 会把 ` #…` 当注释丢掉，所以写的时候必须加引号。
    # 这里用**真的 Settings** 读一遍，证明"我们写出去的、和 pydantic 读到的"一致 ——
    # 只比对自己那个读取器是不够的（两边一起错就永远看不出来）。
    awkward = "123456789:通知群 #1"
    envfile.write_env_values({"CLIENT_GROUP_WHITELIST": awkward})
    check("自己读回来是对的", envfile.read_env()["CLIENT_GROUP_WHITELIST"], awkward)
    from app.config import Settings as _S

    via_pydantic = _S(_env_file=str(env_path))
    check("pydantic 读到的也是同一个值", via_pydantic.client_group_whitelist, awkward)
    check("而且能解析成白名单", sorted(via_pydantic.group_whitelist_map), ["123456789"])
    check("白名单备注里的 # 没丢", via_pydantic.group_whitelist_map["123456789"], "通知群 #1")

    envfile.write_env_values({"CLIENT_ATTACHMENT_ROOT": 'C:\\a "b"\\c'})
    check("带引号的路径也能读回来",
          envfile.read_env()["CLIENT_ATTACHMENT_ROOT"], 'C:\\a "b"\\c')

    print("\n--- 4. 密钥不回显 ---")
    envfile.write_env_values({"CLIENT_TOKEN": "xc_supersecret", "CLIENT_NT_MSG_KEY": "0123456789abcdef"})
    shown = envfile.config_values()
    check("CLIENT_TOKEN 抹空", shown["CLIENT_TOKEN"], "")
    check("CLIENT_NT_MSG_KEY 抹空", shown["CLIENT_NT_MSG_KEY"], "")
    check_true("文件里还在（只是不给你们看）",
               "xc_supersecret" in env_path.read_text(encoding="utf-8"), "...")
    check("不抹的时候读得到原值", envfile.config_values(redact=False)["CLIENT_TOKEN"], "xc_supersecret")

    # ------------------------------------------------------------------
    print("\n--- 5. HTTP：真起一个服务（127.0.0.1 随机端口）---")
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    ui_headers = {"X-XC-UI": "1", "Content-Type": "application/json"}
    try:
        # trust_env=False：**别让测试走这台机器的系统代理**（Windows 注册表里那条 Clash/V2Ray
        # 代理没开着的时候，连 127.0.0.1 都会被发过去然后连接被拒）。
        with httpx.Client(timeout=20, trust_env=False) as c:
            r = c.get(f"{base}/")
            check("页面拿得到", r.status_code, 200)
            check_true("是 HTML", "text/html" in r.headers.get("content-type", ""), r.headers.get("content-type"))
            check_true("页面里有配置表单的定义", "CLIENT_TOKEN" in r.text, r.text[:120])

            r = c.get(f"{base}/api/config")
            check("配置接口 200", r.status_code, 200)
            body = r.json()
            check("密钥在里面是空的", body["values"]["CLIENT_TOKEN"], "")
            check_true("列出了哪些键是密钥", "CLIENT_TOKEN" in body["secrets"], str(body["secrets"]))
            check_true("给的是 .env 的路径", body["path"].endswith("ui.env"), body["path"])
            check_true("不是配置项的键不再被列出来（已经注释掉了）",
                       "CLIENT_BATCH_SIZE" not in body["unknown_keys"], str(body["unknown_keys"]))

            r = c.get(f"{base}/api/state")
            check("状态接口 200（后端不可达也要能打开页面）", r.status_code, 200)
            state = r.json()
            check_true("说了后端可达性", "reachable" in state["backend"], str(state["backend"]))
            check_true("源库不存在时如实报错", state["source"].get("ok") is False, str(state["source"]))
            check_true("运行状态是空闲", state["run"]["busy"] is False, str(state["run"]))

            r = c.get(f"{base}/api/log")
            check("日志接口 200", r.status_code, 200)
            check_true("返回的是列表", isinstance(r.json()["lines"], list), str(r.json())[:80])

            print("\n--- 6. 写接口：必须带 X-XC-UI（跨站请求发不出来）---")
            r = c.post(f"{base}/api/config", json={"values": {"CLIENT_EXTRACTOR": "rule"}})
            check("不带头的 POST → 403", r.status_code, 403)
            # 关键：**被拒的那次请求也把请求体读干净了**。否则 leftover 字节会被
            # 当成下一个请求的起始行，于是下一个请求变成 501（Linux/CI 上必现）。
            r = c.get(f"{base}/api/config")
            check("紧接着的请求仍然正常（keep-alive 没被搞乱）", r.status_code, 200)
            r = c.post(f"{base}/api/config", headers={"Content-Type": "application/json"},
                       content='{"values":{}}')
            check("只有 Content-Type 没有头 → 403", r.status_code, 403)
            r = c.get(f"{base}/api/config")
            check("再一次也正常", r.status_code, 200)

            r = c.post(f"{base}/api/config", headers=ui_headers,
                       json={"values": {"CLIENT_EXTRACTOR": "llm", "CLIENT_TOKEN": ""}})
            check("带头就能写 → 200", r.status_code, 200)
            check("返回值里说明了写了哪些键", r.json()["saved"], ["CLIENT_EXTRACTOR"])
            check("CLIENT_EXTRACTOR 真写进去了", envfile.read_env()["CLIENT_EXTRACTOR"], "llm")
            check("密钥留空 = 不改动（没被清掉）", envfile.read_env()["CLIENT_TOKEN"], "xc_supersecret")

            r = c.post(f"{base}/api/config", headers=ui_headers,
                       json={"values": {"NOT_A_FIELD": "1"}})
            check("写非配置项 → 400", r.status_code, 400)
            check_true("错误里说清了原因", "不是配置项" in r.text, r.text[:160])

            r = c.get(f"{base}/api/nope")
            check("没有的接口 → 404", r.status_code, 404)
            r = c.post(f"{base}/do/evil", headers=ui_headers, json={})
            check("不存在的写接口 → 404", r.status_code, 404)

            print("\n--- 7. 跑一轮：源库不存在时要如实失败，而不是装作跑过 ---")
            r = c.post(f"{base}/api/run", headers=ui_headers, json={"mode": "once"})
            check("启动返回 200", r.status_code, 200)
            check("说已开始", r.json()["started"], True)
            # 等它跑完（这一轮会立刻因为"源库不存在"失败）
            deadline = time.time() + 25
            last = {}
            while time.time() < deadline:
                last = c.get(f"{base}/api/state").json()["run"]
                # 要同时满足"没在跑"和"留下了原因"：`cycles` 是在记完原因之后才加的，
                # 只看 last_error 有可能在加之前就跳出循环（一条时序竞态）。
                if last.get("last_error") and not last.get("busy"):
                    break
                time.sleep(0.3)
            check("最后回到空闲", last.get("busy"), False)
            check_true("留下了失败原因（源库还没配）", "源库" in str(last.get("last_error")),
                       str(last.get("last_error")))
            check("至少完整跑过一轮", last.get("cycles"), 1)
            r = c.post(f"{base}/api/run", headers=ui_headers, json={"mode": "nonsense"})
            check("mode 写错 → 400", r.status_code, 400)
            r = c.post(f"{base}/api/stop", headers=ui_headers, json={})
            check("停止接口也能调（没在跑时说明白）", r.json()["message"], "现在没在跑")

            print("\n--- 8. 标为未读：把镜像里的记录改回「未读」，下一轮重抽一遍 ---")
            # 镜像路径也得指到临时目录：不指的话 `resolved_mirror_path` 给的是
            # `<仓库>/mirror.db` —— 这个测试就会去动仓库里那个真文件。
            envfile.write_env_values({"CLIENT_MIRROR_PATH": str(SCRATCH / "ui-mirror.db")})
            get_settings.cache_clear()
            store = Mirror(SCRATCH / "ui-mirror.db")
            for i in ("1", "2"):
                store.claim(SourceMessage(
                    msg_id=i, timestamp=1000 + int(i), group_id="g1", sender_id="10001",
                    sender_name=None, text="下周三前交材料",
                ))
                store.finish(i, state=STATE_DONE)
            check("镜像里先有 2 条 done", store.stats()["by_state"].get("done"), 2)

            r = c.post(f"{base}/api/mark-unread", json={})
            check("不带 X-XC-UI 头 → 403（它是个写接口）", r.status_code, 403)
            r = c.get(f"{base}/api/config")
            check("被拒之后 keep-alive 仍然正常", r.status_code, 200)

            r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={})
            check("标为未读 → 200", r.status_code, 200)
            body = r.json()
            check("本次标了 2 条", body["marked"], 2)
            check("标完之后共 2 条等着重抽", body["total"], 2)
            check("默认**不**顺手跑一轮（跑是要花钱的，得显式要求）", body.get("run_started"), None)
            check("镜像里变成 reprocess", store.get("1").state, STATE_REPROCESS)
            check("状态接口里看得见这个数（不然用户以为没生效）",
                  c.get(f"{base}/api/state").json()["mirror"]["reprocess"], 2)

            r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={"msg_ids": []})
            check("空数组 = 什么都不标（不是「全部」）", r.json()["marked"], 0)
            r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={"msg_ids": "1"})
            check("msg_ids 不是数组 → 400", r.status_code, 400)
            r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={"msg_ids": ["1"]})
            check("点名标一条 → 200", r.status_code, 200)
            check("但它已经在队列里了，所以没有新标的", r.json()["marked"], 0)
            check("总数还是 2", r.json()["total"], 2)

            # 正在跑的时候必须拒绝：一边跑一边标，正在处理的那几条会被这一轮的
            # 结果（done/skipped）盖掉 —— 用户以为标上了，其实只标了一半。
            ui_server.RUNNER._set(mode="once")
            try:
                r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={})
                check("正在跑 → 409", r.status_code, 409)
                check_true("说清了为什么", "跑" in r.text, r.text[:140])
            finally:
                ui_server.RUNNER._set(mode="idle", stop_requested=False)
            r = c.get(f"{base}/api/config")
            check("被拒之后 keep-alive 还是好的", r.status_code, 200)

            # 页面按钮走的就是这条路：标记 + 立刻跑一轮
            r = c.post(f"{base}/api/mark-unread", headers=ui_headers, json={"run": True})
            check("标记并跑一轮 → 200", r.status_code, 200)
            check("说已开始跑", r.json().get("run_started"), True)
            deadline = time.time() + 25
            last = {}
            while time.time() < deadline:
                last = c.get(f"{base}/api/state").json()["run"]
                if last.get("last_error") and not last.get("busy"):
                    break
                time.sleep(0.3)
            check("跑完回到空闲", last.get("busy"), False)
            check_true("那一轮留下了失败原因（这个测试没配源库）",
                       "源库" in str(last.get("last_error")), str(last.get("last_error")))
    finally:
        httpd.shutdown()
        httpd.server_close()

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
