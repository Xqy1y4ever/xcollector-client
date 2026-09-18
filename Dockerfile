# Xcollector 入库客户端
#
# 它读的是一个**挂进来的聊天记录库**（nt_msg_export.db），所以容器必须能看见
# 那个文件。源库与附件目录都从宿主机挂进来，**只读**：
#
#   docker build -t xcollector-client .
#   docker run --rm \
#     --env-file .env \
#     -v /path/to/nt_msg_export.db:/data/nt_msg_export.db:ro \
#     -v /path/to/attachments:/data/attachments:ro \
#     xcollector-client --once
#
# 或者用 xcollector-deploy 里的 compose（推荐，那边把路径都收在 .env 里）。
FROM python:3.12-slim

# 不要以 root 跑：它读的是别人的聊天记录，被攻破的后果不该是"整个宿主机"。
RUN useradd --create-home --uid 10001 client

WORKDIR /app

# ---------------------------------------------------------------------------
# 构建期默认值（可选）
#
# 这三项可以在这里定成镜像的默认值，方便把一个**预配好**的客户端发给别人：
#
#   docker build \
#     --build-arg BACKEND_BASE_URL=https://collector.example.com \
#     --build-arg CLIENT_GROUP_WHITELIST=123456789:官方通知群 \
#     --build-arg CLIENT_SENDER_WHITELIST=10001:张老师 \
#     -t xcollector-client .
#
# ⚠️ 说清代价：
#   1. 定进去的值**改不了，除非重新构建**。所以它们是"默认值"，
#      运行时用环境变量/--env-file 仍然可以覆盖（ENV 只是默认，docker run -e 优先）；
#   2. **绝不要**把 CLIENT_TOKEN 做成 build-arg。构建参数会留在镜像层里，
#      任何拿到镜像的人都能 `docker history` 看到它 —— 那是这个用户的全部凭据。
#      令牌请走运行时环境变量。
# ---------------------------------------------------------------------------
ARG BACKEND_BASE_URL=http://127.0.0.1:8000
ARG CLIENT_GROUP_WHITELIST=
ARG CLIENT_SENDER_WHITELIST=

ENV BACKEND_BASE_URL=${BACKEND_BASE_URL} \
    CLIENT_GROUP_WHITELIST=${CLIENT_GROUP_WHITELIST} \
    CLIENT_SENDER_WHITELIST=${CLIENT_SENDER_WHITELIST}
# 白名单里没配的项在运行时用 `-e CLIENT_GROUP_WHITELIST=...` 覆盖即可。

# 先装依赖，利用层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests

USER client

# 默认一轮就退出 —— 这个客户端本来就是批处理的，
# 常驻交给 `--loop`，调度交给外部的计划任务/docker 的 restart policy。
ENTRYPOINT ["python", "-m", "app.main"]
CMD ["--once"]
