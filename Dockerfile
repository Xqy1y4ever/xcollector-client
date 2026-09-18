# Xcollector 入库客户端
#
# 它读的是一个**挂进来的聊天记录库**。两种挂法：
#
# A. 只挂加密的 nt_msg.db，客户端自己解密 + 导出（推荐，配 CLIENT_NT_MSG_DB）：
#
#   docker run --rm --env-file .env \
#     -v /path/to/nt_db:/data/nt:rw \
#     xcollector-client --once
#
#   ⚠️ 这里必须是 **rw**：解密产物（nt_msg_plain.db / nt_msg_export.db）会写在
#      nt_msg.db 旁边。想只挂只读的话，就改用 B。
#
# B. 只挂已经导出的 nt_msg_export.db（配 CLIENT_DB_PATH），只读即可：
#
#   docker run --rm --env-file .env \
#     -v /path/to/nt_msg_export.db:/data/nt_msg_export.db:ro \
#     -v /path/to/attachments:/data/attachments:ro \
#     xcollector-client --once
#
# 密钥**只能**走运行时环境变量 / --env-file / CLIENT_NT_MSG_KEY_FILE（挂进来），
# 绝不要做成 build-arg（见下面的说明）。
#
# 或者用 xcollector-deploy 里的 compose（推荐，那边把路径都收在 .env 里）。
#
# 基础镜像选 3.13 而不是 3.12/3.14：解密要的 sqlcipher3 在 Linux 上只有
# `sqlcipher3-wheels` 这个预编译包，它目前有 cp312/cp313 的 manylinux wheel，
# 还没有 cp314 的。Windows 上则相反（官方 sqlcipher3 有 cp312~cp314 的 wheel），
# requirements.txt 里按平台写了这一条，所以本机开发不用改任何东西。
FROM python:3.13-slim

# 不要以 root 跑：它读的是别人的聊天记录，被攻破的后果不该是"整个宿主机"。
RUN useradd --create-home --uid 10001 client

WORKDIR /app

# ---------------------------------------------------------------------------
# 构建期默认值（可选）
#
# 这四项可以在这里定成镜像的默认值，方便把一个**预配好**的客户端发给别人：
#
#   docker build \
#     --build-arg BACKEND_BASE_URL=https://collector.example.com \
#     --build-arg CLIENT_GROUP_WHITELIST=123456789:官方通知群 \
#     --build-arg CLIENT_SENDER_WHITELIST=10001:张老师 \
#     --build-arg CLIENT_NT_MSG_DB=/data/nt/nt_msg.db \
#     -t xcollector-client .
#
# ⚠️ 说清代价：
#   1. 定进去的值**改不了，除非重新构建**。所以它们是"默认值"，
#      运行时用环境变量/--env-file 仍然可以覆盖（ENV 只是默认，docker run -e 优先）；
#   2. **绝不要**把 CLIENT_TOKEN / CLIENT_NT_MSG_KEY 做成 build-arg。构建参数会留在
#      镜像层里，任何拿到镜像的人都能 `docker history` 看到它 —— 那是这个用户的全部
#      凭据（令牌能写他的数据，密钥能解开他的聊天记录）。这两个请走运行时环境变量，
#      或把密钥放进一个挂载进来的文件用 CLIENT_NT_MSG_KEY_FILE 指过去。
# ---------------------------------------------------------------------------
ARG BACKEND_BASE_URL=http://127.0.0.1:8000
ARG CLIENT_GROUP_WHITELIST=
ARG CLIENT_SENDER_WHITELIST=
ARG CLIENT_NT_MSG_DB=

ENV BACKEND_BASE_URL=${BACKEND_BASE_URL} \
    CLIENT_GROUP_WHITELIST=${CLIENT_GROUP_WHITELIST} \
    CLIENT_SENDER_WHITELIST=${CLIENT_SENDER_WHITELIST} \
    CLIENT_NT_MSG_DB=${CLIENT_NT_MSG_DB}
# 没配的项在运行时用 `-e CLIENT_GROUP_WHITELIST=...` 覆盖即可。

# 先装依赖，利用层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# msgdb 是 nt_msg_db_util 的解析套件（逐字搬过来的，见 msgdb/VENDORED.md）。
# 它在仓库根目录而不是 app/ 下面，因为上游内部用的是绝对导入 `from msgdb.proto ...`。
COPY msgdb ./msgdb
COPY tests ./tests

USER client

# 默认一轮就退出 —— 这个客户端本来就是批处理的，
# 常驻交给 `--loop`，调度交给外部的计划任务/docker 的 restart policy。
ENTRYPOINT ["python", "-m", "app.main"]
CMD ["--once"]
