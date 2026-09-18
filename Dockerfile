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
