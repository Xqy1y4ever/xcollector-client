"""读结构化导出库 `nt_msg_export.db`（上游 `nt_msg_db_util` 的产物，或本客户端自己导的）。

## 表结构从哪来的

不是猜的，是从 `nt_msg_db_util` 的 README 与 `db_docs/` 抄下来的（v1 快照）：

    group_messages
      msg_id        TEXT  主键          ← 源字段 40001，消息唯一 ID
      timestamp     INTEGER            ← 40050，**Unix 秒**
      direction     INTEGER            ← 40013
      sender_uid    TEXT               ← 40020
      sender_qq     TEXT               ← 40033
      group_id      TEXT               ← 40021，十进制群号字符串
      group_qq      TEXT
      msg_type / subtype / content_type
      text          TEXT               合并后的纯文本（FTS 来源）
      parse_status  TEXT               null / typed / wire_fallback / invalid
      content       TEXT               40800 解析出来的 JSON，带 type 鉴别字段

`content` 的形状由导出方决定，本读取器两种都认：

* **上游 `nt_msg_db_util` 的 `3.export.py`**（也就是本客户端自己导出的形状）：

      {"type":"msg_body","segments":[
          {"content_type":1,"text":"【教务处】下周三前交材料"},
          {"content_type":2,"img_width":1080,"filesize":204800,
           "md5_raw":"3q2+7w==","cdn_url_1":"http://..."}]}

  段里的字段是 **protobuf 原生名**，二进制字段是 **base64** —— 所以附件提取
  （`attachments_from_content`）要按 `content_type` 判类型、把 `md5_raw` 解成十六进制。

* 带 `type` 鉴别字段的旧形状（`{"type":"image","filename":...,"md5_hex":...}`）。

`parse_status` 的取值（上游 `msgdb/proto/c2c_40800_parser.py`）：
`null` / `typed` / `wire_fallback` / `invalid`。

回复/引用相关的字段（`47401` / `47402` / `reply_msg_seq` 之类）**本读取器不再解析**：
以前用它们识别"这条新消息在补充哪条通知"，那条功能已经删掉了（见 README 的
"它不做什么"）。段里遇到这些字段就原样留在 `raw_content` 里，不解释、不猜。

## 这个读取器刻意做的几件事

1. **只读打开**。导出库可能正被另一个进程写（导出工具在跑），所以用
   `mode=ro` + `immutable=0` 打开，并在读失败时给出能看懂的错，而不是抛一个
   光秃秃的 `sqlite3.OperationalError: database is locked`。
2. **列名不写死单一版本**。先用 `PRAGMA table_info` 读实际列名，缺列时按"这一列
   没有"处理并记一行 warning —— 上游换版本加/改列时，客户端应该退化而不是崩。
   唯一真正必需的是 `msg_id` 和 `timestamp`。
3. **"读没读过"由镜像决定，不由时间决定**。扫描就是一条反连接："镜像里没有的
   都要读"（`iter_unread` / `count_unread`，靠 `ATTACH` 镜像库）。**没有时间窗口**
   —— 窗口表达不了"这条处理过没有"，比窗口更老又没读过的消息会被永远跳过，
   而"该看到的没看到"正是这套系统最怕的失败。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from ..utils import quote_sql, sqlite_uri

logger = logging.getLogger(__name__)

# 真正必需的列。少了它们，这个库就不是我们认识的形状，直接报错比猜好。
REQUIRED_COLUMNS = ("msg_id", "timestamp")

# 其余列缺了就降级使用。
OPTIONAL_COLUMNS = (
    "sender_qq",
    "sender_uid",
    "group_id",
    "group_qq",
    "content_type",
    "text",
    "content",
    "parse_status",
)


class SourceDatabaseError(RuntimeError):
    """源库读不了 / 不是我们认识的形状。消息里会写清"该怎么办"。"""


@dataclass
class SourceMessage:
    """一条群消息（已经规整成入库用的形状）。"""

    msg_id: str
    timestamp: int  # Unix **秒**
    group_id: str
    sender_id: str
    sender_name: str | None
    text: str
    attachments: list[dict] = field(default_factory=list)
    parse_status: str | None = None
    raw_content: dict | None = None

    @property
    def ts_ms(self) -> int:
        return int(self.timestamp) * 1000


# 段级内容类型（`45002`）→ 附件类型。数字含义来自上游的 `db_docs/*/40800.md`。
SEGMENT_MEDIA_TYPES: dict[int, str] = {
    2: "image",
    3: "file",
    4: "file",      # 语音：当文件处理（客户端只需要一个能下载/命名的东西）
    5: "video",
    11: "sticker",  # 表情
}

# protobuf-JSON 里二进制字段是 base64，所以 `md5_raw` 得先解码再转十六进制。
_MD5_KEYS = ("md5_hex", "md5_raw", "md5")
_URL_KEYS = ("cdn_url", "cdn_url_1", "cdn_url_2", "cdn_url_3", "url")
_NAME_KEYS = ("filename", "name")
_SIZE_KEYS = ("filesize", "size")
_WIDTH_KEYS = ("width", "img_width")
_HEIGHT_KEYS = ("height", "img_height")
_EXT_KEYS = ("ext", "file_ext")
_PATH_KEYS = ("local_path", "cdn_path")

# 往下找"段"的容器键。**刻意不含 `ref_msg`**：那是被引用消息的完整内容，
# 把它里面的图片算成本条消息的附件是错的。
_SEGMENT_CONTAINERS = ("segments", "content", "items", "children", "mixed")


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return {str(r[1]) for r in rows}


def _json_or_none(text: Any) -> dict | None:
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _decode_md5(value: Any) -> str | None:
    """把 md5 字段统一成十六进制字符串。

    上游导出的 `md5_raw` 是 **bytes**，经 protobuf 的 JSON 转换后是 **base64**
    （`MessageToDict` 的默认行为），所以这里要解 base64 再转 hex。
    也兼容直接就是十六进制字符串的情况（别的导出实现会这么写）。
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    # 已经是十六进制就直接用（奇数长度不可能是十六进制）
    if len(text) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in text):
        return text.lower()
    try:
        import base64

        raw = base64.b64decode(text, validate=True)
    except Exception:  # noqa: BLE001 - 不是 base64 就当它没用
        return None
    return raw.hex() if raw else None


def _pick(node: dict, keys: Sequence[str]) -> Any:
    for key in keys:
        value = node.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _as_int(value: Any) -> int | None:
    """把值转成 int。

    需要这个是因为 **protobuf 的 JSON 把 int64 渲染成字符串**（int32 才是数字）：
    在真实数据里 `"filesize": "354549"`、`"reply_msg_seq": "1070244"` 都是字符串，
    而 `"img_width": 648` 是数字。只看 `isinstance(value, int)` 会把大小丢掉。
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text and (text.lstrip("-").isdigit()):
            try:
                return int(text)
            except ValueError:
                return None
    return None


def _segment_attachment(node: dict) -> dict | None:
    """把一个"段"（`content` 里的一个 segment）翻成附件记录；不是媒体就返回 None。

    支持两种形状：

    * **上游 `3.export.py` 的形状**（现在是主要的）：段是 protobuf 原生 JSON，
      媒体类型看 `content_type`，字段是 `md5_raw`（base64）/`cdn_url_1`/`filesize`…
    * 带 `type` 鉴别字段的旧形状（别的导出实现或早期版本）：`{"type":"image",...}`。
    """
    kind = ""
    explicit = str(node.get("type") or "")
    if explicit in ("image", "video", "file", "sticker"):
        kind = explicit
    else:
        content_type = node.get("content_type")
        if isinstance(content_type, int):
            kind = SEGMENT_MEDIA_TYPES.get(content_type, "")
    if not kind:
        return None

    item: dict[str, Any] = {"type": "image" if kind == "sticker" else kind}

    md5 = None
    for key in _MD5_KEYS:
        md5 = _decode_md5(node.get(key))
        if md5:
            break
    if md5:
        item["md5"] = md5

    url = _pick(node, _URL_KEYS)
    if isinstance(url, str) and url:
        item["url"] = url

    name = _pick(node, _NAME_KEYS)
    if isinstance(name, str) and name:
        item["name"] = name
    else:
        # 没有文件名字段时从本地路径里取（只在真有条路径时；**不编名字**）
        path = _pick(node, _PATH_KEYS)
        if isinstance(path, str) and path and not path.startswith("http"):
            item["name"] = path.replace("\\", "/").rsplit("/", 1)[-1]

    size = _as_int(_pick(node, _SIZE_KEYS))
    if size is not None and size >= 0:
        item["size"] = size
    for src_keys, dst in ((_WIDTH_KEYS, "width"), (_HEIGHT_KEYS, "height")):
        value = _as_int(_pick(node, src_keys))
        if value is not None and value > 0:
            item[dst] = value
    ext = _pick(node, _EXT_KEYS)
    if isinstance(ext, str) and ext:
        item["ext"] = ext if ext.startswith(".") else f".{ext}"
    if kind == "sticker":
        item["sticker_id"] = 0

    # 一条既没有 url 也没有文件名也没有 md5 的附件是没用的：记下来只会让前端
    # 显示一个打不开的空条目。
    if item.get("url") or item.get("name") or item.get("md5"):
        return item
    return None


def attachments_from_content(content: dict | None) -> list[dict]:
    """从 `content` JSON 里挑出附件。

    上游导出的形状（`nt_msg_db_util` 的 `msgdb/group/exporter.py`）：

        {"type":"msg_body","segments":[
            {"content_type":2,"img_width":1080,"filesize":204800,
             "md5_raw":"3q2+7w==","cdn_url_1":"http://..."}, ...]}

    也兼容带 `type` 鉴别字段的旧形状（`{"type":"image","filename":...}`）。

    **不猜附件地址**：只有 `cdn_url` 时也照样记下来（那可能是死链，所以下载失败
    要有一条明确的日志与降级说明，不能安静地当没有）。**不递归进 `ref_msg`** ——
    那是被引用消息的内容，把它里面的图算成本条消息的附件是错的。
    """
    if not isinstance(content, dict):
        return []

    out: list[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        found = _segment_attachment(node)
        if found is not None:
            out.append(found)
            return
        for key in _SEGMENT_CONTAINERS:
            if key in node:
                walk(node[key])

    walk(content)
    return out


class SourceDatabase:
    """`nt_msg_export.db` 的只读读取器。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._columns: set[str] = set()
        self._table = "group_messages"

    # ---------------- 打开与自检 ----------------

    def _connect(self) -> sqlite3.Connection:
        if not self.path.name or self.path.is_dir():
            # 一个都没配时 `Path("")` 就是当前目录：说清楚该怎么配，
            # 而不是让 sqlite 抛一句 "unable to open database file"。
            raise SourceDatabaseError(
                f"源库不是一个文件：{self.path}。请二选一：\n"
                "  · CLIENT_NT_MSG_DB = 加密的 nt_msg.db 路径（+ CLIENT_NT_MSG_KEY），"
                "客户端自己剥头、解密、导出；\n"
                "  · CLIENT_DB_PATH  = 现成的 nt_msg_export.db 路径。"
            )
        if not self.path.exists():
            raise SourceDatabaseError(
                f"源库不存在：{self.path}。请先用 nt_msg_db_util 的 3.export.py "
                f"导出 nt_msg_export.db，再把 CLIENT_DB_PATH 指向它。"
            )
        try:
            # mode=ro：只读打开，绝不因为手滑改动用户的聊天记录库。
            # immutable=0（默认）：允许它在被别的进程追加时仍然可读。
            uri = f"file:{self.path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        except sqlite3.Error as exc:
            raise SourceDatabaseError(f"打不开源库 {self.path}：{exc}") from exc
        conn.row_factory = sqlite3.Row
        return conn

    # ⚠️ 调用方必须用 `contextlib.closing(self._connect())` 包住连接。
    #
    # 为什么专门写这一句：`sqlite3.Connection` 的 `with` 是**事务**上下文
    # （退出时 commit/rollback），**不是**关闭。写成 `with self._connect() as c`
    # 连接会一直留到被 GC 回收 —— 在 Windows 上表现为"源库文件被占用、
    # 导出工具写不进去"，在长时间 --loop 里表现为文件句柄越积越多。
    # 这个坑是 e2e 测试（重写 fixture 库时 unlink 失败）抓出来的。

    def inspect(self) -> dict:
        """自检：这个库能不能读、有没有我们要的表和列。

        启动时调用一次，把结果打出来 —— 上游换版本时，这行日志就是唯一的线索。
        """
        try:
            with closing(self._connect()) as conn:
                tables = {
                    str(r[0])
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
                    ).fetchall()
                }
                if self._table not in tables:
                    raise SourceDatabaseError(
                        f"源库里没有 {self._table} 表（有：{sorted(tables)[:10]}）。"
                        "这个库看起来不是 nt_msg_db_util 导出的结构化库 —— "
                        "注意要用 3.export.py 产出的 nt_msg_export.db，"
                        "而不是 nt_msg_plain.db（那里面是 Protobuf 原始列）。"
                    )
                self._columns = _column_names(conn, self._table)
                missing = [c for c in REQUIRED_COLUMNS if c not in self._columns]
                if missing:
                    raise SourceDatabaseError(
                        f"{self._table} 缺少必需的列 {missing}；"
                        f"实际有：{sorted(self._columns)}"
                    )
                absent = [c for c in OPTIONAL_COLUMNS if c not in self._columns]
                row = conn.execute(f'SELECT COUNT(*) AS n FROM "{self._table}"').fetchone()
                total = int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise SourceDatabaseError(f"读源库失败 {self.path}：{exc}") from exc

        if absent:
            logger.warning(
                "源库 %s 的 %s 少了这些列：%s（会按「没有」处理，功能可能降级）",
                self.path.name,
                self._table,
                absent,
            )
        return {"table": self._table, "columns": sorted(self._columns), "rows": total, "absent": absent}

    # ---------------- 增量读 ----------------

    def latest(self) -> tuple[int, str] | None:
        """库里最新一条的 (timestamp, msg_id)。空库返回 None。

        用途：判断"游标是否比库还新"（导出库被换成了一个更旧的快照时，
        静默什么都不做是最糟的结果 —— 要能看出来）。
        """
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            row = conn.execute(
                f'SELECT timestamp AS t, msg_id AS m FROM "{self._table}"'
                " ORDER BY timestamp DESC, msg_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return int(row["t"]), str(row["m"])

    # ---------------- 按"读没读过"扫描（唯一路径） ----------------
    #
    # 判据是**镜像里有没有这条**（等价于"标没标已读"），**不是时间窗口**。
    #
    # 为什么不用时间窗口：窗口表达不了"这条到底处理过没有"。比窗口更老、而镜像里又
    # 没有的消息（换了导出库、镜像被删过、白名单刚放开、导出曾经漏了一批）永远不会
    # 被读到 —— 而"该看到的没看到"正是这套系统最怕的失败。
    #
    # 白名单（`groups` / `senders`）**下推到 SQL**：扫描、未读统计、镜像里记的东西
    # 都只涉及白名单内的消息。以前是在 Python 里逐条判 —— 于是配了白名单也没用：
    # "没读过的"是整个库（77 万条），每轮还要把白名单外的几十万条挨个写进镜像
    # （记成 skipped）。现在配了白名单就真的只剩那几条。

    def _attach_mirror(self, conn: sqlite3.Connection, mirror_path: Path | str) -> None:
        """把镜像库（只读）挂进源库连接，用来做"没读过"的反连接。

        ⚠️ 两处必须这么写：URI 要当**字面量**传（`ATTACH DATABASE ?` 用绑定参数时
        不解析 URI，会把 `?mode=ro` 当成文件名的一部分）；而源连接本来就是
        `uri=True` 打开的，所以字面量 URI 才生效。
        """
        mirror = Path(mirror_path)
        if not mirror.exists():
            # 镜像还没有 = 一条都没读过 → 反连接全命中。这里仍然挂一个空库，
            # 让后面的 SQL 只有一条路径（少一种分支就少一处错的可能）。
            conn.execute("ATTACH DATABASE ':memory:' AS xc_mirror")
            conn.execute(
                "CREATE TABLE xc_mirror.message (msg_id TEXT PRIMARY KEY)"
            )
            return
        conn.execute(
            "ATTACH DATABASE " + quote_sql(sqlite_uri(mirror, "ro")) + " AS xc_mirror"
        )

    def whitelist_sql(self, groups: Sequence[str], senders: Sequence[str]) -> tuple[str, list, str]:
        """把白名单翻成 SQL 条件。返回 `(where, params, 说明)`。

        `说明` 是给日志/自检用的：哪一部分**没法**下推（源库缺列时），
        那种情况下这一层仍然只能在 Python 里判 —— 要能看出来，否则用户会以为
        "配了白名单就只扫这几条"，而实际上没有。
        """
        if not self._columns:
            with closing(self._connect()) as conn:
                self._columns = _column_names(conn, self._table)
        clauses: list[str] = []
        params: list = []
        notes: list[str] = []

        if groups:
            clauses.append(
                "CAST(t.group_id AS TEXT) IN (" + ",".join("?" * len(groups)) + ")"
            )
            params.extend(str(g) for g in groups)

        if senders:
            # `SourceMessage.sender_id` 是"先 sender_qq、后退 sender_uid"，
            # 所以两边都要认 —— 只按 sender_qq 过滤会漏掉只有 uid 的那些行。
            columns = [c for c in ("sender_qq", "sender_uid") if c in self._columns]
            if columns:
                parts = [
                    f'CAST(t."{c}" AS TEXT) IN (' + ",".join("?" * len(senders)) + ")"
                    for c in columns
                ]
                clauses.append("(" + " OR ".join(parts) + ")")
                for _ in columns:
                    params.extend(str(s) for s in senders)
            else:
                notes.append(
                    "源表没有 sender_qq / sender_uid 列，发送者白名单**没法**在 SQL 里过滤"
                    "（仍在 Python 里判）"
                )

        where = " AND ".join(clauses)
        return where, params, "；".join(notes)

    def _unread_where(self) -> str:
        # `CAST(... AS TEXT)`：镜像里的 msg_id 是 TEXT，而导出表的 msg_id 在
        # nt_msg_db_util 的表结构里是 INTEGER 主键 —— SQLite 比较 INTEGER 与 TEXT
        # **永远不相等**，不转类型的话反连接会"全部命中"，等于每次都把整个库读一遍。
        return (
            "NOT EXISTS (SELECT 1 FROM xc_mirror.message m"
            " WHERE m.msg_id = CAST(t.msg_id AS TEXT))"
        )

    def fetch_unread(
        self,
        mirror_path: Path | str,
        *,
        limit: int,
        cursor: tuple[int, str] | None = None,
        groups: Sequence[str] = (),
        senders: Sequence[str] = (),
    ) -> list[SourceMessage]:
        """取**镜像里没有**的消息，按时间**正序**（从最老的未读开始）。

        翻页用 `cursor`（上一条的 `(timestamp, msg_id)`）而不是靠"处理时会把镜像写
        进去"：那样一来"这一批取多少"就和"写没写镜像"绑在一起了，读取器的行为会
        依赖调用方的副作用（曾经因为 `--dry-run` 不写镜像而在这里死循环过）。

        `groups` / `senders` 是白名单，**直接下推成 SQL 条件**：配了白名单时，
        扫描根本不碰白名单外的消息（于是也不会把它们写进镜像）。
        """
        mirror = Path(mirror_path)
        where = self._unread_where()
        params: list = []
        wl_where, wl_params, _ = self.whitelist_sql(groups, senders)
        if wl_where:
            where = f"({where}) AND ({wl_where})"
            params.extend(wl_params)
        if cursor is not None:
            ts, msg_id = int(cursor[0]), str(cursor[1])
            where += " AND (t.timestamp > ? OR (t.timestamp = ? AND t.msg_id > ?))"
            params.extend([ts, ts, msg_id])
        params.append(int(limit))

        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            select = [
                c
                for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS)
                if c in self._columns
            ]
            columns_sql = ", ".join('"' + c + '"' for c in select)  # 数字列名必须加引号
            self._attach_mirror(conn, mirror)
            sql = (
                f'SELECT {columns_sql} FROM "{self._table}" AS t'
                f" WHERE {where}"
                " ORDER BY t.timestamp ASC, t.msg_id ASC LIMIT ?"
            )
            try:
                rows = conn.execute(sql, tuple(params)).fetchall()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(
                    f"查询源库失败（{self.path.name}）：{exc}。"
                    "如果提示 database is locked，说明导出工具正在写它 —— "
                    "确认导出是「跑完再读」还是「边写边读」，前者要等它写完。"
                ) from exc
        return [self._to_message(row) for row in rows]

    def count_unread(
        self,
        mirror_path: Path | str,
        *,
        groups: Sequence[str] = (),
        senders: Sequence[str] = (),
    ) -> int:
        """还有多少条没读过（给日志与 `--status` 用；**不是**用来限制读取的）。

        `groups` / `senders` 与扫描用的是**同一个** SQL 条件 —— 否则界面上会说
        "还有 77 万条没读过"，而实际只扫白名单内的那几条（这正是它以前的样子）。
        """
        mirror = Path(mirror_path)
        where = self._unread_where()
        params: list = []
        wl_where, wl_params, _ = self.whitelist_sql(groups, senders)
        if wl_where:
            where = f"({where}) AND ({wl_where})"
            params.extend(wl_params)
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            self._attach_mirror(conn, mirror)
            try:
                row = conn.execute(
                    f'SELECT COUNT(*) AS n FROM "{self._table}" AS t'
                    f" WHERE {where}",
                    tuple(params),
                ).fetchone()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(f"统计未读失败（{self.path.name}）：{exc}") from exc
        return int(row["n"] if row else 0)

    def count_matching(
        self,
        *,
        groups: Sequence[str] = (),
        senders: Sequence[str] = (),
    ) -> int:
        """源库里**匹配白名单**的行数（不看镜像）。

        用途：回答"筛选之后到底有多少条" —— 界面上要能同时看到
        "白名单内共 N 条"和"没读过的 M 条"，否则配了白名单的人会以为
        "没读过"那一堆是整个库（以前确实是这样，因为过滤是在 Python 里做的）。
        """
        where, params, _ = self.whitelist_sql(groups, senders)
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            sql = f'SELECT COUNT(*) AS n FROM "{self._table}" AS t'
            if where:
                sql += f" WHERE {where}"
            try:
                row = conn.execute(sql, tuple(params)).fetchone()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(f"统计白名单内行数失败（{self.path.name}）：{exc}") from exc
        return int(row["n"] if row else 0)

    def iter_unread(
        self,
        mirror_path: Path | str,
        *,
        limit: int,
        chunk: int = 500,
        groups: Sequence[str] = (),
        senders: Sequence[str] = (),
    ) -> Iterator[SourceMessage]:
        """分块迭代"没读过的"消息，直到取满 `limit` 或没有更多。

        分块是因为不能把几十万条一次塞进内存；`limit` 是一轮的总预算。
        游标跟着上一条走（而不是靠"处理时会把镜像写进去"），所以读取器的行为
        和调用方有没有写镜像无关。
        """
        taken = 0
        cursor: tuple[int, str] | None = None
        while taken < limit:
            want = min(chunk, limit - taken)
            batch = self.fetch_unread(
                mirror_path, limit=want, cursor=cursor, groups=groups, senders=senders
            )
            if not batch:
                return
            for message in batch:
                yield message
                taken += 1
            cursor = (batch[-1].timestamp, batch[-1].msg_id)
            if len(batch) < want:
                return

    def _to_message(self, row: sqlite3.Row) -> SourceMessage:
        keys = set(row.keys())
        content = _json_or_none(row["content"]) if "content" in keys else None
        text = str(row["text"] or "") if "text" in keys else ""
        sender = ""
        for candidate in ("sender_qq", "sender_uid"):
            if candidate in keys and row[candidate]:
                sender = str(row[candidate])
                break
        group = ""
        for candidate in ("group_id", "group_qq"):
            if candidate in keys and row[candidate]:
                group = str(row[candidate])
                break
        # 正文：text 是导出工具合并好的纯文本，优先用它；
        # 它为空但 content 是纯文本段时，退回 content 里的 text。
        if not text and isinstance(content, dict) and content.get("type") == "text":
            text = str(content.get("text") or "")
        return SourceMessage(
            msg_id=str(row["msg_id"]),
            timestamp=int(row["timestamp"]),
            group_id=group,
            sender_id=sender,
            sender_name=None,  # 导出库里没有群名片/昵称，只有号码
            text=text,
            attachments=attachments_from_content(content),
            parse_status=str(row["parse_status"]) if "parse_status" in keys and row["parse_status"] else None,
            raw_content=content,
        )

    def fetch_by_ids(self, msg_ids: Iterable[str]) -> dict[str, "SourceMessage"]:
        """按 msg_id 精确取若干条（重试 pending/failed 的那些靠它）。

        用 id 取而不是"从头再扫一遍"：失败的消息可能落在很久以前，重扫一遍
        代价太大，而按主键取是常数级的。
        """
        ids = [str(i) for i in msg_ids]
        if not ids:
            return {}
        out: dict[str, SourceMessage] = {}
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            select = [c for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS)
                      if c in self._columns]
            select = list(dict.fromkeys(select))  # 去重但保序
            columns_sql = ", ".join('"' + c + '"' for c in select)  # 数字列名必须加引号
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                marks = ",".join("?" * len(chunk))
                sql = (
                    f'SELECT {columns_sql} FROM "{self._table}"'
                    f' WHERE msg_id IN ({marks})'
                )
                for row in conn.execute(sql, chunk).fetchall():
                    message = self._to_message(row)
                    out[str(message.msg_id)] = message
        return out
