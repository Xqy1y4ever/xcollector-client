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

**本客户端自己导出的库额外带两列**：`"40003"`（群内消息序号）和 `"40850"`
（被回复消息的群内序号）。这是刻意的增强：有了它们，"这条消息是在补充哪条通知"
才能反查到 `msg_id`（见 `seq_column()` / `resolve_seq()`）。上游的导出表没有这两列，
那时这一层会退回从正文里找引用字段，找不到就如实报告"解析不了"而不是硬猜。

## 这个读取器刻意做的几件事

1. **只读打开**。导出库可能正被另一个进程写（导出工具在跑），所以用
   `mode=ro` + `immutable=0` 打开，并在读失败时给出能看懂的错，而不是抛一个
   光秃秃的 `sqlite3.OperationalError: database is locked`。
2. **列名不写死单一版本**。先用 `PRAGMA table_info` 读实际列名，缺列时按"这一列
   没有"处理并记一行 warning —— 上游换版本加/改列时，客户端应该退化而不是崩。
   唯一真正必需的是 `msg_id` 和 `timestamp`。
3. **"读没读过"由镜像决定，不由时间决定**。主扫描是"镜像里没有的都要读"
   （`iter_unread` / `count_unread`，靠 `ATTACH` 镜像库做反连接）。时间窗口只用
   在一个地方：回看最近一段**已读**的消息，看内容有没有被编辑过（`recheck_since`）。
   为什么不拿时间当选取依据：窗口表达不了"这条处理过没有"，比窗口更老又没读过的
   消息会被永远跳过 —— 而"该看到的没看到"正是这套系统最怕的失败。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Collection, Iterable, Iterator, Sequence

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
    # 这条消息是回复/引用时，被引用对象的**原始标识值**（可能是消息 id，
    # 也可能是群内序号 —— 两者形状不同，所以这里不猜，交给上层去试）。
    quote_ref: str | None = None
    # 群内消息序号（nt_msg_db_util 文档里的 `40003`）。
    #
    # **上游的 `group_messages` 没有这一列**，所以读上游导出库时它是 None；
    # 而本客户端自己导出的库会额外带上 `"40003"` / `"40850"`，有了它
    # "回复 → 被回复的那条"才能确定性地解析出来（见 `resolve_seq`）。
    seq: str | None = None

    @property
    def ts_ms(self) -> int:
        return int(self.timestamp) * 1000


# 回复/引用里可能装着"被引用对象"的键名 —— **按顺序就是优先级**。
#
# 上游 `3.export.py` 导出的 `content` 里是 **protobuf 原生字段名**（`reply_msg_seq`
# 这种），所以前几个是原生名；数字键名是留给别的导出实现的（也留着，因为
# `Content` 里我们自己也补过数字键）。
#
# 依据是 nt_msg_db_util 的群字段文档：
#   47402 `reply_msg_seq` 与同一会话的 40003 群内消息序号高度匹配 → 适合当"回复目标"
#   47401 `reply_msg_id`  只是"候选 ID"
#   47422 `reply_source_record_id` 未与主表 40001 匹配 → 最后才试
# 多认几个键不会造成错判：解析出来后还要在镜像/源表里**真的命中某一条**才作数。
DEFAULT_QUOTE_KEYS = (
    "reply_msg_seq",            # 47402，最可靠
    "47402",
    "reply_seq",
    "quoted_seq",
    "quote_seq",
    "reply_msg_id",             # 47401，候选
    "reply_id",
    "quoted_msg_id",
    "quote_id",
    "reply_source_record_id",   # 47422，已知对不上主表
    "47422",
)

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


# 群内序号列的候选名（导出表里有就用，没有就明说解析不了）
SEQ_COLUMN_CANDIDATES = ("40003", "seq", "msg_seq", "message_seq")

# 除了 OPTIONAL_COLUMNS 之外还要一起取出来的列：序号与"被回复消息的序号"。
# 这两个是本客户端自己导出时补上的（见 app/ntmsg_db/export.py）。
_EXTRA_COLUMNS = (*SEQ_COLUMN_CANDIDATES, "40850")



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


def quote_ref_from_content(content: dict | None, keys: Sequence[str] = DEFAULT_QUOTE_KEYS) -> str | None:
    """从 `content` JSON 里找出"这条消息在回复哪一条"的标识值。

    **只找，不解释**：返回的值可能是消息 id，也可能是群内序号，形状不一样。
    解释留给上层（先当消息 id 在镜像里找，找不到再按序号解析），因为把这两种
    混在一起猜会得到一个"看起来对、其实指错任务"的结果 —— 那比不做还糟。

    **按 `keys` 的顺序找**（不是按 JSON 里的键顺序）：`47401` 和 `47402` 常常同时
    出现，而字段文档说 `47402`（群内序号）才是可靠的、`47401` 只是"候选"，
    所以优先级必须由调用方给的这个序列决定。
    """
    if not isinstance(content, dict):
        return None

    def find(node: Any, key: str) -> str | None:
        if isinstance(node, list):
            for item in node:
                hit = find(item, key)
                if hit:
                    return hit
            return None
        if not isinstance(node, dict):
            return None
        value = node.get(key)
        if isinstance(value, (int, str)) and not isinstance(value, bool):
            text = str(value).strip()
            if text and text.lower() not in ("none", "null", "0", ""):
                return text
        for item in node.values():
            hit = find(item, key)
            if hit:
                return hit
        return None

    for key in keys:
        hit = find(content, str(key))
        if hit:
            return hit
    return None


def _first_present_key(columns: set[str], candidates: Sequence[str]) -> str | None:
    """列名里第一个存在的候选（只看名字，不取值）。"""
    for name in candidates:
        if name in columns:
            return name
    return None


def _first_present(row: Any, keys: set[str], candidates: Sequence[str]) -> str | None:
    """从一行里取第一个**有意义**的候选列的值（统一成字符串）。

    `0` 要当成"没有"：`"40003"` / `"40850"` 这类序号列的 0 表示"不适用"，
    把它当成有效值会让每条消息都"看起来在回复第 0 条"（这个坑真踩到过 ——
    400 条消息全都带上了引用关系）。
    """
    for name in candidates:
        if name not in keys or row[name] is None:
            continue
        text = str(row[name]).strip()
        if text and text.lower() not in ("0", "none", "null"):
            return text
    return None


class SourceDatabase:
    """`nt_msg_export.db` 的只读读取器。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._columns: set[str] = set()
        self._table = "group_messages"

    # ---------------- 打开与自检 ----------------

    def _connect(self) -> sqlite3.Connection:
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

    # ---------------- 按"读没读过"扫描（主路径） ----------------
    #
    # 判据是**镜像里有没有这条**（等价于"标没标已读"），**不是时间窗口**。
    #
    # 为什么不用时间窗口：窗口表达不了"这条到底处理过没有"。比窗口更老、而镜像里又
    # 没有的消息（换了导出库、镜像被删过、白名单刚放开、导出曾经漏了一批）永远不会
    # 被读到 —— 而"该看到的没看到"正是这套系统最怕的失败。时间窗口只能用来省扫描量，
    # 省下来的代价是静默漏消息，所以不作为选取依据。
    #
    # 代价说清楚：没有任何过滤时，第一次运行会把导出库里**所有**群消息过一遍
    # （几十万条）。这是刻意的：它们会被逐条标记（订阅外的记 skipped，很便宜），
    # 一轮之后就不再重复读。真正花钱的抽取仍然只发生在订阅命中的消息上。
    # 想少读一点，用 `CLIENT_GROUP_WHITELIST` / `CLIENT_SENDER_WHITELIST` 收窄。

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

    def _unread_where(self, *, recheck_since: int | None) -> str:
        # `CAST(... AS TEXT)`：镜像里的 msg_id 是 TEXT，而导出表的 msg_id 在
        # nt_msg_db_util 的表结构里是 INTEGER 主键 —— SQLite 比较 INTEGER 与 TEXT
        # **永远不相等**，不转类型的话反连接会"全部命中"，等于每次都把整个库读一遍。
        where = (
            "NOT EXISTS (SELECT 1 FROM xc_mirror.message m"
            " WHERE m.msg_id = CAST(t.msg_id AS TEXT))"
        )
        if recheck_since is not None:
            # 回看：镜像里已读、但最近又被碰过的消息也要重看（这是"内容被编辑"
            # 唯一的发现途径）。
            where = f"(({where}) OR (t.timestamp >= ?))"
        return where

    def fetch_unread(
        self,
        mirror_path: Path | str,
        *,
        limit: int,
        recheck_since: int | None = None,
        cursor: tuple[int, str] | None = None,
        exclude: Collection[str] | None = None,
    ) -> list[SourceMessage]:
        """取**镜像里没有**的消息（`recheck_since` 给了就带上回看窗口）。

        翻页用 `cursor`（上一条的 `(timestamp, msg_id)`）而不是靠"处理时会把镜像写
        进去"：`--dry-run` 不写镜像，靠副作用翻页会让同一批被反复读出来。

        两种模式的顺序不同：
          * 未读模式（`recheck_since=None`）：**按时间正序**（从最老的未读开始，
            这样补充关系里的"原文"一定先于"补充"被处理）；
          * 回看模式：**按时间倒序**（窗口比预算大时，先看最新的那一段 ——
            否则最早那几条会把预算吃光，新的内容改动永远排不上）。

        `exclude` 是**这一轮已经处理过**的 msg_id（回看时用）：回看窗口与"没读过"
        是**并集**，刚在未读那一步处理过的消息自然也落在窗口里；不排掉的话同一条
        内容会在一轮里被处理两遍（dry-run 下就是白花一次抽取），`scanned` 也会虚高。
        做成 SQL 条件而不是在 Python 里 `continue`：否则"取 limit 条再丢掉"会让真正
        该回看的老消息被挤掉，而这是静默的。
        """
        mirror = Path(mirror_path)
        descending = recheck_since is not None
        where = self._unread_where(recheck_since=recheck_since)
        params: list = []
        if recheck_since is not None:
            params.append(int(recheck_since))
        if cursor is not None:
            ts, msg_id = int(cursor[0]), str(cursor[1])
            if descending:
                where += " AND (t.timestamp < ? OR (t.timestamp = ? AND t.msg_id < ?))"
            else:
                where += " AND (t.timestamp > ? OR (t.timestamp = ? AND t.msg_id > ?))"
            params.extend([ts, ts, msg_id])
        excluded = [str(i) for i in (exclude or ())]
        if excluded:
            # 分批写：SQLite 对一条语句里的绑定参数个数有上限（老版本 999），
            # 拆成多组 `NOT IN (...)` 而不是把上限赌在版本上。
            clauses = []
            for start in range(0, len(excluded), 500):
                group = excluded[start : start + 500]
                clauses.append(
                    "CAST(t.msg_id AS TEXT) NOT IN (" + ",".join("?" * len(group)) + ")"
                )
                params.extend(group)
            where += " AND " + " AND ".join(clauses)
        params.append(int(limit))
        order = (
            "ORDER BY t.timestamp DESC, t.msg_id DESC"
            if descending
            else "ORDER BY t.timestamp ASC, t.msg_id ASC"
        )

        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            select = [
                c
                for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS, *_EXTRA_COLUMNS)
                if c in self._columns
            ]
            columns_sql = ", ".join('"' + c + '"' for c in select)  # 数字列名必须加引号
            self._attach_mirror(conn, mirror)
            sql = (
                f'SELECT {columns_sql} FROM "{self._table}" AS t'
                f" WHERE {where} {order} LIMIT ?"
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

    def count_unread(self, mirror_path: Path | str) -> int:
        """还有多少条没读过（给日志与 `--status` 用；**不是**用来限制读取的）。"""
        mirror = Path(mirror_path)
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            self._attach_mirror(conn, mirror)
            try:
                row = conn.execute(
                    f'SELECT COUNT(*) AS n FROM "{self._table}" AS t'
                    f" WHERE {self._unread_where(recheck_since=None)}"
                ).fetchone()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(f"统计未读失败（{self.path.name}）：{exc}") from exc
        return int(row["n"] if row else 0)

    def iter_unread(
        self,
        mirror_path: Path | str,
        *,
        limit: int,
        chunk: int = 500,
        recheck_since: int | None = None,
        exclude: Collection[str] | None = None,
    ) -> Iterator[SourceMessage]:
        """分块迭代"没读过的"消息，直到取满 `limit` 或没有更多。

        分块是因为不能把几十万条一次塞进内存；`limit` 是一轮的总预算。
        游标跟着上一条走，所以不依赖"处理时会把镜像写进去"（dry-run 也能正确翻页）。
        `exclude` 透传给 `fetch_unread`（每一页都带上同一份排除名单）。
        """
        taken = 0
        cursor: tuple[int, str] | None = None
        while taken < limit:
            want = min(chunk, limit - taken)
            batch = self.fetch_unread(
                mirror_path,
                limit=want,
                recheck_since=recheck_since,
                cursor=cursor,
                exclude=exclude,
            )
            if not batch:
                return
            for message in batch:
                yield message
                taken += 1
            cursor = (batch[-1].timestamp, batch[-1].msg_id)
            if len(batch) < want:
                return

    def fetch_since(
        self,
        since_ts: int,
        since_msg_id: str = "",
        *,
        limit: int,
    ) -> list[SourceMessage]:
        """取 `(timestamp, msg_id) > (since_ts, since_msg_id)` 的消息，按时间正序。

        元组比较而不是 `timestamp > ?`：同一秒里可能有很多条，只按秒比会漏。
        `since_msg_id` 为空时退化成 `timestamp >= since_ts`（首次运行的路径），
        也就是**会重复读同一秒里的消息** —— 重复由后端幂等挡住，漏一条可没人挡。
        """
        with closing(self._connect()) as conn:
            if not self._columns:
                self._columns = _column_names(conn, self._table)
            select = [
                c
                for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS, *_EXTRA_COLUMNS)
                if c in self._columns
            ]
            # ⚠️ 列名**必须加引号**。`"40003"` / `"40850"` 这种纯数字列名不加引号会被
            # SQLite 当成**数字字面量**（`SELECT 40850` 就是常量 40850），于是每一行
            # 都"看起来"有这个值 —— 读取器当时读到的其实是 40850 这个数字，
            # 而不是那一列的内容。
            columns_sql = ", ".join('"' + c + '"' for c in select)
            sql = (
                f'SELECT {columns_sql} FROM "{self._table}"'
                " WHERE (timestamp > ?) OR (timestamp = ? AND msg_id > ?)"
                " ORDER BY timestamp ASC, msg_id ASC LIMIT ?"
            )
            try:
                rows = conn.execute(sql, (since_ts, since_ts, since_msg_id, int(limit))).fetchall()
            except sqlite3.Error as exc:
                raise SourceDatabaseError(
                    f"查询源库失败（{self.path.name}）：{exc}。"
                    "如果提示 database is locked，说明导出工具正在写它 —— "
                    "确认导出是「跑完再读」还是「边写边读」，前者要等它写完。"
                ) from exc
        return [self._to_message(row) for row in rows]

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
        # 引用目标：**先看表列**（`40850` = 被回复消息的群内序号）。
        # 表列比正文里的字段可靠：字段文档统计过，`40850` 与 `40003` 的匹配率
        # 99.88%，而正文里的 `47401` 只是"候选 ID"、`47422` 明确对不上主表。
        quote = _first_present(row, keys, ("40850",)) or quote_ref_from_content(content)
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
            quote_ref=quote,
            seq=_first_present(row, keys, SEQ_COLUMN_CANDIDATES),
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
            select = [c for c in ("msg_id", "timestamp", *OPTIONAL_COLUMNS, *_EXTRA_COLUMNS)
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

    def seq_column(self) -> str | None:
        """源表里有没有"群内消息序号"这一列（没有就返回 None）。

        这一列决定了「回复 → 被回复的那条」能不能**确定性地**解析出来。
        nt_msg_db_util 的字段文档说回复里的 `47402` 与群内序号匹配、而
        `47422` 与主表 `40001` 不匹配 —— 也就是说**没有这一列就拿不到被引用
        消息的 msg_id**。

        上游 3.export.py 的 `group_messages` 里没有它（所以读上游的库时这里如实
        返回 None，由上层把"识别不了补充关系"明确报出来）；本客户端自己导出的库
        （`app/ntmsg_db/export.py`）会带上 `"40003"`，于是补充关系能落到原任务上。
        """
        if not self._columns:
            with closing(self._connect()) as conn:
                self._columns = _column_names(conn, self._table)
        return _first_present_key(self._columns, SEQ_COLUMN_CANDIDATES)

    def resolve_seq(self, group_id: str, seq_value: str) -> str | None:
        """把"群内序号"解析成那条消息的 msg_id。解析不了返回 None（**不猜**）。"""
        return self.resolve_seq_detail(group_id, seq_value)[0]

    def resolve_seq_detail(self, group_id: str, seq_value: str) -> tuple[str | None, int]:
        """`(命中的 msg_id 或 None, 命中几条)`。

        **命中多条时一定返回 None**：群内序号在真实数据里会被复用（实测 769,003 行
        里有 21,129 个 `(群, 序号)` 组合对应不止一条消息 —— 合并/迁移过的历史里
        序号会重复）。挑一条"看起来最像"的会把补充写到**别人的任务**上，
        那比认不出来糟糕得多，所以宁可交回 None 让上层按新消息处理。
        """
        column = self.seq_column()
        if not column:
            return None, 0
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f'SELECT msg_id FROM "{self._table}" WHERE group_id=? AND "{column}"=? LIMIT 2',
                (str(group_id), str(seq_value)),
            ).fetchall()
        if len(rows) == 1:
            return str(rows[0]["msg_id"]), 1
        return None, len(rows)

    def iter_since(
        self, since_ts: int, since_msg_id: str = "", *, limit: int, chunk: int = 500
    ) -> Iterator[SourceMessage]:
        """分块迭代，直到取满 `limit` 条或没有更多。"""
        taken = 0
        ts, mid = since_ts, since_msg_id
        while taken < limit:
            want = min(chunk, limit - taken)
            batch = self.fetch_since(ts, mid, limit=want)
            if not batch:
                return
            for message in batch:
                yield message
                taken += 1
            ts, mid = batch[-1].timestamp, batch[-1].msg_id
            if len(batch) < want:
                return
