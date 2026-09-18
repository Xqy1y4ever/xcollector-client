"""把源库里的附件描述变成真实字节。

## 为什么这件事需要一个模块

`nt_msg_export.db` 里的附件通常**只有 CDN 地址和 md5**（`nt_msg_db_util` 的
README 里 image 是 `{filename, width, height, filesize, md5_hex, cdn_url}`）。
QQ 的 CDN 链接几小时就过期，所以"照着 URL 去下载"在批处理场景里基本必失败：
等客户端跑到那条消息时，链接早就死了。

真正可靠的办法是按 **md5** 去本地找（NTQQ 的附件缓存 / 你自己导出的附件目录）。
所以这个模块做两件事：

  1. 先按 md5 找（最可靠，内容寻址）；
  2. 再按文件名找（md5 缺失时的退路）。

两条都找不到时**返回带了原因的结果**，绝不假装成功 —— 上层会据此降级成
"只留地址"或"不记附件"，并且都会在日志和通知字段上看得出来。
"""

from __future__ import annotations

import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# 一次最多扫多少个候选文件名（防止附件目录巨大时每次处理一条就全盘扫描）
_SCAN_LIMIT = 20000

# 常见图片/文档类型兜底（源库的 ext 字段有时缺失，靠文件名后缀猜）
_FALLBACK_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
    ".pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".zip": "application/zip",
    ".txt": "text/plain",
}


@dataclass
class ResolvedAttachment:
    """找到的附件字节；`content` 为空表示没找到，`reason` 说明为什么。"""

    filename: str
    content: bytes | None
    content_type: str
    reason: str | None = None

    @property
    def found(self) -> bool:
        return bool(self.content)


def guess_content_type(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix in _FALLBACK_TYPES:
        return _FALLBACK_TYPES[suffix]
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"


class AttachmentResolver:
    """在 `root` 下按 md5 / 文件名找附件字节。

    索引是**按需建立**的（第一次用到才扫目录），并且缓存下来 —— 一个批次里
    常常有几十张图来自同一个目录。
    """

    def __init__(self, root: Path | None, *, enabled: bool = True):
        self.root = root
        self.enabled = bool(enabled and root)
        self._by_md5: dict[str, Path] | None = None
        self._by_name: dict[str, Path] | None = None

    # ---------------- 索引 ----------------

    def _build(self) -> None:
        if self._by_md5 is not None:
            return
        self._by_md5 = {}
        self._by_name = {}
        if not self.enabled or self.root is None or not self.root.exists():
            return
        seen = 0
        for path in self.root.rglob("*"):
            if not path.is_file():
                continue
            seen += 1
            if seen > _SCAN_LIMIT:
                logger.warning(
                    "附件目录 %s 里的文件超过 %d 个，只索引了前面这些 —— "
                    "建议把根目录配得更精确一些（少扫一点，也少漏一点）",
                    self.root,
                    _SCAN_LIMIT,
                )
                break
            name = path.name.lower()
            self._by_name.setdefault(name, path)
            # NTQQ 的缓存文件名常常就是 md5（可能带后缀），所以两种形状都收
            stem = path.stem.lower()
            if 16 <= len(stem) <= 64:
                self._by_md5.setdefault(stem, path)

    def resolve(self, item: dict) -> ResolvedAttachment | None:
        """把源库里的一个附件描述解析成字节。"""
        filename = str(item.get("name") or item.get("filename") or "").strip()
        md5 = str(item.get("md5") or "").strip().lower()
        url = str(item.get("url") or "").strip()

        if not self.enabled:
            return ResolvedAttachment(
                filename=filename or "attachment",
                content=None,
                content_type=guess_content_type(filename),
                reason="没有配置 CLIENT_ATTACHMENT_ROOT，只能留远程地址",
            )

        self._build()
        assert self._by_md5 is not None and self._by_name is not None

        path: Path | None = None
        if md5 and md5 in self._by_md5:
            path = self._by_md5[md5]
        elif filename and filename.lower() in self._by_name:
            path = self._by_name[filename.lower()]
        elif md5:
            # 文件名里带 md5 的另一种常见形态：<md5>.<ext> 之外的 <name>.<md5>.<ext>
            for name, candidate in self._by_name.items():
                if md5 in name:
                    path = candidate
                    break

        if path is None:
            hint = md5 or filename or url or "(附件描述是空的)"
            return ResolvedAttachment(
                filename=filename or "attachment",
                content=None,
                content_type=guess_content_type(filename),
                reason=f"在 {self.root} 下没找到（md5/文件名都没命中：{hint}）",
            )

        try:
            data = path.read_bytes()
        except OSError as exc:
            return ResolvedAttachment(
                filename=path.name,
                content=None,
                content_type=guess_content_type(path.name),
                reason=f"读文件失败：{exc}",
            )
        return ResolvedAttachment(
            filename=filename or path.name,
            content=data,
            content_type=guess_content_type(follow_name(filename or path.name, path.name)),
        )


def follow_name(preferred: str, actual: str) -> str:
    """用哪个名字判断 MIME：优先用户/源库给的名字，它通常带正确的后缀。"""
    return preferred or actual
