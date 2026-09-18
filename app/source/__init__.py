"""源数据：从聊天记录库里读消息（不连 QQ）。"""

from .attachments import AttachmentResolver, ResolvedAttachment, guess_content_type
from .ntmsg import SourceDatabase, SourceDatabaseError, SourceMessage

__all__ = [
    "AttachmentResolver",
    "ResolvedAttachment",
    "SourceDatabase",
    "SourceDatabaseError",
    "SourceMessage",
    "guess_content_type",
]
