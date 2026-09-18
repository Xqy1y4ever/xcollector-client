"""抽取与处理。

`timeparse` / `rule_extract` 是从 `xcollector-bot` 逐字复制的（原因见那两个文件
顶部的说明）；`extract` / `process` 是本仓库自己的实现。
"""

from .process import Outcome, process_message  # noqa: F401
