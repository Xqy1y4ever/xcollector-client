# msgdb：从 `nt_msg_db_util` 搬过来的解析套件

这个目录**不是本仓库写的**，是从
[`QQBackup/nt_msg_db_util`](https://github.com/QQBackup/nt_msg_db_util) 的
`msgdb/` **逐字**搬过来的（MIT 许可，见该仓库的 `LICENSE`）。

| | |
|---|---|
| 来源 | https://github.com/QQBackup/nt_msg_db_util （分支 `master`，本地快照在 `E:\Xcollector\nt_msg_db_util`） |
| 搬运时间 | 与 `app/ntmsg_db/` 的整合同时完成（客户端 `CLIENT_NT_MSG_DB` 那套功能） |
| 搬运方式 | 目录整体复制，**除了 `__pycache__` 一个字节都没改** |
| 为什么单独放在仓库根目录 | 上游内部用的是绝对导入（`from msgdb.proto import ...`），放在根目录就能**原样可用**；改包名或改成相对导入都会让它与上游产生差异，以后升级就得手工 merge |

## 它负责什么

`40800`（消息体）那段 protobuf 的**协议定义与解析**，以及导出库的表结构：

- `proto/` —— `.proto` 定义、`protoc` 生成的 `*_pb2.py`、各类字段的 parser，
  外加一个手写的 wire fallback（protobuf 解不开时把原始字段保留下来）
- `group/` —— `group_msg_table` 的行 → `group_messages` 记录
- `c2c/` —— `c2c_msg_table` 的行 → `c2c_messages` 记录
- `export_schema.py` —— 导出库的 DDL、FTS、索引、批量写入

客户端自己的代码只通过 `app/ntmsg_db/export.py` 用它，并且**不改它的输出形状**：
`content` 就是上游那种 `{"type":"msg_body","segments":[...]}`（读取器那边适配，
见 `app/source/ntmsg.py`）。

## 怎么升级

1. 从上游把新的 `msgdb/` 覆盖过来（删掉 `__pycache__`）；
2. 跑 `python -m tests.check_ntmsg_db` —— 里面有用**真的** `_pb2` 构造夹具、
   再走完整导出的用例；上游改了字段名或表结构，这一步会立刻红；
3. 如果上游新增了字段，客户端侧的 `app/source/ntmsg.py` 里的
   `SEGMENT_MEDIA_TYPES` / `_MD5_KEYS` 之类的映射可能也要跟着看一眼。

**不要**在这个目录里改代码。客户端要对导出做扩展（比如补 `"40003"`/`"40850"` 两列），
都写在 `app/ntmsg_db/export.py` 里 —— 这样上游升级时直接覆盖即可。
