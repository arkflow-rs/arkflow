## MODIFIED Requirements

### Requirement: Failure handling and recovery

对象存储后端的恢复读取 SHALL 对每类异常保留可观测痕迹:段解码因 CRC 校验失败或撕裂尾而提前停止时,SHALL 以 `tracing::warn` 记录 segment 标识、截断点偏移与被丢弃的字节数(损坏数据无法可靠计数条目,故以字节为准);恢复读取遇到 manifest 引用但对象缺失(NotFound)的已封段时,SHALL 以 `tracing::warn` 记录该段标识。告警 MUST NOT 将已确认(cursor 之前)的数据标记为丢失——被截断/缺失的段只可能包含未确认的重放数据。

#### Scenario: 恢复读取遇到段中部 CRC 损坏
- **WHEN** 恢复读取解码某个已封段时在条目 i 处遇到 CRC 不匹配
- **THEN** 该段从 i 起的剩余字节被丢弃(与现状一致)
- **THEN** 发出一条 `tracing::warn`,包含 segment 标识、截断点偏移、被丢弃字节数与丢弃类别
- **THEN** 返回的完整前缀条目照常重放

#### Scenario: 恢复读取遇到 manifest 引用但对象缺失的段
- **WHEN** 恢复读取按 manifest 引用取某个已封段而对象存储返回 NotFound
- **THEN** 发出一条 `tracing::warn`,包含该段的对象键
- **THEN** 恢复继续处理其余段(与现状一致)
