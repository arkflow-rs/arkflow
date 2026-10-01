# debezium-cdc-parsing 增量

## MODIFIED Requirements

### Requirement: Debezium Envelope 解析为列式 Arrow
`debezium_json` codec 收到 Debezium Envelope JSON（含 `before`/`after`/`op`/`source`/`ts_ms`）时，SHALL 输出列式 `MessageBatch`：`after` 的字段扁平化为顶层列（主数据行），并附加 `before`（作为 JSON 文本列）、`op`、`ts_ms` 与 `source` 元信息列。业务字段与 envelope 元信息列同名（`op`/`ts_ms`/`source_db`/`source_table`/`before`/`source`）时，业务字段值 SHALL 保留在其原列，envelope 元信息值 SHALL 改投保留名 `__debezium_<name>` 列，并以 warn 日志记录冲突；元信息列名在无冲突时保持不变。

#### Scenario: create 事件
- **WHEN** codec 解析一条 `op="c"` 的 Envelope（`after` 含新行数据，`before` 为 null）
- **THEN** 输出 `MessageBatch` 的顶层列为 `after` 的字段值，`op` 列为 `"c"`，`before` 列为 null

#### Scenario: update 事件
- **WHEN** codec 解析一条 `op="u"` 的 Envelope（`before` 与 `after` 均非空）
- **THEN** 顶层列为 `after` 的新值，`before` JSON 文本列保留变更前值，`op` 列为 `"u"`

#### Scenario: delete 事件
- **WHEN** codec 解析一条 `op="d"` 的 Envelope（`after` 为 null，`before` 含被删行数据）
- **THEN** 顶层列取自 `before` 的字段值，`op` 列为 `"d"`，下游可据此识别删除

#### Scenario: snapshot/read 事件
- **WHEN** codec 解析一条 `op="r"` 的 Envelope（初始快照行）
- **THEN** 顶层列为 `after` 的初始值，`op` 列为 `"r"`

#### Scenario: 业务字段与元信息同名不互相覆盖
- **WHEN** 一条 Envelope 的 `after` 含名为 `op` 的业务字段（值为 `"custom"`），envelope 的 `op` 为 `"c"`
- **THEN** 输出中 `op` 列为业务值 `"custom"`，envelope 操作类型出现在 `__debezium_op` 列（值为 `"c"`），并记录 warn 日志；无同名冲突的消息不受影响

## ADDED Requirements

### Requirement: tombstone（空 payload）跳过且不失败
codec 收到零长度 payload（tombstone 表示，Kafka 侧通常为 null 值消息）时，SHALL 跳过该条消息并以 warn 日志记录，不得使整批解码失败；同批其他消息正常产出。JSON 字面量 `null`（非零长度）不属于 tombstone，维持既有解析行为。

#### Scenario: 空 payload 混批不拖垮好消息
- **WHEN** 一个 batch 含 1 条零长度 payload 与 2 条合法 Envelope
- **THEN** codec 产出 2 行，零长度消息被跳过并记录 warn，整批不报错
