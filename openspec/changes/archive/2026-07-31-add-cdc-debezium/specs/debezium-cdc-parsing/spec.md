## ADDED Requirements

### Requirement: Debezium Envelope 解析为列式 Arrow
`debezium_json` codec 收到 Debezium Envelope JSON（含 `before`/`after`/`op`/`source`/`ts_ms`）时，SHALL 输出列式 `MessageBatch`：`after` 的字段扁平化为顶层列（主数据行），并附加 `before`（作为 JSON 文本列）、`op`、`ts_ms` 与 `source` 元信息列。

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

### Requirement: 操作类型列 op
codec SHALL 在每条输出中附加顶层 `op` 列，其值为该 Envelope 的 `op` 字段原值（`c`/`u`/`d`/`r` 之一）。

#### Scenario: op 值透传
- **WHEN** Envelope 的 `op` 为 `"u"`
- **THEN** 输出 `op` 列对应行为 `"u"`

### Requirement: 源元信息列
codec SHALL 附加 `source` 关键字段为顶层列（至少 `source_db`、`source_table`），保留完整 `source` 对象为 JSON 文本列（供下游 SQL 用 JSON 函数查询），并附加 `ts_ms` 为顶层列。`before` 同样以 JSON 文本列保留完整变更前值。

#### Scenario: source 字段提取
- **WHEN** Envelope 的 `source` 含 `db="orders"`、`table="orders"`
- **THEN** 输出含顶层列 `source_db="orders"`、`source_table="orders"`，且完整 `source` JSON 文本列保留全部源字段

#### Scenario: ts_ms 透传
- **WHEN** Envelope 含 `ts_ms=1700000000000`
- **THEN** 输出 `ts_ms` 顶层列为 `1700000000000`

### Requirement: 字段缺失容错
当 Envelope 的 `before`、`after` 或 `source` 缺失或为 null 时，codec SHALL 以 null 填充对应列，不得返回错误。

#### Scenario: delete 时 after 为 null
- **WHEN** 一条 `op="d"` 的 Envelope 的 `after` 为 null
- **THEN** codec 正常输出，顶层业务列取自 `before`，未涉及的列以 null 填充，不报错

#### Scenario: source 缺失
- **WHEN** 一条 Envelope 不含 `source` 字段
- **THEN** `source_db`/`source_table` 顶层列为 null，`source`/`before` JSON 文本列保留为 `"null"` 文本，codec 不报错

### Requirement: 作为 codec 注册并经 Kafka input 接入
`debezium_json` codec SHALL 经 `register_codec_builder` 注册，并可被 Kafka input 通过现有 codec 接入点（`apply_codec_to_payload`）调用，无需新增 input 类型或修改 input 代码。

#### Scenario: Kafka input 配置 debezium_json codec
- **WHEN** 一个 Kafka input 配置 `codec: { type: "debezium_json" }` 并读到一条 Debezium Envelope 字节
- **THEN** input 输出的 `MessageBatch` 为该 Envelope 解析后的列式数据（含 `op`/业务列/`before`/`source_*`）

#### Scenario: 未配置 codec 时行为不变
- **WHEN** Kafka input 未配置 codec
- **THEN** 其行为与现状完全一致，不受本 codec 存在的影响

### Requirement: CDC 位点复用 ack-gated Kafka offset
codec SHALL NOT 引入自身的位点/offset 管理；CDC 位点由 Kafka input 现有的 ack-gated offset 承担（消费位点经 `input-durability` 的 ack-gated source-commit 推进）。

#### Scenario: 位点推进与 input-durability 一致
- **WHEN** 下游确认写入、Kafka input 的 ack 被触发
- **THEN** Kafka offset 按 `input-durability` 现有语义推进，codec 不参与位点逻辑
