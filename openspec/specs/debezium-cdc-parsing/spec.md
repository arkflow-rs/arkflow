# debezium-cdc-parsing Specification

## Purpose
TBD - created by archiving change add-cdc-debezium. Update Purpose after archive.
## Requirements
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

### Requirement: tombstone（空 payload）跳过且不失败
codec 收到零长度 payload（tombstone 表示，Kafka 侧通常为 null 值消息）时，SHALL 跳过该条消息并以 warn 日志记录，不得使整批解码失败；同批其他消息正常产出。JSON 字面量 `null`（非零长度）不属于 tombstone，维持既有解析行为。

#### Scenario: 空 payload 混批不拖垮好消息
- **WHEN** 一个 batch 含 1 条零长度 payload 与 2 条合法 Envelope
- **THEN** codec 产出 2 行，零长度消息被跳过并记录 warn，整批不报错

### Requirement: 单趟解析与值域列式构建
`debezium_json` codec 解码一批 Envelope 时，SHALL 对每条 payload 仅执行一次 JSON 解析，并在内存值域直接完成 schema 推断与列式 Arrow 构建，MUST NOT 将扁平化行重新序列化为 JSON 文本后进行第二次解析。delete 事件中 `before` 既作为业务行来源又作为 JSON 文本列保留时，SHALL 以"先序列化为文本、再移入业务行"的方式复用同一份值，不得深拷贝。既有语义不变：tombstone（空 payload）跳过并告警、`null` 字面量与非对象 envelope 产出全 null 元信息行、无效 JSON 报错、字段冲突改名（`__debezium_<name>`）与告警。

#### Scenario: 输出与扁平化行的 JSON 语义差分等价
- **WHEN** 同一批 Envelope 分别经值域列式路径与"扁平化行序列化为 NDJSON 后经 JSON 组件推断+解码"的参考路径解码
- **THEN** 两条路径输出的 schema（含列顺序）与全部列值逐字节一致（差分测试语料覆盖：各标量类型、混合数值拓宽、嵌套对象/数组、跨行字段首现排序、缺失列 null、冲突列名、delete 事件）

#### Scenario: delete 事件不深拷贝 before
- **WHEN** 一条 `op="d"` 的 Envelope 的 `before` 含业务字段
- **THEN** 业务列取自 `before` 的字段值，`before` JSON 文本列完整保留变更前值，二者来自同一次解析的同一份值（无第二次遍历拷贝）

#### Scenario: 非对象 envelope 语义保持
- **WHEN** payload 为 JSON `null` 字面量或数组/数字等非对象值
- **THEN** 产出与既有行为一致的全 null 元信息单行（`op`/`ts_ms`/`source_db`/`source_table` 为 null，`before`/`source` 文本列为 `"null"`），不报错

#### Scenario: tombstone 与无效 JSON 语义保持
- **WHEN** 一批中混有零长 payload 与无效 JSON payload
- **THEN** 零长 payload 被跳过并记录 warn 日志，无效 payload 使整批报错（错误信息含 "Invalid Debezium envelope JSON"），与既有行为一致

