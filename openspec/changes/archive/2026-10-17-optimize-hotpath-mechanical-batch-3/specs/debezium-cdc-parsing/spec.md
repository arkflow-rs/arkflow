## ADDED Requirements

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
