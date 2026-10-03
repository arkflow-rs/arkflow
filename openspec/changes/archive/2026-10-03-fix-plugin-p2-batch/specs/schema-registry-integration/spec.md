# schema-registry-integration 增量

## MODIFIED Requirements

### Requirement: 按 schema id 从 Schema Registry 获取 schema
codec SHALL 经 Schema Registry（Confluent REST：`GET {registry}/schemas/ids/{id}`）按 schema id 获取 schema，并按响应的 `schemaType` 分派解码：`PROTOBUF` 构建 `MessageDescriptor` 解码 payload，`AVRO` 构建 Avro schema 解码 payload。`schemaType` 缺省时 SHALL 按 schema 文本内容判定：文本 trim 后为 JSON 对象则按 `AVRO` 解析，否则按 `PROTOBUF` 解析；两种解析都失败时 SHALL 返回列出两种尝试的错误，不静默猜测。返回其他 `schemaType` 时 SHALL 返回错误。registry 返回错误（非 200 或连接失败）时 SHALL 返回错误。

#### Scenario: 首次拉取 Protobuf schema
- **WHEN** codec 遇到一个未缓存的 schema id，registry 返回 `schemaType: PROTOBUF`
- **THEN** 构建 descriptor 解码 payload

#### Scenario: 首次拉取 Avro schema
- **WHEN** codec 遇到一个未缓存的 schema id，registry 返回 `schemaType: AVRO`
- **THEN** 解析 Avro schema 并按 Avro 映射解码 payload 为 Arrow 列

#### Scenario: schemaType 缺省且 schema 文本为 JSON 对象
- **WHEN** registry 响应不含 `schemaType` 字段且 schema 文本为 Avro JSON 对象（如 `{"type":"record",...}`）
- **THEN** 按 Avro schema 解码（旧版 registry 对 Avro 常省略该字段，不再误判为 Protobuf）

#### Scenario: schemaType 缺省且 schema 文本为 proto 源码
- **WHEN** registry 响应不含 `schemaType` 字段且 schema 文本为 protobuf 源码
- **THEN** 按 Protobuf descriptor 解码

#### Scenario: 两种解析都失败
- **WHEN** `schemaType` 缺省且 schema 文本既非 JSON 对象又无法解析为 protobuf
- **THEN** codec 返回列出两种尝试的错误

#### Scenario: registry 不可达
- **WHEN** registry 返回非 200 或连接失败
- **THEN** codec 返回错误，不静默跳过

### Requirement: 多版本 schema 解码
codec SHALL 支持同一流中不同 schema id（不同 schema 版本）的消息，各自用对应版本的 descriptor 解码。同一 batch 内不同版本解码出的批次 schema 不一致（真实 schema 演进，如新增/缺失字段）时，codec SHALL 先把各批次归一到并集 schema（缺失列以 null 填充、列序取首批次顺序并追加新列）再合并；同名同列类型冲突（无法 null 填充消解）时 SHALL 返回指明列名与两个类型的错误。

#### Scenario: 同 batch 多版本
- **WHEN** 一个 batch 含 schema id=1 与 schema id=2 的消息（两版 schema）
- **THEN** 各自用对应 descriptor 解码，不互相干扰

#### Scenario: 真实 schema 演进合并
- **WHEN** 一个 batch 混有 id=1（少一列）与 id=2（多一列）的消息，两版 schema 文本不同
- **THEN** 产出一个并集 schema 的批次，id=1 消息的新增列为 null，合并不报错

#### Scenario: 同名不同类型报错
- **WHEN** 两版 schema 中同名列类型不同（如 Utf8 与 Int64）
- **THEN** 解码返回错误，错误信息含列名与两个类型

### Requirement: 主题兼容性门禁
codec SHALL 支持可选配置 `subject` 与 `min_compatibility`（`none`/`backward`/`forward`/`full`，缺省 `none` 即不检查）。配置后 codec SHALL 在首次 decode 时经 `GET {registry}/config/{subject}?defaultToGlobal=true` 获取 subject 兼容级别（subject MUST 作为单个路径段 percent-encode 后拼接，含 `/`、空格、`%` 等字符时不得改变 URL 结构），并按秩比较：`NONE`=0 < `BACKWARD`/`FORWARD`（含 `*_TRANSITIVE` 变体）=1 < `FULL`（含 `FULL_TRANSITIVE`）=2。subject 实际级别低于配置的最低秩时 SHALL 返回错误（fail-fast），错误信息含 subject、实际级别与要求级别。检查的**通过**结论 MUST 在 codec 生命周期内永久缓存，不逐消息重复请求；**失败**结论 SHALL NOT 永久固化——后续 decode SHALL 以不小于 30 秒的最小间隔重试检查（间隔内的 decode 直接返回上次错误、不发请求），使 registry 瞬时故障（网络/5xx）恢复后 codec 可自愈。未配置 `subject` 时 MUST 不发起该检查。

#### Scenario: 达到最低兼容级别放行
- **WHEN** 配置 `min_compatibility: backward`，registry 返回 subject 级别 `BACKWARD`
- **THEN** 门禁通过，消息正常解码

#### Scenario: 低于最低兼容级别报错
- **WHEN** 配置 `min_compatibility: full`，registry 返回 subject 级别 `NONE`
- **THEN** 解码返回错误，错误信息含 subject、实际级别 `NONE` 与要求 `full`

#### Scenario: transitive 变体按基础级别比较
- **WHEN** 配置 `min_compatibility: backward`，registry 返回 `BACKWARD_TRANSITIVE`
- **THEN** 门禁通过

#### Scenario: 未配置 subject 不检查
- **WHEN** 配置不含 `subject`
- **THEN** 不请求 config 端点，解码行为不受影响

#### Scenario: subject 含特殊字符按单路径段编码
- **WHEN** 配置 `subject: "orders/v2 prod%final"` 且配置了 `min_compatibility`
- **THEN** config 请求路径为 `/config/orders%2Fv2%20prod%25final`（percent-encoded 单路径段），返回的兼容级别正常参与秩比较

#### Scenario: 门禁通过结论缓存
- **WHEN** 门禁通过后连续解码多条消息
- **THEN** 仅首次发起 config 请求

#### Scenario: 门禁瞬时失败后恢复自愈
- **WHEN** 首次检查因 registry 瞬时故障失败，30 秒间隔到期后 registry 恢复且返回达标的兼容级别
- **THEN** 后续 decode 重试检查并通过，codec 恢复正常解码；间隔期内的 decode 返回上次错误且不发起请求
