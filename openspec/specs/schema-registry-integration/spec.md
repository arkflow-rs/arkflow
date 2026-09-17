# schema-registry-integration Specification

## Purpose
TBD - created by archiving change add-schema-registry. Update Purpose after archive.
## Requirements
### Requirement: Confluent wire format 解析
`schema_registry` codec 收到消息字节时，SHALL 按 Confluent wire format 解析：首字节为 magic（MUST 为 `0x00`），随后 4 字节为大端 schema id，其余为 payload。magic 不为 `0x00` 或消息短于 5 字节时 SHALL 返回错误。

#### Scenario: 有效 wire format
- **WHEN** codec 收到 `0x00 0x00 0x00 0x00 0x01 <payload>`
- **THEN** 剥出 schema id=1 与 payload，进入按 id 解码

#### Scenario: magic byte 非法
- **WHEN** 消息首字节不为 `0x00`
- **THEN** codec 返回错误，不尝试解码

#### Scenario: 消息过短
- **WHEN** 消息不足 5 字节
- **THEN** codec 返回错误

### Requirement: 按 schema id 从 Schema Registry 获取 schema
codec SHALL 经 Schema Registry（Confluent REST：`GET {registry}/schemas/ids/{id}`）按 schema id 获取 schema，并按响应的 `schemaType` 分派解码：`PROTOBUF` 构建 `MessageDescriptor` 解码 payload，`AVRO` 构建 Avro schema 解码 payload。`schemaType` 缺省时 MUST 视为 `PROTOBUF`（保持既有行为）。返回其他 `schemaType` 时 SHALL 返回错误。registry 返回错误（非 200 或连接失败）时 SHALL 返回错误。

#### Scenario: 首次拉取 Protobuf schema
- **WHEN** codec 遇到一个未缓存的 schema id，registry 返回 `schemaType: PROTOBUF`
- **THEN** 构建 descriptor 解码 payload

#### Scenario: 首次拉取 Avro schema
- **WHEN** codec 遇到一个未缓存的 schema id，registry 返回 `schemaType: AVRO`
- **THEN** 解析 Avro schema 并按 Avro 映射解码 payload 为 Arrow 列

#### Scenario: schemaType 缺省视为 Protobuf
- **WHEN** registry 响应不含 `schemaType` 字段
- **THEN** 按 Protobuf descriptor 解码（与既有版本行为一致）

#### Scenario: registry 不可达
- **WHEN** registry 返回非 200 或连接失败
- **THEN** codec 返回错误，不静默跳过

### Requirement: 按 schema id 缓存 descriptor
codec SHALL 按 schema id 缓存已构建的 `MessageDescriptor`，使同一 id 的后续消息不再发起 registry 请求。

#### Scenario: 重复 id 命中缓存
- **WHEN** codec 连续遇到同一 schema id 的多条消息
- **THEN** 仅首次发起 registry 请求，后续命中缓存解码

### Requirement: 多版本 schema 解码
codec SHALL 支持同一流中不同 schema id（不同 schema 版本）的消息，各自用对应版本的 descriptor 解码。

#### Scenario: 同 batch 多版本
- **WHEN** 一个 batch 含 schema id=1 与 schema id=2 的消息（两版 schema）
- **THEN** 各自用对应 descriptor 解码，不互相干扰

### Requirement: 作为 codec 注册并可配置
`schema_registry` codec SHALL 经 `register_codec_builder` 注册，配置含 registry URL 与可选认证；`message_type` SHALL 为可选配置：仅当解码 Protobuf schema 的消息时需要（Avro schema 不需要）。配置 Avro 数据流时可省略 `message_type`；遇到 Protobuf id 而 `message_type` 未配置时 SHALL 返回指明原因的错误。

#### Scenario: 配置 Avro 数据流（无 message_type）
- **WHEN** 一个 input 配置 `codec: { type: "schema_registry", registry_url: "..." }` 且数据为 Confluent Avro wire format
- **THEN** codec 按 registry 返回的 Avro schema 解码消息

#### Scenario: Protobuf id 缺少 message_type
- **WHEN** codec 收到 Protobuf schema id 的消息但配置缺 `message_type`
- **THEN** codec 返回错误，错误信息指明需要 `message_type`

### Requirement: Avro 解码映射
codec 对 Avro payload SHALL 按 writer schema 解析为 Avro 值，并按扁平约定映射为单 schema Arrow batch：顶层 record 字段为列。类型映射 SHALL 至少覆盖：`null`→Null、`boolean`→Boolean、`int`→Int32、`long`→Int64、`float`→Float32、`double`→Float64、`bytes`/`fixed`→Binary、`string`/`enum`→Utf8；逻辑类型 `date`→Date32、`time-millis`→Time32(ms)、`time-micros`→Time64(µs)、`timestamp-millis`→Timestamp(ms, UTC)、`timestamp-micros`→Timestamp(µs, UTC)、`decimal`→Decimal128、`uuid`→Utf8。`[null, T]` 双分支 union SHALL 映射为 T 的 nullable 列。嵌套 record、array、map 与多于两分支的 union SHALL 返回明确错误（与 Protobuf 解码的扁平约定一致）。Encoder 行为 MUST 不变（line-delimited JSON，与 registry 无关）。

#### Scenario: 基础类型与逻辑类型映射
- **WHEN** 一条 Avro 消息含 string/int/long/double/boolean 字段与 timestamp-millis 逻辑类型字段
- **THEN** 解码为对应 Arrow 类型的列，值正确

#### Scenario: nullable union
- **WHEN** 字段 schema 为 `["null", "string"]` 且某行该字段为 null、另一行有值
- **THEN** 产出 nullable Utf8 列，null 行为空值

#### Scenario: 不支持的嵌套结构
- **WHEN** Avro schema 顶层字段为嵌套 record / array / map
- **THEN** 解码返回错误，错误信息指明不支持的字段与类型

#### Scenario: 多版本 Avro 解码
- **WHEN** 同一 batch 含 Avro schema id=1 与 id=2 的消息
- **THEN** 各自用对应版本 schema 解码，互不干扰

### Requirement: 主题兼容性门禁
codec SHALL 支持可选配置 `subject` 与 `min_compatibility`（`none`/`backward`/`forward`/`full`，缺省 `none` 即不检查）。配置后 codec SHALL 在首次 decode 时经 `GET {registry}/config/{subject}?defaultToGlobal=true` 获取 subject 兼容级别（subject MUST 作为单个路径段 percent-encode 后拼接，含 `/`、空格、`%` 等字符时不得改变 URL 结构），并按秩比较：`NONE`=0 < `BACKWARD`/`FORWARD`（含 `*_TRANSITIVE` 变体）=1 < `FULL`（含 `FULL_TRANSITIVE`）=2。subject 实际级别低于配置的最低秩时 SHALL 返回错误（fail-fast），错误信息含 subject、实际级别与要求级别；检查结果 MUST 在 codec 生命周期内缓存，不逐消息重复请求。未配置 `subject` 时 MUST 不发起该检查。

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

#### Scenario: 门禁结果缓存
- **WHEN** 门禁通过后连续解码多条消息
- **THEN** 仅首次发起 config 请求

### Requirement: 可选认证
codec SHALL 支持可选的 registry 认证（Basic auth 或 bearer token），经配置提供。

#### Scenario: 配置 Basic auth
- **WHEN** codec 配置含 `auth: { type: "basic", username, password }`
- **THEN** registry 请求携带 Basic Authorization 头
