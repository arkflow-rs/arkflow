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

### Requirement: 按 schema id 缓存 descriptor
codec SHALL 按 schema id 缓存已构建的 `MessageDescriptor`，使同一 id 的后续消息不再发起 registry 请求。

#### Scenario: 重复 id 命中缓存
- **WHEN** codec 连续遇到同一 schema id 的多条消息
- **THEN** 仅首次发起 registry 请求，后续命中缓存解码

### Requirement: 多版本 schema 解码
codec SHALL 支持同一流中不同 schema id（不同 schema 版本）的消息，各自用对应版本的 descriptor 解码。同 id 的消息 SHALL 列式累积为一个多行批次：行内容、列序、列类型均与逐消息解码后合并的结果一致；列 nullable 一致地按 writer schema（此前多消息批经并集合并会丢失非空标记，而单消息批保留——本变更消除该不一致）。同一 batch 内不同版本解码出的批次 schema 不一致（真实 schema 演进，如新增/缺失字段）时，codec SHALL 先把各分组批次归一到并集 schema（缺失列以 null 填充、列序取首个 schema id 分组内首条消息的字段顺序并追加后续分组的新列；行按 schema id 分组排列——组内保持消息顺序、组间按首次出现）再合并；同名同列类型冲突（无法 null 填充消解）时 SHALL 返回指明列名与两个类型的错误。

#### Scenario: 同 batch 多版本
- **WHEN** 一个 batch 含 schema id=1 与 schema id=2 的消息（两版 schema）
- **THEN** 各自用对应 descriptor 解码，不互相干扰

#### Scenario: 单版本批列式累积等价
- **WHEN** 一个 batch 的全部消息共享同一 schema id
- **THEN** 产出一个多行批次，字段顺序为该 schema 的声明顺序，行内容、列类型与"逐消息解码再合并"的既有结果一致；列 nullable 按 writer schema（非空列不再因合并被放宽为 nullable）

#### Scenario: 真实 schema 演进合并
- **WHEN** 一个 batch 混有 id=1（少一列）与 id=2（多一列）的消息，两版 schema 文本不同
- **THEN** 产出一个并集 schema 的批次，id=1 消息的新增列为 null，合并不报错

#### Scenario: 同名不同类型报错
- **WHEN** 两版 schema 中同名列类型不同（如 Utf8 与 Int64）
- **THEN** 解码返回错误，错误信息含列名与两个类型

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

### Requirement: 可选认证
codec SHALL 支持可选的 registry 认证（Basic auth 或 bearer token），经配置提供。

#### Scenario: 配置 Basic auth
- **WHEN** codec 配置含 `auth: { type: "basic", username, password }`
- **THEN** registry 请求携带 Basic Authorization 头

### Requirement: Protobuf 批级列式解码

一次 decode 调用内，同一 Protobuf schema id 的全部消息 SHALL 累积为单一多行 Arrow 批次（列式构造）：descriptor→Arrow 列映射（列集、类型、字段号）在该批内 SHALL 只计算一次，且 SHALL 在首条消息解码成功后的字段遍历中导出（kind 拒绝不先于消息解析错误）；codec SHALL NOT 为每条消息独立构造单行 RecordBatch 或独立的每消息列分配。列集 SHALL 为 descriptor 全字段集且全部 nullable。输出 SHALL 与既有"逐消息解码后归一合并"逐列一致：行序与组内消息序一致、proto3 隐式存在性的默认值语义不变（未设置标量字段取默认值而非 null）、错误类型、错误文案与首错归因不变。

#### Scenario: 同 id 多消息累积为单一批次

- **WHEN** 一次 decode 收到 N 条同一 Protobuf schema id 的消息
- **THEN** 产出一个多行批次（行数 = N），行序与消息序一致，与逐消息解码加归一合并的输出逐列相等

#### Scenario: 未设置字段保持 proto3 默认值语义

- **WHEN** 某消息未设置 string 标量字段
- **THEN** 该列为空字符串（默认值）而非 null，与既有逐消息路径一致

#### Scenario: kind 拒绝文案与归因不变

- **WHEN** descriptor 含不受支持的字段类型（嵌套 message/repeated/map/oneof）
- **THEN** 返回的拒绝文案与既有逐消息路径逐字一致，且归因于首条消息

#### Scenario: 混合组次序稳定

- **WHEN** 一个 batch 混有不同 schema id 的 Protobuf 消息
- **THEN** 各组按首次出现顺序参与归一合并，组内行序与消息序一致（与既有分组行为一致）

