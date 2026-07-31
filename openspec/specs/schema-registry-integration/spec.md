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
codec SHALL 经 Schema Registry（Confluent REST：`GET {registry}/schemas/ids/{id}`）按 schema id 获取 Protobuf schema，并构建 `MessageDescriptor` 解码 payload。registry 返回错误（非 200 或连接失败）时 SHALL 返回错误。

#### Scenario: 首次拉取 schema
- **WHEN** codec 遇到一个未缓存的 schema id
- **THEN** 向 registry 请求 schema，构建 descriptor 解码 payload

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
`schema_registry` codec SHALL 经 `register_codec_builder` 注册，配置至少含 registry URL 与 message type；可经 input/output 的 codec 接入点使用。

#### Scenario: 配置 codec
- **WHEN** 一个 input 配置 `codec: { type: "schema_registry", registry_url: "...", message_type: "..." }`
- **THEN** codec 按 registry 解码消息，未配置时现有 codec 行为不受影响

### Requirement: 可选认证
codec SHALL 支持可选的 registry 认证（Basic auth 或 bearer token），经配置提供。

#### Scenario: 配置 Basic auth
- **WHEN** codec 配置含 `auth: { type: "basic", username, password }`
- **THEN** registry 请求携带 Basic Authorization 头

