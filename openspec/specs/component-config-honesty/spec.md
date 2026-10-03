# component-config-honesty Specification

## Purpose
TBD - created by archiving change fix-plugin-p2-batch. Update Purpose after archive.
## Requirements
### Requirement: 元数据 schema SHALL 与真实配置面一致
每个组件经注册表暴露的配置 JSON Schema（`arkflow components`/`arkflow schema`/server `/component` API）SHALL 声明且仅声明该组件 serde 反序列化真实接受的字段：字段名、必填性、枚举变体与示例 MUST 与实现一致；MUST NOT 宣告不存在的字段、变体或语义（如不存在的轮询行为）。本变更落地范围内被修正的组件至少包括：`batch` processor（`count`/`timeout_ms`，移除幽灵 `size`/`interval`）、`sql` input（移除 `poll_interval`，修正"轮询"描述）、`redis` output（真实 `publish`/`list`/`hashes`/`strings` 变体）、`json` processor（`value_field`/`fields_to_include`，移除幽灵 `pretty`/`batch_size`）、`stdout` output（`append_newline`，移除幽灵 `pretty`）、`python` processor（真实字段集合，移除幽灵 `extra_packages`）。

#### Scenario: batch 元数据描述真实字段
- **WHEN** 查询 `batch` processor 的组件元数据
- **THEN** schema 字段为 `count`/`timeout_ms`（均必填），不含 `size`/`interval`

#### Scenario: redis 元数据只含真实变体
- **WHEN** 查询 `redis` output 的组件元数据
- **THEN** 变体为 `publish`/`list`/`hashes`/`strings` 且字段名正确，不含 `stream`/`channel`

### Requirement: 内置示例 SHALL 可通过真实构建
组件元数据携带的 `config_example`（存在时）SHALL 能通过该组件声明的 schema 校验，且能反序列化为该组件的真实配置结构（不做网络 IO 的构建路径 SHALL 能真实构建成功）。注册表 SHALL 有 CI 测试遍历全部组件执行上述校验，使"示例缺必填字段"类缺陷无法合入。

#### Scenario: 全量示例校验测试
- **WHEN** `cargo test --workspace` 运行
- **THEN** 存在遍历注册表全部组件元数据的测试，逐个校验 example 可通过 schema 校验与真实反序列化

#### Scenario: 缺必填字段的示例被拦截
- **WHEN** 某组件 example 缺少其必填字段（如 batch 缺 `timeout_ms`）
- **THEN** 上述测试失败并指名该组件与缺失字段

### Requirement: 宣传的能力 SHALL 实装或从配置面移除
被组件配置解析或文档宣传的能力 MUST 二选一：真实实装，或从配置面与文档中移除。本变更落地范围内：`kafka`/`mqtt`/`nats`/`redis` output 的 `value_field`（配置了该字段时逐行取指定列的值作 payload，复用 pulsar 既有语义——列缺失或类型不符报错，未配置时走 codec 编码路径不变）与 `websocket` input 的 `headers`（握手请求携带配置的 HTTP 头）SHALL 实装；`file`/`sql`/`modbus` input 的 `codec` 字段（无解码接入点的死配置）SHALL 从配置结构与元数据中移除。

#### Scenario: value_field 选取列值作 payload
- **WHEN** `kafka` output 配置 `value_field: "payload_col"` 且批次含 `payload_col` 列
- **THEN** 每行以该列的值（Utf8/Binary）作为消息体发送；未配置 `value_field` 时行为与既有版本一致

#### Scenario: value_field 指向缺失列时报错
- **WHEN** 配置的 `value_field` 列不存在于批次 schema
- **THEN** 写入返回指名缺失列的错误，不静默回退

#### Scenario: websocket 握手携带配置头
- **WHEN** `websocket` input 配置 `headers: { Authorization: "Bearer ..." }`
- **THEN** 建立连接的握手请求携带该 HTTP 头；未配置时握手行为不变

#### Scenario: 无接入点的死字段移除
- **WHEN** 查询 `file`/`sql`/`modbus` input 的组件元数据
- **THEN** 配置面不含 `codec` 字段，配置了该字段的 YAML 无法通过 schema 校验

#### Scenario: 死 codec 在构建期被拒绝
- **WHEN** 上述三个 input 的 stream 配置携带顶层 `codec:` 键（InputConfig 的 codec 字段）
- **THEN** 构建返回指名组件与原因的配置错误（对齐 sql output 的 codec 拒绝语义），而不是构建 codec 后静默丢弃其解码结果

