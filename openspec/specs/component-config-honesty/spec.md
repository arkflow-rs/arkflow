# component-config-honesty Specification

## Purpose

Define what an honest component configuration surface is: the JSON Schema, built-in example, and documented capabilities a component publishes through the registry must describe exactly the configuration its implementation accepts — the same fields, requiredness, defaults, and enum values — must advertise nothing the implementation does not do, and must never drop a removed key silently. The agreement is enforced by CI tests (the schema against the config struct, the examples against the real builders), not left to review.

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

### Requirement: 从配置面移除的键 SHALL 在存量配置中显式拒绝
组件配置面移除某字段后，构建路径 SHALL 检测存量配置仍携带该键的情况并返回指名该键与移除原因的显式错误：MUST NOT 依赖 serde 对未知键的静默丢弃，使"配置已失效"对用户不可见。首个落地实例：`pulsar` 的 `ca_file`（驱动无独立 `ca_file` 设置，从未生效；私有 CA 经 `tls.certificate_chain_file` 提供）。

#### Scenario: 存量配置携带 ca_file 启动报错
- **WHEN** pulsar 组件配置包含 `ca_file` 键
- **THEN** 构建返回显式错误，错误信息指名 `ca_file`、说明驱动无独立 `ca_file` 设置并指导改用 `tls.certificate_chain_file` 或移除该键；MUST NOT 静默忽略

#### Scenario: schema 不再宣告 ca_file
- **WHEN** 生成 pulsar 组件的配置 schema 或 `arkflow schema` 输出
- **THEN** 输出中不含 `ca_file` 字段

### Requirement: 注册表元数据 SHALL 通过 serde 字段一致性门禁

注册表中每个带配置 schema 的组件 SHALL 由 CI 结构测试对照其真实配置结构体（含 `#[serde(flatten)]` 与嵌套类型）校验，测试从源码解析 serde 实际接受的字段名、必填性与默认属性，并按以下规则比对 schema：在声明 `additionalProperties: false` 的节点上，serde 接受的每个输入字段 MUST 出现在 `properties` 中；schema `properties` 中的每个键 MUST 是 serde 接受的字段（MUST NOT 宣告结构体不读取的键）；schema `required` 中的每个键 MUST 是 serde 自身必填的字段，或经带理由的豁免清单登记为"由 builder 强制"；serde 自身必填的每个字段 MUST 出现在 schema `required` 中；声明了 `default` 的属性 MUST 对应 serde 可省略的字段；字符串枚举 schema 列出的取值 MUST 恰为 serde 接受的变体名，MUST NOT 列出 serde 拒绝的取值，也 MUST NOT 遗漏可接受的变体。每个注册组件 MUST 被该测试覆盖（无配置的 unit 组件除外）。豁免 SHALL 仅经显式清单（刻意隐藏的内部字段、builder 强制的必填字段），且豁免条目在其不再适用时 MUST 使测试失败。

#### Scenario: 结构体新增字段未进 schema
- **WHEN** 某组件配置结构体新增一个 serde 字段而注册 schema（`additionalProperties: false`）未声明该字段
- **THEN** 一致性测试失败，指名组件类型、结构体与缺失字段

#### Scenario: schema 宣告结构体不读取的键
- **WHEN** 某组件 schema 的 `properties` 含配置结构体不接受的键
- **THEN** 一致性测试失败，指名该幽灵键

#### Scenario: 必填性漂移
- **WHEN** schema `required` 列出 serde 可省略的字段（且未登记为 builder 强制），或 serde 必填字段未列入 schema `required`
- **THEN** 一致性测试失败，指名字段与漂移方向

#### Scenario: 声明了不存在的默认值
- **WHEN** schema 为某属性声明 `default` 而该字段在 serde 中不可省略
- **THEN** 一致性测试失败，指名该字段

#### Scenario: 枚举取值漂移
- **WHEN** 字符串枚举 schema 列出 serde 不接受的取值（如大小写不符的 `Exclusive`）或遗漏某个可接受的变体
- **THEN** 一致性测试失败，指名该取值或变体

#### Scenario: 新组件未纳入门禁
- **WHEN** 注册了一个带配置 schema 的新组件而未在一致性测试中登记其配置结构体
- **THEN** 测试失败并列出未覆盖的组件，提示补登记

#### Scenario: 豁免清单不腐化
- **WHEN** 豁免清单中的某条目已不再需要（被隐藏的字段已进入 schema，或登记为 builder 强制的字段已改为 serde 必填）
- **THEN** 测试失败并指名该过期条目

#### Scenario: 修复后的注册表如实反映配置面
- **WHEN** 查看 `kafka` input、`pulsar` input/output、`milvus_search` 的组件元数据
- **THEN** schema 分别包含 `transactional_offsets`、`tls`、`concurrency`，`http` input schema 不含 `method`/`interval`，`pulsar` input 的 `subscription_type` 只列出 serde 接受的小写取值，`kafka` output 的 `compression` 不含 `zstd`

### Requirement: schema 承诺的默认值与移除键 SHALL 与实现一致

组件 schema 为某字段声明 `default` 时，省略该字段的配置 SHALL 被组件接受并取得所声明的默认值（默认值 MUST 由 serde 属性兑现，而非仅存在于文档）。从配置面移除的键 SHALL 在存量配置中被构建路径显式拒绝并指名该键与原因（沿用"从配置面移除的键 SHALL 在存量配置中显式拒绝"的语义）。首批落地：`kafka` input `start_from_latest` 默认 `false`、`http` output `method` 默认 `POST`；`http` input 的 `method`/`interval`（不存在的轮询客户端模式）被移除并显式拒绝。

#### Scenario: 省略 start_from_latest
- **WHEN** `kafka` input 配置省略 `start_from_latest`
- **THEN** 配置被接受且等价于 `start_from_latest: false`（从最早位点开始）

#### Scenario: 省略 http output method
- **WHEN** `http` output 配置省略 `method`
- **THEN** 配置被接受且请求方法为 `POST`

#### Scenario: http input 携带已移除的客户端键
- **WHEN** `http` input 配置携带 `method` 或 `interval` 键
- **THEN** 构建返回指名该键的配置错误，说明该 input 仅以服务端方式运行，MUST NOT 静默忽略

