## ADDED Requirements

### Requirement: 注册表元数据 SHALL 通过 serde 字段一致性门禁

注册表中每个带配置 schema 的组件 SHALL 由 CI 结构测试对照其真实配置结构体（含 `#[serde(flatten)]` 与嵌套类型）校验，测试从源码解析 serde 实际接受的字段名、必填性与默认属性，并按以下规则比对 schema：在声明 `additionalProperties: false` 的节点上，serde 接受的每个输入字段 MUST 出现在 `properties` 中；schema `properties` 中的每个键 MUST 是 serde 接受的字段（MUST NOT 宣告结构体不读取的键）；schema `required` 中的每个键 MUST 是 serde 自身必填的字段，或经带理由的豁免清单登记为"由 builder 强制"；serde 自身必填的每个字段 MUST 出现在 schema `required` 中；声明了 `default` 的属性 MUST 对应 serde 可省略的字段。每个注册组件 MUST 被该测试覆盖（无配置的 unit 组件除外）。豁免 SHALL 仅经显式清单（刻意隐藏的内部字段、builder 强制的必填字段），且豁免条目在其不再适用时 MUST 使测试失败。

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

#### Scenario: 新组件未纳入门禁
- **WHEN** 注册了一个带配置 schema 的新组件而未在一致性测试中登记其配置结构体
- **THEN** 测试失败并列出未覆盖的组件，提示补登记

#### Scenario: 豁免清单不腐化
- **WHEN** 豁免清单中的某条目已不再需要（被隐藏的字段已进入 schema，或登记为 builder 强制的字段已改为 serde 必填）
- **THEN** 测试失败并指名该过期条目

#### Scenario: 修复后的注册表如实反映配置面
- **WHEN** 查看 `kafka` input、`pulsar` input/output、`milvus_search` 的组件元数据
- **THEN** schema 分别包含 `transactional_offsets`、`tls`、`concurrency`，且 `http` input schema 不含 `method`/`interval`

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
