# component-registry-export Delta

## ADDED Requirements

### Requirement: The engine configuration JSON Schema SHALL match the implemented configuration surface
引擎配置 JSON Schema 中每个配置节点的字段集合、类型与默认值 SHALL 与实现一致：实现支持的合法字段 MUST 出现在对应 schema 节点中（不得因 `additionalProperties: false` 把合法配置判为非法），schema 声明的默认值 MUST 与实现默认值一致（动态默认值如 CPU 数 SHALL 在描述中说明而非编造静态值），已弃用字段 SHALL 标注 deprecated。

#### Scenario: health_check 扩展字段通过校验
- **WHEN** 配置在 `health_check` 节点使用 `hub_urls`、`node_id`、`node_token`、`agent_lease_ttl_ms`、`agent_session_ttl_ms`、`data_port`、`data_host` 或 `observability` 字段并通过 schema 校验
- **THEN** 校验通过（这些是实现支持的合法字段），不被 `additionalProperties: false` 拒绝

#### Scenario: thread_num 默认值诚实
- **WHEN** 读者查看 schema 中 `thread_num` 的默认值信息
- **THEN** schema 不声明失真的静态默认值 1，而是说明默认为 CPU 数量
