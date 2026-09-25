# documentation-quality 变更（Delta）

## ADDED Requirements

### Requirement: pulsar TLS 传输文档

pulsar 组件文档 SHALL 说明 `pulsar+ssl://` URL scheme 的 TLS 加密传输能力，使运维人员能够了解加密连接的启用方式。

#### Scenario: pulsar ssl 文档存在

- **WHEN** 查阅 pulsar 组件文档
- **THEN** 包含 `pulsar+ssl://` URL scheme 的 TLS 说明
