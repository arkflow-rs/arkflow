# secure-durable-control-plane 变更（Delta）

## ADDED Requirements

### Requirement: TLS 支持矩阵文档

部署文档 SHALL 提供一份覆盖全部网络组件的 TLS 支持矩阵，逐组件说明加密传输的启用方式（URL scheme、配置块或连接串参数）；pulsar 组件的 `pulsar+ssl://` scheme 支持 SHALL 有单测覆盖。

#### Scenario: 矩阵覆盖全部网络组件

- **WHEN** 运维查阅 TLS 支持矩阵
- **THEN** 每个具备网络能力的 input/output/temporary 组件均列出其 TLS 启用方式
