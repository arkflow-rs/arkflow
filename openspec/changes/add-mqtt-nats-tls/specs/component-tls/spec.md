# component-tls 变更（Delta）

## ADDED Requirements

### Requirement: MQTT 组件 TLS 传输

MQTT input 与 output SHALL 接受可选 `tls` 配置块：`enabled`（默认 true 当配置块存在）、`ca`（CA 证书文件路径，可选）、`client_cert`/`client_key`（mTLS 双向认证路径，可选）。配置后 rumqttc SHALL 使用 TLS 传输（默认 rustls 根证书；提供 `ca` 时加载自定义 CA；提供客户端证书对时启用 mTLS）。`tls` 缺省时 SHALL 保持 TCP 传输现状。

#### Scenario: 启用 TLS 传输

- **WHEN** 配置 `tls: { enabled: true, ca: "/etc/ca.pem" }`
- **THEN** MQTT 连接使用 TLS 传输并加载该 CA 证书

#### Scenario: mTLS 客户端证书

- **WHEN** 配置 `tls` 含 `client_cert` 与 `client_key`
- **THEN** 连接同时携带客户端证书链（mTLS）

#### Scenario: 未配置 tls 时行为不变

- **WHEN** 不提供 `tls` 块
- **THEN** 连接使用 TCP 传输，与既有行为一致

### Requirement: NATS 组件 TLS 要求

NATS input 与 output SHALL 接受可选 `tls_required` 布尔配置：true 时 SHALL 调用 `ConnectOptions::tls_required(true)` 要求服务端 TLS；连接 URL 的 `tls://` scheme SHALL 由 async-nats 原生处理。未配置时行为不变。

#### Scenario: 要求服务端 TLS

- **WHEN** 配置 `tls_required: true`
- **THEN** 连接选项要求服务端 TLS，明文连接被拒绝
