# Capability: Component TLS

## Purpose

Define the TLS transport configuration surface for network-connected ArkFlow components (MQTT, NATS, and future additions), ensuring encrypted transport is uniformly configurable and documented. (Derived from the `add-mqtt-nats-tls` and `add-tls-support-matrix` changes.)

## Requirements

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

### Requirement: NATS 组件经 URL scheme 支持 TLS

NATS input 与 output SHALL 支持 `tls://` 连接 URL scheme（async-nat s 连接器原生协商 TLS），组件代码无需额外 TLS 配置字段；文档 SHALL 说明该用法。

#### Scenario: tls:// URL 原生协商

- **WHEN** 配置 `url: "tls://nats.example.com:4422"`
- **THEN** 连接经 async-nats 连接器以 TLS 协商建立（原生支持，无组件级开关）

