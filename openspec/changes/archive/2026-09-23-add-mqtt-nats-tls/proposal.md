## Why

全组件 TLS 核查（PLANNING 7.3-5 延伸项）发现：MQTT 与 NATS 组件（input+output 共四个）完全没有 TLS 配置面——rumqttc 与 async-nats 客户端库原生支持 TLS 而组件未接线，生产环境无法安全接入。同批核查确认其余组件已覆盖：kafka（rdkafka security.protocol）、sql（sqlx ssl 配置）、mongodb（连接串）、redis（rediss:// URL）、http/reqwest 系（https URL）、websocket（wss）。本变更补齐 mqtt/nats 两个缺口。

## What Changes

- **mqtt input/output**：新增 `tls` 可选配置块——`enabled`（默认 true 当配置块存在）、`ca`（CA 证书路径，可选）、`client_cert`/`client_key`（mTLS 双向认证，可选）；启用后 rumqttc 走 `Transport::tls*`。
- **nats input/output**：新增 `tls_required: Option<bool>`——true 时 `ConnectOptions::tls_required()`（要求服务端 TLS），连接 URL 支持 `tls://` scheme（async-nats 原生）。
- 文档：组件页补充 TLS 字段；新增全组件 TLS 支持矩阵说明。

## Capabilities

### New Capabilities

<!-- mqtt/nats 的 TLS 属组件配置扩展，随各自组件文档承载；不新增独立能力 spec。 -->

### Modified Capabilities

<!-- 无 spec 级既有行为变更：均为组件配置新增可选字段。 -->

## Impact

- `crates/arkflow-plugin/src/input/mqtt.rs`、`output/mqtt.rs`、`input/nats.rs`、`output/nats.rs`。
- rumqttc/async-nats 均已原生支持 TLS（rumqttc Transport::tls*；async-nats tls_required/tls:// scheme），无新增依赖。
- 文档与组件页更新。

## Non-goals

- 不做 pulsar TLS（pulsar crate TLS 配置复杂，另行立项）；不做 modbus TLS。
- 不做客户端证书轮换/在线吊销检查。
- 不改既有无 TLS 行为（tls 块缺省时与现状逐字节一致）。
