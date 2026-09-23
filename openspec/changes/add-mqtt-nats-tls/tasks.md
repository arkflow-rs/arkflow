# Tasks: add-mqtt-nats-tls

## 1. MQTT TLS（input + output）

- [ ] 1.1 `input/mqtt.rs` + `output/mqtt.rs`：`MqttTlsConfig`（enabled/ca/client_cert/client_key）+ Transport 选择（tls_with_default_config 或带 CA 的 Simple 配置）+ metadata schema
- [ ] 1.2 测试：tls 配置解析三态（缺省/启用+CA/启用+mTLS）、未启用时 Transport 不变

## 2. NATS TLS（input + output）

- [ ] 2.1 `input/nats.rs` + `output/nats.rs`：`tls_required: Option<bool>` → `ConnectOptions::tls_required(true)` + metadata schema
- [ ] 2.2 测试：tls_required 解析与 ConnectOptions 生效路径（无服务端依赖的单元验证）

## 3. 文档与全量验证

- [ ] 3.1 组件页（mqtt input/output、nats input/output）补 TLS 字段说明；`pnpm docs:check` 通过
- [ ] 3.2 `cargo test --workspace --all-targets` 连续 2 轮全绿；clippy 无新告警
- [ ] 3.3 `openspec validate add-mqtt-nats-tls` 通过；同步归档、更新 PLANNING.md、推送
