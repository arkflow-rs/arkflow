## Why

全组件 TLS 核查的最后一项：pulsar 组件原生支持 `pulsar+ssl://` URL scheme（TLS 加密传输），但组件页未文档化此能力，且 `PulsarConfigValidator::validate_service_url` 的 `pulsar+ssl://` 校验逻辑缺少单测覆盖。

## What Changes

- pulsar 组件文档页补充 `pulsar+ssl://` TLS 说明（en/zh）
- `pulsar/common.rs` 新增单测覆盖 `pulsar+ssl://` URL 校验
- 全组件 TLS 核查至此完整

## Capabilities

### New Capabilities

<!-- 无新能力：纯文档与测试补齐。 -->

### Modified Capabilities

<!-- 无需求级变更。 -->

## Impact

- `crates/arkflow-plugin/src/pulsar/common.rs`：新增 `pulsar+ssl://` URL 校验单测。
- 文档：pulsar 组件页补充 TLS 说明。

## Non-goals

- 不做 pulsar mTLS（CA/客户端证书配置，pulsar crate TLS builder 较复杂另行立项）。
- 不改任何组件连接行为。
