## Why

全组件 TLS 核查发现各网络组件的 TLS 能力与用法分散在不同组件页，且 pulsar 的 `pulsar+ssl://` 原生支持未在任何文档中说明（`PulsarConfigValidator::validate_service_url` 已同时校验 `pulsar://` 与 `pulsar+ssl://`）。需要一个集中的 TLS 支持矩阵，让运维一眼看清各组件如何启用加密传输。

## What Changes

- 部署文档新增「TLS 支持矩阵」节：全部网络组件（input/output/temporary）逐组件列出 TLS 启用方式（URL scheme / 配置块 / 连接串参数）。
- pulsar 组件文档补充 `pulsar+ssl://` 说明；pulsar common 新增 `pulsar+ssl://` URL 校验单测。
- 无产品行为变更（pulsar TLS 校验已存在，仅测试与文档补齐）。

## Capabilities

### New Capabilities

<!-- 无新能力：纯文档与测试补齐。 -->

### Modified Capabilities

<!-- 无需求级变更。 -->

## Impact

- `docs/docs/operate/recovery.md` 或部署文档：TLS 矩阵节（en/zh）。
- `crates/arkflow-plugin/src/pulsar/common.rs`：pulsar+ssl URL 校验单测。

## Non-goals

- 不做 pulsar 客户端证书/mTLS 配置扩展（pulsar crate 原生 URL scheme 已覆盖 TLS）。
- 不改任何组件连接行为。
