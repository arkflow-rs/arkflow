# Proposal: add-kafka-sasl-ssl

## Why

企业环境中的 Kafka 集群普遍要求认证与加密，而 ArkFlow 的 Kafka input/output 目前完全没有安全配置面：`KafkaInputConfig`（`crates/arkflow-plugin/src/input/kafka.rs:42-61`）与 `KafkaOutputConfig`（`crates/arkflow-plugin/src/output/kafka.rs:64-84`）没有任何 `security.protocol` / `sasl.*` / `ssl.*` 字段，两处 `ClientConfig` 构建（`input/kafka.rs:226`、`output/kafka.rs:171`）也只设置 `bootstrap.servers` 等明文属性——带 SASL/SSL 的集群一个都接不了。这是社区明确反馈的入场券级缺口（issue #902）。rdkafka 依赖已启用 `sasl` + `ssl-vendored` features（`crates/arkflow-plugin/Cargo.toml:58-64`），缺的只是配置面与属性装配，投入小、解锁面大。

## What Changes

- Kafka input 与 output 新增统一的 `security` 配置块：
  - `protocol`：`plaintext | ssl | sasl_plaintext | sasl_ssl`（可从 `sasl`/`tls` 子块存在性推断，显式声明优先）；
  - `sasl`：`mechanism`（`plain | scram-sha-256 | scram-sha-512`）+ `username` / `password`；
  - `tls`：`ca` / `cert` / `key`（支持文件路径或内联 PEM，按 `-----BEGIN` 前缀自动识别）、`key_password`、`insecure_skip_verify`。
- 两处 `ClientConfig` 装配安全属性，并将 output 侧构建提取为可离线单测的函数（对齐 input 侧已有的 `build_client_config`）。
- 构建期校验：`protocol` 声明为 `sasl_*` 但缺 `sasl` 块、SCRAM/PLAIN 缺用户名或密码、`tls.insecure_skip_verify` 无 `tls` 块等给出清晰错误。
- 组件 JSON schema、生成文档（inventory / config-schema）、组件文档页、示例 YAML 同步更新。

## Capabilities

### New Capabilities

- `kafka-security`: Kafka input/output 的认证与加密配置——protocol 选择与推断、SASL PLAIN/SCRAM 凭据、TLS 信任与客户端证书（路径或内联 PEM）、构建期校验与错误语义。

### Modified Capabilities

（无——现有 spec 的行为要求不变；`input-durability` / `exactly-once-output` / `schema-registry-integration` 与安全配置正交。）

## Impact

- **代码**：`crates/arkflow-plugin/src/input/kafka.rs`、`crates/arkflow-plugin/src/output/kafka.rs`；新增共享的安全配置模块（`crates/arkflow-plugin/src/` 下）。无 breaking：新字段全部 serde default，缺省行为与现状一致（plaintext）。
- **依赖**：无新增——rdkafka 的 `sasl` / `ssl-vendored` features 已启用；workspace 中已声明但未使用的 `aws-msk-iam-sasl-signer` 保持预留。
- **生成文档**：`docs/reference/component-inventory.json`、`docs/static/config-schema.json` 需用 `ARKFLOW_REGENERATE_DOCS=1` 重新生成。
- **文档**：`docs/docs/components/0-inputs/kafka.md`、`docs/docs/components/3-outputs/kafka.md` 增补 security 配置；新增示例 YAML 并注册 `docs/reference/example-manifest.json`。无新增组件，README 清单不受影响。

## Non-goals

- **OAUTHBEARER / AWS MSK IAM**：需要 token 刷新回调与 AWS 凭据链，属独立 change（依赖已预留）。
- **GSSAPI / Kerberos**：需额外启用 rdkafka `gssapi(-vendored)` feature 与平台 Kerberos 库，单独评估。
- **Secret 引用机制**（如 `${env:...}` / Secret Manager 间接引用）：控制面 Hub 阶段 4 的规划项，本 change 密码字段先用明文字符串。
- **其他组件（MinIO/S3、HTTP、Redis 等）的 TLS 核查**：方向⑤后续切片。
- **mTLS 之外的 broker 侧策略**（授权、quota、配额管理）：超出客户端配置面。
