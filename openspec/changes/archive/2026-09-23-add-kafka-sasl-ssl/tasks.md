# Tasks: add-kafka-sasl-ssl

## 1. 共享安全配置模块

- [x] 1.1 新建 `crates/arkflow-plugin/src/kafka_security.rs`：`SecurityProtocol` / `SaslMechanism`（serde snake_case 枚举）/ `SaslConfig` / `TlsConfig` / `KafkaSecurityConfig` 结构体，全部字段 serde default；在 `lib.rs` 挂载模块
- [x] 1.2 实现 `resolved_protocol()`：显式 `protocol` 优先，否则按 `sasl`/`tls` 子块存在性推断（sasl_ssl / sasl_plaintext / ssl / plaintext）
- [x] 1.3 实现 `apply(&self, &mut ClientConfig) -> Result<()>`：SASL 属性装配（`sasl.mechanisms`/`sasl.username`/`sasl.password`）、TLS 装配（`-----BEGIN` 前缀分流 `ssl.*.pem` 与 `ssl.*.location`、`ssl.key.password`、`enable.ssl.certificate.verification=false`）
- [x] 1.4 实现构建期校验：`sasl_*` 协议缺 `sasl` 块、PLAIN/SCRAM 缺 `username`/`password`、显式 `plaintext` 与 `sasl`/`tls` 块矛盾，错误信息指明字段
- [x] 1.5 模块单测：协议推断四态、显式优先、PEM/路径分流、SASL 属性 KV、全部校验错误路径

## 2. Kafka input 接入

- [x] 2.1 `KafkaInputConfig` 增加 `security: Option<KafkaSecurityConfig>`，在 `build_client_config`（`input/kafka.rs:226`）中调用 `apply`
- [x] 2.2 更新 input 组件 schema 元数据（`security` 字段 JSON schema）与描述
- [x] 2.3 input 侧单测：未配置 `security` 时客户端配置不含任何 `security.*`/`sasl.*`/`ssl.*` 属性（回归）；配置 SCRAM+内联 PEM 后属性正确

## 3. Kafka output 接入

- [x] 3.1 把 output 侧内联 `ClientConfig` 构建（`output/kafka.rs:171`）提取为 `build_client_config()`，行为不变；既有 output 单测通过
- [x] 3.2 `KafkaOutputConfig` 增加 `security: Option<KafkaSecurityConfig>`，在提取后的构建函数中调用 `apply`
- [x] 3.3 更新 output 组件 schema 元数据与描述
- [x] 3.4 output 侧单测：与 2.3 对齐（plaintext 回归 + SCRAM/TLS 属性断言 + 校验错误路径）

## 4. 文档与示例

- [x] 4.1 新增 `examples/kafka_input_sasl_ssl.yaml` 与 `examples/kafka_output_sasl_ssl.yaml`（SCRAM + 内联 PEM 占位），注册进 `docs/reference/example-manifest.json`；确认 `--validate` 离线通过，否则按规范标注 `"validate": false` + 原因
- [x] 4.2 更新 `docs/docs/components/0-inputs/kafka.md` 与 `docs/docs/components/3-outputs/kafka.md`：security 配置字段表、协议推断规则、内联 PEM 示例、`insecure_skip_verify` 警示（yaml 代码块按规范加 validate 分类标记）
- [x] 4.3 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重新生成 inventory 与 config-schema

## 5. 全量验证

- [x] 5.1 `cargo test --workspace --all-targets` 全绿
- [x] 5.2 `cargo clippy --workspace --all-targets` 无新告警
- [x] 5.3 `pnpm docs:check` 通过（component 页面所有权、yaml 分类、示例 manifest、锚点、zh-Hans 块对齐）
- [x] 5.4 对照 specs/kafka-security 场景清单逐条核对实现覆盖；`openspec validate add-kafka-sasl-ssl` 通过
