# Design: add-kafka-sasl-ssl

## Context

Kafka input（`crates/arkflow-plugin/src/input/kafka.rs`）与 output（`crates/arkflow-plugin/src/output/kafka.rs`）各自维护配置结构体与 `ClientConfig` 装配：input 侧已把装配提取为可单测的 `build_client_config`（`input/kafka.rs:226`），output 侧仍内联在 connect 路径里（`output/kafka.rs:171`）。两处均只设置 `bootstrap.servers` 等明文属性，无任何 `security.*` / `sasl.*` / `ssl.*`。

有利现状：rdkafka 已启用 `sasl` + `ssl-vendored` features（`crates/arkflow-plugin/Cargo.toml:58-64`），vendored librdkafka 2.12 原生支持内联 PEM 属性 `ssl.ca.pem` / `ssl.certificate.pem` / `ssl.key.pem`（已在 cargo 缓存的 CONFIGURATION.md 中核实）。因此本 change 纯属配置面 + 属性装配，不动依赖。

## Goals / Non-Goals

**Goals:**
- input/output 复用同一个安全配置结构与装配逻辑，一次实现两端生效
- 支持 PLAIN、SCRAM-SHA-256/512 认证与 TLS 加密/mTLS（含客户端证书）
- 证书支持文件路径与内联 PEM 两种形态（控制面把配置存 DB，容器场景无本地文件，内联是刚需）
- 配置错误在构建期（不连 broker）fail-fast，错误信息指明字段
- 全部可离线单测

**Non-Goals:**（与 proposal 一致）
- OAUTHBEARER / MSK IAM、GSSAPI/Kerberos、Secret 引用机制、其他组件 TLS、broker 侧策略

## Decisions

### D1：共享模块 `crates/arkflow-plugin/src/kafka_security.rs`

新增顶层模块（对齐 `auth_middleware.rs`、`context_pool.rs` 的布局惯例），承载：

- `KafkaSecurityConfig { protocol: Option<SecurityProtocol>, sasl: Option<SaslConfig>, tls: Option<TlsConfig> }`（全部 serde default，`deny_unknown_fields` 未开启以保持宽松）；
- `SaslConfig { mechanism: SaslMechanism, username/password: Option<String> }`，`SaslMechanism` 为 serde `snake_case` 枚举（`plain | scram_sha_256 | scram_sha_512`）——未知机制在反序列化层即被拒绝；
- `TlsConfig { ca/cert/key: Option<String>, key_password: Option<String>, insecure_skip_verify: Option<bool> }`；
- `fn apply(&self, client_config: &mut ClientConfig) -> Result<()>`：解析生效协议 → 装配属性 → 一致性校验；
- `fn resolved_protocol(&self) -> Result<SecurityProtocol>` 供单测。

**备选**：input/output 各写一份（DRY 违背，两端易漂移）；放 `arkflow-core`（不行，rdkafka 依赖在 plugin crate，core 必须保持引擎抽象无 Kafka 概念）。YAML 形状定为嵌套 `security:` 块而非平铺字段：与 `exactly_once` 等既有平铺字段相比，安全项是一组强内聚的可选配置，嵌套块让「无安全需求」的配置零噪音，也让 schema 更清晰。

### D2：协议推断——显式声明优先，否则按子块存在性

`resolved_protocol`：显式 `protocol` 直接生效（若与子块矛盾则报错，见 D5）；未声明时 `sasl`+`tls` → `sasl_ssl`，仅 `sasl` → `sasl_plaintext`，仅 `tls` → `ssl`，皆无 → `plaintext`。

**备选**：要求必须显式声明 protocol（更严格但啰嗦，`sasl_ssl` 是压倒性常见组合，推断能少写一行且不会错）；按 mechanism 猜（隐晦，拒绝）。

### D3：路径 vs 内联 PEM——`-----BEGIN` 前缀自动识别

`ca`/`cert`/`key` 值以 `-----BEGIN` 开头 → 写 `ssl.ca.pem` / `ssl.certificate.pem` / `ssl.key.pem`；否则视为路径 → 写 `ssl.*.location`。已核实 vendored librdkafka 2.12 支持全部三个 `.pem` 属性，纯属性映射、无需临时文件。

**备选**：path 与 pem 拆成两个字段（配置面翻倍、用户易混淆）；内联 CA 落临时文件（生命周期管理复杂、凭据泄漏面变大）。识别规则足够可靠——PEM 文本必然以该前缀开始，而合法路径不可能以 `-` 开头加换行的形式出现；单测覆盖两种形态。

### D4：output 侧装配提取为可单测函数

把 `output/kafka.rs:171` 附近的内联 `ClientConfig` 构建提取为 `build_client_config(&self)`（与 input 侧命名对齐），安全属性统一走 `kafka_security::apply`。单测在两侧断言属性 KV（`ClientConfig` 支持 `get(key)` 遍历），不连 broker。

### D5：校验在 builder `build()` 阶段，规则内聚在 `apply`

- `protocol` 为 `sasl_*` 但缺 `sasl` 块 → 错误指明需 `security.sasl`；
- `mechanism` 为 `plain`/`scram_*` 但 `username`/`password` 缺失 → 错误指明缺失字段；
- 显式 `plaintext` 但提供了 `sasl` 或 `tls` 块 → 矛盾错误（诚实优于静默忽略）；
- `insecure_skip_verify` 出现在无 `tls` 语义矛盾的场景不存在（它在 `tls` 块内部，天然受控）。

时点选 build 而非首连：配置错误应与 bad YAML 同级别 fail-fast，`--validate` 即可拦截（validate 会走完整组件构建）。

### D6：机制范围收窄为 PLAIN + SCRAM

PLAIN/SCRAM 覆盖 Confluent、MSK（SCRAM）、Redpanda、EMQX 等主流托管/自建场景，且由已启用的 `sasl` feature 直接支撑。GSSAPI 需追加 `gssapi-vendored` feature（拉入 Kerberos 构建链）、OAUTHBEARER 需 ClientContext token 刷新回调 + AWS 凭据链——均独立成 change，workspace 已预留 `aws-msk-iam-sasl-signer`。

## Risks / Trade-offs

- [ `insecure_skip_verify` 在生产被滥用 ] → 文档显式标注仅限开发/测试；开启时打 `warn` 日志。
- [ SASL 密码以明文驻留 YAML 与控制面存储 ] → 已知限制，文档标注；Secret 引用机制是规划中的后续 change，届时本配置结构无需破坏性变更（`Option<String>` → 支持 `${...}` 表达式即可）。
- [ librdkafka 属性面随版本漂移 ] → workspace 锁定 rdkafka/rdkafka-sys 版本 + vendored 构建，CI 与本地同版本；属性名在单测中逐一断言，升级时会显式失败。
- [ 推断规则未来扩展新机制（如 OAUTHBEARER 需要不同凭据形态）时语义复杂化 ] → 届时 `SaslConfig` 按机制扩展字段即可，推断规则不变。

## Migration Plan

纯增量：新字段全部 serde default，未配置 `security` 时行为与现状逐字节一致（spec 有回归场景）。无数据迁移；发布后回滚 = revert，旧配置文件无需改动。

## Open Questions

无阻塞性未决项。一个实施期注意点：`examples/` 新增的 SASL 示例 YAML 须能离线通过 `--validate`（不连 broker），如构建路径涉及网络调用需在 manifest 中标注 `"validate": false` 并附原因（预计不需要）。
