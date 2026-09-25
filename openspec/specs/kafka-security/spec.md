# Capability: Kafka Security

## Purpose

Define the unified security configuration for Kafka input and output components: a shared `security` configuration block covering security protocol selection (explicit or inferred), SASL authentication assembly, TLS certificate assembly (file path or inline PEM), and build-time validation with field-specific error semantics. (Purpose derived from the `add-kafka-sasl-ssl` change; refine as the capability evolves.)

## Requirements

### Requirement: Kafka input/output 统一安全配置块

Kafka input 与 Kafka output SHALL 接受形状完全相同的 `security` 配置块，包含可选的 `protocol`（`plaintext | ssl | sasl_plaintext | sasl_ssl`）、`sasl`（`mechanism`、`username`、`password`）与 `tls`（`ca`、`cert`、`key`、`key_password`、`insecure_skip_verify`）子块。`security` 及其所有子字段 SHALL 可省略（serde default），组件的 JSON schema 元数据 SHALL 与真实配置结构一致。

#### Scenario: input 与 output 接受相同配置形状

- **WHEN** 同一份 `security` 块（如 `protocol: sasl_ssl` + SCRAM 凭据 + CA）分别配置到 kafka input 与 kafka output
- **THEN** 两者均构建成功，且生成的 rdkafka 安全属性等价

#### Scenario: schema 元数据反映新字段

- **WHEN** 运行文档 inventory 快照生成（`ARKFLOW_REGENERATE_DOCS=1`）
- **THEN** kafka input/output 的组件 schema 含 `security` 字段定义，无需手工编辑生成文件

### Requirement: 安全协议选择与推断

组件 SHALL 按以下规则决定生效的 `security.protocol`：显式 `protocol` 声明优先；未声明时按子块存在性推断——仅有 `sasl` 推断为 `sasl_plaintext`，仅有 `tls` 推断为 `ssl`，两者皆有推断为 `sasl_ssl`，两者皆无为 `plaintext`。

#### Scenario: 显式 protocol 优先于推断

- **WHEN** 配置声明 `protocol: sasl_ssl` 且仅提供 `sasl` 块（无 `tls`）
- **THEN** 生效协议为 `sasl_ssl`，不因缺少 `tls` 块而报错或回落

#### Scenario: 仅 SASL 块推断为 sasl_plaintext

- **WHEN** 仅配置 `sasl` 块（SCRAM 凭据），未声明 `protocol` 与 `tls`
- **THEN** 生效协议为 `sasl_plaintext`

#### Scenario: 仅 TLS 块推断为 ssl

- **WHEN** 仅配置 `tls` 块（CA 证书），未声明 `protocol` 与 `sasl`
- **THEN** 生效协议为 `ssl`

#### Scenario: 未配置 security 时保持 plaintext

- **WHEN** 未提供 `security` 块
- **THEN** 生效协议为 `plaintext`，客户端配置不包含任何 `security.*` / `sasl.*` / `ssl.*` 属性（与现有行为一致）

### Requirement: SASL 认证属性装配

当 `sasl` 块存在时，组件 SHALL 将 `sasl.mechanisms` 设置为机制对应值（`plain` → `PLAIN`，`scram-sha-256` → `SCRAM-SHA-256`，`scram-sha-512` → `SCRAM-SHA-512`），并设置 `sasl.username` 与 `sasl.password`。

#### Scenario: SCRAM-SHA-256 凭据装配

- **WHEN** 配置 `sasl: {mechanism: scram-sha-256, username: u, password: p}`
- **THEN** 客户端配置包含 `sasl.mechanisms=SCRAM-SHA-256`、`sasl.username=u`、`sasl.password=p`

#### Scenario: PLAIN 机制装配

- **WHEN** 配置 `sasl: {mechanism: plain, username: u, password: p}`
- **THEN** 客户端配置包含 `sasl.mechanisms=PLAIN` 及对应用户名密码

### Requirement: TLS 证书装配（路径或内联 PEM）

`tls` 块的 `ca` / `cert` / `key` SHALL 接受文件路径或内联 PEM 文本，以 `-----BEGIN` 前缀自动识别：内联 PEM 写入对应 `ssl.*.pem` 属性，文件路径写入对应 `ssl.*.location` 属性。`key_password` SHALL 映射到 `ssl.key.password`；`insecure_skip_verify: true` SHALL 将 `enable.ssl.certificate.verification` 置为 `false`。

#### Scenario: 内联 PEM 装配

- **WHEN** `tls.ca` 的值以 `-----BEGIN CERTIFICATE-----` 开头
- **THEN** 客户端配置将 PEM 文本写入 `ssl.ca.pem`，而非 `ssl.ca.location`

#### Scenario: 文件路径装配

- **WHEN** `tls.ca` 的值是不以 `-----BEGIN` 开头的路径（如 `/etc/certs/ca.crt`）
- **THEN** 客户端配置将路径写入 `ssl.ca.location`

#### Scenario: 客户端密钥口令与跳过校验

- **WHEN** 配置 `tls: {cert: ..., key: ..., key_password: secret, insecure_skip_verify: true}`
- **THEN** 客户端配置包含 `ssl.key.password=secret` 且 `enable.ssl.certificate.verification=false`

### Requirement: 构建期校验与错误语义

组件构建 SHALL 在不连接 broker 的情况下校验安全配置一致性，并对以下情形返回指明字段的清晰错误：`protocol` 为 `sasl_*` 但缺少 `sasl` 块；`mechanism` 为 `plain`/`scram-*` 但 `username` 或 `password` 缺失；声明了 `sasl` 块但 `mechanism` 缺失。

#### Scenario: sasl 协议缺少凭据块

- **WHEN** 配置 `protocol: sasl_ssl` 但未提供 `sasl` 块
- **THEN** 组件构建失败，错误信息指明需要 `security.sasl`

#### Scenario: SCRAM 缺少密码

- **WHEN** 配置 `sasl: {mechanism: scram-sha-512, username: u}` 而无 `password`
- **THEN** 组件构建失败，错误信息指明缺失 `security.sasl.password`

#### Scenario: 未知机制被拒绝

- **WHEN** 配置 `sasl: {mechanism: gssapi}`
- **THEN** 配置解析失败（枚举反序列化错误），不进入构建

#### Scenario: 显式明文协议与 SASL 块矛盾

- **WHEN** 配置显式声明 `protocol: plaintext` 且同时提供 `sasl` 块
- **THEN** 组件构建失败，错误信息指明 `sasl` 块要求 `sasl_*` 协议
