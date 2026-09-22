# Capability: Secret References

## Purpose

Define the secret-reference syntax for configuration string values, the single resolution point where a configuration document is materialized into `EngineConfig`, and the error/no-leak guarantees that keep secrets out of configuration files, storage, and error output. (Purpose derived from the `add-secret-references` change; refine as the capability evolves.)

## Requirements

### Requirement: 配置字符串值的 secret 引用语法

引擎 SHALL 在配置物化为 `EngineConfig` 时解析字符串值中的 secret 引用，支持三种形式：`${env:VAR}`（读取环境变量）、`${env:VAR:-default}`（未设置或为空时采用默认值）、`${file:/path}`（读取文件内容并剥离尾部换行）。解析 SHALL 递归覆盖嵌套 map 与 array 中的字符串值，SHALL NOT 改动键名、非字符串值与未知形式的 `${...}` 文本。`$${` SHALL 转义为字面 `${`。

#### Scenario: 环境变量引用解析

- **WHEN** 配置某字符串值为 `${env:KAFKA_PASSWORD}` 且环境变量 `KAFKA_PASSWORD` 已设置
- **THEN** 物化后的 `EngineConfig` 中对应字段为该环境变量的值，配置文件中不留明文

#### Scenario: 带默认值的环境变量引用

- **WHEN** 配置值为 `${env:TOKEN:-fallback}` 且 `TOKEN` 未设置（或为空串）
- **THEN** 对应字段值为 `fallback`

#### Scenario: 文件引用解析

- **WHEN** 配置值为 `${file:/run/secrets/db_pass}` 且该文件内容为 `s3cret\n`
- **THEN** 对应字段值为 `s3cret`（尾部换行被剥离）

#### Scenario: 转义与未知形式保持原样

- **WHEN** 配置值包含 `$${env:NOT_A_REF}` 或 `${vault:kv/foo}`（未知 scheme）
- **THEN** 对应字段值分别为字面 `${env:NOT_A_REF}` 与 `${vault:kv/foo}`，解析不报错

#### Scenario: 嵌套结构与子串嵌入

- **WHEN** 引用出现在嵌套 map/array 深处或嵌入更大字符串（如 `host=${env:H};port=${env:P}`）
- **THEN** 每个引用均被独立解析，结果拼入所在字符串

### Requirement: 解析结果不重扫与最小作用域

解析出的值 SHALL NOT 被再次扫描引用（防注入/递归）；配置文本不含任何 `${` 或 `$${` 序列时，引擎 SHALL 走既有直接反序列化路径，行为与错误格式（含行列号）保持不变。

#### Scenario: 值中引用不递归

- **WHEN** 环境变量 `A` 的值为 `${env:B}`
- **THEN** 字段值为字面 `${env:B}`，不继续解析

#### Scenario: 无引用配置保持既有错误格式

- **WHEN** 配置不含引用且存在类型错误（如字段类型不匹配）
- **THEN** 错误信息保留 YAML/JSON/TOML 解析器的行列号定位（与现有行为一致）

### Requirement: 引用解析的错误语义与不泄漏保证

引用无法解析时 SHALL 返回指明配置路径与引用原文的错误，且 SHALL NOT 在错误信息、日志或任何持久化产物中包含解析出的明文；Hub 存储的配置内容 SHALL 保持引用原文。未设置的环境变量、不可读的文件 SHALL 分别返回可区分的明确错误。

#### Scenario: 未设置环境变量

- **WHEN** 配置值为 `${env:MISSING_VAR}` 且该变量未设置
- **THEN** 物化失败，错误信息包含配置路径（如 `streams[0]...sasl.password`）与 `${env:MISSING_VAR}`，不包含任何环境变量值

#### Scenario: 不可读文件

- **WHEN** 配置值为 `${file:/nonexistent/key.pem}` 且文件不存在
- **THEN** 物化失败，错误信息指明文件路径与 IO 错误类别，不包含文件内容

#### Scenario: Hub 存储不落明文

- **WHEN** 含引用的配置经 Hub 校验并存储版本
- **THEN** 存储的配置内容仍为引用原文（`${env:...}`），解析仅发生在消费进程物化时
