# secret-references 变更（Delta）

## MODIFIED Requirements

### Requirement: 配置字符串值的 secret 引用语法

引擎 SHALL 在配置物化为 `EngineConfig` 时解析字符串值中的 secret 引用，支持四种形式：`${env:VAR}`（读取环境变量）、`${env:VAR:-default}`（未设置或为空时采用默认值）、`${file:/path}`（读取文件内容并剥离尾部换行）与 `${secret:NAME}`（读取环境变量 `ARKFLOW_SECRET_<NAME>`，名字逐字映射，支持 `${secret:NAME:-default}` 默认值语法）。解析 SHALL 递归覆盖嵌套 map 与 array 中的字符串值，SHALL NOT 改动键名、非字符串值与未知形式的 `${...}` 文本。`$${` SHALL 转义为字面 `${`。

#### Scenario: 环境变量引用解析

- **WHEN** 配置某字符串值为 `${env:KAFKA_PASSWORD}` 且环境变量 `KAFKA_PASSWORD` 已设置
- **THEN** 物化后的 `EngineConfig` 中对应字段为该环境变量的值，配置文件中不留明文

#### Scenario: 带默认值的环境变量引用

- **WHEN** 配置值为 `${env:TOKEN:-fallback}` 且 `TOKEN` 未设置（或为空串）
- **THEN** 对应字段值为 `fallback`

#### Scenario: 文件引用解析

- **WHEN** 配置值为 `${file:/run/secrets/db_pass}` 且该文件内容为 `s3cret\n`
- **THEN** 对应字段值为 `s3cret`（尾部换行被剥离）

#### Scenario: secret 引用按 ARKFLOW_SECRET 前缀解析

- **WHEN** 配置值为 `${secret:db_password}` 且环境变量 `ARKFLOW_SECRET_db_password` 为 `hunter2`
- **THEN** 对应字段值为 `hunter2`；该变量未设置时物化失败，错误指明 `${secret:db_password}` 与配置路径，不含变量值

#### Scenario: 转义与未知形式保持原样

- **WHEN** 配置值包含 `$${env:NOT_A_REF}` 或 `${vault:kv/foo}`（未知 scheme）
- **THEN** 对应字段值分别为字面 `${env:NOT_A_REF}` 与 `${vault:kv/foo}`，解析不报错

#### Scenario: 嵌套结构与子串嵌入

- **WHEN** 引用出现在嵌套 map/array 深处或嵌入更大字符串（如 `host=${env:H};port=${env:P}`）
- **THEN** 每个引用均被独立解析，结果拼入所在字符串
