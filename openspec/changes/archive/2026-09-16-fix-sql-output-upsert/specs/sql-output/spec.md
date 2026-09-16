## ADDED Requirements

### Requirement: 批量插入契约
`sql` output SHALL 把收到的 `MessageBatch`（Arrow `RecordBatch`）按行转为参数化值，经单条多值 `INSERT INTO <table> (<columns>) VALUES (...), (...)` 语句写入配置的目标表；支持的列类型为 Utf8、Int64、UInt64、Float64、Boolean，其余 Arrow 类型 SHALL 返回错误。未配置 upsert 时行为 MUST 与既有版本一致。

#### Scenario: 批量插入
- **WHEN** output 收到含多行的 batch 且未配置 upsert
- **THEN** 生成一条多值参数化 INSERT 写入 `table_name` 指定的表

#### Scenario: 不支持的列类型
- **WHEN** batch 含不支持类型的列（如 Struct）
- **THEN** 写入返回错误，不静默丢弃该列

### Requirement: upsert 写入语义
`sql` output SHALL 支持可选 upsert 写入：配置 `upsert: true` 且提供非空 `upsert_keys` 时，MySQL 方言 SHALL 追加 `ON DUPLICATE KEY UPDATE`（非 key 列 `col = VALUES(col)`），PostgreSQL 方言 SHALL 追加 `ON CONFLICT (<upsert_keys>) DO UPDATE SET`（非 key 列 `col = EXCLUDED.col`）；`upsert` 未配置或为 `false` 时 MUST 生成普通 INSERT。

#### Scenario: PostgreSQL upsert
- **WHEN** 配置 `upsert: true`、`upsert_keys: [id]`、`output_type.type: postgres`
- **THEN** 生成 `INSERT ... ON CONFLICT ("id") DO UPDATE SET` 且非 key 列以 `EXCLUDED` 赋值

#### Scenario: MySQL upsert
- **WHEN** 配置 `upsert: true`、`upsert_keys: [id]`、`output_type.type: mysql`
- **THEN** 生成 `INSERT ... ON DUPLICATE KEY UPDATE` 且非 key 列以 `VALUES(col)` 赋值

#### Scenario: 未配置 upsert 保持插入行为
- **WHEN** 未配置 `upsert`（或为 `false`）
- **THEN** 生成普通多值 INSERT，不带任何冲突子句

### Requirement: upsert 配置校验
output 构建时 SHALL 校验：`upsert: true` MUST 提供非空 `upsert_keys`，否则构建返回配置错误；`upsert_keys` 中的列 MUST 存在于写入 batch 的 schema 中，否则写入返回错误。

#### Scenario: upsert 缺少 keys
- **WHEN** 配置 `upsert: true` 但未提供 `upsert_keys`（或为空数组）
- **THEN** output 构建失败并返回明确的配置错误

#### Scenario: key 列不存在
- **WHEN** `upsert_keys` 含 batch schema 中不存在的列
- **THEN** 写入返回错误，指明缺失的 key 列

### Requirement: 组件元数据与实现一致
`sql` output 注册的组件 schema 元数据 MUST 与真实配置结构一致：字段为 `output_type`（含 `mysql`/`postgres` 两个变体及各自 `uri`/`ssl`）、`table_name`、`upsert`、`upsert_keys`，MUST NOT 宣称不存在的字段或能力；描述与内置 example MUST 反映真实配置形态。

#### Scenario: 元数据 schema 描述真实配置
- **WHEN** 经 `arkflow components show sql` 或生成的 config schema 查看元数据
- **THEN** 字段集合为 `output_type`/`table_name`/`upsert`/`upsert_keys`，与 `SqlOutputConfig` 反序列化的真实字段一致

#### Scenario: 合法配置通过校验
- **WHEN** 一份使用 `output_type`/`table_name` 真实字段的合法 sql output 配置按元数据 schema 校验
- **THEN** 校验通过（现状会被 `additionalProperties: false` 拒绝）
