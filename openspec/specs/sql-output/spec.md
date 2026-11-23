# sql-output Specification

## Purpose

Defines the behavior of the `sql` output plugin: configuration contract, batch insert semantics, per-dialect upsert semantics for idempotent writes, and the requirement that the registered component metadata matches the real configuration surface.
## Requirements
### Requirement: 批量插入契约
`sql` output SHALL 把收到的 `MessageBatch`（Arrow `RecordBatch`）按行转为参数化值，经单条多值 `INSERT INTO <table> (<columns>) VALUES (...), (...)` 语句写入配置的目标表；支持的列类型为：`Utf8`、`Boolean`、`Int8`/`Int16`/`Int32`/`Int64`（统一以 i64 参数传入）、`UInt8`/`UInt16`/`UInt32`/`UInt64`（统一以 u64 参数传入）、`Float32`/`Float64`（统一以 f64 参数传入）、`Date32`/`Date64`（ISO 日期字符串）、`Timestamp`（任意时间单位，RFC3339 字符串）。其余 Arrow 类型 SHALL 返回错误，错误信息 SHALL 指明列名与该列的实际类型。未配置 upsert 时行为 MUST 与既有版本一致。

#### Scenario: 批量插入
- **WHEN** output 收到含多行的 batch 且未配置 upsert
- **THEN** 生成一条多值参数化 INSERT 写入 `table_name` 指定的表

#### Scenario: 窄整型与时间类型可写
- **WHEN** batch 含 `Int32`、`Float32`、`Date32`、`Timestamp(Nanosecond, _)` 列
- **THEN** 各列按上述映射转为参数值写入成功

#### Scenario: 不支持的列类型
- **WHEN** batch 含不支持类型的列（如 Struct）
- **THEN** 写入返回错误，错误信息含该列的列名与类型，不静默丢弃该列

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

### Requirement: write_batch SHALL be transactional

一次 `write_batch` 调用（一个 ack 区间）SHALL 在单个数据库事务内执行全部批次的写入：任一批次失败，整个事务回滚，不留下部分写入；调用方向上层返回错误使 ack 不推进、恢复时整个区间重放。单条 `write` 路径（单语句）行为不变。upsert 键校验 SHALL 在事务开始前完成。

#### Scenario: 批次中途失败整体回滚

- **WHEN** 一个 write_batch 区间含三个批次且第二个执行失败
- **THEN** 第一个批次的写入被回滚，数据库不包含该区间的任何行，ack 不推进

#### Scenario: 单条写入行为不变

- **WHEN** 输出以逐条 `write` 使用（无 write_batch 聚合）
- **THEN** 每条写入仍是单语句原子提交，行为与现状一致

### Requirement: 配置 codec 时构建 SHALL 拒绝
`sql` output 写入依赖类型化列，不支持 codec 编码。配置了 `codec` 时 output 构建 SHALL 返回配置错误并说明原因，MUST NOT 构建成功后在写入期必然失败（既有行为：构建静默接受，write 因产生 Binary 列必然报错）。

#### Scenario: 配 codec 构建即失败
- **WHEN** sql output 配置携带 `codec` 字段
- **THEN** 构建返回配置错误（指明 sql output 不支持 codec），进程不进入运行期

### Requirement: close SHALL 关闭数据库连接
`close()` SHALL 除取消内部 token 外，显式取出并关闭持有的数据库连接（连接不随对象析构被动释放）。

#### Scenario: close 后连接关闭
- **WHEN** output 的 `close()` 被调用
- **THEN** 持有的数据库连接被显式关闭，不再占用后端连接槽位

### Requirement: 标识符 SHALL 按方言加引号转义

`sql` output 拼接 SQL 文本时，列名、upsert key 与冲突赋值中的标识符以及目标表名 SHALL 经按方言的标识符加引号处理：MySQL 方言以反引号包裹，PostgreSQL 方言以双引号包裹，标识符内部嵌入的引号字符 SHALL 以翻倍方式转义，MUST NOT 原样拼入语句。表名 SHALL 按 `.` 分段处理：未加引号的段按方言加引号（PostgreSQL 上保持未加引号标识符既有的小写折叠语义），已用当地方言引号完整包裹的段 SHALL 原样保留。空段、未闭合的引号、闭合引号之后的多余文本与 NUL 字节 SHALL 使 output 构建返回指名 `table_name` 的配置错误。列名来自 batch schema（可由数据推断）：含 NUL 的列名 SHALL 在事务开始前返回错误。标识符含引号、反引号或分号等字符时，数据库 SHALL 将其视为单个普通标识符，MUST NOT 改变语句结构。`pgvector` output 与 `pgvector_search` processor SHALL 与 `sql` output 共用同一标识符转义实现。

#### Scenario: 列名含引号字符
- **WHEN** batch schema 含名为 `a"b` 与 ``a`b`` 的列
- **THEN** MySQL 语句的列清单为 `` `a"b`, `a``b` ``，PostgreSQL 语句为 `"a""b", `"a`b"`，每个列名都是单个标识符

#### Scenario: 表名含分号
- **WHEN** `table_name` 配置为 `events; DROP TABLE users; --`
- **THEN** 语句以加引号的单一标识符引用该表，数据库将其作为普通表名处理，语句结构不变

#### Scenario: 限定表名
- **WHEN** `table_name` 配置为 `analytics.events`
- **THEN** PostgreSQL 语句引用 `"analytics"."events"`，MySQL 语句引用 `` `analytics`.`events` ``

#### Scenario: 已加引号的段与大小写
- **WHEN** PostgreSQL 上 `table_name` 配置为 `"Events"` 或 `Events`
- **THEN** 前者原样保留为 `"Events"`，后者生成 `"events"`，与未加引号标识符的既有语义一致

#### Scenario: 非法表名在构建期拒绝
- **WHEN** `table_name` 含空段（如 `a..b`）、未闭合引号、闭合引号后的多余文本或 NUL 字节
- **THEN** output 构建返回配置错误，错误信息指名 `table_name` 与原因

#### Scenario: upsert key 同源转义
- **WHEN** 启用 upsert 且 `upsert_keys` 含带引号字符的列名
- **THEN** `ON CONFLICT (...)` / `ON DUPLICATE KEY UPDATE` 子句中的 key 与赋值列使用与列清单相同的转义

#### Scenario: 真实数据库往返
- **WHEN** 以含引号、反引号或分号的列名与表名向 PostgreSQL 与 MySQL 写入
- **THEN** 语句成功执行，只有目标表的行受影响，upsert 重放保持幂等

#### Scenario: pgvector 共享实现
- **WHEN** `pgvector` output 或 `pgvector_search` processor 构造语句
- **THEN** 使用同一转义实现，生成的语句文本与本次变更前一致

