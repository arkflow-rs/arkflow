## ADDED Requirements

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
