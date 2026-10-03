# sql-output 增量

## MODIFIED Requirements

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

## ADDED Requirements

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
