---
description: ArkFlow 文档页面。
---

# 数据类型(Data Types)

DataFusion 在查询执行时使用 Arrow,即 Arrow 的类型系统。
[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/ast/data_type.rs#L27)
的 SQL 类型按照下表映射为 [Arrow 数据类型](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html)。
这一映射发生在用 `CREATE EXTERNAL TABLE` 命令定义模式(schema)时,或执行 SQL `CAST`(类型转换)操作时。

你可以使用 `arrow_typeof` 函数查看任意 SQL 表达式对应的 Arrow 类型。例如:

```sql
select arrow_typeof(interval '1 month');
+---------------------------------------------------------------------+
| arrow_typeof(IntervalMonthDayNano("79228162514264337593543950336")) |
+---------------------------------------------------------------------+
| Interval(MonthDayNano)                                              |
+---------------------------------------------------------------------+
```

你可以使用 `arrow_cast` 函数将 SQL 表达式转换为特定的 Arrow 类型。
例如,把 `now()` 的输出转换为秒精度的 `Timestamp`:

```sql
select arrow_cast(now(), 'Timestamp(Second, None)');
+---------------------+
| now()               |
+---------------------+
| 2023-03-03T17:19:21 |
+---------------------+
```

## 字符类型(Character Types)

| SQL 数据类型 | Arrow 数据类型 |
| ------------ | -------------- |
| `CHAR`       | `Utf8`         |
| `VARCHAR`    | `Utf8`         |
| `TEXT`       | `Utf8`         |
| `STRING`     | `Utf8`         |

## 数值类型(Numeric Types)

| SQL 数据类型                          | Arrow 数据类型                 | 备注                                                                                                  |
| ------------------------------------ | :----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `TINYINT`                            | `Int8`                         |                                                                                                       |
| `SMALLINT`                           | `Int16`                        |                                                                                                       |
| `INT` 或 `INTEGER`                   | `Int32`                        |                                                                                                       |
| `BIGINT`                             | `Int64`                        |                                                                                                       |
| `TINYINT UNSIGNED`                   | `UInt8`                        |                                                                                                       |
| `SMALLINT UNSIGNED`                  | `UInt16`                       |                                                                                                       |
| `INT UNSIGNED` 或 `INTEGER UNSIGNED` | `UInt32`                       |                                                                                                       |
| `BIGINT UNSIGNED`                    | `UInt64`                       |                                                                                                       |
| `FLOAT`                              | `Float32`                      |                                                                                                       |
| `REAL`                               | `Float32`                      |                                                                                                       |
| `DOUBLE`                             | `Float64`                      |                                                                                                       |
| `DECIMAL(precision, scale)`          | `Decimal128(precision, scale)` | 对 DECIMAL 的支持目前仍是实验性的([#3523](https://github.com/apache/datafusion/issues/3523))          |

## 日期/时间类型(Date/Time Types)

| SQL 数据类型 | Arrow 数据类型                    |
| ------------ | :------------------------------- |
| `DATE`       | `Date32`                         |
| `TIME`       | `Time64(Nanosecond)`             |
| `TIMESTAMP`  | `Timestamp(Nanosecond, None)`    |
| `INTERVAL`   | `Interval(IntervalMonthDayNano)` |

## 布尔类型(Boolean Types)

| SQL 数据类型 | Arrow 数据类型 |
| ------------ | :------------- |
| `BOOLEAN`    | `Boolean`      |

## 二进制类型(Binary Types)

| SQL 数据类型 | Arrow 数据类型 |
| ------------ | :------------- |
| `BYTEA`      | `Binary`       |

你可以使用形如 `X'1234'` 的十六进制字符串字面量来创建二进制字面量,
它会创建一个由 `0x12` 和 `0x34` 两个字节组成的 `Binary` 值。

## 不支持的 SQL 类型(Unsupported SQL Types)

| SQL 数据类型 | Arrow 数据类型      |
| ------------- | :------------------ |
| `UUID`        | _暂不支持_          |
| `BLOB`        | _暂不支持_          |
| `CLOB`        | _暂不支持_          |
| `BINARY`      | _暂不支持_          |
| `VARBINARY`   | _暂不支持_          |
| `REGCLASS`    | _暂不支持_          |
| `NVARCHAR`    | _暂不支持_          |
| `CUSTOM`      | _暂不支持_          |
| `ARRAY`       | _暂不支持_          |
| `ENUM`        | _暂不支持_          |
| `SET`         | _暂不支持_          |
| `DATETIME`    | _暂不支持_          |

## 受支持的 Arrow 类型(Supported Arrow Types)

以下类型受 `arrow_typeof` 函数支持:

| Arrow 类型                                                  |
| ----------------------------------------------------------- |
| `Null`                                                      |
| `Boolean`                                                   |
| `Int8`                                                      |
| `Int16`                                                     |
| `Int32`                                                     |
| `Int64`                                                     |
| `UInt8`                                                     |
| `UInt16`                                                    |
| `UInt32`                                                    |
| `UInt64`                                                    |
| `Float16`                                                   |
| `Float32`                                                   |
| `Float64`                                                   |
| `Utf8`                                                      |
| `LargeUtf8`                                                 |
| `Binary`                                                    |
| `Timestamp(Second, None)`                                   |
| `Timestamp(Millisecond, None)`                              |
| `Timestamp(Microsecond, None)`                              |
| `Timestamp(Nanosecond, None)`                               |
| `Time32`                                                    |
| `Time64`                                                    |
| `Duration(Second)`                                          |
| `Duration(Millisecond)`                                     |
| `Duration(Microsecond)`                                     |
| `Duration(Nanosecond)`                                      |
| `Interval(YearMonth)`                                       |
| `Interval(DayTime)`                                         |
| `Interval(MonthDayNano)`                                    |
| `FixedSizeBinary(<len>)`(例如 `FixedSizeBinary(16)`)       |
| `Decimal128(<precision>, <scale>)`,例如 `Decimal128(3, 10)` |
| `Decimal256(<precision>, <scale>)`,例如 `Decimal256(3, 10)` |
