# vrl-processor Specification

## Purpose
TBD - created by archiving change harden-vrl-processor. Update Purpose after archive.
## Requirements
### Requirement: String columns round-trip as strings
An Arrow `Utf8`/`LargeUtf8` column processed by the VRL processor SHALL be emitted as a string column on output. A `Value::Bytes` result column SHALL be emitted as `Utf8` when every value in the column is valid UTF-8, and as `Binary` only when at least one value is not valid UTF-8.

#### Scenario: String column stays a string column
- **WHEN** an input `Utf8` column named `name` with values `["alice", "bob"]` is processed by a passthrough VRL statement
- **THEN** the output column `name` has Arrow datatype `Utf8` (not `Binary`) and the same values

#### Scenario: Genuine binary stays binary
- **WHEN** a VRL result column contains bytes that are not valid UTF-8
- **THEN** the output column is emitted as `Binary`

### Requirement: Runtime errors are observable
When `program.resolve()` returns an error for a batch, the VRL processor SHALL log the error and return `Err`; it SHALL NOT silently drop the batch.

#### Scenario: A fallible statement that fails surfaces an error
- **WHEN** a batch is processed with a VRL statement `parse_json!(.message)` and `.message` is not valid JSON
- **THEN** the processor logs the VRL diagnostic and returns `Err`, and the batch is not silently discarded

### Requirement: All Arrow timestamp units are supported
The VRL processor SHALL accept Arrow `Timestamp` columns of every time unit — `Second`, `Millisecond`, `Microsecond`, and `Nanosecond` — and SHALL NOT drop a column because of its unit.

#### Scenario: Non-nanosecond timestamp column is preserved
- **WHEN** an input `Timestamp(Second, _)` column is processed
- **THEN** the column is read using the second-precision array and reaches the VRL program (it is not dropped)

#### Scenario: Unset timestamp becomes null, not epoch
- **WHEN** a timestamp value cannot be converted to a VRL timestamp
- **THEN** it is represented as a VRL null rather than being silently coerced to 1970-01-01

### Requirement: Unsupported result shapes fail loudly
A VRL result that is not a row object (or array of row objects) — including scalars, nested `Object`, `Array`, and `Regex` — SHALL cause the processor to return a clear error naming the unsupported shape. It SHALL NOT produce an empty batch or silently lose data.

#### Scenario: Scalar result returns an error
- **WHEN** a VRL statement returns a scalar (e.g. `1 + 1`)
- **THEN** the processor returns an `Err` describing the unsupported scalar result, rather than an empty batch

### Requirement: Timezone is configurable
The VRL processor SHALL accept an optional `timezone` configuration field. When absent, it SHALL default to the platform local timezone (VRL's `TimeZone::default()`, which is today's behavior). An invalid timezone string SHALL fall back to the default with a warning rather than failing configuration.

#### Scenario: Default timezone is the platform local timezone
- **WHEN** a VRL processor is configured without a `timezone` field
- **THEN** it uses the platform local timezone, matching today's behavior

#### Scenario: Custom timezone is honored
- **WHEN** a VRL processor is configured with `timezone: "Asia/Shanghai"`
- **THEN** VRL timestamp operations use that timezone

#### Scenario: Invalid timezone falls back to the default
- **WHEN** a VRL processor is configured with an unparseable `timezone` string
- **THEN** configuration does not fail; the processor logs a warning and uses the platform local timezone

### Requirement: Test coverage pins the data-correctness contracts
The VRL processor SHALL ship a unit-test module covering at least: string round-trip, empty input, a failing runtime statement, a compile-error configuration, and each timestamp unit.

#### Scenario: Type round-trip tests exist
- **WHEN** `cargo test -p arkflow-plugin` runs
- **THEN** tests for string, numeric, boolean, and each timestamp-unit round-trips pass

### Requirement: Dependency upgrade preserves operator-facing behavior
The VRL processor's upgrade from vrl 0.30 to 0.36 SHALL NOT change its configuration surface, its error taxonomy, or any behavior codified by the existing `vrl-processor` requirements. User VRL source that compiles on vrl 0.30 SHALL compile identically on 0.36, except for expressions using functions removed upstream — those SHALL fail at processor build time with the VRL compiler diagnostic surfaced to the operator as a configuration error (the existing behavior for invalid source).

#### Scenario: Valid program keeps compiling after the upgrade
- **WHEN** a processor config with VRL source that compiled on vrl 0.30 (e.g. `.message = upcase(.message)`) is built against vrl 0.36
- **THEN** the program compiles and the processor is constructed successfully

#### Scenario: Removed-upstream function surfaces the compiler diagnostic
- **WHEN** a processor config uses a VRL function that no longer exists in the 0.36 stdlib
- **THEN** processor construction fails with the VRL compile diagnostic naming the function, following the existing invalid-source error path

#### Scenario: Existing behavioral requirements hold unchanged
- **WHEN** the full `vrl-processor` test suite runs against vrl 0.36
- **THEN** every requirement of the `vrl-processor` specification — string round-trip typing, runtime-error observability, all timestamp units, unsupported result shapes failing loudly — passes without modification to the asserted behaviors

### Requirement: UInt64 超范围 SHALL 显式报错
输入 Arrow `UInt64` 列的值超出 `i64::MAX` 时，processor SHALL 返回指明列名与原值的错误，MUST NOT 静默回绕为负数（既有 `as i64` 裸转换行为废除）。值在 `i64` 范围内的 UInt64 列行为不变。

#### Scenario: 超大 UInt64 报错
- **WHEN** 一个 `UInt64` 列含值 `u64::MAX`
- **THEN** 处理返回错误，错误信息含列名与原值，不产出回绕后的负数

#### Scenario: 范围内 UInt64 正常处理
- **WHEN** 一个 `UInt64` 列的全部值 ≤ `i64::MAX`
- **THEN** 列正常进入 VRL 程序（与既有行为一致）

### Requirement: 不支持的输入类型 SHALL 显式报错
输入 Arrow 列的类型不在 VRL 映射支持集内（如 `List`/`Struct`/`Decimal`/`Dictionary`）或列 downcast 失败时，processor SHALL 返回指明列名与类型的错误，MUST NOT 以整列 Null 替代（既有静默转 Null 行为废除）。错误信息 SHALL 提示可通过 `filter_columns`/SQL 预投影移除该列。

#### Scenario: List 列报错而非静默 Null
- **WHEN** 输入批次含一个 `List` 类型的列
- **THEN** 处理返回错误，错误信息含列名、类型与预投影规避提示，不产出该列为 Null 的批次

#### Scenario: 支持类型不受影响
- **WHEN** 输入批次仅含 Utf8/数值/Boolean/时间戳等受支持类型列
- **THEN** 处理行为与既有版本一致

### Requirement: 批粒度保持——单输入批产出单输出批
VRL processor 处理一个输入批时，SHALL 至多产出一个输出批：程序对各行返回的结果行（每行一个对象，或数组展开后的多个对象行）合并为一个批，行序保持输入行序与数组展开序；输出批的字段集为批内全部结果行字段的并集（列序为首次出现顺序），某行缺失的字段以 null 填充；每列的类型由该字段首个非 null 值决定（全为 null 的字段为 Null 类型列）；同一字段在不同行的非 null 值类型不一致时（如一行整数、一行字符串或浮点）SHALL 显式报错并指明字段名与两处类型——合并后的单批只能有一种列类型，静默置 null 是不可见的数据丢失。全部行为空时 SHALL 返回空结果（`ProcessResult::None`）。程序对任意一行返回非行对象结果（标量等）SHALL 显式报错——不因该行不是首行而退化为全 null 行。

#### Scenario: 多行输入合并为单批
- **WHEN** 输入批含 3 行，程序对每行返回一个对象
- **THEN** 输出为单个含 3 行的批（`ProcessResult::Single`），行序与输入一致，不再是每输入行一个批

#### Scenario: 数组展开行并入同批
- **WHEN** 输入批含 2 行，程序对第一行返回含 2 个对象的数组、对第二行返回 1 个对象
- **THEN** 输出为单个含 3 行的批，前 2 行来自第一行的数组展开

#### Scenario: 异构行字段并集补 null
- **WHEN** 程序对不同行返回字段集不同的对象（第一行含 `a`，第二行含 `b`）
- **THEN** 输出批同时含 `a`、`b` 列（列序按首次出现），第一行的 `b` 与第二行的 `a` 为 null

#### Scenario: null 首现不冻结列类型
- **WHEN** 第一行某字段为 null、第二行同字段为整数 5
- **THEN** 该列类型为 Int64（取自首个非 null 值），第一行为 null、第二行为 5，而非 Null 类型列丢弃第二行的值

#### Scenario: 中途非对象结果显式报错
- **WHEN** 第一行返回对象、第二行返回标量
- **THEN** 整批返回错误（提示需返回行对象），不产出含全 null 行的批

#### Scenario: 跨行类型冲突显式报错
- **WHEN** 程序对第一行返回 `{ "x": 1 }`、对第二行返回 `{ "x": "s" }`（或 `1.5`、`true` 等不同 VRL 类型）
- **THEN** 整批返回错误，错误信息包含字段名 `x`、两处类型与行号，并提示用 `to_float()`/`to_string()` 等统一类型；不产出把一侧静默置 null 的批

#### Scenario: 空结果保持 None
- **WHEN** 输入批为空，或程序对每行都返回空数组 `[]`
- **THEN** 返回 `ProcessResult::None`（不产出空批）

