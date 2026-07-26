## ADDED Requirements

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
