## 1. String round-trip fix (D1)

- [x] 1.1 In the output path, when building an array from a `Value::Bytes` column, detect whether every value is valid UTF-8; emit `Utf8` (or `LargeUtf8` when total length warrants) when so, `Binary` otherwise → verify: unit test "string column stays Utf8 after VRL"
- [x] 1.2 Ensure genuine non-UTF-8 bytes still emit as `Binary` → verify: unit test "binary column stays Binary"

## 2. Runtime error handling (D2)

- [x] 2.1 Replace `if let Ok(v) = self.program.resolve(&mut ctx)` with explicit `match`; on `Err`, `tracing::error!` the diagnostic and return `Err(Error::Processor(...))` → verify: unit test "parse_json!(.message) on bad JSON returns Err and is logged"
- [x] 2.2 Confirm a successful resolve still produces the batch as today → verify: existing/passthrough unit test passes after change

## 3. Full timestamp-unit support (D3)

- [x] 3.1 Dispatch on `DataType::Timestamp(unit, _)` to the correct concrete array (`TimestampSecondArray` / `MillisecondArray` / `MicrosecondArray` / `NanosecondArray`) instead of only nanoseconds → verify: unit test for each of the four units
- [x] 3.2 Replace `unwrap_or_default()` epoch coercion with explicit conversion that yields a VRL null on failure → verify: unit test "unset timestamp is null, not 1970"

## 4. Unsupported result shapes error loudly (D4)

- [x] 4.1 Make a scalar VRL result return `Err` instead of an empty batch → verify: unit test "scalar result returns Err"
- [x] 4.2 Make `Object` / `Array` / `Regex` results return a clear `Err` naming the unsupported shape (instead of failing opaquely or losing data) → verify: unit test

## 5. Small cleanups

- [x] 5.1 Fix the copy-paste error message at `vrl.rs:117` ("JsonToArrow..." → "VRL processor configuration is missing") → verify: `cargo build -p arkflow-plugin` + grep
- [x] 5.2 Remove the unused `config` field and its `#[allow(unused)]` (`vrl.rs:49-50`) → verify: `cargo build` warning-free for the field
- [x] 5.3 Simplify redundant `map_or_else(|| None, |v| Some(v))` → `.map(|v| v)` (`vrl.rs:402`) → verify: `cargo build`

## 6. Timezone config (D5)

- [x] 6.1 Add `timezone: Option<String>` to the VRL processor config; resolve at build time, defaulting to UTC and `tracing::warn!`-ing on invalid → verify: unit tests "default UTC", "custom timezone", "invalid falls back to UTC"
- [x] 6.2 Update the Component metadata JSON Schema to declare the optional `timezone` field → verify: `arkflow components show processor vrl --format json` includes `timezone`

## 7. Test coverage

- [x] 7.1 Add a `#[cfg(test)]` module with round-trip tests for string, integer, float, boolean, and each timestamp unit → verify: `cargo test -p arkflow-plugin vrl`
- [x] 7.2 Add tests for empty input (returns None), runtime-error handling, and compile-error configuration rejection → verify: `cargo test -p arkflow-plugin vrl`

## 8. Validation

- [x] 8.1 `cargo test -p arkflow-plugin` green → verify: command exit 0
- [x] 8.2 `cargo clippy -p arkflow-plugin --lib` introduces no new warnings vs. baseline → verify: command output
- [x] 8.3 `openspec validate harden-vrl-processor --strict` passes → verify: command exit 0
