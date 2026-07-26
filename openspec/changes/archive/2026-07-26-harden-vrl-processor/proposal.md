## Why

The VRL processor (`crates/arkflow-plugin/src/processor/vrl.rs`) silently corrupts and drops user data in three ways, and has **zero test coverage** — it is the only processor in the plugin crate without tests:

- **String columns don't round-trip.** VRL's `Value` has no `String` variant (strings are `Value::Bytes`), and the output path maps every `Value::Bytes` to `DataType::binary` (`vrl.rs:454`, building only `BinaryArray` at `vrl.rs:414-429`). So any `Utf8` column read in at `vrl.rs:150-157` comes back as a binary column; downstream `arrow_to_json` / SQL see garbled or base64-encoded data.
- **Runtime errors are silently swallowed.** `program.resolve()` is called as `if let Ok(v) = self.program.resolve(&mut ctx)` (`vrl.rs:73`); an `Err` drops the entire batch with no log and no routing to `error_output`. Ironically the metadata's own recommended example `parse_json!(.message)` (`vrl.rs:39`) is a fallible function whose failure silently loses every row.
- **Timestamp input only supports nanoseconds.** The time unit `_unit` is ignored and only `TimestampNanosecondArray` is attempted (`vrl.rs:293-302`); `Second` / `Millisecond` / `Microsecond` timestamp columns are silently dropped column-wide.

## What Changes

- **FIX**: String round-trip — when emitting a column from `Value::Bytes`, detect valid UTF-8 and emit `Utf8`/`LargeUtf8` instead of `Binary`, so string columns stay string columns.
- **FIX**: Runtime error handling — surface `resolve()` errors (log; return `Err`) instead of silently dropping the batch.
- **FIX**: Timestamp input — honor the Arrow time unit and read `Second`/`Millisecond`/`Microsecond`/`Nanosecond` arrays; stop silently dropping non-nanosecond columns.
- **FIX**: Copy-paste error message at `vrl.rs:117` ("JsonToArrow processor configuration is missing" → VRL).
- **FIX**: Remove the unused `config` field suppressed by `#[allow(unused)]` (`vrl.rs:49-50`).
- **FIX**: Stop silently coercing bad timestamps to epoch via `unwrap_or_default()` (`vrl.rs:296`).
- **FIX**: Simplify redundant `map_or_else(|| None, |v| Some(v))` to `.map(|v| v)` (`vrl.rs:402`).
- **FIX**: A scalar VRL result no longer produces an empty batch (silent data loss) (`vrl.rs:332-336`); unsupported result shapes (`Object`/`Array`/`Regex`) return a clear error instead of failing the whole batch silently (`vrl.rs:452-462`).
- **NEW**: Unit-test coverage — type round-trips (incl. the string case), empty input, runtime-error handling, compile-error handling, and all timestamp units.
- **NEW (additive config)**: Optional `timezone` field so users are not locked to the hardcoded `TimeZone::default()` (`vrl.rs:60`); defaults to the platform local timezone (today's behavior).

No **BREAKING** public API changes. Configs without the new optional `timezone` field behave exactly as today (with the data-correctness fixes applied).

## Non-goals

- **VRL version upgrade** (0.30 → 0.33.x) — separate change; the API surface shift is non-trivial and unrelated to these correctness fixes.
- **Full nested `Object`/`Array` schema inference** — out of scope; this change only requires unsupported result shapes to produce a clear error rather than silent data loss.
- **Stateful `RuntimeState` reuse across `process()` calls** (`vrl.rs:59`) — deferred; needs a thread-safety audit of VRL stdlib state and is orthogonal to the correctness fixes.
- **First-row-only schema inference rewrite** (`vrl.rs:324-337`) — the immediate requirement is "no silent loss"; a full multi-row inference algorithm is a larger design effort.

## Capabilities

### New Capabilities
- `vrl-processor`: The VRL processor's data-correctness contracts — type fidelity (strings stay strings), observable runtime errors, full timestamp-unit support, and the test coverage that pins them.

### Modified Capabilities
<!-- None — there is no existing openspec/specs/vrl-processor spec today. -->

## Impact

- **`arkflow-plugin`**: `crates/arkflow-plugin/src/processor/vrl.rs` is the only source file changed. Optional new `timezone` field on the VRL processor config struct.
- **Dependencies**: None added or upgraded.
- **Behavioral**: String columns round-trip as strings (not binary); `resolve()` failures are observable; non-nanosecond timestamp columns are no longer dropped. These are bug fixes against silent data corruption, not API breaks.
- **Tests**: New `#[cfg(test)]` module in `vrl.rs`.
