## Context

The VRL processor (`crates/arkflow-plugin/src/processor/vrl.rs`) wraps Vector's VRL (v0.30). Per batch the flow is:

1. Convert each Arrow column → a VRL `Value` per row (`arrow_to_vrl`, ~`:139-310`).
2. Run the compiled program against a per-row target object (`process`, ~`:55-90`).
3. Convert the VRL result objects back → Arrow columns (`vrl_to_arrow`, ~`:313-410`), inferring a schema from the first row (`get_arrow_data_type`, ~`:452-462`).

Three points in this flow silently destroy data (see proposal `## Why`), and there is no `#[cfg(test)]` module. The entire fix surface is a single file; no trait or cross-module change is needed.

Key constraint: VRL's `Value` enum has **no `String` variant** — strings are `Value::Bytes` — so the string/binary ambiguity is inherent to the boundary and must be resolved on the Arrow side.

## Goals / Non-Goals

**Goals:**
- A `Utf8` Arrow column that goes through VRL comes back as `Utf8` (not `Binary`).
- A failing VRL statement is observable (logged + returned as `Err`), never silently drops a batch.
- `Second`/`Millisecond`/`Microsecond`/`Nanosecond` timestamp columns all survive the round-trip.
- Unsupported result shapes fail loudly instead of producing empty/garbled batches.
- A test module pins every one of the above.

**Non-Goals:**
- VRL 0.30 → 0.33 upgrade (separate change).
- Inferring schemas for nested `Object`/`Array` results (only a clear error is required here).
- Reusing `RuntimeState` across calls / full multi-row schema inference.

## Decisions

### D1 — Resolve the string/binary ambiguity at the Arrow boundary, not in VRL

`Value::Bytes` is the only representation VRL gives us for both text and binary. On the output side, before building an array, inspect the bytes: if `std::str::from_utf8` succeeds for every value in the column, emit `Utf8` (or `LargeUtf8` when the total length warrants it); otherwise emit `Binary`. This keeps string columns as strings while still preserving genuine binary data.

**Alternatives considered:**
- *Remember the input column's datatype and force it on output.* Rejected — VRL statements legitimately change types (e.g. `parse_json!`), so blindly copying the input type would lie about the result.
- *Add a config flag `strings_as_binary`.* Rejected — defaults must be correct; the current binary default is itself the bug.

### D2 — Runtime errors return `Err`, routed by the existing pipeline

`Processor::process` already returns `Result<ProcessResult, Error>`. Today the VRL processor discards `resolve()` errors via `if let Ok(v)`. The fix: on `Err`, `tracing::error!` the diagnostic (VRL errors carry good messages) and return `Err(Error::Processor(...))`. The `Stream` already routes a processor `Err` to `error_output` and applies backpressure — no new plumbing. Failing the batch loudly is strictly better than today's silent full-batch drop.

**Why not drop just the bad row:** VRL runs per-batch with one compiled program; isolating per-row failure would require a per-row try/catch around `resolve` plus a partial-result policy — a larger design, deferred.

### D3 — Honor the Arrow `TimeUnit` on input

Dispatch on `DataType::Timestamp(unit, _)` and read the matching concrete array (`TimestampSecondArray` / `TimestampMillisecondArray` / `TimestampMicrosecondArray` / `TimestampNanosecondArray`), converting each to the VRL timestamp via the unit-correct path. Replace `unwrap_or_default()` (which silently returned epoch) with explicit conversion that represents failure as a VRL null rather than 1970.

### D4 — Unsupported result shapes are an error, never an empty batch

Today a scalar result yields an empty batch (`vrl.rs:332-336`) and `Object`/`Array`/`Regex` results make `get_arrow_data_type` fail the whole batch. Decision: a VRL statement that does not yield a row-shaped object (or an array of rows) returns a clear `Err` naming the unsupported shape. This converts silent data loss into an actionable configuration error.

### D5 — `timezone` is additive config, platform-local default

Add `timezone: Option<String>` to the VRL processor config; resolve it via `TimeZone::parse`, falling back to `TimeZone::default()` (VRL's platform-local default) when absent or unparseable (with a `tracing::warn!`). Defaults preserve today's behavior. No breaking change.

## Risks / Trade-offs

- **Behavior change for users relying on the buggy binary output** → strings now come back as strings. This is the intended correctness fix, not an API break; no config compat change.
- **Failing the whole batch on one bad row (D2)** → coarser than per-row isolation, but strictly better than silent full-batch loss; per-row isolation deferred.
- **UTF-8 heuristic (D1)** → a column of binary values that happens to be valid UTF-8 is emitted as `Utf8`. Acceptable: such data is by definition valid text, and genuine binary almost never survives a full-column UTF-8 check.
- **`timezone` parse failure** → fall back to the default timezone with a warning rather than failing config load, so a bad timezone string never blocks startup.

## Migration Plan

1. Apply the four code fixes (D1–D4) and the small cleanups in `vrl.rs`.
2. Add the optional `timezone` field (D5); existing configs unchanged.
3. Add the `#[cfg(test)]` module; `cargo test -p arkflow-plugin` green.
4. Rollback: revert `vrl.rs`; no schema/config/trait change to undo.
