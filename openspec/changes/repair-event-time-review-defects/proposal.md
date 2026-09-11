# Repair event-time review defects

## Why

An independent code review of the event-time/windowing layer confirmed two defects that silently corrupt window aggregates:

1. **NULL values fabricate phantom aggregates and truncate float sums** (`crates/arkflow-core/src/executor/window.rs:1060`): `accumulate` creates the buffer entry (`or_default`, kind=Int64, count=0) before the NULL check, so a row whose value is NULL produces a zero-observation buffer that fires as a fabricated `count=0/sum=0/min=0/max=0` row; and `fire_ready` picked the whole fired batch's output kind from the first buffer's value (`window.rs:1314` region), so an Int64-kind phantom firing next to Float64 aggregates truncated every float sum via `as i64` and flapped the output schema between Int64 and Float64.
2. **A released held row can re-open a fired-and-cleaned window** (`crates/arkflow-core/src/executor/window.rs:1011` region): the operator's watermark frontier can advance between the source gate's release decision and admission (batch-embedded `__watermark_ms` columns observed at `window.rs` `observe_watermark`, shared-tracker forwarding from another source edge). An unmarked row whose window already fired and was cleaned (default `allowed_lateness_ms = 0`) was admitted into a fresh buffer and re-emitted as a duplicate initial window result, bypassing the Drop/Route policy.

## What Changes

- **Skip zero-observation buffers at fire time**: windows whose rows carried only NULL values are dropped instead of emitting fabricated zero-sentinel rows (legacy payloads keep the original-row emission contract).
- **Widen the fired batch output kind**: the output `sum`/`min`/`max` columns use the widest numeric kind across the fired buffers (Int64 < Float32 < Float64), never truncating float aggregates back to integers.
- **Guard window admission against the moved frontier**: an unmarked row membership whose window end is already behind the operator's watermark frontier and whose buffer no longer exists is treated as a late membership instead of re-opening the window; session timing stays owned by `session_late_masks`.
- Regression tests cover both defects at the operator level.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `columnar-window-operators`: keyed aggregation emission excludes zero-observation buffers and keeps the widest numeric kind; late-event policy explicitly covers memberships whose window already fired and was cleaned behind the operator's frontier.

## Impact

- Runtime: `crates/arkflow-core/src/executor/window.rs` — the only production code change.
- Tests: `crates/arkflow-core/src/executor/window.rs` tests module — two regression tests.
- Specs: 1 master spec updated via this change's delta at archive time.

## Non-goals

- redb "table missing vs IO error" conflation in `state.rs` (review P2).
- Gate/operator divergence on overflowing window-boundary arithmetic (review P2).
- Legacy `LegacyAggregateBuffer::migrate` always migrating as Int64 (review P2).
