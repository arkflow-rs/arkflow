## Why

The write pump (`pump_edge`) only calls `pending.abort_all()` when it exits with an error. When it exits via `shutdown.cancelled()` or channel closure (both `Ok(())`), pending branch acknowledgements are silently leaked — their source acks are never aborted, so the source positions are never advanced and the affected deliveries are stuck in at-least-once limbo.

This was root-caused during the harden-test-stability cycle: the `disconnect_aborts_pending_and_closes_edge` test failed because abort_all was only called on the pump's Err path, not on Ok shutdown. The receipt loop already aborts unconditionally on exit, so the write pump should do the same.

## What Changes

- `pump_edge` now calls `pending.abort_all()` on every exit path (Ok and Err), not just on Err.
- No new dependencies or configuration; purely a correctness fix in the exit path.

## Capabilities

### New Capabilities

<!-- 无新能力。 -->

### Modified Capabilities

<!-- 无 spec 级既有行为变更： abort_all 的调用条件从 Err-only 放宽为全路径。 -->

## Impact

- `crates/arkflow-core/src/executor/remote.rs`: pump exit path fix.

## Non-goals

- No change to the receipt loop (already aborts unconditionally).
- No change to the drain window or retry semantics.
