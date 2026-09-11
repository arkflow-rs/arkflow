# Design: repair-event-time-review-defects

## Context

`accumulate` (`crates/arkflow-core/src/executor/window.rs`) creates the `(window, key)` buffer entry before checking whether the row's value is NULL, so a NULL row registers a zero-observation `AggregateBuffer` with the `Int64` default kind. `fire_ready` fires any buffer with `end <= threshold && (!emitted || updated_since_emit)`; a zero-observation buffer therefore emits a fabricated `count=0` row, and because the fired batch's output kind was derived from the first fired buffer's value, a phantom Int64 buffer truncated Float64 sums in the same batch via `numeric_array`'s `as i64` coercion.

Separately, the gate's held-row release path emits the latest containing window membership as an on-time row in the same decision that forwards the closing watermark. That ordering guarantee protects the gate edge (per-edge FIFO plus the window vertex's min-aggregation), but the window operator also advances its own frontier from batch-embedded `__watermark_ms` columns (`observe_watermark`) and from shared-tracker watermarks forwarded by other source edges — so an unmarked released row can reach the operator after its window already fired and was cleaned (default `allowed_lateness_ms = 0`). Admission then re-opens the window and the next fire duplicates an already emitted initial result.

## Goals / Non-Goals

**Goals:**

- Zero-observation buffers never emit fabricated aggregates and are removed at fire time.
- The fired batch's output kind is the widest kind across the fired buffers; widening never truncates.
- A membership whose window fired and was cleaned behind the operator's frontier is never re-opened by an unmarked row.
- Session behavior is unchanged: `session_late_masks` keeps owning dynamic session lateness, and bridged sessions may legitimately re-key closed windows.

**Non-Goals:**

- redb error-as-empty conflation (review P2).
- Overflow-arithmetic divergence between gate and operator (review P2).
- Legacy state migration kind fixes (review P2).

## Decisions

### Decision 1: Drop zero-observation buffers in the fire loop, before marking emitted

In `fire_ready`, a `count == 0` non-legacy buffer with no legacy payload is removed from the buffers map and skipped before the emitted/journal bookkeeping. The removal mirrors the existing expired-buffer cleanup (map removal only). Removing only at fire time (not at admission) keeps still-open windows alive: a NULL row must not pre-drop a window that later observations can still join.

Alternative rejected: skipping buffer creation on NULL rows in `accumulate` — a session's `session_end_ms` tracking and sliding memberships legitimately need the entry, and an early drop would lose windows that later non-NULL rows would join.

### Decision 2: Derive the fired batch kind with `NumericKind::wider`

`NumericKind::wider` folds over the fired buffers' kinds (Int64 < Float32 < Float64), matching the existing merge rule in `AggregateBuffer::merge`. `numeric_array` already widens integers to the batch's float kind, so the widest-kind selection makes mixed batches lossless and keeps single-kind schemas unchanged.

### Decision 3: Guard admission with the operator's own frontier, excluding sessions

In `accumulate`, a membership is skipped when the row carries no late-update flag, the configured kind is not a session, the buffer does not exist, and `window_end <= current_watermark` (the operator's frontier read once per batch). This is the point where all orderings converge, so it covers every path that can deliver an unmarked row after the frontier moved (embedded watermark columns, shared-tracker forwarding, release races). Skipped rows settle through the existing empty-`touched` acknowledgement path.

Alternative rejected: fixing only the gate's release branch — the gate cannot observe the operator's frontier, so any gate-side classification can be stale by the time the row is admitted.

## Risks / Trade-offs

- [A genuinely on-time row could be skipped if the operator's frontier is already past its window] → [then its window already fired; admission would duplicate the result, so skipping matches the late policy's Drop default and the row is acknowledged.]
- [Zero-observation windows are dropped rather than emitted] → [an aggregate over zero observations has no meaningful value; consumers no longer see fabricated `count=0` rows.]
- [Mixed-kind batches emit the widest kind] → [schema matches the declared value column type in the homogeneous case; only genuinely mixed legacy state widens.]

## Open Questions

None — both fixes are covered by deterministic operator-level regression tests.
