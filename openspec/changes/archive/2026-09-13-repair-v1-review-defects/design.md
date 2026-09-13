# Design: repair-v1-review-defects

## Context

The `v1` branch replaces the legacy executors with the unified kernel and adds the Hub–Agent
control plane. A seven-slice code review of `main...v1` verified 7 high-severity and ~13
medium-severity residual defects (see proposal). The branch has already absorbed several
review-repair rounds, so the fixes here are targeted closures with regression tests, not
rewrites. All quoted line numbers refer to the `v1` tree as of the review.

Constraints inherited from the workspace: single execution kernel (no ad-hoc runtimes), bounded
flume backpressure (capacity 1024), acks advance only through the highest contiguous frontier,
stateful mutations stage until the processing ack commits, `flume =0.11`, surgical diffs.

## Goals / Non-Goals

**Goals:**
- Close every P1 and P2 defect from the review with a regression test per defect.
- Keep each fix local to the defective path; no protocol or format changes.

**Non-Goals:**
- P3 findings (metrics fidelity, `OperationStore` eviction order, observation attribution,
  clock-skew handling, doc-comment drift).
- Any redesign of reconcile scheduling, checkpoint format, or the Hub–Agent API.
- Console feature work beyond defect closure.

## Decisions

### D1. Hub stop-churn: terminal-state memory in the dispatch loop (hub.rs:788-837)
Treat a completed lifecycle operation for the current (job_id, generation, operation) as a skip
condition, symmetric with the existing `job_start`/Succeeded check. The skip applies when the
desired state still matches the completed operation (`stopped` for `job_stop`, and for
`job_start` only while the placement matches). Reconcile keeps re-dispatching when the desired
state changes or the generation bumps, so this cannot fight "Failure classification and retry"
(which re-queues after *failed* attempts).
*Alternative considered*: mark intents converged instead of skipping dispatch — rejected; it
touches the convergence state machine and risks masking genuinely needed re-dispatches.

### D2. Operation/checkpoint record retention (hub.rs:1348-1360, storage.rs)
Extend the existing `prune_events`-style sweep with bounded retention for `cp_operations` and
non-completed `cp_job_checkpoints` (pending/failed older than a retention window, count-bounded).
The reconciler's new terminal-state memory (D1) removes the per-tick growth source; the sweep is
the backstop for long-running deployments.
*Alternative*: delete-on-convergence only — rejected; fenced/abandoned records also accumulate.

### D3. Reconcile fault isolation (hub.rs:729-751, reconcile_jobs:551-565)
Downgrade per-Job enqueue failures (`NodeUnavailable`, `Capacity`, …) to a warn + skip of that
Job for the tick; only storage-level errors abort the scan. The online pre-check stays as a
fast path; the TOCTOU window is closed by tolerating the enqueue failure rather than by adding
a second lock acquisition.

### D4. Hub: no storage await under `operations.write()` (hub.rs:711-723)
Collect the superseded operation ids under the lock, drop the lock, then persist the state
transitions. Persisting after the in-memory transition matches how the rest of the reconcile
loop treats storage (best-effort durable mirror of the authoritative in-memory state, recovered
from durable state on restart).

### D5. Agent: registration-first kernel start with cancellation backstop (agent.rs:507-534)
Register a `JobTask` entry (holding the cancellation token) *before* `spawn_kernel_job`, with an
intermediate `Starting` state; `stop`/`stop_all`/abort paths cancel through the registered token.
On spawn failure the entry is removed and the token cancelled. A `Drop` guard on the kernel
handle is added as a belt-and-braces backstop that cancels the token if the handle is ever
dropped without an explicit stop. This keeps `spawn_kernel_job`'s signature unchanged.
*Alternative*: spawn in a structured `tokio::scope` — rejected; larger refactor of JobRuntime.
*Implementation note*: the belt-and-braces `Drop` guard on `KernelJobHandle` was not added; the
registration-first placeholder (join handle awaiting the token or the swap-in signal) already makes
the invariant testable and holds — every abort path leaves either a registered, cancellable entry or
nothing running (`aborted_start_leaves_no_unregistered_running_kernel`).

### D6. Agent dedup cache: per-entry eviction (agent.rs:1949-1958)
Replace `cache.clear()` with oldest-entry eviction (the cache is already keyed by command id in
insertion order via a VecDeque index or by switching to a small LRU maintained manually — no new
dependency). The Hub only redelivers commands whose operations are still active, so per-entry
eviction preserves at-most-once for every realistically redelivered command; wholesale clearing
was the only violation.

### D7. Agent: unblock the async runtime (agent.rs:460-495, 546-671, 682-796)
Route recovery manifest/snapshot reads through the same `spawn_blocking` wrapper the checkpoint
path already uses. Shrink the tasks-lock critical sections to map insertion/removal only;
checkpoint bookkeeping copies what it needs, releases the lock, then performs object-store I/O.

### D8. Kernel: checkpoint rounds fail closed (task.rs:469-531, 1842-1877, kernel_handle.rs:199-304)
- `current_positions()` failure: seed the frontier with an explicit `FrontierUnavailable` marker
  instead of an empty Vec; the round fails when a seeded chain reports unavailable, and the
  waiting loop drains `checkpoint_errors` before returning success (select loops over both
  receivers until `remaining` is empty *and* the error queue is drained).
- Collector drain timeout: set the join error so the round fails, and join/abort the abandoned
  collector before EOS dispatch so it cannot write after sink close.
- Error-exited chains: keep the existing "reports precede finished" ordering, but only remove a
  chain from `remaining` when it delivered a snapshot or the round is already failing; a round
  whose graph is failing is invalidated before persist.

### D9. Journal: skipped Increment compensation must not replay (state_journal.rs:663-664, 1105-1115)
When `restore_previous` skips a transaction's `Increment` because a later transaction owns the
key, arm the same version fence used for destructive mutations for the retry: mark the staged
mutation as `already_applied_before_skip` in the undo snapshot so the restage replays it as a
no-op (or fails the apply explicitly if the key moved again). Forward-path `Increment` keeps its
commutative fast path.
*Alternative*: make Increment destructive always — rejected; it serializes concurrent counters.

### D10. Window: expired cleanup never discards a pending correction (window.rs:1384-1408)
The expired filter additionally requires `!buffer.updated_since_emit`; buffers that owe an
update are routed to the `ready` path first (EOS included, since `threshold=i64::MAX` makes
`end <= threshold` true) and only reclaimed after their correction is emitted. Re-verify the
`late_session_bridge` regression test covers the EOS ordering.

### D11. Window: end-of-stream classification uses real deadlines (event_time_gate.rs:572-587)
`finish()` classifies held rows per membership against each window's own lateness deadline as
computed from the last real watermark, not against a sentinel `i64::MAX` watermark fed through
the late policy. Only memberships genuinely past their deadline (never had one, or deadline
exceeded before EOS) are excluded.

### D12. Window: NULL-key rows follow an explicit policy (window.rs:1112-1115)
A row with a valid event time but NULL key is treated as invalid for keyed windowing: counted in
the invalid/late metric, routed to the late side output when configured, otherwise dropped and
acknowledged — mirroring "Invalid event timestamps SHALL not be held indefinitely". Silently
skipping is removed.

### D13. Window: non-journal paths roll back the backend (window.rs:2150, 2551-2567, 626-631)
Direct constructors register their fired-buffer writes in a backend-level rollback (delete the
emitted marker / restore prior aggregate bytes on `WindowRollback::restore`), or route through
the journal when a journal is present. The kernel path is unaffected (it already uses the
journal); this closes the trap for direct callers.

### D14. WAL undo ordering (wal/mod.rs:662-663, state_journal.rs:1100-1104)
Reorder `CommitOnAck::undo` to compensate journal state *before* persisting the source/WAL
cursor rewind; the cursor rewind becomes the last durable step. If compensation itself fails,
the cursor is not rewound and the transaction stays retryable — the same recovery contract the
forward path uses (cursor advances only after state commits).

### D15. FileCheckpointStore atomicity (checkpoint.rs:157-165)
Write to `<path>.tmp`, `fsync` the file, rename onto the final path, and `fsync` the parent
directory. Retention deletion of the previous checkpoint happens only after the new manifest is
durable. Read-side checksum validation stays as the second line of defense.

### D16. Kafka assignment wait lock scope (kafka.rs:794-810, 334)
Poll the assignment in short iterations, each taking the read guard only for the
`partition_assigned` check and releasing it before sleeping. (The design first considered cloning
the consumer handle for the wait; rdkafka's `StreamConsumer` does not implement `Clone`, so the
poll form was used instead — it preserves the same invariant: the read lock is never held across
the wait, and each iteration observes the CURRENT consumer, so a reconnect interleaves
immediately.) The regression test asserts the guard is released before the wait sleeps and that
the wait stays outside the acknowledgement lock scope.

### D17. Compilation: legacy duration grammar + deprecation docs (stream_compiler.rs:405-418)
`parse_duration_ms` accepts the legacy `humantime` grammar (µs/us/ns units and compound forms
like `1h30m`) by delegating to `humantime::parse_duration` and converting to ms with overflow
checks, matching "Legacy behavior equivalence". `sliding_window`/`join` buffer plugins stay
registered but are documented as deprecated/unreachable via the compiler, and README/README_zh
buffer lists drop them from "available" (CI-checked docs).

### D18. Remove streaming_sql (streaming_sql.rs, lib.rs:51)
No production caller exists (`crates/arkflow-plugin/src/processor/sql.rs` uses DataFusion's own
`sql_to_statement`); the module's "validation" never resolves tables, so keeping it misleads.
Delete the module and its tests. **BREAKING** only for the unshipped `pub mod` surface.

### D19. Console editor determinism (job-editor.tsx:43-81, job-dag.ts)
- Reset editor state via a keyed remount (React `key` on mode+job id) or an explicit
  `useEffect` reset when `mode`/target changes, so an upgrade opened from the detail panel never
  submits a Create draft.
- Guard `validate()`: keep the request's serialized spec; apply the response only if the current
  spec still matches (functional setState comparing the captured spec).
- Generate node ids from a monotonically increasing ref counter (not `length + 1`).

## Risks / Trade-offs

- [D1 skip condition masks a needed re-dispatch] → Condition is scoped to identical
  (job_id, generation, operation) with desired state unchanged; generation bumps and desired-state
  changes always re-dispatch. Covered by reconcile tests.
- [D5 registration-first widens the "start failed" cleanup path] → Failure path removes the entry
  and cancels the token; stop of a `Starting` entry is a no-op that waits for removal. Covered by
  an agent-level abort test.
- [D9 fences Increment on the skip path] → Counter-heavy workloads only lose concurrency in the
  rare skip case; forward path unchanged. Covered by a journal replay test.
- [D10 EOS emits corrections before reclaim] → Slight change to EOS output ordering: corrections
  now appear at EOS instead of being lost; matches the documented Update semantics.
- [D15 rename-based atomicity requires same-directory tmp files] → Object-store-backed stores are
  unaffected (already atomic PUTs); local-only change.
- [D18 removes a pub module] → No workspace caller; main is unshipped, so no downstream consumers.

## Migration Plan

No data migration. Checkpoints written by the old `FileCheckpointStore` remain readable
(read path unchanged). Deploy order is irrelevant; all fixes are backward compatible. Rollback is
a plain revert.

## Open Questions

None — scope and approaches were fixed by the review findings and the user's P1+P2 selection.
