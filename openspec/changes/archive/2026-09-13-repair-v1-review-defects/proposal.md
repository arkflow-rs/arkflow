# Proposal: repair-v1-review-defects

## Why

A full code review of `main...v1` (~40k lines of Rust + 600 lines of TS) found 7 high-severity
defects and ~13 medium-severity defects that must be closed before the branch merges. The most
damaging are self-inflicted control-plane churn (`crates/arkflow-server/src/hub.rs:788-837` re-enqueues
a `job_stop` command plus a persistent operation row every 1s reconcile tick with no terminal-state
memory and no retention on `cp_operations`), an unregisterable zombie kernel
(`crates/arkflow-server/src/agent.rs:507-534` spawns the kernel before registering it in the task map,
so an abort in between leaves a running Job that no report, stop, or stop-all can ever reach), and
data-correctness bugs: a replayed `Increment` double-counts when its compensation was skipped
(`crates/arkflow-core/src/executor/state_journal.rs:663-664` exempts `Increment` from the version
fence while `:1105-1115` still restages the transaction), and the EOS/expired path deletes emitted
window buffers that still owe a correction emit
(`crates/arkflow-core/src/executor/window.rs:1384-1408`), silently losing late session-bridge updates.

## What Changes

- **Control plane (Hub)**: `job_stop` dispatch gains terminal-state memory so a stopped Job is not
  re-enqueued every reconcile tick; operation records gain retention; a fencing-stop enqueue failure
  for one Job no longer aborts the rest of the reconcile tick; checkpoint retention also reclaims
  pending/failed records; storage persistence no longer runs under the operations write lock across
  await points.
- **Control plane (Agent)**: kernel spawn and task-map registration become atomic with a
  cancellation backstop (Drop / registration-first) so an aborted `job_start` cannot leave a
  zombie kernel; the completed-command dedup cache evicts LRU instead of `clear()`-ing wholesale
  (preserves the at-most-once lifecycle guarantee); recovery no longer performs blocking
  object-store I/O on the async runtime; checkpoint no longer holds the tasks lock across
  spawn_blocking object-store writes.
- **Kernel (task/checkpoint)**: a `current_positions()` failure fails the checkpoint round instead
  of sealing a stale/empty frontier and swallowing the queued error; a collector drain timeout
  fails the round instead of returning Ok while an abandoned collector keeps writing after sink
  close; error-exited chains no longer seal a manifest that is missing a participant.
- **State/journal**: skipped `Increment` compensation arms the version fence (or marks the delta
  already-applied) so replay cannot double-count.
- **Event-time/window**: expired-buffer cleanup on EOS no longer discards emitted buffers with
  pending corrections (`updated_since_emit`); `finish()` no longer classifies held rows against a
  sentinel `i64::MAX` watermark that permanently truncates sliding-window updates; rows with a
  timestamp but NULL key are no longer silently dropped and acked (explicit policy/error);
  non-journal window constructors roll back the backend, not just memory.
- **Durability**: `FileCheckpointStore` writes checkpoints atomically (temp file + fsync + rename)
  so a crash cannot leave an empty/torn manifest as the only recoverable point; the WAL undo path
  closes the crash window between cursor rewind and state compensation.
- **Kafka input**: `wait_for_assignment` no longer holds the consumer read lock across the 60s
  await, which currently blocks `connect()` (and waits on the wrong, soon-to-be-replaced consumer).
- **Config compilation**: legacy duration strings (`us`, `ns`, compound `1h30m`) parse again per
  the legacy `humantime` contract; unreachable sliding-window/join buffer plugins and README/doc
  claims are marked deprecated (docs are CI-checked); the unused `streaming_sql` module is removed
  (**BREAKING** for its `pub mod` surface only — no runtime caller exists).
- **Console**: the Job editor resets internal state when the mode/target changes (an upgrade opened
  from the detail panel no longer submits a Create-mode draft); a late validate response can no
  longer unlock submit for a graph edited afterwards; component node ids are generated collision-free.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `control-plane-reconciliation`: terminal-state memory for stop operations, per-Job reconcile
  fault isolation, checkpoint record retention covering pending/failed, and no storage await under
  the operations write lock.
- `distributed-job-runtime`: Agent job start SHALL NOT leave an unregisterable running kernel, and
  duplicate lifecycle delivery SHALL remain at-most-once under dedup-cache eviction.
- `keyed-state-backend`: replayed relative mutations (`Increment`) whose compensation was skipped
  SHALL NOT double-apply.
- `unified-execution-kernel`: checkpoint rounds SHALL fail on frontier-snapshot errors instead of
  sealing stale positions; abandoned collectors SHALL fail the round instead of writing after sink
  close; error-exited chains SHALL NOT seal incomplete manifests.
- `event-time-processing`: EOS/expired cleanup SHALL NOT discard unemitted corrections; end-of-stream
  classification SHALL NOT permanently truncate sliding-window updates; timestamped NULL-key rows
  SHALL follow an explicit policy; window rollback SHALL cover the backend on all construction paths.
- `input-durability`: Kafka reconnection SHALL NOT block on a consumer lock held across the
  assignment wait.
- `checkpoint-recovery`: local checkpoint persistence SHALL be crash-atomic (no torn/empty manifest
  as the sole recoverable point).
- `stream-config-compilation`: legacy duration grammar SHALL keep parsing (or fail with a migration
  pointer); unreachable buffer plugins SHALL be documented as deprecated.
- `control-plane-console`: the Job editor SHALL reset state on mode/target change, SHALL gate submit
  on a validation that matches the current graph, and SHALL generate collision-free node ids.

## Impact

- `crates/arkflow-server/src/hub.rs`, `agent.rs`, `lib.rs`, `storage.rs` (control plane fixes + retention).
- `crates/arkflow-core/src/executor/{task.rs, kernel_handle.rs, state_journal.rs, window.rs,
  event_time_gate.rs, commit.rs}` and `crates/arkflow-core/src/{checkpoint.rs, wal/mod.rs}`.
- `crates/arkflow-plugin/src/input/kafka.rs` (lock scope), `crates/arkflow-plugin/src/buffer/`
  (deprecation), `README.md` / `README_zh.md` / `docs/` (CI-checked doc accuracy).
- `crates/arkflow-core/src/streaming_sql.rs` (removal) and `crates/arkflow-core/src/lib.rs`.
- `console/src/features/job-editor.tsx`, `jobs.tsx`, `job-dag.ts`.
- Tests: `crates/arkflow-core/src/executor/tests.rs`, `crates/arkflow-server/tests/*`,
  `crates/arkflow-plugin/tests/*`, `console/src/features.test.tsx` gain regression coverage for
  each closed defect.

## Non-goals

- P3 findings from the same review (metrics fidelity, OperationStore eviction order, observability
  attribution, clock-skew handling, doc-comment drift, and similar robustness nits) are deferred.
- No redesign of the Hub–Agent protocol, the reconcile scheduler, or the checkpoint format.
- No new console features; only defect closure in the existing editor/jobs UI.
- No performance work beyond removing the specific lock-across-await and blocking-I/O defects listed.
