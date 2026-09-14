## Why

The four commits on `v1` (`86c6033`, `7c7f1aa`, `0f77194`, `047c029`) closed the
kernel, durability, control-plane, and console defects found by the previous
review rounds, then added streaming-Job documentation and a local Jobs example.
A max-effort review of that range (`git diff origin/v1...HEAD`, 33 files, +2867)
confirmed 15 defects that the range itself introduces or re-exposes. Three of
them are data-losing or state-wedging under configurations the range advertises:

- `crates/arkflow-core/src/executor/window.rs:174` still seeds `min_i64`/`max_i64`
  from `count == 0` while `observe_float` (line 192) was switched to
  `float_observations == 0`. `count` counts both representations, so an Int64
  delivery that follows a Float64 one takes the `else` branch and folds a
  fabricated `0` boundary into the emitted aggregate through the new
  `widened_min`/`widened_max` (lines 220-234). The range's own comment calls
  per-batch Int64/Float64 flapping "routine".
- `crates/arkflow-core/src/wal/store.rs:347` reclaims every entry with
  `key <= seq` inside `advance_cursor`, but the acknowledgement path advances the
  cursor *before* the wrapped source commit and compensates a failure with
  `rewind_cursor` (`crates/arkflow-core/src/wal/mod.rs:502` then `:516`).
  `rewind_cursor` (store.rs:365) only writes META and `read_after_cursor` (store.rs:396)
  iterates the table, so the rewound entry is unrecoverable — the durability
  requirement `WAL cursor advancement precedes wrapped source commit` is now
  satisfied only in form.
- `crates/arkflow-core/src/state.rs:715` decrements the new incremental `bytes`
  counter with a wrapping `fetch_sub`, while `restore_entry` (line 638) and
  `restore` (line 1012) still reset the counters from `metrics()`, a scan that
  *hides* expired rows. One expiry-plus-compensation cycle makes the subtraction
  wrap to ~`u64::MAX` and every later state write fails `state budget exceeded`
  for the process lifetime.

The `47c029`/`86c6033` fixes also left a rolling-upgrade break and two
silently-degrading control-plane paths (Agent header-only session token, the
rollback generation fence, and the rollback format check), so the range is not
yet safe to ship as the v1 Job runtime.

## What Changes

**Aggregation correctness (`arkflow-core`)**

- Seed `AggregateBuffer::observe_i64` on `int_observations == 0` so each
  representation keeps its own first-observation anchor; no fabricated `0`
  boundary survives into a widened aggregate.
- Keep the per-representation observation counters consistent for buffers that
  did not come from `observe_*`: back-fill them in `decode_buffer` for typed V2
  payloads written before the counters existed, and set them in
  `LegacyAggregateBuffer::migrate`.
- Make `LegacyAggregateBuffer::migrate` carry `sum_float` instead of discarding
  it, so a migrated buffer folds the float contribution into the widened sum.

**Durability (`arkflow-core`)**

- Reclaim only the entries the rewind path cannot need: keep the entries between
  a rewound cursor and the previous high-water mark replayable, or make
  `rewind_cursor` fail loudly when its entry is already reclaimed. The S3 WAL
  backend (HWM-only) and the local redb backend SHALL keep the same replay
  guarantee.
- Replace the wrapping counter arithmetic in `state.rs` with an exact
  accounting that cannot underflow: derive the freed bytes from the row that was
  actually removed, and make the budget check use the same definition of "size"
  as the counter (or recompute exactly when the counter and the table can
  disagree).
- Correct the `keys` accounting for an overwritten expired row so the counter
  does not drift upward when the amortized purge has not run.

**State journal (`arkflow-core`)**

- Gate the apply-side mutation by the transaction's staged version for every
  variant, or fail the whole transaction, so a retried transaction can never
  replay a stale `Delete` over a newer commit.
- Preserve apply-time versions in `Applied::previous_versions` so an undo cannot
  write a version older than a concurrent commit back into `key_versions` and
  disable the ownership check for a later transaction.

**Kernel (`arkflow-core`)**

- Surface a dead processor worker pool as a chain failure: a disconnected
  failure channel SHALL NOT be read as a clean shutdown, and the pool's fences
  SHALL stay effective (or the chain SHALL fail) so in-flight deliveries are
  settled.
- Bound the pool's `flush()` wait so one panicking worker cannot park the chain
  silently.

**Kafka input (`arkflow-plugin`)**

- Do not await a partition (re)assignment while holding the per-input
  acknowledgement lock or the consumer read guard.
- Settle a tombstone delivery without running the ack inside `read()`, so the
  source loop keeps polling and the delivery's compensation path stays
  reachable.

**Control plane and Agent (`arkflow-server`)**

- Send the session token in both the `Authorization` header and the query
  parameter for one transition window so an upgraded Agent keeps working
  against an un-upgraded Hub (and vice versa).
- Extend the job-write generation fence to cover `checkpoint_id` (or bump the
  generation when the checkpoint pointer moves), so a rollback cannot write back
  a stale recovery pointer.
- Apply the upgrade path's state-format check to rollback as well, and refuse a
  target version whose state format the current Job cannot restore, instead of
  accepting the request and silently restarting without state.

**Console (`console`)**

- Report the stopped/pending-recovery state an upgrade produces and offer the
  start action, instead of silently closing the editor.
- Stop clearing validation on node selection; keep the submit button enabled
  after a successful validation.
- Do not let the detail-panel poll overwrite a newer selection.

## Non-goals

- No redesign of the unified execution kernel, the state journal's transaction
  model, or the Hub's storage schema. Every fix is a surgical correction to the
  behavior the range already introduced.
- No new configuration surface beyond validating the one knob the range added
  (`state.max_pending_transactions`) and documenting it in the config reference
  and JSON schema.
- No change to the window's dual int/float representation itself, and no
  migration of already-written checkpoints beyond the decode-time back-fill.
- No new WAL backend, no change to the S3 segment format, and no change to the
  acknowledgement ordering contract (`WAL cursor advancement precedes wrapped
  source commit`).
- Not in scope: the areas the review left as latent-only (the aligner's
  non-`release()` exits, `fanout_ack(parent, 0)`, `TrackingAck::mark_held`
  after completion, and the amortized-purge byte-accounting drift) — recorded
  here so a later change can pick them up, but no reachable wrong output
  exists today.

## Capabilities

### New Capabilities

None. Every requirement below modifies an existing capability.

### Modified Capabilities

- `columnar-window-operators`: window aggregates SHALL keep each numeric
  representation's observations independent (no fabricated min/max boundary),
  and migrated or restored buffers SHALL preserve their accumulated range and
  sum.
- `input-durability`: WAL acked-prefix reclamation SHALL NOT make a rewound
  entry unreplayable; the local and object-store backends SHALL keep the same
  recovery guarantee.
- `keyed-state-backend`: state-size accounting SHALL stay exact and
  non-wrapping across every mutation and resync, and the version guard SHALL
  bind both directions (a retried apply SHALL NOT overwrite a newer commit, and
  a rollback SHALL NOT regress the observed version).
- `unified-execution-kernel`: a dead processor worker pool SHALL fail the chain
  instead of retiring silently, and the chain's fencing operations SHALL have a
  bounded wait.
- `message-acknowledgment`: settling a delivery SHALL NOT block the source loop
  or hold a shared acknowledgement lock across an unbounded external wait.
- `compute-node-agent`: the session credential SHALL be accepted from the
  header and, during the transition window, from the legacy query parameter, so
  an Agent and a Hub at adjacent versions interoperate in both directions.
- `control-plane-api`: job upgrade and rollback SHALL fence the whole written
  record (including the recovery pointer) and SHALL validate the target's state
  format before accepting.
- `checkpoint-recovery`: the recovery compatibility result SHALL be applied
  identically on the rollback path as on the upgrade path, and a filtered-out
  artifact SHALL surface instead of degrading to a stateless start.
- `control-plane-console`: the Job editor SHALL surface the state an upgrade
  leaves the Job in and SHALL NOT discard a successful validation on node
  interaction.

## Impact

- `crates/arkflow-core/src/executor/window.rs` — `AggregateBuffer`
  (seeding, counters, migration), `decode_buffer`, `LegacyAggregateBuffer`.
- `crates/arkflow-core/src/executor/state_journal.rs` — `apply`'s fence and
  `Applied::previous_versions`, `restore_previous`.
- `crates/arkflow-core/src/executor/task.rs` — `ProcessorWorkerPool::fail`,
  `flush`, and the chain loop's pool-failure arm.
- `crates/arkflow-core/src/state.rs` — incremental `keys`/`bytes` accounting,
  the amortized purge, and the `max_bytes` budget check.
- `crates/arkflow-core/src/wal/store.rs` — `advance_cursor` reclamation,
  `rewind_cursor`, `next_seq_hint`.
- `crates/arkflow-plugin/src/input/kafka.rs` — `KafkaAck::ack` lock scope,
  `wait_for_assignment`, the tombstone path in `read`.
- `crates/arkflow-server/src/{agent,hub,lib,storage}.rs` — token transport,
  `update_job_with_expected_generation`, the rollback handler, the
  `record_observed`/checkpoint-pointer write paths.
- `console/src/features/{job-editor,jobs}.tsx` — upgrade submission flow,
  node-change handling, detail polling.
- Tests: new regression tests per finding; `crates/arkflow-plugin/tests/kafka_eos.rs`
  keeps its Docker-less skip but the skip SHALL be reported, not silent.
- Docs: `docs/docs/configuration/1-top-level.md` and the generated JSON schema
  gain `state.max_pending_transactions`.
- Backwards compatibility: existing checkpoints and WAL directories keep
  loading; no on-disk format changes. The Agent/Hub token transition is
  explicitly bidirectional for one release.
