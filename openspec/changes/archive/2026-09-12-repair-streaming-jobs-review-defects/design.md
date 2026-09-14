## Context

`v1` carries the unified execution kernel plus four commits of review repairs
(`86c6033`, `7c7f1aa`, `0f77194`, `047c029`, 33 files, +2867). A max-effort
review of that range confirmed 15 defects; the proposal lists them with the
file:line evidence. The affected code sits on four different layers (window
operator state, WAL/state durability, the chain event loop, Hub/Agent/console),
so the fixes need a shared design vocabulary even though each is small.

Constraints that shape the design:

- Checkpoints and WAL directories written by the current build must keep
  loading; the change may add decode-time normalization but may not change an
  on-disk format.
- The window and journal hot paths run per delivery (or per fired window
  group); the repair must not reintroduce a full-table scan or a per-mutation
  lock acquisition where the range deliberately removed one.
- Hub and Agent ship in one workspace but deploy independently; a rolling
  upgrade must survive either side going first for one release.

## Goals / Non-Goals

**Goals:**

- Make the nine correctness defects unreachable, with a regression test per
  defect that fails on the current tree.
- Keep every fix surgical: no new architectural layer, no new dependency, no
  changes to the kernel's transaction or acknowledgement contracts.
- Leave the two known-latent issues documented rather than "fixed" by adding
  more state.

**Non-Goals:**

- Redesigning the state journal (a per-mutation CAS in the backend is a
  plausible future design and is called out as an open question, not taken now).
- Rewriting the window aggregate representation; the dual int/float fields stay.
- Changing the WAL segment format on the object store.
- Fixing the pre-existing console polling race (see Risks) or the latent
  held-acknowledgement paths the review could not reach.

## Decisions

### D1. Anchor each numeric representation on its own counter, and make the counters consistent at decode

`observe_i64`/`observe_float` guard their initialization with the *representation*
counter (`int_observations == 0` / `float_observations == 0`). The `count` field
counts both representations, which is exactly the bug: an integer arriving after
a float found `count != 0`, took the extend branch against the field's zero
default, and fabricated a boundary that `widened_min`/`widened_max` then folded
into the emitted aggregate. `observe_float` already used its own counter; only
the integer side was wrong.

Adding a `count == 0` term as well was tried and **rejected**: `float_observations
== 0` is the correct guard for a payload that observed values before the counters
existed (a restored float buffer has `count > 0` but no float observations when
the producer predates them), and requiring `count == 0` made the next float
observation re-seed and discard the restored range — it broke the existing
`mixed_int_and_float_observations_keep_both_contributions` test. The counters are
made consistent at the **decode** boundary instead (`decode_buffer` →
`normalize_observation_counters`), which is the single place state enters the
operator.

`normalize_observation_counters` keeps the counters a payload already names and
attributes only the unnamed remainder: a payload with float observations carries
a non-empty float sum, so the integer side takes the remainder; otherwise the
representation implied by `kind` takes every observation. A buffer with no
observations stays fresh. The invariant `count == int_observations +
float_observations` is asserted in debug builds after observe and merge.

### D2. Migration reconstructs the integer payload and drops unrepresentable contributions

`LegacyAggregateBuffer::migrate` sets `int_observations = count` for the integer
aggregate form it reconstructs, so a later merge keeps its min/max instead of
treating it as an unobserved side. Two legacy artifacts are explicitly **not**
carried:

- The legacy float sum. The writer that produced the envelope set `is_float`
  together with a non-zero `sum_float`, and `is_float && count > 0` is already a
  migration error, so a migratable payload has no float contribution to
  preserve. The field is removed from the legacy struct rather than carried.
- A payload that observed nothing carries `i64::MIN`/`i64::MAX` as min/max
  placeholders; migration returns a fresh default buffer so those sentinels
  cannot be folded into a later merge as a boundary no row produced.

A legacy payload that cannot be represented losslessly (the `is_float && count >
0` sentinel form) keeps the hard error: a refused group is recoverable, a
silently truncated aggregate is not.

### D3. WAL reclaim is driven by a committed floor, not by the cursor

The defect is structural: `advance_cursor` moves before the source commit, and
`rewind_cursor` is the compensation. Reclaiming at `advance_cursor` deletes
compensation fodder. The fix splits the two watermarks the store already has on
the object-store side:

- `advance_cursor(seq)` persists the acknowledged cursor and reclaims **nothing**
  beyond `cursor - 1` (the entry a rewind to `cursor - 1` still needs).
- A new `mark_committed(seq)` runs after the wrapped source acknowledgement
  succeeds and reclaims entries strictly below `seq`.

*Alternative considered:* reclaim `..=seq` but make `rewind_cursor` return an
error when the entry is gone. Rejected — it turns a data-loss bug into a
shutdown bug and still loses the record for a non-replayable source.

*Alternative considered:* keep reclaiming in `advance_cursor` but retain the last
N entries. Rejected — N is a guess, and the WAL's whole purpose is the records a
source cannot re-deliver.

### D4. State-size accounting reconciles exactly when the budget is enforced

Two separate problems live in `state.rs`: the counters can wrap, and they can
drift. Both disappear if the accounting is *derived* rather than *maintained*
where it matters:

- When `max_bytes` is configured, the write path computes the exact size
  (row bytes removed, row bytes added) inside the same write transaction, as the
  earlier implementation did. The budget check is then exact by construction and
  a wrapped counter cannot wedge it.
- Without a budget, the incremental counters stay for observability, mutate only
  through saturating arithmetic, and never gate a write.
- `put_with_ttl` distinguishes "replaced a live row" from "wrote over an absent
  or expired row" so the key count does not increment for a row that was already
  physically present.
- `restore_entry`/`restore` adjust the counters incrementally for the row they
  touch instead of resetting them from a scan whose definition differs, so the
  counters never disagree with the budget's definition of size.

*Alternative considered:* store the counters in a redb metadata row inside the
same write transaction. Cleaner, but it changes the on-disk layout, which the
constraints forbid for this change.

### D5. The journal fence binds the destructive kinds, and a fenced mutation owns nothing

Three edits to the same mechanism:

- `Applied::previous_versions` records the version each mutation **actually
  replaced** at apply time (which is what `restore_previous` writes back), not
  the version observed when the mutation was staged.
- The forward fence covers the destructive kinds: a `Put` whose staged version
  is stale is skipped as before, and a `Delete` whose staged version is stale
  fails the whole apply with an explicit error, because a delete cannot be
  partially skipped without leaving the transaction's post-conditions
  inconsistent. The caller already compensates and retries a failed apply.
- `Increment` stays exempt, and this is the load-bearing part of the design: a
  staged increment is a relative delta the backend applies to the current value,
  so it composes across transactions in any order — `staged_increments_compose_
  across_pending_transactions_in_any_order` pins that contract. Fencing it would
  break the documented counter semantics rather than protect them.

*Alternative considered:* per-mutation CAS in the state backend
(`put_if_version`). That is the deeper fix and is recorded as an open question;
it is a larger change than this repair should carry.

*Alternative considered:* skip the stale `Delete` like the stale `Put`. Rejected
— a skipped delete silently leaves state the transaction promised to remove, and
unlike a skipped put there is no replay that restores it.

### D6. A dead worker pool is a chain failure, and every chain wait is bounded

`ProcessorWorkerPool::fail()` returns an error for a disconnected channel. The
`None` case only exists because a worker can exit without recording a failure;
the chain loop treats that as a failure too, cancels and joins the pool so queued
deliveries are settled, and the pool's `flush()` gains a bound so a single
panicking worker cannot park the loop silently.

*Alternative considered:* catch panics inside each worker. That is the deeper
fix (it is what the range did for the chain and graph tasks) but it does not
address the collector, and the chain-level handling covers both.

### D7. The credential transition sends both transports

The Agent sends the token in `Authorization: Bearer` **and** the legacy query
parameter for one release; the Hub prefers the header and keeps the deprecation
warning. This is the only shape that survives either side upgrading first, which
is what the rolling-upgrade constraint requires. The query parameter is removed
in a follow-up once no supported Agent version sends it alone.

### D8. The conditional job write covers the fields the request owns, and never the recovery pointer

`update_job_with_expected_generation` keeps its generation compare-and-set and
writes the fields a rollback or upgrade request owns; it deliberately does **not**
write the recovery pointer. The pointer is owned by the checkpoint path, which
moves it *without* bumping the generation, so any statement that writes it is
writing the handler's earlier read.

Two candidate shapes were rejected. `COALESCE(?checkpoint_id, checkpoint_id)`
does not help: the request's pointer is non-NULL whenever the handler read one,
so the expression always takes the request's older value — the first version of
this change shipped exactly that bug and a test caught it. Comparing the two
pointers to keep the newer one is not possible either, because a checkpoint id
is an opaque string with no ordering.

Dropping the pointer from the write is safe because artifact selection already
filters by job version and state format: after a rollback the newest pointer
cannot be *restored* by the job it is attached to, so it is filtered out and the
operator sees an explicit recovery error instead of a silent stateless start
(which the same change also fixes: the rollback path now runs the upgrade
path's state-format check before accepting). The rollback handler applies that
check against the artifact the job currently points at.

The row is re-read after a successful update, so the returned record reports
what is stored rather than what the handler sent.

### D9. Console: invalidate on content change, not on interaction

`updateSpec` stays the single place that clears validation; the ReactFlow
`onNodesChange` handler calls it only when the derived spec actually changed
(node add/remove/connect/field edit), not for `select`/`dimensions`/`position`
changes. The upgrade path keeps the Hub's recorded lifecycle state and renders it
with a start action instead of closing the editor silently.

## Risks / Trade-offs

[Skipping a fenced `Put` still returns `Ok` from apply, so a transaction can be
reported complete with one mutation unapplied] → The skipped mutation owns
nothing and is excluded from compensation (the range already made
`applied_versions` an `Option` list); the regression test asserts the newer
value survives, and the journal logs the skip at debug level.

[Making a stale `Delete`/`Increment` fail the whole apply changes a
previously-succeeding path] → Only reachable when a later commit took the same
key between staging and applying; failing is the safe direction and the caller
already retries, but the change is behind a test that pins both directions.

[Reclaiming at `mark_committed` instead of `advance_cursor` delays space
reclamation until the source commit returns] → The object-store backend already
reclaims on that boundary, so the two backends converge; the local store's bound
is unchanged, only its reclamation point moves.

[Exact accounting when `max_bytes` is set keeps the per-write scan] → This is
the pre-range behavior and only for budget-configured deployments (which are not
the default); the unbudgeted path keeps the range's incremental accounting.

[Console shows a stopped Job after upgrade where it previously auto-started] →
Deliberate: the Hub records `stopped`/`pending_recovery`, so the console now
tells the truth and offers the action, which is what the review's finding
required.

[The console detail-poll race (a response for a previously selected Job
overwriting the panel) is pre-existing and not fixed here] → Recorded in the
review; out of scope for this change, which only touched that effect's error
handler.

## Migration Plan

No data migration. Existing checkpoints, savepoints, WAL directories, and the
control-plane SQLite database keep working.

1. Land the core fixes (window, WAL, state, journal, pool) with their regression
   tests; `cargo test --workspace` and `cargo clippy --workspace --all-targets`
   are the gate.
2. Land the control-plane and Agent fixes. A mixed-version fleet is supported in
   both directions for one release because D7 sends both transports.
3. Land the console changes; they only change client behavior against an
   unchanged API.
4. Rollback: each layer is independently revertible. The only stateful artifact
   is the WAL reclaim behavior — reverting it restores the (leaky) previous
   reclaim, which is forward-compatible, and the committed floor is derived from
   the existing cursor metadata.

## Open Questions

- Should the state journal move to a per-mutation conditional write in the
  backend (`put_if_version`/`delete_if_version`), removing the parallel
  `apply_fences` map entirely? This is the deeper fix for the class of bugs D5
  patches, and it needs a spec-level decision about the backend trait.
- Should `state.max_pending_transactions` be validated in `JobSpec::validate`
  (reject 0, warn above a documented ceiling) and added to the generated JSON
  schema? This change documents it; validating it is a small follow-up.
- When can the legacy query-parameter session token be removed? It needs a
  supported-version floor for Agents, which the release process owns.
- Should `LegacyAggregateBuffer` migration for the `is_float` sentinel form be
  recoverable (skip the group, warn, and let the window re-accumulate) rather
  than a hard restore failure? Today it fails the whole restore.
