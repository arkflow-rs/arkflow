# Design: atomic-job-upgrade

## Context

Today a job upgrade is a six-step manual pipeline (stop → converge → savepoint → wait `completed` → `POST /jobs/{id}/upgrades` → start), gated by `job_must_be_stopped` (`lib.rs:1327`) and `savepoint_not_ready`. Three verified properties of the current code make an atomic, Hub-only orchestration possible without touching the Agent or the kernel:

1. **Recovery pointer protocol** — `record_job_checkpoint` moves `JobRecord.checkpoint_id` only while `record.job_version == job.version` (`hub/checkpoint.rs:33`), and `update_job_with_expected_generation` never writes the pointer column (it is owned by the checkpoint path; `hub/jobs.rs:130`, storage SET list at `sqlite.rs:1575`). Therefore: a savepoint that completes *before* the version bump leaves the pointer already aimed at it, and the fenced upgrade write preserves that. If ordering ever inverts, reconciliation falls back to the newest compatible completed artifact (`placement.rs:976`) — misordering degrades, never corrupts.
2. **Old-generation teardown is already automatic** — when a new generation starts, `reconcile_job` fences historical nodes: nodes outside the new target set receive `job_stop` and their succeeded starts are flipped `Superseded` (`placement.rs:932-974`). The orchestrator never needs a "stop the old one" command.
3. **Barriers do not freeze the pipeline** — checkpoint/savepoint barriers are aligned but asynchronous ("no global write gate", `kernel_handle.rs:70`), so the old generation keeps processing while its final savepoint is taken.

The repo also contains a proven multi-phase orchestration precedent: `hub/rollout.rs` (durable two-level records, tick-idempotent dispatch, fail-pause, rollback-as-new-orchestration). This design is "rollout, but for a job upgrade, with artifact-gated phases and a reconciler fence" — the deltas from rollout are called out in D2.

Known codebase constraint that shapes everything: `JobRecord` holds exactly one spec/version/desired/observed/pointer/generation. This design deliberately does **not** change that (that is blue-green territory); the orchestrator sequences single-generation writes.

## Goals / Non-Goals

**Goals:**
- One API call performs savepoint → version commit → recovery start, with a cutover window bounded by savepoint + restore time.
- Failure-tolerant: savepoint failure leaves the old generation running untouched; Hub restart/failover re-enters idempotently; verification failure rolls back to the previous version from the same savepoint.
- Zero Agent changes, zero kernel changes, zero data-plane changes; additive storage.
- The phase machine, its conflicts, and its replay semantics are observable (events, audit, status API) and specified.

**Non-Goals:**
- Blue-green / warm-standby cutover (source release gate, dual-generation agents) — future change building on this one.
- Autoscaling; in-place rolling rescale; partial-subset checkpoints; watermark resharding.
- Exactly-once guarantees for non-transactional sinks (the replay window is inherent to checkpoint-mediated cutovers; eliminating it requires transactional sinks).

## Decisions

### D1: Hub-only orchestration over existing primitives
The orchestrator chains existing operations — `record_job_checkpoint(kind=savepoint)` + `dispatch_job_artifact`, `update_job_with_expected_generation`, and ordinary reconciliation — rather than introducing new commands.
*Alternative rejected*: an Agent-side `job_upgrade` command. It would duplicate the Hub's placement/recovery logic on the Agent and break the "Hub decides, Agent executes single commands" contract for zero benefit; every primitive needed already exists.

### D2: Orchestration record modeled on `rollout.rs`
New table `cp_job_upgrades` (one row per orchestration: `upgrade_id, job_id, from_version, to_version, phase, savepoint_id, deadline_ms, actor, correlation_id, created_at_ms, updated_at_ms` + `last_error`), in-memory `BTreeMap` cache, boot recovery via the `recover_rollouts` pattern (select non-terminal rows in `recover_persisted_state`). Phases advance on the leader reconcile tick — idempotent because every phase's write is either a deduped dispatch (idempotency key `{upgrade_id}:{phase}`) or a generation-CAS.
*Alternative rejected (a)*: encode phases into `JobRecord.convergence` — it is display-only today (`pending_recovery` is write-only), has no room for deadline/cursor, and the single-record CAS would collide with pointer-ownership rules. *Alternative rejected (b)*: reuse `cp_intents` — job lifecycle commands deliberately do not flow through the intent/outbox path (`enqueue_with_metadata` instead); retrofitting artifact-gated phases onto intents would entangle two state machines.

### D3: Phase machine and CAS-conflict interpretation
```
pending ──▶ saving_savepoint ──▶ committing_version ──▶ verifying ──▶ succeeded
   │             │ failure/deadline (bounded retries)        │ deadline or
   │             ▼                                        observed failure
   └────────▶ aborted                                        ▼
                                                     rollback (new orchestration
                                                     over from_version, same savepoint)
                                          any non-terminal ──▶ paused / cancelled (operator)
```
- **saving_savepoint**: dispatch a savepoint at the *current* generation (targets = current placement, exactly `dispatch_job_artifact`'s generation-keyed selection). Bounded orchestration-level retries (default 2) on a failed round; then `aborted`. The old generation is unaffected throughout (barrier failure does not stop data flow).
- **committing_version**: single `update_job_with_expected_generation` write carrying spec V+1, `desired_state = "running"`, `observed_state = "stopped"`, `convergence = "pending_recovery"`. The pointer is *not* in the write (D5). **Conflict interpretation rule**: on `GenerationConflict`, re-read the job — if `version == to_version && desired_state == "running"`, the commit already happened (crash between write and phase update) → advance to `verifying`; otherwise abort. Idempotency is judged by observable state, never by trusting the phase row.
- **verifying**: normal reconciliation drives the start (recovery payload assembly, historical-node fencing of the old generation). Success = `observed_state == "running"` at `to_version` and the newest succeeded start is at the current generation. Deadline (default 10 min, request-overridable `timeout_ms`) → `rollback`.
- **rollback**: a new orchestration restoring the previous `JobVersionRecord` with the *same* savepoint and `desired = running`, reusing the existing rollback compatibility checks. Rollback failure → terminal `failed` + event; the job is left `stopped`/`pending_recovery` for the operator exactly as the manual path would leave it.

### D4: Phase-aware reconciler fence
Gate at the first line of `reconcile_job` (`placement.rs:530`): if an active orchestration exists for the job, `saving_savepoint`/`committing_version` phases return early (skip the whole body); `verifying` and `rolling_back` (the observation phases) fall through to normal reconciliation. Rationale: a node going stale mid-savepoint would otherwise trigger re-placement + generation bump, and the savepoint's generation-keyed dispatch targets (`operation.generation == job.generation`) would evaporate. The observation phases (`verifying`, `rolling_back`) must *not* be fenced — ordinary reconcile **is** the start mechanism (recovery payload, `historical_nodes` stop fan-out), for the new generation and for a restored previous one alike.
*Alternative rejected*: fencing all phases and having the orchestrator dispatch starts itself — it would re-implement placement, recovery gating, and historical fencing behind a second code path.

### D5: Commit relies on the pointer protocol; no explicit pointer write
The savepoint completes while the job is still at version V, so `record_job_checkpoint` moves the pointer; the commit write preserves it (`checkpoint_id` absent from the fenced write). `job.checkpoint_id.is_some()` then acts as the explicit recovery request (`placement.rs:607`).
*Alternative rejected*: writing `checkpoint_id` in the commit — the SET list deliberately excludes it precisely so handler reads cannot regress the pointer (a pointer retention may already have deleted). Fighting that invariant re-creates the regression class the column rule exists to prevent.

### D6: Retention pin
`enforce_checkpoint_retention_for_job` skips any completed artifact referenced by a non-terminal orchestration (the orchestration's savepoint, including during rollback). Unpinned on every terminal transition. Without this, a long verification phase with a small retention count could GC the very artifact a rollback needs.

### D7: Exclusivity and API surface
One active (non-terminal) orchestration per job, enforced at the API (409 `orchestration_in_progress` for `PUT desired-state`, job actions, a second upgrade) with the D3 CAS rule as the backstop for races the API check misses. The atomic mode is `POST /jobs/{id}/upgrades` with `{"mode": "atomic", "spec": {...}, "expected_generation", "timeout_ms?"}` → 202 + `{upgrade_id, phase}`; `GET /jobs/{id}/upgrades/{upgrade_id}` returns phase/progress/savepoint/error; `POST /jobs/{id}/upgrades/{upgrade_id}/actions` supports `pause|resume|cancel|rollback` with the same guards as rollouts (terminal states reject; resume only from `paused`).

### D8: Periodic checkpoints during orchestration
No special handling needed (verified): a pending savepoint record suppresses `schedule_periodic_checkpoints` for the interval (`checkpoint.rs:324`, any-status newest-record check), and in-flight `(node, resource, operation, generation)` dedupe suppresses duplicate dispatches. A periodic checkpoint that completes after the savepoint simply moves the pointer to a *newer* compatible artifact — the commit stays correct.

### D9: Cutover semantics (documented, not hidden)
Events processed by the old generation after the savepoint barrier are replayed by the new generation: exactly-once for transactional sinks, at-least-once otherwise. The orchestrator minimizes the window by committing and letting verification start in the same tick that the savepoint completes. This is stated in the API response docs and the operations page.

### D10: Observability
Phase transitions emit `ControlEvent`s (`event_type: "job.upgrade"`, outcome = phase) through the existing deque + broadcast path (SSE-visible immediately) and audit rows (`job.upgrade.atomic.initiate/pause/resume/cancel/rollback/phase`) via `record_job_operation_audit`. In-memory, hub-originated events are not persisted to `cp_events` — consistent with `hub.leadership` today.

## Risks / Trade-offs

- [Orchestrator bug disrupts running jobs] → every phase only sequences operations already operator-reachable; every failure path lands in a state the manual pipeline also produces (`aborted` = old gen still running; `failed` = stopped + `pending_recovery`); state machine gets exhaustive unit tests including the CAS-interpretation corner.
- [Flappy node starves the savepoint phase] → orchestration-level retry bound + phase deadline → `aborted`; normal reconciliation (unfenced once terminal) handles the degraded placement afterward.
- [Hub failover between commit and phase update] → D3 conflict-interpretation rule; the resume path decides from the job record, not the possibly-stale phase.
- [Retention pin leak from a stuck orchestration] → pins exist only while non-terminal; `cancel` is always available; terminal transitions unpin unconditionally.
- [Orphan savepoint records from retried rounds after failover] → new rounds use fresh ids; existing 24h pending/failed record pruning (`prune_stale_checkpoint_records`) reclaims orphans; no new GC machinery.
- [Duplicated side effects in the replay window] → documented semantics (D9), window minimized by same-tick commit; true elimination belongs to transactional sinks, out of scope.

## Migration Plan

Additive: one new table (`cp_job_upgrades`), new code paths behind a request mode; no existing table or contract changes. Deploy = normal Hub upgrade; the boot recovery pass adopts non-terminal orchestrations (none exist pre-deploy). Rollback = redeploy previous binary — the extra table is ignored and legacy behavior is untouched.

## Open Questions

- Default deadlines (savepoint 5 min, commit 1 min, verify 10 min) — confirm against fleet checkpoint-duration metrics before freezing defaults; all overridable per request.
- Should v1 expose `pause`/`resume`, or only `cancel`/`rollback`? (Rollout parity is cheap, but pause extends the replay window — the operations doc must state that resuming after a long pause may warrant a fresh savepoint.)
- Orchestration history retention: keep terminal `cp_job_upgrades` rows bounded (count/age) like audit, or rely on audit events alone and prune rows aggressively?
