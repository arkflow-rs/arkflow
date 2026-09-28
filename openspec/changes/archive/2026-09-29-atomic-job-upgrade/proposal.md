# Proposal: atomic-job-upgrade

## Why

Job spec upgrades today require the operator to run a six-step manual sequence — `stop` → wait for convergence → trigger `savepoint` → wait for `completed` → `POST /jobs/{id}/upgrades` → `start` — because the upgrade API hard-requires a fully stopped job (`job_must_be_stopped` guard, `crates/arkflow-server/src/lib.rs:1327`) and a pre-existing completed savepoint (`savepoint_not_ready`, `lib.rs` upgrade handler). Every gap between those steps is unbounded operator latency, and every intermediate state is a place where a job sits stopped (or a savepoint goes stale) while a human babysits the pipeline. The building blocks for a single atomic operation already exist and are correct: the recovery pointer moves on checkpoint completion while versions still match (`crates/arkflow-server/src/hub/checkpoint.rs:33`), the fenced upgrade write preserves that pointer (`crates/arkflow-server/src/hub/jobs.rs:130`), and the reconciler already stops superseded generations when a new one starts (`crates/arkflow-server/src/hub/placement.rs:932`). What is missing is orchestration: a durable, failure-tolerant state machine that chains those steps behind one API call with a bounded cutover window.

## What Changes

- New Hub-side orchestration: `POST /jobs/{id}/upgrades` gains an atomic mode that performs savepoint → version commit → recovery start as one supervised operation with per-phase deadlines, audit, and events. The stop-the-world upgrade path remains available and unchanged.
- New durable orchestration record (`cp_job_upgrades` table + in-memory cache + boot recovery) modeled on the existing rollout orchestration (`hub/rollout.rs`): tick-idempotent phases, fail-pause semantics, rollback as a new orchestration over the previous version.
- Reconciler fence: while an orchestration is active for a job, `reconcile_job` defers to it (skips re-placement during the savepoint phase; lets normal reconcile drive the start during verification). Job-level mutations (desired-state PUT, actions, second upgrade) are rejected with `409 orchestration_in_progress`.
- Checkpoint retention pin: retention enforcement must not delete an artifact referenced by an active (or rolling-back) orchestration.
- Cutover semantics are explicit and documented: events processed by the old generation after the savepoint barrier are replayed by the new generation — exactly-once for transactional sinks, at-least-once otherwise.
- Console: atomic upgrade entry point on the jobs page (kick off, watch phase progress).
- Docs: bilingual coverage of the API, the phase machine, and the replay semantics.

No Agent or kernel code changes. No changes to the checkpoint protocol, barrier mechanics, or data plane.

## Capabilities

### New Capabilities
- `job-upgrade-orchestration`: durable multi-phase Hub orchestration for atomic job upgrades/rescales — phase machine (savepoint → commit → verify → succeed/rollback), per-phase deadlines, reconciler fencing, retention pinning, observability (events/audit/status API), and failure-mode guarantees (old generation survives savepoint failure; idempotent re-entry after Hub restart/failover).

### Modified Capabilities
- `control-plane-reconciliation`: new requirement — the general reconciler SHALL defer job re-placement while an active orchestration owns the job, and SHALL resume normal reconciliation for the verification phase (where ordinary reconciliation is the start mechanism).
- `control-plane-api`: the job upgrade endpoint gains the atomic mode request/response contract and the `orchestration_in_progress` conflict condition for concurrent job mutations.

## Non-goals

- Blue-green / warm-standby zero-gap cutover (source release gate, dual-generation agents) — separate future change.
- Autoscaling (lag-driven parallelism recommendation or automation) — depends on this change's mechanisms.
- In-place rolling rescale without a savepoint cut; partial-subset checkpoints; watermark resharding across partition-count changes.
- Any Agent-side or kernel-side behavioral change; new Agent commands; data-plane protocol changes.
- Multi-hub orchestration coordination beyond what the existing leader lease already provides.

## Impact

- **Code**: `crates/arkflow-server/src/hub/` (new `job_orchestration.rs`; fence hook at the top of `reconcile_job` in `placement.rs`; retention guard in `checkpoint.rs`), `crates/arkflow-server/src/storage/` (new `cp_job_upgrades` schema + methods for SQLite and Postgres + `mod.rs` contract), `crates/arkflow-server/src/lib.rs` (handlers, route wiring, API contract types).
- **API**: additive — new request mode, orchestration status/actions endpoints; existing upgrade/rollback contracts unchanged.
- **Storage**: one new table (`cp_job_upgrades`); no migration of existing tables.
- **Console**: `console/src/features/jobs.tsx` (+ job editor) — atomic upgrade trigger and phase progress.
- **Docs**: `docs/docs/operate/control-plane/` and the zh-Hans mirror.
- **Dependencies**: none new.
