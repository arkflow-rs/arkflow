## 1. Storage layer

- [x] 1.1 Define `JobUpgradeRecord` in `crates/arkflow-server/src/storage/mod.rs` (upgrade_id, job_id, from_version, to_version, phase, savepoint_id, deadline_ms, retries_used, actor, correlation_id, last_error, created_at_ms, updated_at_ms) plus `StorageCommand` variants and async trait methods: upsert, get, list_non_terminal, delete (prune)
- [x] 1.2 Implement `cp_job_upgrades` schema + methods in `storage/sqlite.rs` (CREATE TABLE in the schema block near the other cp_ tables, upsert/get/list_non_terminal/prune with an age/count bound)
- [x] 1.3 Mirror schema + methods in `storage/postgres.rs` / `postgres_methods.rs`
- [x] 1.4 Extend `migrate_tool.rs` table list with `cp_job_upgrades` and add a unit test round-tripping a record through the SQLite backend (create → upsert phase → read back → prune)

## 2. Hub orchestration core

- [x] 2.1 Create `crates/arkflow-server/src/hub/job_orchestration.rs` with the phase enum (`Pending, SavingSavepoint, CommittingVersion, Verifying, Succeeded, Aborted, Failed, RolledBack, Cancelled, Paused`), terminal predicate, and a `Hub.job_upgrades` in-memory cache mirroring `rollouts`
- [x] 2.2 Implement `create_job_upgrade` (guards: version monotonic, format compat vs current artifact, deep validate via existing `deep_validate_job`, exclusivity against non-terminal orchestrations) returning the 202-shaped record
- [x] 2.3 Implement `reconcile_job_upgrades` (leader tick): savepoint phase — dispatch via `record_job_checkpoint(kind=savepoint)` with idempotency key `{upgrade_id}:savepoint`, bounded retries (default 2), abort on exhaustion/deadline; on completed savepoint advance phase in the same tick
- [x] 2.4 Implement the commit phase: single `update_job_with_expected_generation` write (spec V+1, desired running, observed stopped, convergence pending_recovery) + the CAS conflict-interpretation rule (re-read: version==target && desired==running → advance to verifying; else abort) — unit-test both branches
- [x] 2.5 Implement the verification phase: observe `observed_state`/version/generation; succeed on running-at-target; deadline or irrecoverable failure → spawn rollback; wire `job.upgrade` phase-transition events (deque + `updates.send`) and `job.upgrade.atomic.*` audit rows via `record_job_operation_audit`
- [x] 2.6 Implement rollback as a fenced restore of the previous `JobVersionRecord` with the same savepoint (reuse the existing rollback compatibility checks); terminal `failed` + event on rollback failure, leaving the Job stopped/pending_recovery with pointer intact
- [x] 2.7 Implement boot recovery: extend `recover_persisted_state` to load non-terminal upgrades (order after jobs/operations restore) and re-enter `reconcile_job_upgrades` idempotently; add orchestration reconcile call to the leader tick in `lib.rs` `serve_hub`
- [x] 2.8 Implement operator actions (pause/resume/cancel/rollback) with terminal-state rejection and paused-only resume, each audited

## 3. Fencing and retention pin

- [x] 3.1 Add the phase-aware fence at the top of `reconcile_job` (`hub/placement.rs`): skip the whole body while a savepoint/commit-phase orchestration owns the Job; fall through during verification; no behavior change otherwise — unit test: stale node during savepoint phase does not re-place or bump generation
- [x] 3.2 Reject conflicting job mutations with 409 `orchestration_in_progress` in the desired-state PUT, job action, and upgrade handlers while an orchestration is non-terminal
- [x] 3.3 Add the retention pin in `enforce_checkpoint_retention_for_job` (`hub/checkpoint.rs`): skip artifacts referenced by non-terminal orchestrations; unit test: sweep keeps the referenced savepoint and releases the pin after a terminal state

## 4. HTTP API

- [x] 4.1 Extend `POST /jobs/{id}/upgrades` with `mode: "atomic"` (request: spec, expected_generation, optional timeout_ms; response 202 + upgrade_id + phase) keeping stopped-mode behavior untouched — `api_contract.rs` types for the request/response
- [x] 4.2 Add `GET /jobs/{id}/upgrades/{upgrade_id}` (phase, savepoint_id, deadline, retries, last_error, timestamps) and `POST /jobs/{id}/upgrades/{upgrade_id}/actions` (pause/resume/cancel/rollback)
- [x] 4.3 Extend `console/src/api.ts` client and add the atomic-upgrade trigger + phase progress + actions to `console/src/features/jobs.tsx` (job detail), including the replay-semantics note from the design
- [x] 4.4 Add handler tests: 202 happy path, exclusivity 409s, action guard responses; run `pnpm` console checks from `console/`

## 5. Tests

- [x] 5.1 Unit: full state-machine walk on an in-memory Hub (running job → savepoint completes → commit → observed running at target → succeeded), asserting pointer preservation and version-history upsert
- [x] 5.2 Unit: savepoint failure → bounded retries → `aborted` with the Job record untouched (still running at old version/generation)
- [x] 5.3 Unit: simulated crash between commit and phase update (pre-apply the commit, then run the recovered orchestration) → conflict interpretation advances to verifying without a second write
- [x] 5.4 Unit: verification deadline → rollback applies the previous version with the same savepoint; rollback failure → terminal `failed` with pointer intact and savepoint pinned
- [x] 5.5 Integration-style test over the existing hub test harness: restart/recovery of a non-terminal orchestration mid-savepoint re-dispatches a fresh round and settles the lost command as TimedOut
- [x] 5.6 `cargo test --workspace --all-targets` and `cargo clippy --workspace --all-targets` green

## 6. Documentation

- [x] 6.1 `docs/docs/operate/control-plane/reconciliation.md` (or a new `job-upgrades.md` if cleaner for the sidebar): atomic mode API, phase machine diagram, deadlines/defaults, failure matrix summary, replay semantics (exactly-once vs at-least-once), orchestration conflicts
- [x] 6.2 zh-Hans mirror under `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/operate/control-plane/`
- [x] 6.3 Update `docs/docs/operate/control-plane/overview.md` endpoint list (and zh-Hans counterpart) with the new orchestration endpoints; classify any new ```yaml/JSON fences per `docs/DOCUMENTATION.md` and run `pnpm docs:check`
