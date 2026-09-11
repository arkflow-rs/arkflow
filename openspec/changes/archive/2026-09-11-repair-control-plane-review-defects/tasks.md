# Tasks: repair-control-plane-review-defects

## 1. Intent retry re-dispatch

- [x] 1.1 In `enqueue_with_metadata` (`crates/arkflow-server/src/hub.rs`): short-circuit the operation-id override only for in-flight or `Succeeded` records; terminal failures fall through and enqueue the replacement operation with the attempt's fresh command id.

## 2. Session token entropy

- [x] 2.1 Add `rand = "0.9"` to `[workspace.dependencies]` and `crates/arkflow-server`; mint the registration session token from `OsRng` (32 bytes, hex) and delete `SESSION_SEQUENCE`.

## 3. Observation CAS

- [x] 3.1 Add `ControlPlaneStore::update_job_observation` (generation CAS inside `immediate_transaction`, zero rows → `GenerationConflict`), the `StorageCommand::UpdateJobObservation` variant + actor arm, and the `StorageActor` wrapper.
- [x] 3.2 In `observe_job`: use the CAS with the read generation; on conflict re-read and return the current record; re-check the generation under the write lock in the in-memory branch.

## 4. Placement fencing

- [x] 4.1 In `reconcile_job`'s start path: mark current-generation `Succeeded` starts outside the target set as `Superseded` (map + `persist_operation`) before dispatching, so sticky placement and `nodes_to_stop` fence the abandoned node.

## 5. Regression tests

- [x] 5.1 `terminal_failure_intent_reenqueues_a_fresh_command_on_retry`: transient failure → intent `retrying` → after backoff `reconcile_once` enqueues a fresh command.
- [x] 5.2 `stale_job_observation_cannot_rollback_generation`: CAS rejects the stale write with `GenerationConflict`, desired-state bump survives, fresh observation applies.
- [x] 5.3 `replaced_placement_supersedes_abandoned_start_and_stops_it`: blip → move → abandoned start `Superseded`; on return the node receives a stop, not a start.

## 6. Spec deltas (drafted in this change; verify at archive)

- [x] 6.1 `openspec validate repair-control-plane-review-defects --strict --no-interactive` passes.
- [x] 6.2 Each delta's `### Requirement:` header matches its master spec verbatim.

## 7. Final verification

- [x] 7.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 7.2 `cargo test -p arkflow-server --lib` passes (84 tests).
- [x] 7.3 `cargo clippy -p arkflow-server --all-targets` reports no new warnings in touched files.
