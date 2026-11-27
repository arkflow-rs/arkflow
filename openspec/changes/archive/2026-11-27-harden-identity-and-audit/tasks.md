## 1. Non-silent audit failure accounting

- [x] 1.1 Add `arkflow_audit_write_failures_total{category}` with closed label vocabulary (`auth_denial`, `identity`, `job`, `approval`, `rollout`, `node`, `other`) to `CommandMetrics` (`hub/command_metrics.rs`).
- [x] 1.2 Add `Hub::record_audit_event_for` and `Hub::record_audit_best_effort` in `hub/observability.rs` that increment the counter and emit `tracing::warn!` (scalar metadata only) on durable storage write failures while leaving volatile Hubs (`storage == None`) quiet on best-effort paths.
- [x] 1.3 Route all audit write sites (`api/mod.rs`, `hub/identity.rs`, `hub/observability.rs`, `hub/job_pause.rs`, `hub/nodes.rs`, `hub/operations.rs`, `hub/approval.rs`, `hub/rollout.rs`, `hub/job_orchestration.rs`) through the instrumented helpers; remove the redundant `operator_principal` lookup in `operator_denied`.
- [x] 1.4 Add storage fault-injection tests proving 401/403 denials still return 401/403 while incrementing `arkflow_audit_write_failures_total{category="auth_denial"}` and emitting a warning log, and identity/job audit failures increment their respective categories.

## 2. Atomic `create_user` with `INSERT` semantics and `409 Conflict`

- [x] 2.1 Add `StorageError::UserAlreadyExists` and `StorageError::OidcSubjectConflict`, plus fenced `create_user` (`INSERT INTO cp_users` without `ON CONFLICT DO UPDATE`) in `storage/mod.rs`, `storage/sqlite.rs`, and `storage/postgres_methods.rs`.
- [x] 2.2 Acquire `identity_write_lock` in `Hub::create_user`, call `storage.create_user`, add `HubError::UserAlreadyExists`, and map it to HTTP `409 Conflict` (`user_already_exists`) in `api/users.rs` and `api/mod.rs`.
- [x] 2.3 Add concurrency tests (concurrent `Hub::create_user` and concurrent HTTP `POST /api/v1/users` with the same `user_id`) asserting exactly one `201` and one `409 user_already_exists` with the first user intact, plus storage contract test verifying `create_user` rejects duplicate `user_id` and duplicate `oidc_subject`.

## 3. OIDC subject session revocation & JWKS hygiene

- [x] 3.1 Add fenced `delete_sessions_by_oidc_subject` to `StorageBackend`, `SqliteBackend`, and `PostgresBackend`; update `delete_user` transactions to also clean up unbound sessions matching the deleted user's `oidc_subject`.
- [x] 3.2 Split `apply_federated_directory_with_binding` and add `Hub::create_session_for_user` so `hub_oidc_callback` never mis-binds an unbound OIDC subject whose `sub` collides with a local `user_id`.
- [x] 3.3 Wire subject session revocation atomically inside the `create_user` and `upsert_user` (`save_user`) storage transactions (SQLite + PostgreSQL) and in `revoke_user_tokens`, and add read-side defense-in-depth in `Hub::resolve_session` for `user_id == None` rows (deny when `managed_users_only`, when subject is now bound, or on storage error).
- [x] 3.4 Clean up stale comment in `crates/arkflow-server/src/oidc.rs` (lines 40-42); verify existing periodic JWKS refresh tests pass.
- [x] 3.5 Add unit and HTTP tests covering unbound OIDC session → bind (`create_user` / `save_user`) → 401, disable → 401, delete → 401, revoke-tokens → 401, atomic rollback on session-delete failure, and `sub` == unbound local `user_id` collision safety.

## 4. Approval segregation threat model & tests

- [x] 4.1 Add tests locking the decided segregation policy: direct self-approval is rejected (`approver_id == requester_id`), while a second distinct admin principal can approve and every step (`user.create`, `credential.create`, `config_approval.submit`, `config_approval.approve`) is attributed to its real actor in the audit trail.

## 5. Documentation & verification

- [x] 5.1 Update EN and zh-Hans docs (`operate/control-plane/users-and-credentials.md`, `operate/control-plane/reconciliation.md`, `reference/api.md`) for `409 user_already_exists`, `arkflow_audit_write_failures_total`, unbound OIDC session revocation on binding/disabling/deletion, and the approval segregation threat model.
- [x] 5.2 Update `CHANGELOG.md` `[Unreleased]` and `PRODUCT-ITERATION-2026-11-22.md` (`PA-P1-08`).
- [x] 5.3 Local verification: `cargo test -p arkflow-server --no-default-features --features engine-minimal --lib -- --skip split_l3_pair_stays_pinned_across_incremental_replacement`, `cargo clippy -p arkflow-server --no-default-features --features engine-minimal --lib --tests -- -D warnings`, `cargo fmt --check`, and `node docs/scripts/docs-check.mjs` (full workspace test/clippy matrix including `connectors-kafka` and `pnpm docs:check` verified in PR CI).
