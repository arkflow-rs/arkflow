## Why

`PRODUCT-ITERATION-2026-11-22.md` registers PA-P1-08 — four security and consistency gaps across control-plane auditing, managed user creation, OIDC session revocation, and approval segregation:

1. **Silent audit drops on storage failure**: security denials (`crates/arkflow-server/src/api/mod.rs:1344`), identity mutations and rejections (`crates/arkflow-server/src/hub/identity.rs:131,171`), Job lifecycle/pause audits (`crates/arkflow-server/src/hub/observability.rs:332`, `crates/arkflow-server/src/hub/job_pause.rs:82`), and agent/command rejections (`crates/arkflow-server/src/hub/nodes.rs:291`, `crates/arkflow-server/src/hub/operations.rs:364`) discard `record_audit` errors with `let _ = ...`. When the audit store fails or is fenced, security-critical audit records disappear without a metric or log signal.
2. **Concurrent `create_user` overwrite race and wrong conflict status**: `Hub::create_user` (`crates/arkflow-server/src/hub/identity.rs:228-260`) checks `list_users()` without holding `identity_write_lock` and then calls `storage.upsert_user` (`ON CONFLICT(user_id) DO UPDATE`, `crates/arkflow-server/src/storage/sqlite.rs:3376`, `postgres_methods.rs:2753`). Two concurrent creates for the same `user_id` can both pass the check and silently overwrite the first user; even sequential duplicates return `400 invalid_user` instead of `409 Conflict`.
3. **Unbound OIDC sessions survive subject binding, disabling, deletion, and token revocation**: an OIDC browser login for a not-yet-bound subject persists `cp_sessions.user_id = NULL` (`crates/arkflow-server/src/hub/identity.rs:600-623`). Subsequent binding (`create_user`/`save_user`), disabling, deletion, or `revoke_user_tokens` only deletes `WHERE user_id = ?1`, and `resolve_session` (`hub/identity.rs:628-648`) skips directory checks when `user_id` is `None` — leaving old claim-mapped sessions valid for up to 8 hours. Additionally, `create_session` looks up `principal.id` in `cp_users` even for unbound OIDC subjects, which could mis-associate an unbound subject whose `sub` collides with an unbound local `user_id`. (Note: JWKS periodic whole-table refresh mentioned in PA-P1-08 was verified as already implemented and tested in `crates/arkflow-server/src/oidc.rs:282-323,1652-1714` and specified in `openspec/specs/hub-oidc-auth/spec.md`; only a stale comment at `oidc.rs:40-42` needs cleanup.)
4. **Approval segregation threat model is undocumented**: `configuration-approval` enforces `approver_id != requester_id`, while the `admin` role holds both `ManageUsers` and `ApproveRollouts` (`crates/arkflow-server/src/api_contract.rs:86`). A malicious admin can mint a second identity and approve their own request; the threat boundary (four-eyes review, not anti-collusion against a malicious admin) must be made explicit in specs and docs and locked by regression tests.

## What Changes

- **Observable audit write failures**:
  - Add a closed-label Prometheus counter `arkflow_audit_write_failures_total{category="..."}` on `CommandMetrics` (categories: `auth_denial`, `identity`, `job`, `approval`, `rollout`, `node`, `other`) exported via Hub `/metrics`.
  - Add `Hub::record_audit_best_effort(category, record)` and categorized `Hub::record_audit_event_for(category, record)` that increment the counter and emit a structured `tracing::warn!` (scalar metadata only, never credentials) whenever a configured durable store fails an audit write. Volatile Hubs without durable storage (`self.storage.is_none()`) do not count as write failures on best-effort paths.
  - Route every `let _ = ...record_audit...` call site (`api/mod.rs`, `hub/identity.rs`, `hub/observability.rs`, `hub/job_pause.rs`, `hub/nodes.rs`, `hub/operations.rs`) and propagating audit sites (`hub/approval.rs`, `hub/rollout.rs`, `hub/job_orchestration.rs`) through the instrumented helpers.
  - Keep security denials (`401`/`403`) unaffected by audit write failure, and remove the duplicate `operator_principal` lookup in `operator_denied`.
- **Atomic `create_user` with `INSERT` semantics and `409 Conflict`**:
  - Acquire `self.identity_write_lock` in `Hub::create_user`.
  - Add fenced `StorageBackend::create_user` using plain `INSERT INTO cp_users ...` (no `ON CONFLICT DO UPDATE`) in both SQLite and PostgreSQL, returning structured `StorageError::UserAlreadyExists` vs `StorageError::OidcSubjectConflict`.
  - Add `HubError::UserAlreadyExists` and map it to `409 Conflict` (`user_already_exists`) in `api/users.rs`.
- **Complete OIDC subject session revocation**:
  - Add fenced `StorageBackend::delete_sessions_by_oidc_subject` (SQLite + PostgreSQL) to delete unbound sessions (`user_id IS NULL`) whose principal id matches the OIDC subject.
  - Revoke matching subject sessions on `create_user` (when binding `oidc_subject`), `save_user` (when binding/changing/clearing `oidc_subject`, disabling, or changing role/scopes), `delete_user` (atomically inside the delete transaction), and `revoke_user_tokens`.
  - Add defense-in-depth in `Hub::resolve_session`: an unbound session (`user_id IS NULL`) is rejected immediately if `managed_users_only` is enabled, if its subject is now bound to any directory user, or if the lookup fails (fail-closed).
  - Pass explicit `bound_user_id` from `apply_federated_directory_with_binding` into `Hub::create_session_for_user` in `hub_oidc_callback` so an unbound OIDC `sub` can never collide with an unrelated local `user_id`.
  - Clean up the stale comment at `crates/arkflow-server/src/oidc.rs:40-42` (periodic JWKS refresh is already implemented).
- **Approval segregation threat model**:
  - Keep the existing three-role RBAC model (`admin` / `operator` / `viewer`) unchanged.
  - Explicitly document the threat boundary in `configuration-approval` spec and EN/ZH operator docs: the gate enforces two-principal four-eyes review (`approver_id != requester_id`) against accidental or operator self-approval, does not prevent a malicious admin who holds `ManageUsers` from creating a second identity, and relies on the actor-attributed audit trail and/or external OIDC identity governance for admin-collusion defense.
  - Add regression tests verifying direct self-approval rejection and cross-admin approval with complete actor-attributed audit records (`user.create`, `credential.create`, `config_approval.submit`, `config_approval.approve`).

## Non-goals

- Do not add a fourth RBAC role or split `ManageUsers` and `ApproveRollouts` across roles (preserving v1 role contract, OIDC claim mapping, and Console gating).
- Do not add a database schema version bump or new `cp_sessions`/`cp_users` columns (zero-migration approach using existing `cp_sessions` columns).
- Do not introduce an asynchronous retry outbox table for failed audit rows (counter + structured warning log ensures failures are never silent without risking unbounded memory/DB growth during storage outages).
- Do not change the already-implemented periodic JWKS refresh behavior (`oidc.rs:282-323`).
- Do not change static-credential-only deployments when no managed users exist.

### New Capabilities
<!-- None; all changes strengthen existing control-plane specifications. -->

### Modified Capabilities
- `control-plane-identity`: non-silent audit failure accounting and logging; approval segregation note cross-reference.
- `control-plane-credentials`: atomic `create_user` (`INSERT` + `409 user_already_exists`), OIDC subject session revocation on bind/update/disable/delete/revoke-tokens, and read-side defense-in-depth for unbound sessions.
- `control-plane-observability`: export closed-label `arkflow_audit_write_failures_total{category}` counter.
- `configuration-approval`: document the two-principal segregation threat boundary (four-eyes review vs malicious-admin collusion).

## Impact

- **Code**: `crates/arkflow-server/src/hub/{command_metrics,error,identity,observability,job_pause,nodes,operations,approval,rollout,job_orchestration}.rs`, `crates/arkflow-server/src/api/{mod,users,diagnostics}.rs`, `crates/arkflow-server/src/storage/{mod,sqlite,postgres_methods}.rs`, `crates/arkflow-server/src/oidc.rs` (comment fix only).
- **Tests**: Unit, HTTP API, concurrency, and storage fault-injection tests in `crates/arkflow-server/`.
- **Docs**: EN + zh-Hans updates to `users-and-credentials.md`, `reconciliation.md`, `reference/api.md`, and `CHANGELOG.md`.
- **Compatibility**: Duplicate `POST /api/v1/users` changes from `400 invalid_user` to `409 user_already_exists` (intentional correctness fix matching `last_admin_required`/`oidc_subject_conflict`). All other existing routes and contracts are preserved.
