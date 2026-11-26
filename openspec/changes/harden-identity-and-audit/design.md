## Design Decisions

### D1. Non-silent audit failure observability (no outbox, per-Hub closed-label counter)

- **Metric**: Add `audit_write_failures: std::sync::Mutex<BTreeMap<String, u64>>` to `CommandMetrics` (`crates/arkflow-server/src/hub/command_metrics.rs`), rendered as:
  `arkflow_audit_write_failures_total{category="<cat>"} <total>`
  with closed vocabulary `AUDIT_FAILURE_CATEGORIES = ["auth_denial", "identity", "job", "approval", "rollout", "node", "other"]`. Unknown categories collapse to `"other"`. Like `arkflow_operator_auth_total` and `arkflow_job_pause_total`, series render only after their first increment so healthy scrapes stay compact and per-Hub tests remain isolated.
- **Hub helpers** (`crates/arkflow-server/src/hub/observability.rs`):
  - `record_audit_event_for(&self, category: &str, record: AuditRecord) -> Result<i64, HubError>`: returns `Err(HubError::StorageUnavailable)` without counting when `self.storage.is_none()` (volatile Hub has no audit store by design). When `self.storage` is `Some` and `storage.record_audit(record)` fails, calls `self.command_metrics.record_audit_write_failure(category)` and logs `tracing::warn!(category, action = %action, resource_type = %resource_type, outcome = %outcome, %error, "control-plane audit write failed")` before returning `Err(HubError::from(error))`.
  - `record_audit_best_effort(&self, category: &str, record: AuditRecord)`: returns early on `self.storage.is_none()`, otherwise delegates to `record_audit_event_for` and discards only the `Result` after metrics + warning have been recorded.
  - Existing public `record_audit_event(&self, record: AuditRecord)` delegates to `record_audit_event_for("other", record)` for backwards compatibility.
- **Auth denial double-lookup fix** (`crates/arkflow-server/src/api/mod.rs`): `operator_denied` takes `authenticated: bool` (derived from `principal.is_some()` already computed in `require_operator_action`) instead of re-invoking `hub.operator_principal(...)`, preventing double counting and duplicate storage reads on denied requests.

### D2. Atomic `create_user` with `INSERT` semantics and `409 Conflict`

- **Storage layer**:
  - Add `StorageError::UserAlreadyExists` and `StorageError::OidcSubjectConflict` in `crates/arkflow-server/src/storage/mod.rs`.
  - Add fenced `create_user(user: UserRecord) -> Result<UserRecord, StorageError>` to `storage_ops!`, `StorageBackend`, `SqliteBackend`, and `PostgresBackend`.
  - SQLite executes plain `INSERT INTO cp_users (...) VALUES (...)` inside `immediate_transaction` and classifies unique errors by column (`cp_users.user_id` → `UserAlreadyExists`, `cp_users.oidc_subject` → `OidcSubjectConflict`).
  - PostgreSQL executes plain `INSERT INTO cp_users (...) VALUES (...)` inside a transaction, commits ONLY on `Ok(_)`, and maps SQLSTATE `23505` by constraint (`cp_users_pkey` → `UserAlreadyExists`, `cp_users_oidc_subject` → `OidcSubjectConflict`).
  - `upsert_user` remains for `save_user` updates and also maps `cp_users_oidc_subject` violations to `StorageError::OidcSubjectConflict`.
- **Hub & API layer**:
  - `Hub::create_user` validates the user first (`400 invalid_user` for malformed payloads), then acquires `self.identity_write_lock.lock().await`, checks `MAX_USERS`, calls `storage.create_user(user)`, and maps `StorageError::UserAlreadyExists` → `HubError::UserAlreadyExists` → HTTP `409 Conflict` (`code: "user_already_exists"`).
  - Concurrent `create_user` calls on one Hub are serialized by `identity_write_lock`; concurrent calls across processes or direct storage calls are enforced atomically by the `PRIMARY KEY` `INSERT`.

### D3. Complete OIDC subject session revocation without schema bump

- **No schema migration needed**: Unbound OIDC browser sessions already have `cp_sessions.user_id IS NULL` and `principal_json` carrying `OperatorPrincipal { id: "<oidc_subject>", ... }`. Bound sessions carry `user_id = Some("<user_id>")` and are already deleted by `DELETE FROM cp_sessions WHERE user_id = ?1`.
- **Prevent `sub` ↔ `user_id` collision on login**: Split `apply_federated_directory` into `apply_federated_directory_with_binding(...) -> Option<(OperatorPrincipal, Option<String>)>` returning `Some(user.user_id)` only when matched via `cp_users.oidc_subject` and `None` when unbound. `hub_oidc_callback` passes that explicit `bound_user_id` into `Hub::create_session_for_user(principal, bound_user_id)`. Existing `Hub::create_session(principal)` stays compatible for direct callers/tests.
- **Write-side revocation (atomic in-transaction + `delete_sessions_by_oidc_subject`)**:
  - Add fenced `delete_sessions_by_oidc_subject(oidc_subject: &str) -> Result<usize, StorageError>` to `StorageBackend` (SQLite + PostgreSQL) and shared in-transaction helpers (`sqlite_delete_unbound_sessions_by_subject` / `pg_delete_unbound_sessions_by_subject`): inside a transaction, selects `(session_hash, principal_json)` from `cp_sessions WHERE user_id IS NULL`, parses `OperatorPrincipal`, and deletes matching `session_hash` rows.
  - Execute atomically inside the storage transaction on:
    - `create_user`: if `user.oidc_subject` is `Some(sub)`, delete matching unbound sessions before commit so a session-delete failure rolls back the `cp_users` insert.
    - `upsert_user` (`save_user`): read previous `cp_users` row inside the transaction, apply the upsert, and revoke `user_id` sessions on disable/role/scope/oidc_subject change plus `oidc_subject` unbound sessions for any newly bound/changed/cleared subject or when a bound user is disabled or has authorization changed — all before commit so failure never leaves committed user changes with live stale sessions.
    - `delete_user`: read `oidc_subject` before deleting from `cp_users` and delete matching unbound sessions in the same transaction alongside `cp_credentials` and `user_id` sessions.
  - On `revoke_user_tokens`: delete both `user_id` sessions and `oidc_subject` unbound sessions and sum the count.
- **Read-side defense-in-depth (`Hub::resolve_session`)**:
  - When `record.user_id` is `None`:
    - If `self.oidc` has `managed_users_only == true`, return `None`.
    - Parse `principal` from `record.principal_json` and check `storage.get_user_by_oidc_subject(&principal.id)`: if `Ok(Some(_))` (now bound to a directory user) or `Err(_)` (fail-closed), return `None`.
  - Closes any race window between an in-flight OIDC callback creating an unbound session and a concurrent admin binding that subject.

### D4. Approval segregation threat model

- Keep `OperatorRole::{Admin, Operator, Viewer}` and `OperatorAction::ALL` unchanged.
- Document in `openspec/specs/configuration-approval/spec.md`, `docs/docs/operate/control-plane/reconciliation.md`, `docs/docs/operate/control-plane/users-and-credentials.md`, and their `zh-Hans` counterparts:
  - Segregation prevents accidental self-approval and operator→admin self-promotion (`approver_id != requester_id`, `ApproveRollouts` admin-only).
  - Because `admin` holds both `ManageUsers` and `ApproveRollouts` (and static `ARKFLOW_OPERATOR_TOKEN` is break-glass), a malicious admin — whether static, directory-managed, or OIDC-authenticated — can create a second local user and credential to self-approve; the Hub does not enforce anti-collusion in software. Detection relies on external review and alerting over the actor-attributed audit trail (`user.create`, `credential.create`, `config_approval.submit`, `config_approval.approve`), and any policy restricting local user/credential issuance must be enforced externally by operators.
- Lock both behaviors (direct self-approval rejected; distinct second admin approved with full audit attribution) in API/Hub tests.
