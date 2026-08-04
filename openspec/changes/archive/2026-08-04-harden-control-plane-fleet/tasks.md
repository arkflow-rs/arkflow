## 1. Durable control-plane foundation

- [x] 1.1 Extend SQLite schema and storage actors for durable operations, audit events, rollout records, rollout targets, and retention metadata.
- [x] 1.2 Add additive migrations, legacy defaults, transaction helpers, and recovery queries for active intents, attempts, operations, and rollouts.
- [x] 1.3 Replace Hub operation lookups that require restart durability with storage-backed records while preserving bounded in-memory read caches.
- [x] 1.4 Add tests for atomic mutation, Hub restart recovery, retention pruning, idempotency, and preservation of active desired state.

## 2. Identity, authorization, and audit

- [x] 2.1 Define operator principal, role, action, resource-scope, and compatibility-token types in the control-plane contract.
- [x] 2.2 Implement authentication and resource-scoped RBAC middleware for operator routes while keeping Agent session authentication separate.
- [x] 2.3 Persist audit records for accepted and rejected mutations with actor, action, target, correlation ID, outcome, and stable failure code.
- [x] 2.4 Redact credentials, secret configuration, and unbounded error content from audit records, logs, and API responses.
- [x] 2.5 Add route-level tests for authentication, allowed actions, denied scopes, compatibility token behavior, and audit results.

## 3. Fleet resource and Agent contract

- [x] 3.1 Add stable Fleet resource representations for node compatibility, rollout, rollout target, durable operation, and audit history.
- [x] 3.2 Extend Agent registration, heartbeat, report, command, and result contracts with protocol version, software version, capabilities, metrics, and rollout identity.
- [x] 3.3 Implement report validation using authenticated session, boot identity, and monotonic report sequence without regressing observations.
- [x] 3.4 Implement capability and protocol compatibility checks before command dispatch and persist stable incompatibility outcomes.
- [x] 3.5 Add tests for stale reports, duplicate commands, reconnects, unsupported capabilities, and protocol incompatibility.

## 4. Configuration rollout state machine

- [x] 4.1 Add rollout creation, target selection, immutable configuration version association, and durable batch state transitions.
- [x] 4.2 Implement bounded batch scheduling that respects node maintenance state and current-generation fencing.
- [x] 4.3 Implement configuration/version, Stream lifecycle, and health-gate evaluation before batch advancement or rollout convergence.
- [x] 4.4 Implement pause, resume, cancel, and rollback actions with durable operation identity and audit records.
- [x] 4.5 Adapt existing node-level apply and rollback routes to single-node rollouts without breaking response compatibility.
- [x] 4.6 Add state-machine tests for success, retry, permanent failure, paused gates, resume, cancel, rollback, node drain, and Hub restart.

## 5. Events, metrics, and operational APIs

- [x] 5.1 Add durable event IDs, event retention, filtered event queries, and bounded payload serialization.
- [x] 5.2 Implement authenticated SSE endpoints with event filtering, `Last-Event-ID` replay, and snapshot/resync fallback.
- [x] 5.3 Populate Agent metric reports from runtime snapshots and enforce a fixed metric/label allowlist at Hub ingestion.
- [x] 5.4 Extend JSON and Prometheus metrics with rollout, compatibility, operation, node, and bounded convergence signals.
- [x] 5.5 Add contract tests for SSE authorization, ordering, replay, pruning fallback, secret redaction, and metrics cardinality.

## 6. Console and deployment workflow

- [x] 6.1 Add Console API types and views for rollout creation, batch progress, pause/resume/cancel/rollback, audit history, and compatibility status.
- [x] 6.2 Replace control-plane polling-only updates with SSE subscription plus REST snapshot recovery and visible stale-connection status.
- [x] 6.3 Document local and reverse-proxy deployment, operator identity configuration, Agent compatibility, drain/maintenance, and safe rollout procedures.
- [x] 6.4 Add Console tests for permission failures, rollout state transitions, reconnect recovery, and secret-safe rendering.

## 7. Verification and handoff

- [x] 7.1 Run focused core/server/console tests and add an end-to-end Hub plus multiple-Agent rollout smoke test.
- [x] 7.2 Run `cargo fmt --all -- --check`, focused clippy, `cargo test -p arkflow-server --lib`, and workspace regression tests.
- [x] 7.3 Run documentation checks and `openspec validate "harden-control-plane-fleet" --strict`.
- [x] 7.4 Review API compatibility, migration rollback, security redaction, and working-tree diff before implementation handoff.
