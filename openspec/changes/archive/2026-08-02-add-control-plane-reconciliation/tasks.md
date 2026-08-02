## 1. Domain state model

- [x] 1.1 Add serializable desired/observed/convergence types and Intent/Attempt state enums in `arkflow-core::control`, with stable snake_case wire names and backward-compatible defaults.
- [x] 1.2 Extend Stream snapshots with desired generation, target configuration version, observed generation, convergence state, retry metadata, and explicit divergence fields.
- [x] 1.3 Add generation and one-shot action identity handling for start/stop/restart, including unit tests for stale and duplicate commands.
- [x] 1.4 Refactor local runtime snapshots so desired state is stored independently instead of being derived from `StreamState`.

## 2. Durable Hub state

- [x] 2.1 Define a storage-neutral repository interface for desired resources, observed snapshots, reconciliation Intents, command Attempts, configuration targets, events, and outbox records.
- [x] 2.2 Add SQLite schema and connection initialization with WAL, foreign keys, busy timeout, synchronous mode, indexes, and active-Attempt uniqueness constraints.
- [x] 2.3 Implement a single-writer Storage Actor and read-only query path; verify that no transaction remains open across Agent/network calls.
- [x] 2.4 Implement atomic desired mutation: increment generation with CAS, supersede old Intents, insert the new Intent, append an event, and enqueue an outbox row in one transaction.
- [x] 2.5 Implement outbox claim/ack/reclaim leases and idempotent event keys; add crash-recovery tests for a worker exiting after claim.
- [x] 2.6 Load non-terminal Intents and unprocessed outbox rows on Hub startup and expose repository errors as stable control-plane problem codes.
- [x] 2.7 Add migration/default initialization for existing nodes and Streams without silently changing their operator intent.

## 3. Agent protocol and Hub reconciliation

- [x] 3.1 Extend registration/report/command contracts with protocol version, boot ID, report sequence, generation, configuration version, and completed action ID.
- [x] 3.2 Make Agent command handling generation-aware, idempotent, expiry-aware, and explicit about superseded, ambiguous, and permanent failures.
- [x] 3.3 Implement the Hub Reconciler with event triggers for desired writes, reports, registration, lease recovery, and expired Attempts, plus a periodic recovery scan.
- [x] 3.4 Claim one active Attempt per resource generation in a short transaction, persist retry count/next retry time/failure classification, and dispatch only after commit.
- [x] 3.5 Enforce `(boot_id, report_seq)` ordering and atomically update observed state, Attempt state, Intent convergence, and resulting events for each valid report.
- [x] 3.6 Recover expired Attempts as ambiguous and require a fresh full report before retrying; add tests for Hub crash before and after command dispatch.
- [x] 3.7 Confirm Intent convergence from observed reports rather than command acknowledgement; add restart confirmation using action IDs.

## 4. Configuration convergence

- [x] 4.1 Associate published configuration versions with node desired state and expose the target version in Hub resources.
- [x] 4.2 Reconcile configuration application and affected Stream lifecycle states as one observable Intent without replacing the previous observed version on failure.
- [x] 4.3 Add rollback and permanent-configuration-failure tests covering blocked Intents and recovery through a new generation.

## 5. API and console contract

- [x] 5.1 Update lifecycle and configuration mutation handlers to create/update desired Intents and return converging responses with generation and convergence fields.
- [x] 5.2 Extend operation and event resources with Intent/Attempt identity, retry state, failure class, superseded generation, and observed result.
- [x] 5.3 Update the console runtime and configuration views to display desired versus observed state and convergence status without treating acknowledgement as success.
- [x] 5.4 Preserve compatibility aliases and add API contract tests for offline, retrying, blocked, superseded, and converged responses.
- [x] 5.5 Add canonical desired-state, reconciliation-detail, and restart-action request/response schemas with `If-Match`, `Idempotency-Key`, `Location`, and stable problem details.
- [x] 5.6 Make existing lifecycle and configuration routes compatibility adapters and verify offline writes return accepted Intents rather than immediate node-unavailable errors.
- [x] 5.7 Extend Agent report/command wire tests for boot ID, report sequence, generation fencing, action IDs, expiry, and ambiguous results.

## 6. Verification and rollout

- [x] 6.1 Add deterministic state-machine tests for all terminal and retryable transitions, including concurrent desired writes and stale command delivery.
- [x] 6.2 Add Hub restart and Agent reconnect integration tests proving persisted desired state is restored and reconciled.
- [x] 6.3 Add configuration convergence tests for successful apply, ambiguous result, permanent failure, and rollback.
- [x] 6.4 Run `cargo fmt --all -- --check`, `cargo test --workspace`, and `openspec validate add-control-plane-reconciliation --strict`.
- [x] 6.5 Document observe-only rollout, feature enablement, rollback behavior, and planned node drain semantics.
- [x] 6.6 Publish the HTTP contract examples and compatibility mapping for v1 clients and Console migration.
