## Context

ArkFlow currently has a Hub-side node registry and an Agent command channel, but the Hub's `HubOperation` and command queue are bounded in-memory records. A Stream's `desired_state` is derived from its local observed lifecycle state, so the system cannot preserve an operator intent across node disconnection or Hub restart. The existing control API also treats command completion as the primary outcome, while durable convergence requires comparing desired and observed state after execution.

This change introduces a transport-neutral reconciliation model while preserving the existing local RuntimeManager and HTTP polling protocol. The first implementation targets one Hub and many nodes, with no scheduling or cross-node migration.

## Goals / Non-Goals

**Goals:**

- Persist independent desired state for node-owned Streams and configuration versions.
- Distinguish Intent, command Attempt, and Convergence state in the domain model and API.
- Use monotonic generations to supersede stale commands safely.
- Reconcile after Hub restart, Agent reconnect, report ingestion, and retry timers.
- Define bounded retry, ambiguous-result, permanent-failure, and supersession semantics.
- Keep lifecycle commands idempotent and model restart as a one-shot action identity.

**Non-Goals:**

- Cross-node scheduling, placement, migration, or data movement.
- Hub clustering, leader election, or multi-writer consensus.
- User-defined retry policies, rollout orchestration, RBAC, or multi-tenancy.
- Changes to Stream data-plane delivery, WAL, or backpressure semantics.

## Decisions

### 1. Hub owns desired state; nodes own observed state

The Hub stores the operator's desired Stream state and target configuration version. Agents report observed runtime state, applied configuration version, boot identity, and report sequence. Agent reports MUST NOT overwrite desired state. This prevents a node restart or local default from silently changing operator intent.

Alternative considered: continue deriving desired state from `StreamState`. Rejected because offline and failed nodes need to retain a target such as `running`.

### 2. Use three related state machines

An Intent represents a durable target and moves through `accepted`, `converging`, `retrying`, `converged`, `blocked`, or `superseded`. An Attempt represents one command delivery and execution attempt. Convergence is a computed resource condition: `unknown`, `pending`, `applying`, `in_sync`, `degraded`, or `blocked`.

An Attempt reaching `succeeded` does not complete the Intent until a report proves the desired generation and configuration are observed. A new intent supersedes older generations instead of treating them as failures.

Alternative considered: extend the existing operation enum with every state. Rejected because transient command attempts and long-lived desired intents have different lifetimes and terminal semantics.

### 3. Generation is the stale-command fence

Every desired-state mutation increments a per-resource generation. Commands carry the generation and target configuration version. Agents reject or report commands older than their latest accepted generation, and duplicate command IDs return the prior result. A report carries the observed generation so the Hub can prove convergence.

Alternative considered: rely only on timestamps. Rejected because clock skew and reordered HTTP polling make timestamps insufficient for ordering.

### 4. Reconciliation is event-driven with periodic recovery

The Hub schedules reconciliation after desired-state writes, node registration, report ingestion, lease recovery, and operation expiry. A bounded periodic scan remains as a safety net after process or task failure. Only one active Attempt may target a resource generation at a time.

Alternative considered: fixed-interval polling only. Rejected because it adds avoidable convergence latency; event triggers provide prompt recovery while the scan handles missed notifications.

### 5. Retry by failure class

Validation and authorization failures reject the intent before persistence. Network, lease, and temporary execution failures preserve desired state and retry with exponential backoff. Permanent execution failures move the Intent to `blocked`. Ambiguous results require a fresh full report before retrying. A retry budget and next-attempt timestamp prevent hot loops.

Alternative considered: mark every failed command terminal and require a new user request. Rejected because transient node failures should recover automatically without losing intent.

### 6. Stable desired state, explicit one-shot actions

Start and stop modify the stable desired state. Restart retains the desired state but creates an action identity and generation; an Agent reports the completed action ID. This avoids treating `running -> running` as proof that a restart occurred.

### 7. Persist intent history before dispatch

The Hub writes the desired mutation and intent record before enqueueing a command. On restart it loads non-converged intents and resumes reconciliation. The first storage implementation may use a single-writer embedded store; the domain API must not depend on a specific database so a server database can be added later.

Node-level configuration targets use the reserved resource key `__configuration__` in the same generation/Intent/Attempt pipeline. The published version metadata is stored in `cp_config_versions`, while the Intent retains the version ID and an inline candidate reference for dispatch. This keeps offline apply and rollback writes durable without pretending that a configuration mutation is a Stream lifecycle transition; configuration observation and affected-Stream convergence remain a follow-up step based on reported applied versions.

### 8. API reports intent and observation separately

Stream resources expose `desired`, `observed`, generation, and convergence. Mutation responses return an intent/operation ID and `converging` status when execution is not yet proven. Existing operation fields remain available for compatibility, but clients must not interpret command acknowledgement as final convergence.

### 9. Use SQLite as the first durable repository

The first Hub repository SHALL use SQLite in WAL mode. The logical schema consists of current desired resources, observed snapshots, Intent history, command Attempts, configuration-version metadata, events, and an outbox. Configuration content remains in the existing file/object reference path; the database stores its digest and reference rather than duplicating secret-bearing content.

SQLite is preferred over adding a new distributed database because the first Hub is single-writer and needs atomic multi-record transitions, unique constraints, and queryable history. The repository interface remains storage-neutral so a server database can be introduced later.

Alternative considered: reuse `redb` because it is already a workspace dependency. Rejected for this control-plane state because relational uniqueness, filtered active-attempt constraints, ordered history queries, and multi-entity transactions map more directly to SQLite.

### 10. Serialize writes through a Storage Actor

All state-changing repository commands SHALL pass through one Storage Actor and one write connection. Read-only API queries MAY use a small read pool. Every write transaction uses `BEGIN IMMEDIATE`, and SQLite connections configure WAL, foreign keys, busy timeout, and synchronous mode explicitly.

The actor prevents concurrent generation allocation and reduces `database is locked` retries. A transaction MUST NOT remain open during an HTTP request to an Agent or any other network operation.

### 11. Couple desired mutation with an outbox event

A desired-state mutation, supersession of old Intents, creation of the new Intent, and insertion of a reconciliation outbox row SHALL commit atomically. The Reconciler consumes the outbox after commit, so a successful API response cannot leave a durable intent without a wake-up record.

Outbox rows use a unique event key and a reclaimable claim lease. A worker claims and commits the row before dispatching; after a worker crash, another worker may reclaim the row after the lease expires.

### 12. Claim Attempts before network dispatch

The Reconciler SHALL create or claim an Attempt in a short transaction before sending a command. A unique constraint SHALL allow at most one active Attempt for a `(node_id, stream_id, generation)` tuple. Attempt leases and expiry represent in-flight uncertainty; they MUST NOT be interpreted as execution failure without a fresh Node report.

After the network call, acknowledgement and result updates use separate short transactions. This prevents a slow or disconnected Agent from holding the SQLite write lock.

### 13. Apply compare-and-swap and report ordering

Generation updates SHALL use a compare-and-swap condition in addition to Storage Actor serialization. Agent reports SHALL be accepted only when their `(boot_id, report_seq)` is newer than the stored node cursor. Desired/observed/Intent/Attempt convergence updates from one valid report SHALL commit in one transaction.

### 14. Recover by reconciliation, not command replay

On Hub restart, non-terminal Intents and unprocessed outbox rows are loaded. Expired Attempt leases become ambiguous, then the Hub waits for or requests a full Node report before deciding whether to mark the Intent converged or create another Attempt. The Hub MUST NOT blindly replay every command that was in flight at shutdown.

The implementation records the command's Attempt ID in the Agent command and writes the dispatch expiry only when the command is actually handed to the Agent. A periodic lease scan changes an expired active Attempt to `ambiguous` and the Intent to degraded/converging without creating a retry outbox. A newer `(boot_id, report_seq)` report is the recovery barrier: report ingestion persists the observed snapshot and creates a reconciliation outbox only if the current Intent is still divergent. This prevents a shutdown window from duplicating a possibly completed one-shot action.

### 15. Make desired state the canonical operator API

The canonical mutation is `PUT /api/v1/nodes/{node_id}/streams/{stream_id}/desired-state` with a body containing the desired lifecycle state and optional configuration version. It returns an Intent representation and `202 Accepted` while convergence is pending, including the new generation and a `Location` header for the operation resource. Existing `/start`, `/stop`, configuration apply, and rollback routes remain compatibility adapters that create the same Intent types.

The v1 wire contract is deliberately small and stable:

```http
PUT /api/v1/nodes/node-a/streams/orders/desired-state
Authorization: Bearer <operator-token>
Content-Type: application/json
If-Match: "generation-3"
Idempotency-Key: orders-desired-4

{"state":"running","config_version":"cfg-17","action_id":null}
```

An accepted mutation returns `202` with `Location: /api/v1/operations/{intent_id}`, `ETag: "generation-4"`, and a body containing `operation_id`, `intent_id`, `node_id`, `stream_id`, `generation`, `desired_state`, `config_version`, `action_id`, and `convergence`. Replaying the same idempotency key returns the original representation and generation. A stale `If-Match` returns `412` with problem code `generation_conflict`; invalid state returns `422` with `validation_failed`; storage failure returns `503` with `repository_unavailable`. The operation resource is queryable immediately from durable Intent storage, including while the node is offline or before command dispatch.

`GET` Stream resources expose `desired`, `observed`, and `convergence` objects. Legacy `state` and `desired_state` fields remain aliases for `observed.state` and `desired.state` during v1 migration. `If-Match` protects against stale desired-state writes, while `Idempotency-Key` deduplicates client retries. A node being offline does not reject a desired-state write; it produces an accepted Intent with unknown or degraded convergence.

Restart is a separate `POST .../actions/restart` one-shot action and carries an action ID (the legacy `POST .../restart` route remains an adapter). It is converged only when the Agent reports that action ID completed and the Stream is observed running. Operation resources expose Intent state, latest Attempt state, convergence state, generation, retry metadata, and failure classification; an acknowledged or successful Attempt alone is not final Intent success.

Operator API and Agent API remain separate contracts. Agent reports include `boot_id`, monotonic `report_seq`, observed generation, applied configuration version, and completed action IDs. Agent commands include generation, expiry, idempotency identifiers, and the target `config_version_id`; a successful configuration command updates the Agent's node-level observed version, which the Hub compares with the persisted configuration Intent before marking it converged.

The operator contract is resource-oriented and separates three observations:

```text
Stream
├── desired:   { state, generation, config_version }
├── observed:  { state, generation, config_version, boot_id, report_seq }
├── convergence: { state, reason, since }
└── reconciliation: { intent_id, attempt_id, retry_count, next_retry_at, failure_class }
```

`GET /api/v1/nodes/{node_id}/streams/{stream_id}` is the authoritative point-in-time view. Collection endpoints use the same envelope (`items`, `page`, `page_size`, `total`) and a bounded page size of 100. Operation listing supports resource, node, operation, state, and correlation filters. Results are newest-first with a stable identifier tie-breaker so polling clients do not see reordering within a page.

Mutation headers have precise roles: `If-Match: "generation-N"` is a compare-and-swap guard, `Idempotency-Key` deduplicates a client retry for the same principal/resource/body, and `X-Correlation-ID` is echoed in both response headers and problem bodies. A reused idempotency key with a different body is a `409 idempotency_key_reused`; a stale generation is a `412 generation_conflict`. Accepted mutations return `202`, `Location: /api/v1/operations/{intent_id}`, and an `ETag` for the resulting generation.

The problem envelope is stable and machine-oriented:

```json
{
  "code": "generation_conflict",
  "message": "Expected generation 3, current generation 4",
  "correlation_id": "req-123",
  "resource": { "node_id": "node-a", "stream_id": "orders" },
  "current_generation": 4
}
```

`DELETE /api/v1/operations/{id}` is cancellation of the durable Intent, not compensation. Before dispatch it suppresses pending work and transitions the Intent to `cancelled`; after dispatch it records cancellation while retaining the Attempt outcome. The API never claims a non-idempotent restart was undone. Operator authentication and node authentication are separate middleware policies; Agent reports are accepted by cursor ordering but do not expose operator-facing success until durable reconciliation observes the target.

## Risks / Trade-offs

- **[Risk]** Persisted desired state may restart a Stream after an operator expected a node shutdown. → Require explicit drain/disable intent for planned shutdown and expose desired state prominently in health and console views.
- **[Risk]** Retrying after an ambiguous result may repeat a non-idempotent action. → Reconcile from a full report first; use action IDs for restart and make lifecycle commands idempotent at the Agent.
- **[Risk]** A stuck reconciliation can remain pending indefinitely. → Record attempt deadlines, retry budgets, last failure code, and expose `degraded`/`blocked` with operator remediation.
- **[Risk]** Hub and Agent versions may disagree on generation fields. → Version the report/command contract and reject unsupported protocol versions during registration.
- **[Risk]** Persisting full configuration may expose secrets. → Reuse redaction rules for snapshots; store operator-controlled version content under deployment-specific access controls.
- **[Risk]** Concurrent desired mutations can surprise callers. → Serialize per-resource updates, return the resulting generation, and mark prior intents superseded.
- **[Risk]** SQLite has one concurrent writer. → Use a Storage Actor, short transactions, WAL readers, busy timeout, and an explicit migration path to a server database later.
- **[Risk]** A committed outbox row may be claimed more than once after a worker crash. → Use unique event keys, Attempt uniqueness, command idempotency, and reclaimable leases.
- **[Risk]** A command may execute while its Hub transaction is later rolled back or times out. → Treat the Attempt as ambiguous and reconcile from a fresh observed report instead of replaying blindly.
- **[Risk]** Existing clients interpret `202` or `succeeded` as immediate execution. → Preserve legacy fields/routes, document that terminal success requires observed convergence, and update the Console to use the new fields.

## Migration Plan

1. Add the new domain types and report fields with backward-compatible defaults.
2. Persist initial desired state from the current local configuration without deriving future intent from observed state.
3. Run reconciliation in observe-only mode and compare desired/observed state without dispatching new commands.
4. Enable lifecycle reconciliation for one node, then enable configuration-version reconciliation.
5. On Hub restart, load non-terminal intents and reconcile only after a fresh Agent report or explicit unknown state.
6. Roll back by disabling active reconciliation and retaining the existing direct command endpoints; do not delete persisted desired state.

## Open Questions

- Should planned node drain automatically rewrite Stream desired state, or only suspend reconciliation while preserving it?
- What exact retry budget and permanent-error taxonomy should be exposed in the first API version?
