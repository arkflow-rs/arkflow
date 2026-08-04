## Context

ArkFlow already separates the Hub's durable desired-state reconciliation from Agent-owned execution. The existing server exposes resource, operation, maintenance, and health routes, while the core runtime keeps local operation and event stores for standalone mode. The next target is a single Hub managing a small fleet of Agents, with production-grade recovery, authorization, auditability, and controlled configuration rollout.

The design must preserve the current Node → Stream → Config model, at-least-once stream semantics, generation fencing, and compatibility lifecycle routes. It must also avoid making data-plane tasks wait on control-plane persistence or network delivery.

## Goals / Non-Goals

**Goals:**

- Make Hub operation, audit, and rollout state durable and recoverable.
- Add authenticated users, roles, resource authorization, and actor-aware audit records.
- Model configuration publication as a resumable, bounded rollout with health gates.
- Provide complete Agent observations, version/capability compatibility, and real-time REST/SSE consumption.
- Keep standalone mode functional and keep existing API shapes compatible where possible.

**Non-Goals:**

- Multi-Hub HA, sharding, or multi-tenant isolation.
- A new DAG/Pipeline resource model.
- Binary download, replacement, or full software-upgrade orchestration.
- Automatic destructive remediation without an explicit operator action or policy.

## Decisions

### 1. Hub remains the durable control-plane authority

Persist desired state, observed snapshots, Intents, Attempts, Operations, Audit Events, and Rollouts in the existing SQLite control-plane store. Agent reports update observations only; they never overwrite desired state. Standalone mode may retain its local in-process facade, but Hub mode is the production durability boundary.

We choose this over event sourcing because the current reconciliation queries and recovery paths already operate on materialized state, and the target fleet size does not justify replay complexity. Audit events remain append-only records alongside materialized state.

### 2. Use principal/role/resource authorization

Introduce a request principal resolved from a configured local user store or trusted authentication adapter. Roles grant actions over resource scopes such as fleet, node, stream, configuration, rollout, and audit. Agent credentials remain separate from operator credentials. Every accepted or rejected mutation records actor, correlation ID, resource, action, outcome, and failure code without secrets.

This is preferred over one global operator token because the stated production goal requires accountability and least privilege. External identity integration remains an adapter boundary rather than a dependency on a specific OIDC provider.

### 3. Represent configuration publication as a Rollout state machine

Creating a rollout first validates and stores an immutable configuration version and target selection. The Hub advances bounded batches of nodes. A batch is complete only when selected nodes are authenticated, target configuration is observed, affected Streams meet their desired lifecycle state, and configured health gates pass. Failures pause the rollout; operators may resume, cancel, or create a rollback rollout.

Rollout state and per-node results are durable and generation-fenced. Existing node-level apply/rollback routes remain adapters that create a single-node rollout.

We choose explicit orchestration over one Fleet-wide intent because a small production fleet needs blast-radius control and a safe pause point.

### 4. Extend the Agent protocol without changing its pull transport

Keep the current register/heartbeat/report/command-poll/result flow. Add protocol version, capability requirements, software version, metric snapshot, boot identity, report sequence, and explicit incompatible/unsupported outcomes. Commands carry rollout ID where applicable. The Hub rejects incompatible dispatches before creating an executable Attempt.

This preserves the existing operational model and avoids introducing a streaming transport before the control semantics are stable.

### 5. Add SSE as a read-side projection

Expose authenticated SSE streams for operation, rollout, node, stream, and audit changes. Events include event ID, type, resource identity, correlation ID, and bounded payload. The server supports `Last-Event-ID` replay from the durable event window; clients fall back to REST snapshots when the requested event is unavailable.

SSE is chosen over WebSocket because mutations remain REST commands and the Console primarily needs server-to-client updates.

### 6. Keep metrics bounded and separate from audit data

Agents report fixed-name counters and gauges keyed only by stable node, stream, stage, and state dimensions. Hub aggregation exposes Prometheus and JSON snapshots without correlation IDs, error text, secrets, or arbitrary configuration labels. Audit records carry diagnostic identity separately and are not used as metric labels.

### 7. Stage migration compatibly

Existing SQLite schemas receive additive migrations with defaults for legacy rows. Existing token configuration remains supported through a compatibility principal during migration. Existing start/stop/restart and node-level configuration routes continue to create canonical intents or single-node rollouts. A failed migration or recovery keeps the Hub unready while liveness remains available.

## Risks / Trade-offs

- [Rollout state can diverge from node reality] → Require generation, boot identity, fresh report sequence, and observed configuration version before marking a batch complete.
- [A Hub restart can duplicate work] → Persist idempotency keys, active Attempts, command IDs, and outbox leases; reconcile only the current generation.
- [RBAC can accidentally block existing deployments] → Preserve the configured token as a compatibility principal and make authorization decisions explicit in contract tests.
- [SSE clients can miss events] → Include event IDs, bounded replay, snapshot endpoints, and client reconnect fallback.
- [Metrics can create unbounded cardinality] → Whitelist metric names and labels and reject arbitrary Agent labels at report ingestion.
- [A failed rollout can leave mixed versions] → Pause on gate failure, preserve per-node outcomes, and require an explicit resume or rollback action.
- [Durable writes can add control-plane latency] → Keep persistence off data-plane task paths and return accepted asynchronous operations after the transaction commits.
