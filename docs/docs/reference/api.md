---
sidebar_position: 20
title: HTTP API reference
description: Complete route reference for the node API and the Hub operator/agent API (/api/v1).
---

# HTTP API reference

The control plane exposes three API surfaces from one binary role-dependent
service:

| Surface | Base path | Audience |
|---------|-----------|----------|
| Node API | `/` | A single ArkFlow node: local stream lifecycle, configuration, events, metrics. |
| Hub operator API | `/api/v1` | Fleet operators and the web console: jobs, nodes, rollouts, audit. |
| Hub agent API | `/api/v1/agent/*` | Compute nodes (Agents): registration, heartbeats, reports, command pull. |

Health, readiness, and liveness probes (configurable paths) plus `GET
/metrics` (Prometheus text format) are served on both node and Hub services.

The Hub operator API is a **desired-state API**: a successful HTTP mutation
means an Intent was durably accepted — it does not mean a node has executed
the command or that a stream has converged. Poll the resource or subscribe to
events to observe convergence.

## Conventions

### Resource model

A stream has three independent views:

```text
desired       operator target: state, generation, config_version
observed      latest node report: state, generation, config_version
convergence   comparison: unknown, pending, applying, in_sync, degraded, blocked
```

Operation records additionally expose `intent_id`, `attempt_id`, generation,
retry metadata, failure classification, and the latest observed result.

### Optimistic concurrency and idempotency

Canonical lifecycle mutation:

```http
PUT /api/v1/nodes/{node_id}/streams/{stream_id}/desired-state
Authorization: Bearer <operator-token>
Content-Type: application/json
If-Match: "generation-3"
Idempotency-Key: orders-desired-4
X-Correlation-ID: request-123

{"state":"running","config_version":"cfg-17"}
```

The Hub returns `202 Accepted` with `Location` pointing to the operation and
an `ETag` for the new generation:

```json
{
  "operation_id": "intent-4-17",
  "intent_id": "intent-4-17",
  "node_id": "node-a",
  "stream_id": "orders",
  "generation": 4,
  "desired_state": "running",
  "config_version": "cfg-17",
  "convergence": "pending"
}
```

- `If-Match` is a compare-and-swap guard: an old generation returns `412` with
  `generation_conflict`.
- `Idempotency-Key` deduplicates retries for the same principal, resource, and
  request body; reusing a key with another body returns `409` with
  `idempotency_key_reused`.
- A node being offline does **not** reject a desired-state write; the Intent is
  dispatched when the node reconnects.

### Pagination

Collection endpoints return:

```json
{"items": [], "page": 1, "page_size": 50, "total": 0}
```

`page_size` is bounded to 100. Operation listing supports `node_id`,
`resource_id`, `operation`, `state`, and `correlation_id` filters. Use
`intent_id` and `generation` as stable reconciliation references instead of
assuming that a command ID represents final success.

### Problem envelope

Errors use a stable `code`, human-readable `message`, echoed `correlation_id`,
and optional machine-readable `details`:

```json
{
  "code": "generation_conflict",
  "message": "Expected generation 3, current generation 4",
  "correlation_id": "request-123",
  "details": {
    "expected_generation": 3,
    "current_generation": 4,
    "resource": {"node_id": "node-a", "stream_id": "orders"}
  }
}
```

## Node API

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/system` | Static system descriptor (node identity, capabilities). |
| `GET` | `/status` | Runtime status of the node. |
| `GET` | `/nodes` | Node view (single-node deployment alias). |
| `GET` | `/node` | Node view (singular alias). |
| `GET` | `/streams` | List streams with observed state. |
| `GET` | `/streams/{id}` | One stream's resource view. |
| `POST` | `/streams/{id}/start` | Start a stream. |
| `POST` | `/streams/{id}/stop` | Stop a stream. |
| `POST` | `/streams/{id}/restart` | Restart a stream. |
| `GET` | `/operations` | List operations (`node_id`, `resource_id`, `operation`, `state`, `correlation_id` filters). |
| `GET` | `/operations/{id}` | One operation record. |
| `DELETE` | `/operations/{id}` | Cancel a pending Intent. |
| `GET` | `/events` | Query the local event log. |
| `GET` | `/configuration` (`/config`) | Current configuration. |
| `POST` | `/configuration/validate` (`/config/validate`) | Validate a configuration body. |
| `GET`/`PUT` | `/configuration/draft` | Read/save a working draft configuration. |
| `GET` | `/configuration/diff` | Diff draft against the live configuration. |
| `GET` | `/configuration/versions` (`/config/versions`) | Configuration version history. |
| `POST` | `/configuration/apply` (`/config/apply`) | Apply a configuration version. |
| `POST` | `/configuration/rollback/{id}` (`/config/rollback/{id}`) | Roll back to a previous configuration version. |
| `GET` | `/components` | Registered components. |
| `GET` | `/components/{kind}/{name}` | One component's schema and example. |
| `GET` | `/schema` | Engine configuration JSON Schema. |
| `GET` | `/metrics` | Prometheus metrics. |

## Hub operator API (`/api/v1`)

### Jobs

Jobs are managed with the same desired-state semantics as streams. All
mutations accept a validated job specification and return `202 Accepted` with
an operation reference.

| Method | Route | Purpose |
|--------|-------|---------|
| `POST` | `/jobs` | Create a job: `{"spec": ..., "desired_state": "stopped" \| "running"}`. |
| `POST` | `/jobs/validate` | Deep-validate a spec against the target `node_ids` before creating anything. |
| `GET` | `/jobs` | List jobs. |
| `GET` | `/jobs/{id}` (`/jobs/{id}/status`) | Observed job state. |
| `GET` | `/jobs/{id}/detail` | Assignment and convergence detail. |
| `GET` | `/jobs/{id}/versions` | Version history. |
| `GET` | `/jobs/{id}/plan` | Explain the plan: operator boundaries, partition routes, stateful operators, checkpoint policy. |
| `PUT` | `/jobs/{id}/desired-state` | Start/stop with the `If-Match` / `Idempotency-Key` contract. |
| `GET`/`POST` | `/jobs/{id}/checkpoints` | List recovery artifacts / trigger a barrier checkpoint. |
| `GET`/`POST` | `/jobs/{id}/savepoints` | List artifacts / trigger a savepoint. |
| `POST` | `/jobs/{id}/upgrades` | Upgrade to a new version (job must be stopped and converged). |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/rollback` | Roll back to a compatible savepoint. |
| `POST` | `/jobs/{id}/actions/{action}` | One-shot actions, equivalent in lifecycle semantics to stream actions. |

Recommended submit flow: `validate` first, submit with
`desired_state: "stopped"`, inspect `detail` and `plan`, then switch to
`running`. Recovery artifacts are bound to the job version and state format
version; an incompatible artifact is rejected before restore instead of
corrupting state.

### Nodes and streams

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/system` | Hub system descriptor. |
| `GET` | `/nodes` | Fleet node registry. |
| `GET` | `/streams` | Streams across the fleet. |
| `GET` | `/nodes/{node_id}/streams/{id}` | Authoritative point-in-time stream view on a node. |
| `PUT` | `/nodes/{node_id}/streams/{id}/desired-state` | Canonical lifecycle mutation (see [Conventions](#conventions)). |
| `POST` | `/nodes/{node_id}/streams/{id}/{action}` | Targeted one-shot command (`start`, `stop`, ...). |
| `POST` | `/nodes/{node_id}/streams/{id}/actions/restart` | Restart action; converges only after the Agent reports the matching `action_id`. |
| `GET` | `/nodes/{node_id}/configuration` | Node configuration view. |
| `GET` | `/nodes/{node_id}/configuration/versions` | Node configuration history. |
| `POST` | `/nodes/{node_id}/configuration/apply` | Apply configuration to one node. |
| `POST` | `/nodes/{node_id}/configuration/rollback/{version}` | Roll node configuration back. |
| `POST` | `/nodes/{node_id}/drain` | Drain a node (evacuate assignments before maintenance). |
| `POST`/`DELETE` | `/nodes/{node_id}/maintenance` | Enter/exit maintenance mode. |

The legacy un-targeted stream mutation routes remain as adapters that create
the same durable Intent pipeline.

### Operations, events, audit

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/operations` | List operations fleet-wide. |
| `GET` | `/operations/{id}` | One operation record. |
| `DELETE` | `/operations/{id}` | Cancel a pending Intent (never an executed side effect). |
| `GET` | `/operations/status` | Operational status summary for dashboards. |
| `GET` | `/events` | Query the fleet event log. |
| `GET` | `/events/stream` | Server-Sent Events stream of live events. |
| `GET` | `/audit` | Bounded audit history (`?resource_id={id}`), accepted and rejected mutations. |

`DELETE /operations/{intent_id}` cancels an Intent, not an already executed
side effect: pending work is suppressed before dispatch; after dispatch, the
Attempt outcome remains visible.

Audit records carry actor, target resource, node, correlation ID, outcome
(`accepted`/`rejected`), and a stable failure code (`node_unavailable`,
`capacity`, `incompatible_capability`, `expired`). Messages contain scalar
operation metadata only — never credentials or job configuration bodies.

### Rollouts

| Method | Route | Purpose |
|--------|-------|---------|
| `GET`/`POST` | `/rollouts` | List rollouts / create a rollout plan. |
| `GET` | `/rollouts/{id}` | Rollout state and per-node progress. |
| `POST` | `/rollouts/{id}/actions` | Rollout actions: pause, resume, cancel, rollback. |

See [Reconciliation rollout and recovery](../operate/control-plane/reconciliation.md)
for the state machine behind these routes.

### Discovery and metrics

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/components` | Registered components across the fleet image. |
| `GET` | `/components/{kind}/{name}` | One component's schema and example. |
| `GET` | `/schema` | Engine configuration JSON Schema. |
| `GET` | `/metrics` | Prometheus text exposition. |

Beyond readiness, reconciliation, outbox, and fleet-state gauges, command
dispatch exposes `arkflow_command_duration_bucket{command,le}` (enqueue-to-
acknowledgement latency with `_count`/`_sum` companions) and
`arkflow_command_total{command,outcome}`. Labels come from fixed vocabularies
— command types (`job_start`, `job_stop`, `job_checkpoint`, `job_savepoint`,
stream actions) and outcome classes (`enqueued`, `acknowledged`, `succeeded`,
`failed`, `timed_out`, `node_unavailable`, `capacity`, `rejected`); unknown
command names collapse into `other`, and resource IDs, correlation IDs, and
error text never become labels. Counters reset on Hub restart.

## Hub agent API (`/api/v1/agent/*`)

The Agent contract is separate from the operator contract: agents authenticate
with node session credentials, pull commands, and push observations.

| Method | Route | Purpose |
|--------|-------|---------|
| `POST` | `/agent/register` | Register a node; establishes a session. The payload declares node `capabilities` (e.g. `network_shuffle`) and, when the data plane is enabled, the routable `data_address` peers use for remote edges. |
| `POST` | `/agent/heartbeat` | Heartbeat with lease renewal; re-asserts capabilities and data address. |
| `POST` | `/agent/report` | Observed stream/node state; carries `boot_id` and monotonic `report_seq`. |
| `POST` | `/agent/job-observations` | Report job-level observations from the co-located kernel runtime. |
| `GET` | `/agent/commands` | Pull pending commands. |
| `POST` | `/agent/commands/{id}/result` | Report a command result. |

Commands carry generation, Attempt ID, configuration version, and expiry. A
`job_start` for a `split` placement additionally carries the full
`task_nodes` map and `node_data_ports`, so every node can derive its remote
edges without extra lookups; the Hub only dispatches such placements to nodes
that advertised the `network_shuffle` capability and a data address.
Command acknowledgement is transport state only — convergence is always
derived from reports, never from acks.

## Related pages

- [Control plane overview](../operate/control-plane/overview.md) — architecture behind these routes.
- [Control-plane operations](../operate/control-plane/operations.md) — operator workflows using this API.
- [CLI reference](./cli.md) — the local single-binary alternative to the node API.
