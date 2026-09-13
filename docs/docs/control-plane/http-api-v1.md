---
sidebar_position: 20
title: Control-plane HTTP API v1
---

# Control-plane HTTP API v1

The Hub API is a desired-state API. A successful HTTP mutation means that an
Intent was durably accepted; it does not mean that a node has executed the
command or that the Stream has converged.

## Resource model

A Stream has three independent views:

```text
desired       operator target: state, generation, config_version
observed      latest node report: state, generation, config_version
convergence   comparison: unknown, pending, applying, in_sync, degraded, blocked
```

Operation records additionally expose `intent_id`, `attempt_id`, generation,
retry metadata, failure classification, and the latest observed result.

## Canonical lifecycle mutation

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

`If-Match` is a compare-and-swap guard. An old generation returns `412` with
`generation_conflict`. `Idempotency-Key` deduplicates retries for the same
principal, resource, and request body. Reusing a key with another body returns
`409` with `idempotency_key_reused`.

## Read and list resources

`GET /api/v1/nodes/{node_id}/streams/{stream_id}` is the authoritative point-in-
time resource view. Collection endpoints use:

```json
{"items": [], "page": 1, "page_size": 50, "total": 0}
```

`page_size` is bounded to 100. Operation listing supports `node_id`,
`resource_id`, `operation`, `state`, and `correlation_id`. Clients should use
`intent_id` and `generation` as stable reconciliation references instead of
assuming that a command ID represents final success.

## Restart and compatibility routes

Restart is a one-shot action:

```http
POST /api/v1/nodes/{node_id}/streams/{stream_id}/actions/restart
```

It converges only after the Agent reports the matching `action_id` and the
Stream is observed running. The legacy `POST .../restart`, start/stop, and
configuration apply/rollback routes remain adapters that create the same
durable Intent pipeline. A node being offline does not reject a desired-state
write.

## Cancellation

`DELETE /api/v1/operations/{intent_id}` cancels an Intent, not an already
executed side effect. Before dispatch, pending work is suppressed. After
dispatch, the Attempt outcome remains visible and the API does not claim that
a restart or other non-idempotent action was undone.

## Jobs

Jobs are managed with the same desired-state semantics as streams. All
mutations accept a validated job specification (`spec`) and return `202
Accepted` with an operation reference; they do not imply that a node has
converged.

| Route | Purpose |
|-------|---------|
| `POST /api/v1/jobs/validate` | Deep-validate a spec (same checks as deployment) against the target `node_ids` before creating anything. |
| `POST /api/v1/jobs` | Create a job with `{"spec": ..., "desired_state": "stopped" \| "running"}`. |
| `GET /api/v1/jobs/{id}` / `/{id}/detail` / `/{id}/versions` | Observed state, assignment/convergence detail, and version history. |
| `GET /api/v1/jobs/{id}/plan` | Explain the plan: operator boundaries, partition routes, stateful operators, checkpoint policy. |
| `PUT /api/v1/jobs/{id}/desired-state` | Start/stop with the same `If-Match` / `Idempotency-Key` contract as streams. |
| `POST /api/v1/jobs/{id}/checkpoints` / `/{id}/savepoints` | Trigger a barrier checkpoint or savepoint; `GET` lists recovery artifacts. |
| `POST /api/v1/jobs/{id}/upgrades` and `/{id}/upgrades/{upgrade_id}/rollback` | Version upgrades (job must be stopped and converged) and rollback to a compatible savepoint. |
| `POST /api/v1/jobs/{id}/actions/{action}` | One-shot actions, equivalent in lifecycle semantics to stream actions. |

Recommended submit flow: `validate` first, submit with `desired_state:
"stopped"`, inspect `detail` and `plan`, then switch to `running`. Recovery
artifacts are bound to the job version and state format version; an
incompatible artifact is rejected before restore instead of corrupting state.

## Audit trail

`GET /api/v1/audit?resource_id={id}` lists bounded audit history for accepted
and rejected mutations. Job lifecycle operations are audited with dotted
actions derived from the dispatched operation:

| Action | Recorded when |
|--------|---------------|
| `job.start` / `job.stop` | A `job_start` / `job_stop` operation is accepted or rejected by dispatch. |
| `job.checkpoint` / `job.savepoint` | A recovery-artifact trigger is accepted or rejected. |

Records carry actor, target resource, node, correlation ID, outcome
(`accepted`/`rejected`), and a stable failure code (`node_unavailable`,
`capacity`, `incompatible_capability`, `expired`). Messages contain scalar
operation metadata only — never credentials or job configuration bodies.
History is retained within a bounded window and pruned by age and count.

## Metrics

`GET /api/v1/metrics` renders Prometheus text exposition. Beyond readiness,
reconciliation, outbox, and fleet-state gauges, command dispatch exposes
`arkflow_command_duration_bucket{command,le}` (enqueue-to-acknowledgement
latency with `_count`/`_sum` companions) and
`arkflow_command_total{command,outcome}`. Labels come from fixed
vocabularies — command types (`job_start`, `job_stop`, `job_checkpoint`,
`job_savepoint`, stream actions) and outcome classes (`enqueued`,
`acknowledged`, `succeeded`, `failed`, `timed_out`, `node_unavailable`,
`capacity`, `rejected`); unknown command names collapse into `other`, and
resource IDs, correlation IDs, and error text never become labels. Counters
reset on Hub restart.

## Problem envelope

Errors use a stable `code`, human-readable `message`, echoed
`correlation_id`, and optional machine-readable `details`:

```json
{
  "code": "generation_conflict",
  "message": "Expected generation 3, current generation 4",
  "correlation_id": "request-123",
  "details": {
    "expected_generation": 3,
    "current_generation": 4,
    "resource": {"node_id":"node-a", "stream_id":"orders"}
  }
}
```

The Agent contract is separate from the operator contract. Agent reports are
authenticated with node session credentials and carry `boot_id` and monotonic
`report_seq`; commands carry generation, Attempt ID, configuration version,
and expiry. Command acknowledgement is transport state only.
