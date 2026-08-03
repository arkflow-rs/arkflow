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
