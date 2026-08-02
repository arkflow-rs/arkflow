---
sidebar_position: 1
---

# Control plane overview

ArkFlow has an optional control plane for operating many compute nodes as a
fleet: a **Hub** (the authoritative registry and command broker), per-node
**Agents** that report runtime state and execute commands, and a web **console**.

The control plane shares the same HTTP server as the health checks. Configure
its bind address and versioned prefix with `health_check.address` and
`health_check.api_prefix`; an optional `health_check.api_token` enables Bearer
authentication for lifecycle commands and configuration reads/writes. Keep the
listener local or behind an authenticated reverse proxy. When `health_check.hub_url`
is absent, ArkFlow runs in standalone mode and only the compatibility health
routes are served.

## API endpoints

Resource endpoints include `/api/v1/system`, `/nodes`, paginated `/streams`,
`/operations`, `/events`, `/configuration`, `/components`, and `/schema`.
`/metrics` exposes Prometheus text exposition.

Lifecycle commands return `202 Accepted` with an operation ID; poll
`/api/v1/operations/{id}` until it reaches a terminal state. Each command is
idempotent for the same resource/action while an equivalent operation is
active. Lifecycle commands target
`/api/v1/nodes/{node_id}/streams/{stream_id}/{action}`.

The compatibility health routes `/health`, `/readiness`, `/liveness`, `/metrics`,
and the legacy `/api/v1/config*` aliases remain available.

## Configuration reconciliation

Configuration changes are parsed and validated before reconciliation:

- **unchanged** streams keep their running task;
- **changed or removed** streams are stopped; new/changed streams are built
  before they are started;
- versions are written atomically under `.arkflow/config-history`, and rollback
  creates a new child version rather than rewriting history.

Stream IDs must contain only ASCII letters, digits, `-`, or `_`; legacy
configurations receive deterministic `stream-0`, `stream-1`, … IDs.

Node configuration is read from `/api/v1/nodes/{node_id}/configuration`; apply
and rollback are dispatched through the selected node's Agent session.
Configuration snapshots are redacted before they are retained by the Hub.

## Node sessions and leases

The Hub uses short-lived node sessions and leases. A stale node remains visible
but cannot receive new commands; a node marked stale shows its last-seen time,
and mutating actions that require Agent dispatch are disabled or explained
rather than silently queued.

If a compute node cannot reach the Hub, it keeps its local data-plane policy,
retries registration and heartbeats with bounded backoff, and surfaces a
disconnected status locally; its in-flight streams keep running against their
configured inputs and outputs. The WAL remains at-least-once — restarting a
stream replays unacknowledged entries according to its configured WAL cursor
semantics (see [Delivery semantics](../concepts/4-delivery-semantics.md)).

## Reconciliation model

The Hub reconciles **desired versus observed** state at two levels:

- the **node** — registered capabilities and lease status versus the last
  reported runtime snapshot;
- each **stream** — configured spec versus running task.

Reconciliation is driven by the durable desired state, so a node that reconnects
after a disconnect resumes toward the same desired configuration rather than a
stale one. The console presents fleet health and per-node summaries first;
select a node to scope runtime, configuration, events, and operations views to
it.

## Next steps

- [Deploy](./2-deploy.md) — run the Hub, compute nodes, and console.
- [Operations](./3-operations.md) — day-2 operations runbook.
- [HTTP API v1](./http-api-v1.md) — endpoint reference.
- [Reconciliation rollout & recovery](./reconciliation-rollout.md).
