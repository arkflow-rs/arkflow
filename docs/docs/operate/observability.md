---
sidebar_position: 6
title: Observability
description: Metrics, events, logging, and dashboards for running ArkFlow in production.
---

# Observability

ArkFlow is observable on three channels: **Prometheus metrics**, a structured
**event log** (with a live SSE stream), and **tracing logs**. All three are
available on a single node and, with the control plane, aggregated across the
fleet.

## Metrics

Both the engine node and the Hub expose Prometheus text exposition at
`GET /metrics`. Scrape them with any Prometheus-compatible collector:

```yaml
scrape_configs:
  - job_name: arkflow
    static_configs:
      - targets: ["node-a.example:7491", "hub.example:7492"]
```

Key metric families:

| Metric | Source | Meaning |
|--------|--------|---------|
| `arkflow_command_duration_bucket{command,le}` | Hub | Enqueue-to-acknowledgement latency per command type. |
| `arkflow_command_total{command,outcome}` | Hub | Dispatch counters with outcome classes (`succeeded`, `failed`, `timed_out`, `node_unavailable`, `capacity`, `rejected`). |
| Readiness / reconciliation / outbox / fleet-state gauges | Hub | Hub health and fleet convergence posture. |
| Per-node stream and processing metrics | Node | Stream state and throughput on each compute node. |

Labels use fixed vocabularies — resource IDs, correlation IDs, and error text
never become labels, so cardinality stays bounded. Hub counters reset on Hub
restart; use `rate()`/`increase()` over stable windows.

## Events

Every accepted or rejected mutation, node state transition, and convergence
change is recorded in the event log:

```bash
# Query the fleet event log
curl -H "Authorization: Bearer $TOKEN" \
  "https://hub.example/api/v1/events?node_id=node-a&page_size=50"

# Or subscribe to live events over SSE
curl -N -H "Authorization: Bearer $TOKEN" \
  "https://hub.example/api/v1/events/stream"
```

A single node exposes the same channel locally at `GET /events`. The console
[Events view](./control-plane/console.md) renders both the query and the
live stream.

## Logging

The engine logs through `tracing`. Level and output are configuration
settings (not CLI flags):

```yaml
logging:
  level: info        # trace | debug | info | warn | error
  format: json       # json | plain
  file_path: /var/log/arkflow/arkflow.log   # omit for stdout
```

`json` format is the right choice for shippers (Vector, Fluent Bit, Loki);
`plain` is for interactive debugging. When `file_path` cannot be opened the
engine falls back to stdout logging and says so on stderr.

## Operational status for dashboards

`GET /api/v1/operations/status` returns a bounded summary designed for
status pages and alerts: dispatch health, pending operations, and fleet
convergence. Alert on its fields rather than scraping raw operations lists.

## Suggested alerts

- Node heartbeat/lease lost (node offline or partitioned).
- `arkflow_command_total{outcome="timed_out"|"failed"}` increasing.
- A stream's convergence stuck in `pending`/`applying` beyond your rollout
  budget, or `degraded`/`blocked` at all.
- Hub readiness probe failing (storage/recovery impaired).

## Related pages

- [HTTP API reference](../reference/api.md) — every metrics/events route.
- [Recovery](./recovery.md) — what to do when these signals fire.
