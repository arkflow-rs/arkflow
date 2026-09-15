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

### Data-plane metrics

Every process exports metrics for the data plane it runs: legacy Streams under
`arkflow_stream_*`, and unified-kernel Jobs under `arkflow_job_*`. All series
carry `# HELP`/`# TYPE` metadata; monotonically increasing counters are typed
`counter` and named with a `_total` suffix, backlog and latency series are
`gauges`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `arkflow_job_chain_batches_in_total` / `_batches_out_total` | counter | `job`, `chain` | Batches accepted / emitted per chain. |
| `arkflow_job_chain_rows_total` | counter | `job`, `chain` | Rows flowing through the chain. |
| `arkflow_job_chain_errors_total` | counter | `job`, `chain` | Processing errors. |
| `arkflow_job_chain_in_flight` | gauge | `job`, `chain` | Envelopes parked on the chain's outbound edges (backlog). |
| `arkflow_job_chain_mean_latency_us` | gauge | `job`, `chain` | Mean per-batch processing time (µs). |
| `arkflow_job_checkpoint_duration_ms` | gauge | `job` | Last checkpoint duration. |
| `arkflow_job_checkpoint_failures_total` | counter | `job` | Failed checkpoints. |
| `arkflow_job_watermark_lag_ms` | gauge | `job` | Max watermark lag across sources. |
| `arkflow_job_late_events_total` | counter | `job` | Late events dropped/routed/updated. |
| `arkflow_stream_input_messages` / `_output_messages` / `_restarts` / `_checkpoint_failures` / `_late_events` | counter | `stream_id` | Legacy Stream counters (names unchanged). |
| `arkflow_stream_in_flight` / `_mean_latency_us` / `_checkpoint_duration_ms` / `_watermark_lag_ms` | gauge | `stream_id` | Legacy Stream gauges. |

Labels use fixed vocabularies — Job/chain/Stream identifiers plus an optional
`node` label. Message content, correlation IDs, and error text never become
labels, so cardinality stays bounded by the configured workload
(O(jobs × chains) per process). Counters reset when the process restarts; use
`rate()`/`increase()` over stable windows.

### Readiness and liveness

Every process exposes `GET /ready` and `GET /live` (paths configurable under
`health_check.observability`):

- `/ready` succeeds once the engine finished starting the configured Streams
  and Jobs; while starting — or after a startup failure — it responds `503`
  with a stable `not_ready` status.
- `/live` succeeds while the process is running and does not depend on any
  external system.

When the control-plane API server is enabled it serves the same endpoints on
its address; the legacy `/health`, `/readiness`, and `/liveness` paths keep
their original semantics for compatibility. When the API server is disabled,
a dedicated listener (default `127.0.0.1:8081`) keeps `/metrics`, `/ready`,
and `/live` available — so a pure data-plane deployment can still be scraped
and probed. See [`health_check.observability`](../reference/configuration.md#health_checkobservability).

### Hub fleet export

The Hub re-exports the data-plane vocabulary reported by its Agents on its own
`/metrics` endpoint, with an extra `node` label distinguishing reporters. Only
Agents with a live lease are exported — an Agent that stops reporting (expired
lease or deregistration) drops out of the exposition. The endpoint requires
the operator credential (`Authorization: Bearer <operator token>`), matching
the rest of the Hub metrics API.

Key metric families:

| Metric | Source | Meaning |
|--------|--------|---------|
| `arkflow_command_duration_bucket{command,le}` | Hub | Enqueue-to-acknowledgement latency per command type. |
| `arkflow_command_total{command,outcome}` | Hub | Dispatch counters with outcome classes (`succeeded`, `failed`, `timed_out`, `node_unavailable`, `capacity`, `rejected`). |
| Readiness / reconciliation / outbox / fleet-state gauges | Hub | Hub health and fleet convergence posture. |
| `arkflow_job_*{node,job,chain}` | Hub (from Agents) | The data-plane vocabulary above, per reporting node. |
| Per-node stream and processing metrics | Node | Stream state and throughput on each compute node. |

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
- `arkflow_job_chain_errors_total` increasing, `arkflow_job_checkpoint_failures_total`
  increasing, or `arkflow_job_watermark_lag_ms` trending up.

## Related pages

- [HTTP API reference](../reference/api.md) — every metrics/events route.
- [Recovery](./recovery.md) — what to do when these signals fire.
