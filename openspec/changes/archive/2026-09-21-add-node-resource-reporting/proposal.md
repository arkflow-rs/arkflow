## Why

The fleet's placement decisions are blind: placement is currently round-robin over connected nodes (`crates/arkflow-core/src/job.rs:331` `PlacementStrategy::Split` = "physical plan order"), and the Hub has no signal about node CPU or memory because nodes report only flow counters — the whitelist at `crates/arkflow-server/src/hub.rs:24-52` (`ALLOWED_NODE_METRICS`) contains exclusively data-plane counters, and the Agent's report builder (`crates/arkflow-server/src/agent.rs:2004-2036`) samples nothing from the host. Phase 1 of the placement roadmap closes the data gap: nodes start shipping a bounded, fixed-vocabulary resource snapshot so capacity becomes observable. Changing placement decisions is explicitly phase 2 and out of scope here.

## What Changes

- Agent gains a host resource sampler (via the `sysinfo` crate, added to `[workspace.dependencies]`): a dedicated sampler task refreshes CPU/memory gauges on its own cadence, decoupled from heartbeat/report ticks, and the report tick merges the latest snapshot into the existing `metrics` map — no new wire structure.
- New gauge keys, fixed vocabulary: `node_cpu_usage_percent`, `node_memory_used_bytes`, `node_memory_total_bytes`, `node_memory_available_bytes`.
- `ALLOWED_NODE_METRICS` whitelist is extended with the four resource keys; Hub ingestion, the node metrics view, and the Hub Prometheus export (`arkflow_node_metric{node_id,metric}` series, `crates/arkflow-server/src/lib.rs:2518-2526`) work unchanged on top.
- Resource gauges are ephemeral registry state, like all node metrics today (`storage.rs` persists none of them): after a Hub restart they are absent until the next report tick, bounded by the report interval.
- Sampling is best-effort: a sampler failure or unsupported platform yields a report without resource gauges; the node stays online and reporting is never blocked (same posture as the optional `data_port` at `crates/arkflow-server/src/agent.rs:1672`).

## Capabilities

### New Capabilities

(none — the behavior belongs to existing control-plane capabilities)

### Modified Capabilities

- `compute-node-agent`: new requirement — the Agent SHALL sample host resource gauges and merge them into its report; sampling failure SHALL NOT block or fail reporting.
- `control-plane-hub`: new requirement — the Hub SHALL ingest a bounded, fixed vocabulary of node resource gauges into the node registry view, drop unknown keys, and treat gauges as ephemeral (not durable history).
- `data-plane-observability`: new requirement — resource gauges SHALL flow through the Hub metrics export carrying the closed `node` label vocabulary.

## Non-goals

- No change to placement, scheduling, or rebalancing decisions (phase 2: add-aware-placement-rebalance).
- No per-process, per-cgroup, or per-Job resource attribution.
- No durability/history for gauges (ephemeral like all node metrics).
- No alerting, dashboards, or Console UI.

## Impact

- Code: `crates/arkflow-server/src/agent.rs` (sampler task + report merge), `crates/arkflow-server/src/hub.rs` (whitelist constants), tests under `crates/arkflow-server/tests/`.
- Dependencies: `sysinfo` added to root `Cargo.toml` `[workspace.dependencies]`, referenced by `arkflow-server` (centralized version rule).
- APIs: purely additive — optional keys in the existing node `metrics` map and new Prometheus series. Old Agents against new Hubs (keys absent) and new Agents against old Hubs (unknown keys silently dropped by `sanitize_metrics`, `crates/arkflow-server/src/hub.rs:2686`) both degrade gracefully.
- No persistence, schema, or config-format changes; no kernel (`arkflow-core`) changes.
