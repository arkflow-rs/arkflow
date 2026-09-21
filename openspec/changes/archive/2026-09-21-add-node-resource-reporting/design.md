## Context

Nodes already ship a bounded metrics map to the Hub on every report tick (`crates/arkflow-server/src/agent.rs:2026-2036`); the Hub validates it against `ALLOWED_NODE_METRICS` (`crates/arkflow-server/src/hub.rs:24-52`), keeps it in the in-memory node registry, surfaces it in the nodes API view, and re-exports it through the Hub Prometheus endpoint with a closed label vocabulary. None of the reported metrics describe the host. Phase 2 (resource-aware placement) needs that data flowing first; this change only opens the tap.

Constraints: workspace pins dependency versions centrally (root `Cargo.toml`); the dev/test platform includes macOS while production is Linux, so any host-metrics reading must be cross-platform; the Agent session loop is a single `select` that must never be blocked by observability work (precedent: `data_port` failure is non-fatal, `crates/arkflow-server/src/agent.rs:1672`).

## Goals / Non-Goals

Goals:
- Node CPU/memory gauges observable at the Hub (registry → nodes API → Prometheus export) within one report interval of node startup.
- Fixed, bounded key vocabulary; no new wire structure; graceful version skew in both directions.
- Sampling that can never impair reporting, heartbeat, or command execution.

Non-Goals:
- Any change to placement, scheduling, or rebalancing (phase 2).
- Per-process, per-cgroup, or per-Job resource attribution.
- Durability/history of gauges (they are ephemeral like all node metrics).
- Alerting, dashboards, or Console UI (rides existing endpoints; UI can follow later).

## Decisions

1. **Reuse the `metrics` map with four `node_*` gauge keys; no dedicated `resources` wire object.**
   The Hub's whitelist, sanitize, registry, API view, and export all operate on this map today. A dedicated object would touch handler/storage/API/export code for zero functional gain and would break version skew (old Hub would reject or ignore a new top-level field depending on parser strictness). Alternative rejected: `resources` object (more "typed", but phase 1 gains nothing and phase 2 can still add one later without breaking the gauge keys).

2. **`sysinfo` crate with default features disabled**, added to `[workspace.dependencies]` per the central-version rule.
   Hand-rolled `/proc/stat` + `/proc/meminfo` parsing is Linux-only and breaks the macOS dev/test platform; sysinfo covers both. Disable unneeded feature components (disks, networks, users, …) to bound compile time and binary size.

3. **Dedicated sampler task with a shared latest-snapshot slot, not inline sampling in the report tick.**
   sysinfo CPU usage requires two refreshes separated by `MINIMUM_CPU_UPDATE_INTERVAL`; doing this inline would block the session `select` loop (delaying heartbeats/commands) on every report tick. A small task sampling on its own fixed interval (≈5s, well under any report interval) publishes into a shared slot; the report tick does one lock-free read. Shutdown: task aborted with the session (existing `JoinSet`/select-drop semantics).

4. **Freshness rule: gauges older than 2× sampler interval are omitted, not sent stale.**
   A dead sampler must not produce a lying dashboard. The snapshot slot carries `sampled_at_ms`; `report()` drops resource keys past the freshness window. Alternative rejected: sending stale values with a timestamp field — adds wire surface for the Hub to ignore.

5. **First-interval warmup: no CPU gauge until the sampler completes its first full interval.**
   sysinfo's first CPU refresh yields a meaningless value; publish only from the second interval onward. Memory gauges are valid immediately and may publish earlier.

6. **No persistence.** All node metrics today are in-memory registry state repopulated by the next report (`storage.rs` persists none of them); gauges behave identically. Hub restart clears gauges for at most one report interval — bounded staleness, zero prune burden, no durable-history questions.

## Risks / Trade-offs

- [sysinfo grows compile time / binary] → default-features off, only the needed components enabled.
- [Host-level CPU in containers differs from cgroup allocation] → accepted for phase 1; it is still monotone with load and sufficient to rank nodes. Container-aware budgets belong to phase 2 design.
- [Sampler task dies silently → gauges disappear] → freshness window turns failure into omission (visible as missing series) rather than stale lies; sampler panic is caught by the session's existing task supervision semantics.
- [Old Hub + new Agent: `sanitize_metrics` silently drops the new keys] → intentional; both skew directions degrade to today's behavior.

## Migration Plan

Additive rollout: ship Agent + Hub together; version skew in either direction is spec'd and harmless. Rollback = revert; no state, schema, or config migration exists. Fleet integration test asserts the gauges appear end-to-end.

## Open Questions

None blocking. Key names and the four-gauge vocabulary are fixed by the spec; sampler interval default (5s) may be tuned by constant during implementation without spec change.
