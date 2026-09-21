## Why

Phase 1 (add-node-resource-reporting) made node CPU/memory observable, but the Hub still ignores the data when placing Jobs: the eligible target set is "every online, leased, active node" in registry (BTreeMap) id order (`crates/arkflow-server/src/hub.rs:832-849`), split round-robin consumes that set as-is (`crates/arkflow-core/src/job.rs:991-995`), and a running placement is never revisited even when its node is starved. With gauges flowing, placement can stop being blind: pick the set deliberately, and — only when an operator opts in — move a placement off a node that stays under sustained pressure.

## What Changes

- **Resource-aware node selection (always on, fail-safe)**: when a Job's target set is not explicitly pinned (`node_ids` empty), the Hub ranks eligible candidates by resource headroom — memory available ratio first, CPU second — with deterministic tie-breaks (node id); nodes without fresh gauges rank last and never block placement. Explicit `node_ids` remain an absolute operator override.
- **Deterministic ordering feeds the existing assignment**: the ranked, deterministically ordered node list becomes the input to `assignments_for_nodes` unchanged — split round-robin and colocated single-target semantics stay bit-compatible for a given node set/order, so `split-placement`'s determinism contract ("same input, same mapping") is preserved by construction.
- **Opt-in rebalancing**: new optional `RebalancePolicy` on the Job placement spec (`off` by default, `auto` enables). With `auto`, when a placed node reports sustained over-pressure (memory available ratio or CPU above threshold for a bounded consecutive-sampling window), the Hub re-places the Job onto the best-ranked fresh set: generation bump, superseded starts, stop commands to abandoned nodes via the existing fencing protocol — exactly one live runner remains.
- **Stability preserved by default**: with the policy off, today's guarantee is unchanged — a stable placement is never disturbed. The spec's stability requirement gains one bounded exception scoped to the opt-in policy.

## Capabilities

### New Capabilities

- `resource-aware-placement`: headroom-based candidate ranking and ordering for non-pinned placements, fail-safe treatment of missing/stale gauges, and the opt-in sustained-pressure rebalance trigger with its fencing invariants.

### Modified Capabilities

- `distributed-job-runtime`: the "stable placement is not disturbed" requirement is narrowed — stability holds unless the Job's explicit rebalance policy enables a pressure-driven move, which then follows the same supersede/stop fencing as re-placement.

## Non-goals

- No change to assignment algorithms (round-robin arithmetic, side-edge checks, capability validation).
- No bin-packing across Jobs, global optimization, or per-Job resource requests/limits.
- No rebalancing of pinned placements.
- No autoscaling, node provisioning, or multi-cluster concerns.

## Impact

- Code: `crates/arkflow-server/src/hub.rs` (candidate ranking/ordering in reconcile paths, pressure evaluation in the reconcile tick, generation bump on rebalance), `crates/arkflow-core/src/job.rs` (optional `RebalancePolicy` on `PlacementStrategy` carrier — serde-default, old specs deserialize unchanged).
- Config: additive optional Job-spec field; no breaking change; generated config schema regenerated only if the registry snapshot covers it (verified by the docs inventory test).
- Invariants untouched: exactly-one-runner fencing, bounded backpressure, split side-edge co-location, split capability fail-closed validation, durable checkpoint/recovery contracts.
- Risks carried into design: gauge-staleness races (mitigated by fail-safe "no fresh gauges → no action"), rebalance thrash (mitigated by hysteresis: sustained window + cooldown), and interplay with maintenance/drain (drained nodes already leave the candidate set).
