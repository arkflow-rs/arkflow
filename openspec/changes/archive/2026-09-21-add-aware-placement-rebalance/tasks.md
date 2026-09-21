## 1. Rebalance policy surface

- [x] 1.1 Add serde-default `RebalancePolicy` (mode off|auto, pressure_streak, cooldown_ms) as an additive optional field beside `placement` on `JobSpec` in `crates/arkflow-core/src/job.rs`; unit test: old spec JSON without the field deserializes byte-identically (off)
- [x] 1.2 Plan validation: `auto` policy combined with explicit `node_ids` is rejected with a named error; unit test

## 2. Headroom ranking

- [x] 2.1 Implement pure `rank_candidates` in `crates/arkflow-server/src/hub.rs`: fresh gauges first (memory-available ratio desc, CPU headroom desc, node id asc), gauge-less nodes last in id order; freshness mirrors the phase-1 staleness rule; unit tests for ordering and determinism
- [x] 2.2 Wire ranking into the non-pinned placement candidate path in `reconcile_jobs` (pinned `node_ids` pass through verbatim); colocated head and split round-robin input both consume the ordered set
- [x] 2.3 Hub unit tests: first placement prefers the higher-headroom node; all-gauge-less fleet reproduces today's id-order behavior

## 3. Pressure streak and rebalance

- [x] 3.1 Track a per-node `pressure_streak` counter on report ingestion using Hub pressure constants (memory used ratio ≥ 0.9 OR CPU ≥ 90%, fresh gauges): increment when over, reset when under; unit test for streak/reset
- [x] 3.2 Rebalance detection in `reconcile_jobs`: policy `auto` + placed-node streak ≥ threshold + cooldown elapsed → exclude the placed node from candidates and re-place through the existing supersede/stop fencing path; skip (and retry next tick) when the exclusion leaves no eligible set
- [x] 3.3 Cooldown gate derived from the latest start dispatch (no extra state); unit test: a pressure trigger inside the cooldown window does not move the Job
- [x] 3.4 Hub integration test: default-off Job on a pressured node is never disturbed; policy-auto Job relocates with superseded start + stop command to the abandoned node and exactly one live runner; pinned auto-policy submission rejected
- [x] 3.5 Hub unit test: single-node fleet with pressure never attempts relocation

## 4. Validation

- [x] 4.1 `cargo test -p arkflow-core -p arkflow-server` passes
- [x] 4.2 `cargo clippy --workspace --all-targets` adds no new warnings
- [x] 4.3 `cargo test --workspace --all-targets` green; docs inventory/examples snapshot tests still pass (regenerate artifacts if they cover the new field)
