## Context

Phase 1 landed four resource gauges (`node_cpu_usage_percent`, memory used/total/available) in every Agent report, whitelisted into the Hub's per-node metrics view with a freshness rule on the Agent side. Placement today ignores them: eligible candidates are every online, leased, maintenance-active node in BTreeMap id order (`crates/arkflow-server/src/hub.rs:832-849`); a first placement lands on the head of that list; split round-robin consumes the list as-is (`crates/arkflow-core/src/job.rs:991-995`); and a running placement is retained by the `previous_nodes` logic until its node drops out of the eligible set (`hub.rs:880-940`), with the supersede/stop fencing protocol handling moves.

Hard constraints from the spec surface: `split-placement` requires the task→node mapping to be a deterministic function of its input (node set + order); `distributed-job-runtime` guarantees "a stable placement is not disturbed" and exactly-one-runner fencing on re-placement; `authenticated-network-shuffle` and side-edge co-location are untouched by node *selection*.

## Goals / Non-Goals

Goals:
- New placements land on nodes with the most headroom, deterministically, with explicit `node_ids` still winning over everything.
- Operators can opt a Job into pressure-driven relocation with bounded, fenced semantics.
- Every behavior degrades to today's behavior when gauges are missing, stale, or the policy is off.

Non-Goals:
- Any change to the assignment algorithms (round-robin arithmetic, side-edge checks, capability validation) — they receive a better-ordered input.
- Bin-packing across Jobs, global optimization, predictive scheduling, or per-Job resource requests/limits.
- Rebalancing pinned placements (explicit `node_ids`) or anything during an active rollout (rollout ownership wins).
- Autoscaling, node provisioning, or multi-cluster concerns.

## Decisions

1. **Rank candidates, never rewrite assignments.** A pure function `rank_candidates(node_ids, metrics_by_node, now_ms)` orders the eligible set: nodes with fresh gauges first (memory-available ratio desc, CPU headroom desc, node id asc), gauge-less nodes last in id order. Determinism survives because the output is a pure function of (candidate set, gauge values, ids). The colocated path takes the head; split feeds the ordered list to the untouched round-robin. Alternative rejected: scoring weights/tunable score functions — unexplainable and untestable at this stage.

2. **"Fresh" mirrors the Agent-side rule.** A gauge set is fresh when its node reported within the Hub's staleness bound (lease-relative; concretely: last report younger than 2× the report interval floor used by phase 1). No fresh gauges ⇒ node ranks last but stays eligible; no fresh gauges anywhere ⇒ exact today's behavior. Placement is never blocked by observability.

3. **Opt-in policy on the Job spec, off by default.** `RebalancePolicy { mode: off|auto, memory_threshold, cpu_threshold, streak, cooldown_ms }` attached to the placement carrier with serde defaults; old specs deserialize bit-identically. Pinned (`node_ids`) Jobs reject `auto` at plan validation (nothing to rebalance to is an operator question, not a scheduler guess).

4. **Sustained pressure = consecutive over-threshold reports, not a time series.** "Node pressuring" is a fleet-level judgment with Hub constants (memory used/total ratio ≥ 0.9 OR CPU ≥ 90%, gauges fresh within the phase-1 freshness window): the Hub keeps one counter per node (`pressure_streak`) incremented while the latest report exceeds that predicate and reset when under — no gauge history, bounded state, no prune. The per-Job policy carries only how many consecutive pressured reports justify a move (`pressure_streak`) and a `cooldown_ms`; per-policy memory/CPU thresholds were rejected because the Hub cannot recompute a historical streak per policy.

5. **Rebalance reuses the existing re-placement fencing.** The pressured node is excluded from the candidate set; the reconcile path then does what a node-blip re-placement does: supersede the abandoned start, queue its stop, dispatch starts to the new set, exactly one live runner. If excluding the node leaves an empty/ineligible set (single-node fleet, or the only shuffle-capable node), the move is skipped and re-evaluated next tick — eviction into nothing is never attempted. A rebalance marks a cooldown stamp so thrash is bounded at one move per cooldown per Job.

6. **Maintenance wins.** Nodes in maintenance never enter candidate sets (already true), so pressured-or-drained nodes are never relocation destinations; drain semantics are unchanged. Rollout interaction needs no special case: a relocated Job's start carries its own config version, so a config rollout and a placement move do not conflict.

## Risks / Trade-offs

- [New-placement distribution changes vs today] → that is the feature; determinism and stability contracts are unaffected, and pinned placements are byte-identical.
- [Gauge flapping causes rebounce] → streak counter (consecutive over-threshold reports) + per-Job cooldown; both defaulted, policy-tunable.
- [Ranking and pressure read different nodes' gauges at different moments] → acceptable: ranking tolerates staleness by design; rebalance only ever moves off a node with a *sustained* fresh-pressure streak.
- [Job-spec serde drift] → serde defaults + `docs_inventory_snapshot` / examples validation suites already gate generated artifacts; regenerate only if the snapshot covers JobSpec.

## Migration Plan

Ship as one additive release: default-off policy keeps every existing Job's behavior identical; new placements get better targets with no config change. Rollback = revert; no persisted-format migration (policy rides the existing spec JSON blob; streak/cooldown are in-memory only).

## Open Questions

None blocking; threshold defaults (0.9 memory-used ratio / 0.9 CPU / streak 3 / cooldown 5 min) are constants at implementation time and policy-tunable from day one.
