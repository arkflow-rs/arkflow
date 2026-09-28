# job-resource-quotas Specification

## Purpose

Job resource requests (per-task cpu/memory), placement accounting with feasibility gating, effective-headroom ranking, and dedicated bounded-runtime execution for declared CPU jobs.

## ADDED Requirements

### Requirement: Job SHALL declare optional per-task resource requests

JobSpec SHALL accept an optional `resources` object with `cpu_millicores` and `memory_bytes`, both optional and interpreted as **per-task requests**: a Job's total request is the per-task value times the planned task count, and a node's share is the per-task value times that node's assignment count. Jobs without the declaration SHALL behave byte-identically to before: no accounting, no feasibility gating, shared-runtime execution.

#### Scenario: Declared request parses per task

- **WHEN** a Job spec declares `resources: {cpu_millicores: 500, memory_bytes: 268435456}` with 4 planned tasks
- **THEN** the total request is 2000 millicores and 1 GiB, and a node hosting 1 of the 4 tasks is charged 500 millicores and 256 MiB

#### Scenario: Undeclared resources change nothing

- **WHEN** a Job spec omits `resources`
- **THEN** placement, ranking, execution, and every existing behavior are unchanged

### Requirement: Placement SHALL account for declared allocations and gate feasibility

For a Job with declared resources, the Hub SHALL recompute each candidate node's allocated CPU (millicores) and memory (bytes) from the current placements of other desired-running Jobs (per-task request × per-node assignment count, using the remembered dispatch order or previous placement nodes) and SHALL place only on nodes where (allocated + this Job's share) fits capacity: CPU within `node_cpu_cores × 1000` and memory within 90% of `node_memory_total_bytes`. Nodes without fresh capacity gauges SHALL be exempt from the gate (existing fail-open) while still ranking last by gauge freshness. When no candidate is feasible the reconcile SHALL fail with an explicit insufficient-capacity error and retry on the next tick instead of stacking the Job onto a loaded node. Ranking SHALL use effective headroom (observed minus allocated) instead of raw observed values.

#### Scenario: A loaded node is skipped for a declared Job

- **WHEN** node-a already hosts declared Jobs consuming 1800 of its 2000 millicores and a new Job requests 500 millicores there, while node-b has ample effective headroom
- **THEN** the placement lands on node-b (or fails explicitly if no node is feasible), never on node-a

#### Scenario: No feasible node surfaces an explicit error

- **WHEN** every candidate's effective capacity is smaller than the Job's declared share
- **THEN** the reconcile fails with an insufficient-capacity error naming the resource, and the Job retries on later ticks instead of being placed

#### Scenario: Ranking prefers effective headroom

- **WHEN** two nodes report equal observed memory availability but node-a carries declared allocations and node-b does not
- **THEN** node-b ranks ahead of node-a for the next placement

#### Scenario: Gauge-less nodes stay fail-open

- **WHEN** a candidate node reports no capacity gauges
- **THEN** it is exempt from the feasibility gate for declared Jobs (and keeps ranking last), preserving today's placement semantics

### Requirement: Declared CPU jobs SHALL run on a dedicated bounded runtime

A Job declaring `cpu_millicores` SHALL execute its kernel on a dedicated tokio runtime with `max(1, ceil(cpu_millicores / 1000))` worker threads, owned by the Job's lifecycle (created at start, shut down at stop) — one Job cannot occupy the Agent's shared runtime workers. Jobs without the declaration keep running on the shared runtime. Memory enforcement remains the existing `state.max_bytes` state budget; the declaration is a scheduling and worker-isolation bound, not a cgroup-style hard limit.

#### Scenario: Worker count follows the declaration

- **WHEN** a Job declaring `cpu_millicores: 2500` starts on an Agent
- **THEN** its kernel runs on a dedicated runtime with 3 worker threads, torn down when the Job stops

#### Scenario: Undeclared jobs keep the shared runtime

- **WHEN** a Job without `resources` starts
- **THEN** it spawns on the Agent's shared runtime exactly as before
