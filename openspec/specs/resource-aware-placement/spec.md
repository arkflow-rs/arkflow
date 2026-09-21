# resource-aware-placement Specification

## Purpose
Resource-aware placement for distributed Jobs: headroom-ranked candidate selection for unpinned placements, fail-safe treatment of missing resource gauges, and the opt-in sustained-pressure rebalance trigger with its fencing invariants. Synced from change add-aware-placement-rebalance.

## Requirements
### Requirement: Non-pinned placements SHALL rank candidates by resource headroom

When the Hub selects the target node set for a Job whose target set is not explicitly pinned, it SHALL order eligible candidates by resource headroom — freshest-gauges first, memory-available ratio descending, CPU headroom descending, node id ascending — and feed that ordered set to the unchanged placement assignment logic. Nodes without fresh resource gauges SHALL rank after gauged nodes, in node id order. An explicit `node_ids` pin SHALL override ranking entirely.

#### Scenario: Headroom decides a first placement

- **WHEN** a new Job with no `node_ids` is placed while node A reports high memory usage and node B reports ample memory
- **THEN** the ordered candidate set places B ahead of A, and a colocated Job lands on B

#### Scenario: Ranking is deterministic

- **WHEN** the same candidate set with the same gauge values is ranked twice
- **THEN** both rankings produce the identical node order

#### Scenario: Gauge-less nodes still receive work

- **WHEN** no candidate node has fresh resource gauges
- **THEN** placement proceeds with today's node-id ordering and never fails or defers for lack of gauges

#### Scenario: Explicit pin overrides ranking

- **WHEN** a Job declares `node_ids`
- **THEN** the pinned set is used verbatim, in its declared order, with no headroom reordering

### Requirement: Rebalancing SHALL be opt-in, sustained, and fenced

A Job MAY declare a rebalance policy; it SHALL default to off. With the policy enabled, the Hub SHALL treat a placed node as pressuring only after its resource gauges exceed the policy thresholds in consecutive reports (a sustained streak), and SHALL then relocate the Job by excluding that node from the candidate set and reusing the existing re-placement fencing: the abandoned node's start is superseded, it receives a stop command, and exactly one live runner remains. The Hub SHALL NOT rebalance Jobs with pinned `node_ids` or when the exclusion leaves no eligible target set, and SHALL bound relocation frequency with a per-Job cooldown.

#### Scenario: Default keeps placements stable

- **WHEN** a Job without a rebalance policy runs on a node that reports sustained pressure
- **THEN** the placement is not disturbed

#### Scenario: Sustained pressure relocates with fencing

- **WHEN** a policy-enabled Job's placed node exceeds the memory or CPU threshold for the configured streak of consecutive reports and the cooldown has elapsed
- **THEN** the Job is re-placed onto the best-ranked remaining eligible set, the abandoned node's start is superseded with a stop command, and no second runner exists

#### Scenario: Single-node fleet skips the move

- **WHEN** the pressuring node is the only eligible target for the Job
- **THEN** no relocation is attempted and the Job keeps running on the pressured node

#### Scenario: Thrash is bounded

- **WHEN** a relocation just happened for a Job
- **THEN** further relocations of that Job wait for the configured cooldown even if pressure re-appears

#### Scenario: Pinned Jobs are skipped

- **WHEN** a policy-enabled Job has explicit `node_ids`
- **THEN** rebalancing does not touch the Job (and the combination is rejected at submission)
