# resource-aware-placement Delta

## MODIFIED Requirements

### Requirement: Non-pinned placements SHALL rank candidates by resource headroom

When the Hub selects the target node set for a Job whose target set is not explicitly pinned, it SHALL order eligible candidates by **effective** resource headroom — the reported memory availability and CPU headroom **minus the declared resource allocations already placed on each node** (per-task request × per-node assignment count of every desired-running Job declaring `resources`, see `job-resource-quotas`) — freshest-gauges first, effective memory-available descending, effective CPU headroom descending, node id ascending — and feed that ordered set to the unchanged placement assignment logic. Nodes without fresh resource gauges SHALL rank after gauged nodes, in node id order, and remain exempt from any resource feasibility gating. An explicit `node_ids` pin SHALL override ranking entirely.

#### Scenario: Headroom decides a first placement

- **WHEN** a new Job with no `node_ids` is placed while node A reports high memory usage and node B reports ample memory
- **THEN** the ordered candidate set places B ahead of A, and a colocated Job lands on B

#### Scenario: Ranking is deterministic

- **WHEN** the same candidate set with the same gauge values is ranked twice
- **THEN** both rankings produce the identical node order

#### Scenario: Effective headroom subtracts placed allocations

- **WHEN** node A and node B report identical gauges but node A already carries a declared allocation (for example 1 GiB of memory requests)
- **THEN** node B ranks ahead of node A
