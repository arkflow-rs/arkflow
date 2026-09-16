## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts

Compute nodes SHALL execute fenced task attempts: each task attempt is bound to one (job, generation, node), stale generations are rejected, and a task's placement SHALL respect execution topology — colocated placement keeps every connected component on one node (an edge MUST NOT be split), while explicitly enabled split placement MAY place the endpoints of a data edge on different nodes when both nodes advertise the cross-node data plane (side edges — error and late-event routes — and nodes without the data plane keep the co-location constraint). Command dispatch, generation fencing, and per-node assignment filtering are unchanged by the placement strategy.

#### Scenario: colocated placement rejects a split edge

- **WHEN** colocated placement assigns an edge's endpoints to different nodes
- **THEN** the assignment is rejected with "connected tasks must be co-located"

#### Scenario: split placement executes a cross-node data edge

- **WHEN** split placement places a partitioned data edge's endpoints on two data-plane-capable nodes
- **THEN** both assignments are accepted and each node's graph contains the remote edge
