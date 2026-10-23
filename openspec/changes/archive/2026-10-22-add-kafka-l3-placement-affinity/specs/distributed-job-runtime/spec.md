## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts

Compute nodes SHALL execute fenced task attempts: each task attempt is bound to one (job, generation, node), stale generations are rejected, and a task's placement SHALL respect execution topology — colocated placement keeps every connected component on one node (an edge MUST NOT be split), while explicitly enabled split placement MAY place the endpoints of an ordinary data edge on different nodes when both nodes advertise the cross-node data plane. Side edges — error outputs and late-event routes — MUST remain co-located, component-declared placement affinity groups (for example the Kafka L3 transactional input/output pairing, see `split-placement`) MUST remain co-located under either strategy, and nodes without the data plane keep the co-location constraint. Command dispatch, generation fencing, and per-node assignment filtering are unchanged by the placement strategy.

#### Scenario: colocated placement rejects a split edge

- **WHEN** colocated placement assigns an edge's endpoints to different nodes
- **THEN** the assignment is rejected with "connected tasks must be co-located"

#### Scenario: split placement executes a cross-node data edge

- **WHEN** split placement places an ordinary partitioned data edge's endpoints on two data-plane-capable nodes
- **THEN** both assignments are accepted and each node's graph contains the remote edge

#### Scenario: split placement rejects a cross-node side edge

- **WHEN** split placement places an error or late-event route endpoint on a different node from its origin
- **THEN** the assignment is rejected before any task attempt starts

#### Scenario: split placement keeps an affinity group co-located

- **WHEN** split placement round-robin would separate two tasks that declare the same placement affinity key
- **THEN** the pinned assignment places both tasks on the first member's node, is accepted, and the data edges between them and any operator on another node become remote edges
