## ADDED Requirements

### Requirement: Partitioned edges preserve complete downstream ownership

For a partitioned edge, graph construction SHALL connect each upstream task to the complete eligible set of downstream subtasks. Dispatch SHALL select the downstream task using the `JobPlan` key-group ownership mapping rather than a same-subtask shortcut.

#### Scenario: A key maps to another downstream subtask

- **WHEN** a source subtask emits a keyed record whose planned key-group owner is downstream subtask 1 while the source subtask is 0
- **THEN** the record is delivered to downstream subtask 1 and its keyed state is not split into the source-indexed task

### Requirement: Partitioned routing remains stable across source partitions

Partitioned routing SHALL produce the same downstream task for the same key regardless of which physical source partition or upstream subtask delivered it.

#### Scenario: Same key arrives from two source partitions

- **WHEN** identical keys arrive through two physical source partitions assigned to different upstream tasks
- **THEN** both records are routed to the one planned key-group owner and update one logical keyed state namespace
