## MODIFIED Requirements

### Requirement: The local backend SHALL support durable working state

The initial state backend SHALL support low-latency local reads and writes, bounded disk-backed state, consistent snapshotting, restore, and state-size reporting. A durable Job SHALL reopen a stable configured working-state root rather than a process-temporary directory, SHALL isolate state by effective Job/operator/task namespace, and SHALL expose whether the state is ephemeral. Expired entries SHALL not count toward reported key or byte usage, and concurrent mutations SHALL perform state-budget validation and counter updates under one write-side critical section.

#### Scenario: Stateful Job reopens its durable root

- **WHEN** the process exits and restarts with the same Job state configuration
- **THEN** the backend uses the same stable root or a validated checkpoint restore path, not an unrelated temporary directory

#### Scenario: State exceeds the memory budget

- **WHEN** keyed state grows beyond the configured memory budget
- **THEN** the backend spills or persists state to its local working store without silently dropping state and reports the resulting live size

#### Scenario: Expired state is measured

- **WHEN** a state entry has passed its TTL but has not yet been read or purged
- **THEN** size metrics and a new budget check exclude the expired entry
