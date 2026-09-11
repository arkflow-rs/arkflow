## MODIFIED Requirements

### Requirement: The local backend SHALL support durable working state
The initial state backend SHALL support low-latency local reads and writes, bounded disk-backed state, consistent snapshotting, restore, and state-size reporting. Expired entries SHALL not count toward reported key or byte usage, and concurrent mutations SHALL perform state-budget validation and counter updates under one write-side critical section.

#### Scenario: State exceeds the memory budget
- **WHEN** keyed state grows beyond the configured memory budget
- **THEN** the backend spills or persists state to its local working store without silently dropping state and reports the resulting live size

#### Scenario: Expired state is measured
- **WHEN** a state entry has passed its TTL but has not yet been read or purged
- **THEN** size metrics and a new budget check exclude the expired entry

### Requirement: Input WAL and operator state SHALL remain separate
The runtime SHALL preserve the existing input WAL contract for input replay and output acknowledgement, while operator state SHALL use the StateBackend and checkpoint contract. A window state mutation and the source acknowledgements that produced its output SHALL share one rollback-capable commit boundary.

#### Scenario: Recover after an output failure
- **WHEN** a Job recovers after input records were persisted in WAL and a state checkpoint was completed
- **THEN** the runtime restores operator state from the checkpoint and replays only the source range required by the checkpoint positions and delivery policy
