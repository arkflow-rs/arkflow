## MODIFIED Requirements

### Requirement: The local backend SHALL support durable working state

The initial state backend SHALL support low-latency local reads and writes, bounded disk-backed state, consistent snapshotting, restore, and state-size reporting. When a Job configures `max_bytes`, the backend SHALL enforce that live-byte budget atomically on every mutation and restore path without silently dropping state. Expired entries SHALL not count toward reported key or byte usage, and concurrent mutations SHALL perform state-budget validation and counter updates under one write-side critical section.

#### Scenario: State exceeds the configured byte budget

- **WHEN** keyed state grows beyond the configured `max_bytes` budget
- **THEN** the backend rejects the exceeding mutation without partial commit and reports the resulting live size

#### Scenario: State exceeds the memory budget

- **WHEN** keyed state grows beyond the configured memory budget
- **THEN** the backend spills or persists state to its local working store without silently dropping state and reports the resulting live size

#### Scenario: Expired state is measured

- **WHEN** a state entry has passed its TTL but has not yet been read or purged
- **THEN** size metrics and a new budget check exclude the expired entry
