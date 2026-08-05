## ADDED Requirements

### Requirement: Stateful operators SHALL use keyed namespaces
Stateful operators SHALL access state through a namespaced keyed-state API that supports get, update, delete, iteration where declared, TTL where configured, and operator identity isolation.

#### Scenario: Update two keys in one operator
- **WHEN** an operator processes records for two distinct keys
- **THEN** each key's state is updated independently and no state is visible across keys or unrelated operator namespaces

### Requirement: The local backend SHALL support durable working state
The initial state backend SHALL support low-latency local reads and writes, bounded disk-backed state, consistent snapshotting, restore, and state-size reporting.

#### Scenario: State exceeds the memory budget
- **WHEN** keyed state grows beyond the configured memory budget
- **THEN** the backend spills or persists state to its local working store without silently dropping state and reports the resulting size

### Requirement: State formats SHALL be versioned
State namespaces and serialized values SHALL carry a compatible format version, and a Job deployment SHALL be rejected when the requested restore version has no supported migration path.

#### Scenario: Restore with incompatible state
- **WHEN** a Job requests a savepoint whose state format cannot be read or migrated by the target Job version
- **THEN** deployment is blocked before task execution and the incompatibility is recorded

### Requirement: Input WAL and operator state SHALL remain separate
The runtime SHALL preserve the existing input WAL contract for input replay and output acknowledgement, while operator state SHALL use the StateBackend and checkpoint contract.

#### Scenario: Recover after an output failure
- **WHEN** a Job recovers after input records were persisted in WAL and a state checkpoint was completed
- **THEN** the runtime restores operator state from the checkpoint and replays only the source range required by the checkpoint positions and delivery policy
