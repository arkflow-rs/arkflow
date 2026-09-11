# keyed-state-backend Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Stateful operators SHALL use keyed namespaces

Stateful operators SHALL access state through a namespaced keyed-state API that supports get, update, delete, iteration where declared, TTL where configured, and operator identity isolation. State mutation acknowledgements SHALL preserve the value's expiration metadata when a mutation is compensated or rolled back.

#### Scenario: Update two keys in one operator

- **WHEN** an operator processes records for two distinct keys
- **THEN** each key's state is updated independently and no state is visible across keys or unrelated operator namespaces

#### Scenario: Roll back a TTL value

- **WHEN** a staged mutation for a TTL-enabled key is compensated after a failed acknowledgement
- **THEN** the prior value and its expiration metadata are restored and the value still expires according to the original TTL

### Requirement: The local backend SHALL support durable working state
The initial state backend SHALL support low-latency local reads and writes, bounded disk-backed state, consistent snapshotting, restore, and state-size reporting. Expired entries SHALL not count toward reported key or byte usage, and concurrent mutations SHALL perform state-budget validation and counter updates under one write-side critical section.

#### Scenario: State exceeds the memory budget
- **WHEN** keyed state grows beyond the configured memory budget
- **THEN** the backend spills or persists state to its local working store without silently dropping state and reports the resulting live size

#### Scenario: Expired state is measured
- **WHEN** a state entry has passed its TTL but has not yet been read or purged
- **THEN** size metrics and a new budget check exclude the expired entry

### Requirement: State formats SHALL be versioned
State namespaces and serialized values SHALL carry a compatible format version, and a Job deployment SHALL be rejected when the requested restore version has no supported migration path. Serialized aggregate values SHALL include enough type information to preserve their declared numeric representation.

#### Scenario: Restore with incompatible state
- **WHEN** a Job requests a savepoint whose state format cannot be read or migrated by the target Job version
- **THEN** deployment is blocked before task execution and the incompatibility is recorded

#### Scenario: Restore a typed aggregate state
- **WHEN** a checkpoint contains an integer, Float32, or Float64 window aggregate
- **THEN** the restored state retains the aggregate's numeric type and can emit a result without integer truncation or sentinel corruption

### Requirement: Input WAL and operator state SHALL remain separate
The runtime SHALL preserve the existing input WAL contract for input replay and output acknowledgement, while operator state SHALL use the StateBackend and checkpoint contract. A window state mutation and the source acknowledgements that produced its output SHALL share one rollback-capable commit boundary.

#### Scenario: Recover after an output failure
- **WHEN** a Job recovers after input records were persisted in WAL and a state checkpoint was completed
- **THEN** the runtime restores operator state from the checkpoint and replays only the source range required by the checkpoint positions and delivery policy

### Requirement: Stateful mutations SHALL commit only after successful processing
A stateful operator SHALL stage mutations for the current processing unit and SHALL make them durable in the StateBackend only after the downstream processing unit has succeeded. A failed output or task attempt SHALL discard or roll back the staged mutation so replaying the source record cannot double-apply it.

#### Scenario: Downstream output fails
- **WHEN** a stateful operator produces an enriched record but the downstream sink write fails before acknowledgement
- **THEN** the state mutation is not committed as durable working state, the source acknowledgement remains uncommitted, and replaying the record applies the mutation exactly once for the successful attempt

#### Scenario: Checkpoint observes pending state
- **WHEN** a state mutation is still attached to an unacknowledged output at checkpoint time
- **THEN** the checkpoint snapshot contains only the last committed state epoch and recovery replays the pending record

### Requirement: Window aggregates SHALL preserve numeric types
Window aggregation SHALL preserve Int64, Float32, and Float64 input types through accumulation, state serialization, min/max calculation, and output schema. Unsupported numeric types SHALL be rejected explicitly rather than counted through a fallback integer path.

#### Scenario: Aggregate Float64 values
- **WHEN** a Float64 window receives values 1.2 and 1.3
- **THEN** the emitted sum is a Float64 value representing 2.5, and min/max are valid Float64 values

#### Scenario: Aggregate Float32 values
- **WHEN** a Float32 window receives numeric values
- **THEN** the values are summed as numeric values and emitted with the configured Float32-compatible schema, not treated as a count

### Requirement: State rollback SHALL not erase later commits

The state backend and journal SHALL serialize conflicting finalization or use a version-conditional rollback. A rollback SHALL restore a previous value only when the current value still carries the version produced by the transaction being rolled back; it SHALL NOT overwrite a later committed mutation.

#### Scenario: A later key update commits first

- **WHEN** transaction A is applied, transaction B commits a later update to the same key, and A's wrapped acknowledgement fails
- **THEN** A's rollback does not restore its old bytes over B and the key retains B's committed value

#### Scenario: An isolated transaction rolls back

- **WHEN** a transaction is applied and no later mutation has committed for its key before its acknowledgement fails
- **THEN** the prior value and TTL are restored and the transaction remains retryable

### Requirement: State mutation failures SHALL remain replayable

The runtime SHALL not make an input durable solely because an in-memory state mutation was attempted. A failed state finalization SHALL leave the source/WAL acknowledgement pending or replayable.

#### Scenario: State finalization fails after sink success

- **WHEN** the output accepts a record but the corresponding state mutation cannot be finalized
- **THEN** the source/WAL cursor is not advanced past that record and replay can retry the state mutation

