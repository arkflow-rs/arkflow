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

A stateful operator SHALL stage mutations for the current processing unit and SHALL make them durable in the StateBackend only after the downstream processing unit has succeeded, and a failed output or task attempt SHALL discard or roll back the staged mutation so replaying the source record cannot double-apply it. A retried apply SHALL NOT leave a partially committed transaction in the backend, and a mutation fenced by a newer committed value SHALL NOT be restored or compensated as if it had been applied.

#### Scenario: Downstream output fails

- **WHEN** a stateful operator produces an enriched record but the downstream sink write fails before acknowledgement
- **THEN** the state mutation is not committed as durable working state, the source acknowledgement remains uncommitted, and replaying the record applies the mutation exactly once for the successful attempt

#### Scenario: Checkpoint observes pending state

- **WHEN** a state mutation is still attached to an unacknowledged output at checkpoint time
- **THEN** the checkpoint snapshot contains only the last committed state epoch and recovery replays the pending record

#### Scenario: Partial apply is compensated

- **WHEN** applying a transaction fails part-way through its mutation list
- **THEN** every already-applied mutation is restored to its pre-apply value, mutations skipped by the version fence are left untouched, and the transaction can be retried

#### Scenario: Fenced mutation is not restored

- **WHEN** a retried transaction's mutation is skipped because a newer commit owns the key and the transaction is then compensated
- **THEN** the compensation does not restore the skipped mutation's previous value over the newer committed value

### Requirement: Window aggregates SHALL preserve numeric types
Window aggregation SHALL preserve Int64, Float32, and Float64 input types through accumulation, state serialization, min/max calculation, and output schema. Unsupported numeric types SHALL be rejected explicitly rather than counted through a fallback integer path.

#### Scenario: Aggregate Float64 values
- **WHEN** a Float64 window receives values 1.2 and 1.3
- **THEN** the emitted sum is a Float64 value representing 2.5, and min/max are valid Float64 values

#### Scenario: Aggregate Float32 values
- **WHEN** a Float32 window receives numeric values
- **THEN** the values are summed as numeric values and emitted with the configured Float32-compatible schema, not treated as a count

### Requirement: State rollback SHALL not erase later commits

The state backend and journal SHALL serialize conflicting finalization or use a version-conditional rollback, SHALL restore a previous value only when the current value still carries the version produced by the transaction being rolled back, and SHALL NOT overwrite a later committed mutation. The version a mutation owns SHALL be recorded when the mutation is applied rather than when it is staged, so a compensation writes back the version it actually replaced. The same conditional guard SHALL apply in the forward direction: re-applying a compensated or retried mutation SHALL NOT overwrite a value a later transaction owns.

#### Scenario: A later key update commits first

- **WHEN** transaction A is applied, transaction B commits a later update to the same key, and A's wrapped acknowledgement fails
- **THEN** A's rollback does not restore its old bytes over B and the key retains B's committed value

#### Scenario: An isolated transaction rolls back

- **WHEN** a transaction is applied and no later mutation has committed for its key before its acknowledgement fails
- **THEN** the prior value and TTL are restored and the transaction remains retryable

#### Scenario: Undo records the version it actually replaced

- **WHEN** a transaction is applied while another transaction commits the same key between staging and applying, and the first transaction's acknowledgement then fails
- **THEN** the compensation compares against the version it replaced, so a later compensation of the other transaction still recognizes the value as its own

#### Scenario: A retried mutation does not overwrite a newer commit

- **WHEN** a transaction is compensated and retried after a later transaction committed the same key
- **THEN** the retried mutation leaves the newer committed value in place

### Requirement: State mutation failures SHALL remain replayable

The runtime SHALL not make an input durable solely because an in-memory state mutation was attempted. A failed state finalization SHALL leave the source/WAL acknowledgement pending or replayable.

#### Scenario: State finalization fails after sink success

- **WHEN** the output accepts a record but the corresponding state mutation cannot be finalized
- **THEN** the source/WAL cursor is not advanced past that record and replay can retry the state mutation

### Requirement: State size accounting SHALL never wrap or drift

The local state backend SHALL maintain its tracked key count and value bytes with exact accounting on every mutation path — insertion, replacement, exhaustion, deletion, purge, and restore — SHALL NOT perform counter arithmetic that can underflow, and SHALL reconcile the tracked counters with the definition the byte budget enforces so that a configured `max_bytes` boundary rejects exactly the writes that exceed it.

#### Scenario: Expired entry is purged after a counter resync

- **WHEN** an entry expires, a compensation resynchronizes state, and a later write purges the expired entry
- **THEN** the tracked byte count stays consistent with the physical table and subsequent writes within the budget succeed

#### Scenario: Overwriting an expired entry

- **WHEN** a write replaces an entry whose TTL has already passed but whose row has not yet been purged
- **THEN** the key count does not increase for a row that was physically present and the tracked bytes match the rows stored

#### Scenario: Budget boundary is exact

- **WHEN** a write would bring the state exactly to the configured `max_bytes` bound
- **THEN** that write succeeds and the next write that would exceed it is rejected with the budget error

### Requirement: State journal version fences SHALL gate every replayed mutation

The state journal SHALL prevent a retried transaction from replaying a mutation whose staged snapshot predates a newer committed value, for every mutation kind that can overwrite state, including absolute writes and deletes. A fenced mutation SHALL be treated as never applied for compensation and completion accounting, a mutation kind that cannot be partially skipped SHALL fail the whole apply with an explicit error, and the journal SHALL bound its pending transactions with a validated limit.

#### Scenario: Stale delete against a newer commit

- **WHEN** a transaction carrying a delete is compensated and later retried after another transaction committed the same key
- **THEN** the delete does not erase the newer committed value and the apply either skips the mutation or fails explicitly

#### Scenario: Pending-bound exhaustion

- **WHEN** the number of simultaneously staged transactions reaches the configured `state.max_pending_transactions`
- **THEN** the journal rejects the new transaction with an error naming the bound and the configuration key, and a Job that leaves the bound unset keeps the documented default

#### Scenario: An invalid pending bound is rejected

- **WHEN** a Job declares `state.max_pending_transactions` as zero
- **THEN** Job validation rejects the spec before the Job starts instead of failing every journal begin at runtime

