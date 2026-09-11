## MODIFIED Requirements

### Requirement: State formats SHALL be versioned
State namespaces and serialized values SHALL carry a compatible format version, and a Job deployment SHALL be rejected when the requested restore version has no supported migration path. Serialized aggregate values SHALL include enough type information to preserve their declared numeric representation.

#### Scenario: Restore with incompatible state
- **WHEN** a Job requests a savepoint whose state format cannot be read or migrated by the target Job version
- **THEN** deployment is blocked before task execution and the incompatibility is recorded

#### Scenario: Restore a typed aggregate state
- **WHEN** a checkpoint contains an integer, Float32, or Float64 window aggregate
- **THEN** the restored state retains the aggregate's numeric type and can emit a result without integer truncation or sentinel corruption

## ADDED Requirements

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
