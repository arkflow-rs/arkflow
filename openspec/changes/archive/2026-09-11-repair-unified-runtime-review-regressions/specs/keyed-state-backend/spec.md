# keyed-state-backend Specification

## MODIFIED Requirements

### Requirement: Stateful operators SHALL use keyed namespaces

Stateful operators SHALL access state through a namespaced keyed-state API that supports get, update, delete, iteration where declared, TTL where configured, and operator identity isolation. State mutation acknowledgements SHALL preserve the value's expiration metadata when a mutation is compensated or rolled back.

#### Scenario: Update two keys in one operator

- **WHEN** an operator processes records for two distinct keys
- **THEN** each key's state is updated independently and no state is visible across keys or unrelated operator namespaces

#### Scenario: Roll back a TTL value

- **WHEN** a staged mutation for a TTL-enabled key is compensated after a failed acknowledgement
- **THEN** the prior value and its expiration metadata are restored and the value still expires according to the original TTL

## ADDED Requirements

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
