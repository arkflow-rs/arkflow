## MODIFIED Requirements

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

## ADDED Requirements

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
