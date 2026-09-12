# Capability: Message Acknowledgment

## Purpose

Define the cross-cutting `Ack` contract used to confirm that a message has been durably processed. Acknowledgement is fallible so that failures during durable cursor advancement or source-side commit propagate to the stream instead of being silently swallowed, enabling the stream to apply backpressure or stop on persistent errors. Composite acknowledgements (e.g. `VecAck`) must surface partial failures rather than hide them.
## Requirements
### Requirement: Acknowledgement is fallible

The `Ack` trait SHALL return `Result<(), Error>` from `ack()`, so that failures during durable cursor advancement or source-side commit propagate to the stream instead of being swallowed, enabling dependent state finalization to retry or roll back safely.

#### Scenario: Successful acknowledgement

- **WHEN** a downstream output confirms a write and the WAL cursor advances successfully
- **THEN** `ack()` returns `Ok(())` and the source-side commit (if any) is performed

#### Scenario: Cursor advancement failure is surfaced

- **WHEN** the durable cursor advancement fails (e.g. storage error, disk full)
- **THEN** `ack()` returns `Err` and the stream is able to observe the failure to apply backpressure or stop, rather than silently continuing

### Requirement: Composite acknowledgement propagates errors
Composite acknowledgements SHALL return `Err` if any constituent acknowledgement fails, SHALL not report success for a partially committed logical source delivery, and SHALL compensate already-successful durable constituents before returning a sibling failure whenever the constituent supports compensation. Retries SHALL remain possible after a transient failure.

#### Scenario: One constituent fails
- **WHEN** a composite acknowledgement acks multiple constituents and one returns `Err`
- **THEN** the composite acknowledgement returns `Err`, compensates successful durable constituents, and leaves the logical delivery retryable

#### Scenario: A retry follows a failed fan-out acknowledgement
- **WHEN** a fan-out acknowledgement is retried after a transient wrapped acknowledgement failure
- **THEN** the staged state transaction and every child acknowledgement are still available for the retry

#### Scenario: No-op acknowledgement succeeds
- **WHEN** a `NoopAck` is acked
- **THEN** it returns `Ok(())` without side effect

### Requirement: State finalization SHALL be ordered with dependent acknowledgements

The runtime SHALL treat state/window finalization, WAL cursor advancement, and source commit as one ordered acknowledgement unit. If any earlier step fails, later dependent steps SHALL NOT be considered durable.

#### Scenario: Window journal apply fails

- **WHEN** a sink write succeeds but applying the staged window mutation fails
- **THEN** the source acknowledgement is withheld and the input remains replayable

#### Scenario: All dependent acknowledgements succeed

- **WHEN** state finalization, WAL advancement, and source commit all complete successfully
- **THEN** the composite acknowledgement returns `Ok(())` and no staged mutation remains pending

### Requirement: Failed processor deliveries settle all acknowledgement branches

When a processor expands one delivery into multiple outputs and a later processor rejects one output, the executor SHALL settle, route, or retain every sibling output acknowledgement. A failed branch SHALL NOT strand the original source acknowledgement indefinitely.

#### Scenario: One expanded output fails

- **WHEN** a multiple-output processor creates three child acknowledgements and a later processor fails on the second child
- **THEN** the error path handles the failed delivery and the first and third child acknowledgements are explicitly settled or retained for retry

### Requirement: Composite frontier waits are cancellation-aware

Any acknowledgement waiting for an earlier contiguous frontier SHALL observe the owning input or job cancellation, SHALL be woken when the input closes, and SHALL NOT hold a shared per-input serialization lock or the input's connection guard while it waits for an external condition — an assignment to return, a gap to close, or a broker round trip — so sibling acknowledgements on other partitions continue to settle. Shutdown SHALL NOT depend on an abandoned earlier acknowledgement completing, and a wait serving an external condition SHALL be bounded: when it expires the input SHALL either fail the delivery explicitly or record the documented at-least-once degradation.

#### Scenario: Kafka gap closes during shutdown

- **WHEN** offset N+1 is waiting for offset N and the Kafka input is closed or the job is cancelled
- **THEN** the waiting acknowledgement returns a cancellation/closed error and the shutdown path can finish

#### Scenario: Rebalance while an acknowledgement is in flight

- **WHEN** a partition is unassigned during a rebalance and an in-flight acknowledgement for that partition waits for the assignment to return
- **THEN** acknowledgements for the input's other partitions keep advancing, the checkpoint drain is not blocked behind the wait, and the wait ends on reassignment, cancellation, or its bound

### Requirement: Source acknowledgement failure compensates window state

When a fired window's output has been accepted but its source or WAL acknowledgement fails, the staged window state SHALL remain retryable or be rolled back as one acknowledgement unit. A source replay SHALL NOT observe a finalized state mutation that cannot be safely reconciled.

#### Scenario: Source commit fails after output success

- **WHEN** the sink write succeeds and the source acknowledgement fails transiently
- **THEN** the window transaction is not irreversibly finalized; retrying the delivery applies the aggregate exactly once

### Requirement: Composite durable acknowledgement preserves failure ordering

Composite acknowledgements SHALL report the first durable failure without silently committing later dependent work. Cursor, state, source, and sibling acknowledgement operations SHALL remain idempotent across retry.

#### Scenario: Retry after a partial composite failure

- **WHEN** one constituent of a composite acknowledgement fails after another constituent completed
- **THEN** a retry does not duplicate the completed durable effect and eventually reaches one contiguous successful frontier

### Requirement: Delivery settlement SHALL NOT block the source loop

Settling a delivery SHALL NOT require the input's read loop to wait on a durable commit, a broker round trip, or an acknowledgement that depends on later input. An input producing a delivery it does not forward — a tombstone, an empty batch, or a filtered record — SHALL hand the settlement to the same compensation path as a forwarded delivery instead of committing inline in `read`, so the source keeps polling for records and injected control events, and a settlement failure surfaces against that delivery for retry rather than as an input error that terminates the source.

#### Scenario: Tombstone delivery is settled out of band

- **WHEN** a compacted topic delivers a record with a null payload
- **THEN** the input acknowledges its position without invoking the acknowledgement inline in the read loop, and the source keeps polling for the next record and for injected control events

#### Scenario: Settlement failure is retryable

- **WHEN** settling a non-forwarded delivery fails because of a frontier fence, a broker offset store failure, or a closed connection
- **THEN** the failure is reported against that delivery so replay or the input's reconnect path can retry it, instead of failing the source permanently

