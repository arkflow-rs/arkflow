## ADDED Requirements

### Requirement: Failed processor deliveries settle all acknowledgement branches

When a processor expands one delivery into multiple outputs and a later processor rejects one output, the executor SHALL settle, route, or retain every sibling output acknowledgement. A failed branch SHALL NOT strand the original source acknowledgement indefinitely.

#### Scenario: One expanded output fails

- **WHEN** a multiple-output processor creates three child acknowledgements and a later processor fails on the second child
- **THEN** the error path handles the failed delivery and the first and third child acknowledgements are explicitly settled or retained for retry

### Requirement: Composite frontier waits are cancellation-aware

Any acknowledgement waiting for an earlier contiguous frontier SHALL observe the owning input or job cancellation and SHALL be woken when the input closes. Shutdown SHALL NOT depend on an abandoned earlier acknowledgement completing.

#### Scenario: Kafka gap closes during shutdown

- **WHEN** offset N+1 is waiting for offset N and the Kafka input is closed or the job is cancelled
- **THEN** the waiting acknowledgement returns a cancellation/closed error and the shutdown path can finish

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
