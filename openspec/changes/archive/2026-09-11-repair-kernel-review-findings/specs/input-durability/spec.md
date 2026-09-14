## MODIFIED Requirements

### Requirement: Ack-gated cursor advancement and source commit
The WAL cursor SHALL advance through a contiguous acknowledged frontier, and the source-side acknowledgement SHALL be performed only as part of the corresponding delivery boundary. The implementation SHALL keep each delivery's source outcome independent, SHALL not let an unrelated later acknowledgement make an earlier caller fail after its own commit, and SHALL preserve retryability when a source commit fails.

#### Scenario: Source commits only after output success
- **WHEN** the output confirms a write
- **THEN** the WAL cursor advances through the delivery's contiguous sequence and only then is the source-side commit performed

#### Scenario: Output failure withholds commit
- **WHEN** the output fails to write a message
- **THEN** the WAL cursor is not advanced past that sequence and the source is not committed, so the message is retried or replayed

#### Scenario: Later source acknowledgement fails
- **WHEN** an earlier WAL acknowledgement closes a gap and a later source acknowledgement fails
- **THEN** the earlier caller retains its successful result, the later delivery reports its own retryable failure, and the WAL does not skip the failed source commit

#### Scenario: Out-of-order acknowledgements do not skip a gap
- **WHEN** records at offsets N and N+1 are delivered and N+1 is acknowledged before N
- **THEN** the exposed checkpoint position remains at the next offset after the last contiguous acknowledgement and advances past N+1 only after N is acknowledged

#### Scenario: Fan-out acknowledgements preserve a gap
- **WHEN** a WAL sequence `N` fans out to multiple children and a child for `N+1` completes before the children for `N`
- **THEN** the durable cursor remains at the last contiguous sequence before `N` and recovery still replays `N`

#### Scenario: Duplicate child acknowledgement is idempotent
- **WHEN** the same fan-out child acknowledgement is delivered more than once
- **THEN** it does not advance the frontier twice or move the durable cursor beyond the highest contiguous completed sequence

#### Scenario: Restored cursor survives an immediate checkpoint
- **WHEN** a stream restores a WAL cursor and reaches a checkpoint before acknowledging a new input record
- **THEN** the checkpoint reports the restored cursor rather than replacing it with an empty or earlier position

### Requirement: Durability is orthogonal to windowing
A stream MAY combine a durable ingest WAL with event-time window operators. Enabling durability SHALL NOT disable or conflict with window operator behavior: windowed streams compile to window operators in the execution kernel (no buffer plugin is instantiated), and durability wraps only the input boundary.

#### Scenario: Durability and window operators coexist
- **WHEN** a stream is configured with both `durability.enabled: true` and a windowing `buffer`
- **THEN** messages are persisted to the WAL on read and the compiled Job processes them through the kernel's window operator with unchanged window semantics
