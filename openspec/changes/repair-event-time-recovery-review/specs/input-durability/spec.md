## MODIFIED Requirements

### Requirement: Durable ingestion at the input boundary
When durability is enabled for a stream, every message returned by `input.read()` SHALL be persisted (body + sequence) and durably flushed to the WAL before it enters the pipeline. Recovery SHALL reconcile WAL entries already covered by a restored source position before new reads, and normal shutdown SHALL close the WAL flusher after pending data is flushed.

#### Scenario: Message is durable before processing
- **WHEN** an input reads a message on a durability-enabled stream
- **THEN** the message body and an assigned sequence are written and flushed to the WAL before the message is handed to the buffer/processor

#### Scenario: Covered WAL prefix is recovered
- **WHEN** a restored connector position covers entries already present in the WAL
- **THEN** those entries are removed from replay and the WAL cursor is advanced past the covered prefix before new input is acknowledged

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

### Requirement: Crash recovery replays unacknowledged entries
On startup, the Engine SHALL open each durability-enabled stream's WAL and replay every entry past the committed cursor into the stream before normal processing resumes. A replayed entry SHALL reconstruct the wrapped source-position acknowledgement when the input connector supports it; otherwise it SHALL use the connector's documented recovery behavior.

#### Scenario: Replay after crash
- **WHEN** the engine starts with a WAL whose committed cursor is behind the maximum written sequence
- **THEN** all entries past the cursor are replayed into the stream in sequence order before new input is read and a supported source position can be committed by the replay acknowledgement
