# Capability: Input Durability

## MODIFIED Requirements

### Requirement: Ack-gated cursor advancement and source commit

The WAL cursor SHALL advance past a message's sequence, and the source-side acknowledgement SHALL be performed, only after the downstream output confirms the write. The source commit SHALL happen after the WAL cursor advances. When one input delivery fans out into multiple child acknowledgements, completion SHALL be recorded against the original input sequence and the cursor SHALL advance only through the highest contiguous completed sequence; an out-of-order child SHALL NOT skip an earlier incomplete sequence.

#### Scenario: Source commits only after output success

- **WHEN** the output confirms a write
- **THEN** the WAL cursor advances past that message's sequence and only then is the source-side commit performed

#### Scenario: Output failure withholds commit

- **WHEN** the output fails to write a message
- **THEN** the WAL cursor is not advanced past that sequence and the source is not committed, so the message is retried or replayed

#### Scenario: Fan-out acknowledgements preserve a gap

- **WHEN** a WAL sequence `N` fans out to multiple children and a child for `N+1` completes before the children for `N`
- **THEN** the durable cursor remains at the last contiguous sequence before `N` and recovery still replays `N`

#### Scenario: Duplicate child acknowledgement is idempotent

- **WHEN** the same fan-out child acknowledgement is delivered more than once
- **THEN** it does not advance the frontier twice or move the durable cursor beyond the highest contiguous completed sequence

#### Scenario: Restored cursor survives an immediate checkpoint

- **WHEN** a stream restores a WAL cursor and reaches a checkpoint before acknowledging a new input record
- **THEN** the checkpoint reports the restored cursor rather than replacing it with an empty or earlier position

### Requirement: Durable ingestion at the input boundary

When durability is enabled for a stream, every message returned by `input.read()` SHALL be persisted (body + sequence) and durably flushed to the WAL before it enters the pipeline. The stream SHALL also flush all pending WAL appends and stop the WAL background flusher before a normal graceful shutdown completes.

#### Scenario: Message is durable before processing

- **WHEN** an input reads a message on a durability-enabled stream
- **THEN** the message body and an assigned sequence are written and flushed to the WAL before the message is handed to the buffer/processor

#### Scenario: Crash after read does not lose data

- **WHEN** the process crashes after `input.read()` returns but before the message is processed or output
- **THEN** the message is present in the WAL on restart and is replayed

#### Scenario: Pending WAL data is flushed on graceful shutdown

- **WHEN** a durability-enabled stream using `group-commit` or `periodic` receives a message and then completes its normal shutdown sequence before the background flush interval
- **THEN** the stream stops the WAL flusher, flushes pending appends, and the message is available after reopening the WAL for recovery
