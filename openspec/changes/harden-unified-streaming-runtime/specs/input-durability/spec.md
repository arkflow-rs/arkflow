## MODIFIED Requirements

### Requirement: Durable ingestion at the input boundary
When durability is enabled for a stream, every message returned by `input.read()` SHALL be persisted (body + sequence) and durably flushed to the WAL before it enters the pipeline. The stream SHALL also flush all pending WAL appends and stop the WAL background flusher before a normal graceful shutdown completes, including when the input is wrapped by `WalInput`.

#### Scenario: Message is durable before processing
- **WHEN** an input reads a message on a durability-enabled stream
- **THEN** the message body and an assigned sequence are written and flushed to the WAL before the message is handed to the buffer/processor

#### Scenario: Crash after read does not lose data
- **WHEN** the process crashes after `input.read()` returns but before the message is processed or output
- **THEN** the message is present in the WAL on restart and is replayed

#### Scenario: Pending WAL data is flushed on graceful shutdown
- **WHEN** a durability-enabled stream using `group-commit` or `periodic` receives a message and then completes its normal shutdown sequence before the background flush interval
- **THEN** the stream stops the WAL flusher, flushes pending appends, closes the WAL, and the message is available after reopening the WAL for recovery

### Requirement: Ack-gated cursor advancement and source commit
The WAL cursor SHALL advance past a message's sequence, and the source-side acknowledgement SHALL be performed, only after the downstream output confirms the write. The source commit SHALL happen after the WAL cursor advances. A source position exposed to checkpointing SHALL represent the highest contiguous acknowledged frontier, not a maximum out-of-order observation.

#### Scenario: Source commits only after output success
- **WHEN** the output confirms a write
- **THEN** the WAL cursor advances past that message's sequence and only then is the source-side commit performed

#### Scenario: Output failure withholds commit
- **WHEN** the output fails to write a message
- **THEN** the WAL cursor is not advanced past that sequence and the source is not committed, so the message is retried or replayed

#### Scenario: Out-of-order acknowledgements do not skip a gap
- **WHEN** records at offsets N and N+1 are delivered and N+1 is acknowledged before N
- **THEN** the exposed checkpoint position remains at the next offset after the last contiguous acknowledgement and advances past N+1 only after N is acknowledged

### Requirement: Crash recovery replays unacknowledged entries
On startup, the Engine SHALL open each durability-enabled stream's WAL and replay every entry past the committed cursor into the stream before normal processing resumes. If a compatible checkpoint cursor is installed, entries already covered by that cursor SHALL NOT be replayed, while entries after the cursor remain eligible for replay.

#### Scenario: Replay after crash
- **WHEN** the engine starts with a WAL whose committed cursor is behind the maximum written sequence
- **THEN** all entries past the committed cursor are replayed into the stream in sequence order before new input is read

#### Scenario: Clean restart replays nothing
- **WHEN** the engine starts with a WAL whose committed cursor equals the maximum written sequence
- **THEN** no entries are replayed and the stream begins reading new input

#### Scenario: Checkpoint cursor covers WAL entries
- **WHEN** recovery installs a checkpoint position that covers a previously written WAL entry
- **THEN** that entry is not replayed, while later unacknowledged WAL entries are replayed before new reads

### Requirement: WAL recovery failure fails the stream
When a durability-enabled stream starts and WAL recovery cannot complete—either because `read_after_cursor` returns an error, or because forwarding a replayed entry into the stream's downstream channel/buffer fails—the `Stream::run` SHALL return `Err` and the stream SHALL NOT enter its normal running state. The Engine SHALL observe the error and prevent the stream (and, by existing behavior, the process) from continuing as if recovery had succeeded. All opened WAL/input resources SHALL be closed on this failure path.

#### Scenario: WAL read failure surfaces to Stream.run
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns `Err`
- **THEN** `Stream::run` returns `Err` without spawning the input/processor/output workers, the runtime enters a failed state, and the WAL is closed via the existing close chain

#### Scenario: Replay forward failure surfaces to Stream.run
- **WHEN** a durability-enabled stream starts, `Wal::read_after_cursor()` returns entries to replay, and forwarding one of those entries returns `Err`
- **THEN** `Stream::run` returns `Err` without reading new input, without advancing the WAL cursor for the failed entry, and with the WAL flusher closed

### Requirement: At-least-once delivery
The system SHALL provide at-least-once delivery: after a crash and recovery, in-flight messages MAY be delivered more than once. Outputs MUST tolerate duplicates, and stateful operators MUST not durably apply an unacknowledged replay more than once.

#### Scenario: Duplicate delivery after recovery
- **WHEN** a message was output successfully but the WAL cursor had not yet advanced before a crash
- **THEN** on recovery the message is replayed and MAY be delivered to the output again, while keyed state follows the successful acknowledgement boundary

## ADDED Requirements

### Requirement: WAL wrappers SHALL close their inner input and flusher
`WalInput::close` SHALL close the wrapped input first, stop and flush the WAL flusher, and close the WAL. The operation SHALL be idempotent and SHALL surface a flush or close failure to the owning runtime.

#### Scenario: Close a running WAL input
- **WHEN** a stream shuts down normally or is replaced
- **THEN** the wrapped connector is closed, pending WAL entries are flushed, the redb handle is released, and a subsequent stream can reopen the same WAL path

#### Scenario: Close after a partial startup
- **WHEN** temporary validation or graph startup fails after a WAL has been opened
- **THEN** the temporary WAL is closed before another adapter opens the same path, preventing an exclusive-lock failure

### Requirement: Kafka restore SHALL preserve the full configured assignment
Kafka recovery SHALL merge checkpoint positions into the complete configured assignment or subscription. A checkpoint containing only a subset of topic partitions SHALL NOT unassign omitted configured partitions, and restored positions SHALL seed the in-memory acknowledged frontier used by later checkpoints.

#### Scenario: Restore a subset of partitions
- **WHEN** a checkpoint contains positions for only some configured topic partitions
- **THEN** all configured partitions remain assigned or subscribed, matching positions seek to the checkpoint offsets, and omitted partitions retain their configured starting behavior

#### Scenario: Checkpoint immediately after restore
- **WHEN** a recovered Kafka task reaches a checkpoint before acknowledging a new record
- **THEN** `current_positions()` still returns the restored positions rather than an empty cursor
