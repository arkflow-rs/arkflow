# Capability: Input Durability

## Purpose

Provide durable ingestion at the stream input boundary so that no data entering from any input is lost across crashes. Every message read by an input is persisted (body + sequence) and `fsync`'d to a Write-Ahead Log (WAL) before it enters the pipeline. The WAL cursor advances, and the source is committed, only after the downstream output confirms the write. On startup, the Engine replays any WAL entries past the committed cursor before streams resume, delivering at-least-once semantics.

## Requirements

### Requirement: Durable ingestion at the input boundary
When durability is enabled for a stream, every message returned by `input.read()` SHALL be persisted (body + sequence) and durably flushed to the WAL before it enters the pipeline.

#### Scenario: Message is durable before processing
- **WHEN** an input reads a message on a durability-enabled stream
- **THEN** the message body and an assigned sequence are written and flushed to the WAL before the message is handed to the buffer/processor

#### Scenario: Crash after read does not lose data
- **WHEN** the process crashes after `input.read()` returns but before the message is processed or output
- **THEN** the message is present in the WAL on restart and is replayed

### Requirement: Ack-gated cursor advancement and source commit
The WAL cursor SHALL advance past a message's sequence, and the source-side acknowledgement SHALL be performed, only after the downstream output confirms the write. The source commit SHALL happen after the WAL cursor advances.

#### Scenario: Source commits only after output success
- **WHEN** the output confirms a write
- **THEN** the WAL cursor advances past that message's sequence and only then is the source-side commit performed

#### Scenario: Output failure withholds commit
- **WHEN** the output fails to write a message
- **THEN** the WAL cursor is not advanced past that sequence and the source is not committed, so the message is retried or replayed

### Requirement: Crash recovery replays unacknowledged entries
On startup, the Engine SHALL open each durability-enabled stream's WAL and replay every entry past the committed cursor into the stream before normal processing resumes.

#### Scenario: Replay after crash
- **WHEN** the engine starts with a WAL whose committed cursor is behind the maximum written sequence
- **THEN** all entries past the committed cursor are replayed into the stream in sequence order before new input is read

#### Scenario: Clean restart replays nothing
- **WHEN** the engine starts with a WAL whose committed cursor equals the maximum written sequence
- **THEN** no entries are replayed and the stream begins reading new input

### Requirement: At-least-once delivery
The system SHALL provide at-least-once delivery: after a crash and recovery, in-flight messages MAY be delivered more than once. Outputs MUST tolerate duplicates.

#### Scenario: Duplicate delivery after recovery
- **WHEN** a message was output successfully but the WAL cursor had not yet advanced before a crash
- **THEN** on recovery the message is replayed and MAY be delivered to the output again

### Requirement: Durability is orthogonal to windowing
A stream MAY combine a durable ingest WAL with a windowing buffer. Enabling durability SHALL NOT disable or conflict with the configured `buffer`, and the buffer continues to operate on in-memory windowing semantics.

#### Scenario: Durability and window buffer coexist
- **WHEN** a stream is configured with both `durability.enabled: true` and a windowing `buffer`
- **THEN** messages are persisted to the WAL on read AND pass through the windowing buffer as before

### Requirement: Configurable and opt-in durability
Durability SHALL be opt-in per stream via a `durability` configuration section. Streams without `durability` (or with `enabled: false`) SHALL retain today's in-memory, non-durable behavior. The sync policy (`per-entry` | `group-commit` | `periodic`) SHALL be configurable.

#### Scenario: Opt-in default
- **WHEN** a stream has no `durability` section
- **THEN** the stream behaves as today (in-memory only, no WAL)

#### Scenario: Explicit enable
- **WHEN** a stream has `durability.enabled: true`
- **THEN** a WAL is created at the configured path and durable ingestion is active