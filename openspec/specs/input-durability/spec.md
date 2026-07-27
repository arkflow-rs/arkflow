# Capability: Input Durability

## Purpose

Provide durable ingestion at the stream input boundary so that no data entering from any input is lost across crashes. Every message read by an input is persisted (body + sequence) and `fsync`'d to a Write-Ahead Log (WAL) before it enters the pipeline. The WAL cursor advances, and the source is committed, only after the downstream output confirms the write. On startup, the Engine replays any WAL entries past the committed cursor before streams resume, delivering at-least-once semantics.
## Requirements
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

### Requirement: WAL recovery failure fails the stream
When a durability-enabled stream starts and WAL recovery cannot complete—either because `read_after_cursor` returns an error, or because forwarding a replayed entry into the stream's downstream channel/buffer fails—the `Stream::run` SHALL return `Err` and the stream SHALL NOT enter its normal running state. The Engine SHALL observe the error and prevent the stream (and, by existing behavior, the process) from continuing as if recovery had succeeded.

#### Scenario: WAL read failure surfaces to Stream::run
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns `Err`
- **THEN** `Stream::run` returns `Err` without spawning the input/processor/output workers, and the WAL is closed via the existing close chain

#### Scenario: Replay forward failure surfaces to Stream::run
- **WHEN** a durability-enabled stream starts, `Wal::read_after_cursor()` returns entries to replay, and forwarding one of those entries (via `Stream::forward`) into the configured buffer or input channel returns `Err`
- **THEN** `Stream::run` returns `Err` without reading new input, without advancing the WAL cursor for the failed entry, and without spawning the input/processor/output workers past what was needed for replay

#### Scenario: Clean restart still replays nothing
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns an empty vector (cursor at max written sequence)
- **THEN** `Stream::run` proceeds normally and reads new input

#### Scenario: Normal recovery still works
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns entries that are all successfully forwarded
- **THEN** `Stream::run` proceeds normally, the replayed entries flow through the pipeline with `WalAck` decorators so the cursor advances on downstream confirmation, and new input is read only after replay completes

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

### Requirement: Pluggable WAL storage backend
The WAL SHALL support a configurable storage backend selected per stream via a `backend` setting. The `local` backend (the existing embedded store) SHALL be the default. An `object_store` (S3-compatible) backend SHALL be available as an opt-in alternative.

#### Scenario: Local backend is the default
- **WHEN** a stream has `durability.enabled: true` with no `backend` field (or `backend: local`)
- **THEN** the WAL persists to a local embedded store exactly as before — process-crash recovery, single-node, no behavioral change

#### Scenario: Object-store backend is opt-in
- **WHEN** a stream has `backend: s3` (or another registered object-store backend)
- **THEN** the WAL persists segments and a manifest to the configured object store

### Requirement: Per-node namespace isolation
When the object-store backend is in use, the WAL SHALL isolate its object namespace by a node identity (`node_id`) and a stream identity (`stream_id`) in the object key prefix. Multiple arkflow nodes sharing one bucket SHALL NOT read or overwrite each other's WAL. The `node_id` SHALL be an explicit configuration value.

#### Scenario: Nodes sharing a bucket are isolated
- **WHEN** two arkflow nodes are configured with the same object-store bucket and root prefix but different `node_id` values
- **THEN** each node reads and writes only its own `{node_id}/` namespace and neither observes the other's WAL

#### Scenario: node_id is explicit and stable across restarts
- **WHEN** a node restarts after being lost
- **THEN** recovery uses the same configured `node_id` to locate the node's prior WAL in object storage

### Requirement: Object-store WAL survives node loss
When the object-store backend is in use, every entry that has been flushed to a segment object SHALL be recoverable after the node (pod/host) is lost — not only after a process crash. Only entries still in the in-memory staging queue (not yet flushed to a segment) are at risk on node loss.

#### Scenario: Flushed entries survive pod disappearance
- **WHEN** a node has flushed entries to segment objects and then the node/pod disappears
- **THEN** on restart (same `node_id`) those flushed entries are present in object storage and are replayed during recovery

#### Scenario: Un-flushed entries are the loss window
- **WHEN** a node disappears with entries still in the in-memory staging queue
- **THEN** those un-flushed entries are lost, while all previously flushed entries are recovered

### Requirement: Segment-based batching with a bounded loss window
The object-store backend SHALL persist entries as immutable segment objects written in batches. The loss window (entries at risk on node loss) SHALL be bounded by configurable segment flush triggers (`max_entries`, `max_bytes`, `flush_interval`). The `per-entry` sync policy SHALL be rejected for the object-store backend.

#### Scenario: Loss window is configurable
- **WHEN** the segment flush triggers are set
- **THEN** the maximum number of entries at risk on node loss is bounded by those triggers

#### Scenario: per-entry sync is rejected on the object-store backend
- **WHEN** a stream is configured with `backend: s3` and `sync: per_entry`
- **THEN** the configuration is rejected at load time with an error

### Requirement: Recovery is consistent under partial writes
Recovery from the object-store backend SHALL NOT rely solely on the manifest. It SHALL enumerate the actual segment objects (LIST) as a fallback, SHALL include segments present on the store but absent from the manifest, and SHALL verify each entry's checksum to discard a torn tail of a partially-written active segment.

#### Scenario: Segment present but manifest not updated
- **WHEN** a segment object was written but the manifest was not yet updated before a crash
- **THEN** recovery enumerates the segment via LIST and replays its entries past the cursor

#### Scenario: Torn active-segment tail is discarded
- **WHEN** the active segment's final entry is truncated by a mid-write crash
- **THEN** recovery detects the bad checksum, truncates at the last good entry, and replays only intact entries

### Requirement: Segment reclaim
The object-store backend SHALL reclaim (delete) sealed segment objects whose entries are all behind the committed cursor. Reclaim SHALL be best-effort and SHALL NOT block ingestion. A segment referenced by the manifest but missing on the store SHALL be ignored during recovery.

#### Scenario: Reclaimed segments are behind the cursor
- **WHEN** the cursor advances past the last sequence of a sealed segment
- **THEN** that segment object is deleted and removed from the manifest on the next manifest write

#### Scenario: Missing segment is ignored
- **WHEN** recovery reads a manifest that references a segment that no longer exists on the store
- **THEN** recovery skips that segment without error

