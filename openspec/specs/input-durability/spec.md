# Capability: Input Durability

## Purpose

Provide durable ingestion at the stream input boundary so that no data entering from any input is lost across crashes. Every message read by an input is persisted (body + sequence) and `fsync`'d to a Write-Ahead Log (WAL) before it enters the pipeline. The WAL cursor advances, and the source is committed, only after the downstream output confirms the write. On startup, the Engine replays any WAL entries past the committed cursor before streams resume, delivering at-least-once semantics.
## Requirements
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

### Requirement: Crash recovery replays unacknowledged entries
On startup, the Engine SHALL open each durability-enabled stream's WAL and replay every entry past the committed cursor into the stream before normal processing resumes. A replayed entry SHALL reconstruct the wrapped source-position acknowledgement when the input connector supports it; otherwise it SHALL use the connector's documented recovery behavior.

#### Scenario: Replay after crash
- **WHEN** the engine starts with a WAL whose committed cursor is behind the maximum written sequence
- **THEN** all entries past the cursor are replayed into the stream in sequence order before new input is read and a supported source position can be committed by the replay acknowledgement

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

### Requirement: Durability is orthogonal to windowing
A stream MAY combine a durable ingest WAL with event-time window operators. Enabling durability SHALL NOT disable or conflict with window operator behavior: windowed streams compile to window operators in the execution kernel (no buffer plugin is instantiated), and durability wraps only the input boundary.

#### Scenario: Durability and window operators coexist
- **WHEN** a stream is configured with both `durability.enabled: true` and a windowing `buffer`
- **THEN** messages are persisted to the WAL on read and the compiled Job processes them through the kernel's window operator with unchanged window semantics

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

### Requirement: WAL wrappers SHALL close their inner input and flusher
`WalInput::close` SHALL close the wrapped input first, stop and flush the WAL flusher, and close the WAL. The operation SHALL be idempotent and SHALL surface a flush or close failure to the owning runtime.

#### Scenario: Close a running WAL input
- **WHEN** a stream shuts down normally or is replaced
- **THEN** the wrapped connector is closed, pending WAL entries are flushed, the redb handle is released, and a subsequent stream can reopen the same WAL path

#### Scenario: Close after a partial startup
- **WHEN** temporary validation or graph startup fails after a WAL has been opened
- **THEN** the temporary WAL is closed before another adapter opens the same path, preventing an exclusive-lock failure

### Requirement: Kafka restore SHALL preserve the full configured assignment
Kafka recovery SHALL merge checkpoint positions into the complete configured assignment or subscription. A checkpoint containing only a subset of topic partitions SHALL NOT unassign omitted configured partitions, and restored positions SHALL seed the in-memory acknowledged frontier used by later checkpoints. Waiting for a partition assignment SHALL NOT hold the consumer lock across the wait: a reconnect triggered while an assignment wait is in flight SHALL acquire the consumer lock without waiting out the assignment timeout, and a wait started against a consumer that is about to be replaced SHALL resolve without blocking the replacement.

#### Scenario: Restore a subset of partitions
- **WHEN** a checkpoint contains positions for only some configured topic partitions
- **THEN** all configured partitions remain assigned or subscribed, matching positions seek to the checkpoint offsets, and omitted partitions retain their configured starting behavior

#### Scenario: Checkpoint immediately after restore
- **WHEN** a recovered Kafka task reaches a checkpoint before acknowledging a new record
- **THEN** `current_positions()` still returns the restored positions rather than an empty cursor

#### Scenario: Reconnect is not blocked by an in-flight assignment wait
- **WHEN** a partition assignment is lost, an in-flight acknowledgement starts waiting for the old consumer's assignment, and the kernel immediately calls `connect()` to rebuild the consumer
- **THEN** the reconnect acquires the consumer lock without waiting for the in-flight wait's timeout, and the new consumer is not stuck behind a wait bound to the replaced consumer

### Requirement: Durable reads complete before delivery

When input durability is enabled, `WalInput::read()` SHALL return a batch only after its WAL entry is durably flushed according to the configured local WAL policy. A background group or periodic flusher SHALL NOT leave a returned batch outside the crash-recovery boundary.

#### Scenario: Group-commit read survives an immediate crash

- **WHEN** a group-commit durable input reads a batch and returns it to the executor
- **THEN** reopening the WAL immediately after process loss finds the returned entry even if the normal group interval has not elapsed

### Requirement: Checkpoint-covered WAL entries advance recovery state

When a restored checkpoint position covers entries already present in a WAL, recovery SHALL reconcile the covered contiguous prefix with the WAL cursor/frontier before admitting new reads. Filtering covered entries from replay SHALL NOT leave acknowledgement gaps for later entries.

#### Scenario: Covered prefix does not block the next acknowledgement

- **WHEN** the WAL contains sequences 1 and 2 covered by a checkpoint and sequence 3 is the first entry delivered after restore
- **THEN** acknowledging sequence 3 can advance the durable cursor without waiting for nonexistent acknowledgements for sequences 1 and 2

### Requirement: WAL cursor advancement precedes wrapped source commit

For a WAL acknowledgement wrapping a native source acknowledgement, the durable WAL cursor SHALL be advanced before the wrapped source commit is invoked, and an entry SHALL be reclaimable only once its wrapped source commit has succeeded. If cursor advancement fails, the source acknowledgement SHALL NOT run and the WAL acknowledgement SHALL return an error; if the source commit fails after the cursor advanced, the cursor SHALL be compensated and the unwound entry SHALL remain replayable. A WAL backend SHALL NOT delete an entry a rewind can still expose.

#### Scenario: Cursor failure prevents source commit

- **WHEN** the WAL store cannot persist the next cursor during acknowledgement
- **THEN** the wrapped Kafka or input acknowledgement is not invoked and the entry remains recoverable

#### Scenario: Source commit failure after cursor advancement

- **WHEN** the durable WAL cursor advanced for sequence N and the wrapped source acknowledgement then fails and compensates the cursor
- **THEN** sequence N is replayed after a restart, or the compensation reports an explicit failure instead of silently discarding the entry

#### Scenario: Rewind after an intervening reclaim

- **WHEN** the acked-prefix reclaim has removed entries below the cursor and a rewind moves the cursor back
- **THEN** the entries the rewind exposes are still readable and replay returns every entry the acknowledged frontier does not cover

#### Scenario: Both WAL backends keep the replay guarantee

- **WHEN** the local embedded WAL backend and the object-store WAL backend both advance and rewind a cursor across a failed wrapped acknowledgement
- **THEN** each backend exposes the same replayable range for the same sequence history

### Requirement: Retryable Kafka receives reconnect

The Kafka input SHALL classify retryable receive errors as reconnectable input failures, retrying with the existing bounded backoff and cancellation semantics. Non-retryable errors MAY fail the source, but a transient broker or network error SHALL NOT permanently terminate an otherwise running stream.

#### Scenario: Temporary broker failure resumes consumption

- **WHEN** Kafka receive reports a retryable broker or network error and the source cancellation token is not cancelled
- **THEN** the input reconnects and resumes reading without requiring a full stream restart

### Requirement: Acked-prefix reclamation SHALL be replay-safe

A WAL backend MAY reclaim entries covered by the acknowledged cursor once their source commit has succeeded, but SHALL NOT reclaim an entry that a cursor rewind can still expose, SHALL preserve the per-partition ordering of the remaining entries, and SHALL NOT assign a sequence at or below any cursor that has ever been persisted.

#### Scenario: Reclaim stays behind the acknowledged frontier

- **WHEN** a WAL acknowledgement advances the durable cursor for sequence N and the wrapped source commit succeeds
- **THEN** entries up to N may be reclaimed and a restart replays only entries above N

#### Scenario: Sequence assignment after reclamation

- **WHEN** the local backend has reclaimed every entry up to the cursor and the process restarts
- **THEN** the next appended sequence is strictly greater than the persisted cursor and no previously acknowledged sequence is reused

### Requirement: 优雅关闭时的 parked 确认 drain

WAL 关闭触发时，仍在等待前序 in-flight 交付 settle 的 parked 确认 SHALL 在有界 drain 窗口（30s）内与截止时间竞速等待：前序交付 settle（通知唤醒）后 SHALL 正常完成其源提交并返回成功；计时器先到期 SHALL 返回 `WAL closed while acknowledgement was pending`（恢复时重放，at-least-once 不变）。等待期间若发现前序序列已记录源失败，SHALL 立即返回「被前序源失败阻塞」错误——该路径不经过 drain 窗口。已可执行的确认在 close 后 SHALL 照常完成其源提交。

#### Scenario: 前序交付在窗口内 settle

- **WHEN** 序列 2 的确认 parked 在序列 1 的 in-flight 源提交之后，此时 WAL close 触发，且序列 1 的源提交在 drain 窗口内成功
- **THEN** 序列 2 的确认正常完成并返回成功，序列 1 与 2 均被提交，流关闭不产生错误

#### Scenario: 窗口耗尽回落错误路径

- **WHEN** drain 窗口耗尽时 parked 确认仍未 settle（前序源提交失败或长期阻塞）
- **THEN** 返回 `WAL closed while acknowledgement was pending`，该确认在恢复时重放（at-least-once 不变）

#### Scenario: 前序源失败立即阻塞报错

- **WHEN** 序列 1 的源提交已失败并记录错误，序列 2 的确认变为 parked
- **THEN** 序列 2 立即返回「被前序源失败阻塞」错误，不等待 drain 窗口

### Requirement: 前序源失败立即阻塞报错

等待中的 parked 确认在发现前序序列已记录源失败时，SHALL 立即返回「被前序源失败阻塞」错误——该路径不经过 drain 窗口，与 drain 超时的 `WAL closed while acknowledgement was pending` 错误可区分。

#### Scenario: 前序源失败立即阻塞报错

- **WHEN** 序列 1 的源提交已失败并记录错误，序列 2 的确认变为 parked
- **THEN** 序列 2 立即返回「被前序源失败阻塞」错误，不等待 drain 窗口
