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

### Requirement: read() is cancellation-safe at the source boundary
The engine's source loop multiplexes `input.read()` with control events (idle ticks, checkpoint barriers, cancellation) inside a `select!`: a pending `read()` future MAY be dropped and re-issued at any loop turn. An input SHALL therefore be cancellation-safe: it SHALL NOT take any observable side effect before its first `await` (claiming a queued record, advancing an internal cursor, popping a channel), and a re-issued read SHALL observe the same stream state. A read that claimed data and was then dropped would lose that delivery silently — no acknowledgement exists for it, so nothing replays it under at-least-once semantics.

#### Scenario: Pending read is dropped and re-issued
- **WHEN** the source loop's idle tick fires while a `read()` future is in flight
- **THEN** the dropped future leaves no side effect and the next `read()` call returns the same next delivery — no record is skipped or double-claimed

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

Remote/object-store backends SHALL be drivable from asynchronous engine contexts: constructing the backend and invoking its store operations from an async task SHALL neither panic (a backend that internally parks on a private runtime via `block_on` MUST be driven on a thread where that is legal) nor block an async runtime worker for the duration of its network I/O — the engine's WAL wrapper drives such stores' calls on the blocking pool. The embedded local backend runs inline on the caller by design: its commit latency is bounded (µs–ms) and redb's fcntl flock must not contend with the blocking pool on the database file.

#### Scenario: Local backend is the default
- **WHEN** a stream has `durability.enabled: true` with no `backend` field (or `backend: local`)
- **THEN** the WAL persists to a local embedded store exactly as before — process-crash recovery, single-node, no behavioral change

#### Scenario: Object-store backend is opt-in
- **WHEN** a stream has `backend: s3` (or another registered object-store backend)
- **THEN** the WAL persists segments and a manifest to the configured object store

#### Scenario: Object-store backend works through the engine's async paths
- **WHEN** a stream with `backend: object_store` runs append / cursor-advance / acknowledge / read-after-cursor / close through the WAL's async API from a tokio runtime
- **THEN** no "cannot start a runtime from within a runtime" panic occurs, the operations complete with their durable effects, and no async worker thread is parked for the duration of the store's network I/O

### Requirement: Per-node namespace isolation
When the object-store backend is in use, the WAL SHALL isolate its object namespace by a node identity (`node_id`) and a stream identity (`stream_id`) in the object key prefix. Multiple arkflow nodes sharing one bucket SHALL NOT read or overwrite each other's WAL. The `node_id` SHALL be an explicit configuration value.

#### Scenario: Nodes sharing a bucket are isolated
- **WHEN** two arkflow nodes are configured with the same object-store bucket and root prefix but different `node_id` values
- **THEN** each node reads and writes only its own `{node_id}/` namespace and neither observes the other's WAL

#### Scenario: node_id is explicit and stable across restarts
- **WHEN** a node restarts after being lost
- **THEN** recovery uses the same configured `node_id` to locate the node's prior WAL in object storage

### Requirement: Object-store WAL survives node loss
When the object-store backend is in use, every entry that has been flushed to a segment object SHALL be recoverable after the node (pod/host) is lost — not only after a process crash. Entries not yet sealed into a segment object SHALL be re-delivered by their source after restart (the source acknowledgement for such entries cannot have completed), preserving at-least-once semantics; no acknowledged entry SHALL be lost on node loss.

#### Scenario: Flushed entries survive pod disappearance
- **WHEN** a node has flushed entries to segment objects and then the node/pod disappears
- **THEN** on restart (same `node_id`) those flushed entries are present in object storage and are replayed during recovery

#### Scenario: Un-sealed entries are redelivered by the source
- **WHEN** a node disappears with entries staged in memory but not yet sealed to a segment object
- **THEN** those entries' source acknowledgements have not completed, and the source re-delivers them after restart (at-least-once), while all previously sealed entries are recovered from object storage

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

#### Scenario: Object-store rewind survives an intervening manifest flush

- **WHEN** the object-store backend advances its cursor to sequence N, the wrapped source commit fails, and a manifest flush runs before the cursor is compensated
- **THEN** the persisted manifest cursor SHALL NOT advance past `N - 1` (the failed sequence stays replayable: reads after the rewind include N), and once the source re-acknowledges through N the backend resumes advancing normally

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

### Requirement: Storage backend upgrade preserves the WAL durability contract

The redb-backed local WAL store SHALL preserve its operator-facing durability contract across the redb 2→4 engine upgrade: append/replay/trim semantics, watermark advancement, and the exclusive-lock lifecycle around `wal.redb` behave exactly as before, with every newly created database using the current redb file format. A `wal.redb` written by a prior ArkFlow release (file-format v2) SHALL fail loudly at open — an explicit error naming the file — rather than being silently recreated, silently truncated, or corrupting reads.

#### Scenario: WAL round-trip and trim still work after the bump

- **WHEN** a stream appends entries beyond the persisted watermark, restarts, and replays to the highest contiguous acked sequence
- **THEN** the redb-backed store returns exactly the appended entries in sequence order, and trimming below the watermark frees them, as before the upgrade

#### Scenario: A legacy v2 file fails loudly at open

- **WHEN** the local WAL store opens a `wal.redb` created by a pre-upgrade ArkFlow release (redb file-format v2)
- **THEN** startup fails with an error identifying the offending file path and the storage-format boundary, and the file is left untouched on disk

#### Scenario: The exclusive-lock lifecycle is unchanged

- **WHEN** a stream closes and a subsequent stream reopens the same WAL path
- **THEN** the redb handle is released on close and the reopen succeeds, preserving the existing flock-based single-writer behavior

### Requirement: WAL 驻留确认 SHALL 有界等待
因更低序号未结算而驻留的 WAL 确认 SHALL 有界等待：非关闭场景下驻留超过有界租约（`WAL_ACK_PARK_TIMEOUT`，默认 60s）SHALL 返回可重试错误且不写入失败栅栏（不栅栏仅仅较慢的 gap 持有者；驻留条目保持注册、可重试，持有者结算后重试照常放行），使流以显式周期性错误失败而非静默停滞。关闭后的既有 `WAL_ACK_DRAIN_WINDOW` 语义保持不变。

#### Scenario: gap 持有者静默死亡时流显式失败

- **WHEN** 最低未结算序号的调用方死亡且未记录失败，后续序号的确认驻留等待
- **THEN** 驻留者在租约超时后收到可重试错误（错误信息指明卡住的序号），流失败可见，不再无限等待

#### Scenario: 正常排空不受影响

- **WHEN** 更低序号在租约内正常结算
- **THEN** 驻留者被唤醒并照常执行（既有行为，租约不引入额外延迟）

#### Scenario: 关闭窗口语义不变

- **WHEN** WAL 关闭且有驻留确认
- **THEN** 既有 30s 排空窗口与显式失败路径原样保持

### Requirement: Segment-based batching with a bounded replay window
The object-store backend SHALL persist entries as immutable segment objects written in batches. A source acknowledgement SHALL complete only after the acknowledged entry's sequence is contained in a sealed segment object: un-sealed work is redone from source re-delivery on restart (replay window), never silently lost. The replay window SHALL be bounded by the configurable segment flush triggers (`max_entries`, `max_bytes`, `flush_interval`), which also bound the acknowledgement latency added by this gating. The `per-entry` sync policy SHALL be rejected for the object-store backend.

#### Scenario: Acknowledged entries are always sealed
- **WHEN** a source acknowledgement for sequence N completes on the object-store backend
- **THEN** a sealed segment object containing sequence N exists in object storage, and a crash immediately after the acknowledgement replays at most from N (never loses N)

#### Scenario: Replay window is configurable
- **WHEN** the segment flush triggers are set
- **THEN** the maximum number of entries redone on node loss — and the maximum acknowledgement latency added by seal gating — are bounded by those triggers

#### Scenario: per-entry sync is rejected on the object-store backend
- **WHEN** a stream is configured with `backend: s3` and `sync: per_entry`
- **THEN** the configuration is rejected at load time with an error

### Requirement: WAL flusher failures SHALL be observable
The WAL background flusher SHALL NOT silently swallow flush failures on its wake path: each failed flush SHALL be counted in a flush-failure metric and reported via a rate-limited warning, and a persistently failing flusher SHALL escalate to error-level logging. The shutdown path SHALL continue to surface the final flush result through `close()` as today.

#### Scenario: Persistent store failure is visible
- **WHEN** the WAL flusher's store writes fail persistently on the wake path
- **THEN** a flush-failure counter increments per failed attempt, warnings are emitted at a bounded rate, and sustained failure is logged at error level (no silent hot retry)

#### Scenario: Graceful close still surfaces the final flush
- **WHEN** the WAL is closed while the flusher has pending entries
- **THEN** `close()` performs the final flush and propagates its error, unchanged from the existing behavior

