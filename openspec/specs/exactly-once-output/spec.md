# exactly-once-output Specification

## Purpose
TBD - created by archiving change add-end-to-end-exactly-once. Update Purpose after archive.
## Requirements
### Requirement: Batched transaction-unit write contract
The `Output` trait SHALL provide `write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error>`. The default implementation SHALL write each message via `write()` in order and return `Err` if any single write failed (continue-on-error, collecting the last error), preserving today's per-message behavior. The stream's output worker SHALL invoke `write_batch` exactly once per ack range instead of looping `write` in the stream layer.

#### Scenario: Default implementation preserves per-message behavior
- **WHEN** an output does not override `write_batch`
- **THEN** its behavior is identical to today's per-message loop: every message is attempted, and the ack is triggered only when all writes succeed

#### Scenario: Output worker calls write_batch once per ack range
- **WHEN** the output worker processes a `ProcessorData::Ok` batch of N messages
- **THEN** it calls `write_batch` exactly once with all N messages, not `write` N times

#### Scenario: write_batch failure withholds the ack
- **WHEN** `write_batch` returns `Err`
- **THEN** the ack is not called, the WAL cursor does not advance, and the ack range is replayed on recovery

### Requirement: Transaction boundary equals the buffer aggregation unit
When deliveries are aggregated before one output call — a window operator emission batch or a batched composite delivery — that batch's composite ack (e.g. `VecAck` / `ArrayAck`) SHALL be delivered to a single `write_batch` call. A transactional output SHALL treat one `write_batch` call as one atomic transaction unit covering all constituent input acks. No aggregation point SHALL drop, split, or silently merge acks in a way that breaks the one-`write_batch`-per-ack-range invariant.

#### Scenario: Window aggregation is one transaction unit
- **WHEN** a tumbling window operator emits one aggregate covering messages from three input reads whose acks are combined into a single composite ack
- **THEN** the aggregated batch is delivered to exactly one `write_batch` call, and a transactional sink commits the whole window atomically

### Requirement: Kafka transactional output (L2)
A Kafka output configured for exactly-once SHALL use a transactional producer: `init_transactions` at `connect()`, `begin_transaction` before sending, and `commit_transaction` after every message in the `write_batch` is sent. The blocking transaction calls (`init_transactions`, `commit_transaction`, `abort_transaction`) SHALL NOT run on the async worker. Downstream consumers with `isolation.level=read_committed` SHALL observe each `write_batch` atomically — all messages or none.

#### Scenario: Multi-message atomic commit
- **WHEN** `write_batch` receives multiple messages and the producer commits the transaction
- **THEN** a `read_committed` downstream consumer observes all of them together, or none

#### Scenario: In-transaction failure aborts and replays
- **WHEN** `commit_transaction` fails with a `txn_requires_abort` error
- **THEN** `abort_transaction` is called, `write_batch` returns `Err`, the ack is withheld, and the range is replayed on recovery

#### Scenario: Zombie producer fenced across restart
- **WHEN** the process restarts using the same `transactional.id`
- **THEN** the broker fences the prior producer epoch and aborts that producer's in-flight transaction, so zombie writes are not visible to `read_committed` consumers

### Requirement: Effectively-once boundary is honestly scoped
The Kafka transactional output (L2) SHALL eliminate in-transaction partial writes and zombie duplicates. It SHALL NOT guarantee the absence of duplicates when a crash occurs after the producer transaction is committed and before the source offset is committed. Such residual duplicates MUST be absorbed by downstream idempotency (dedup key, business idempotency) or by a future L3 mechanism that commits the source offset inside the producer transaction. This limitation SHALL be documented. L3 (Kafka `send_offsets_to_transaction`) is delivered for same-process Kafka→Kafka flows: a process-internal group registry hands the input's consumer-group metadata to the transactional output, which folds the covered source offsets (derived from batch metadata columns) into its producer transaction; the paired input suppresses its own broker offset stores (see the "Kafka 输出 SHALL 支持事务内源位点提交（L3）" requirement). Cross-process pairings (distributed jobs splitting input and output across nodes) are not covered and fail closed with an explicit configuration error. SQL outputs cover the aggregated-batch window transactionally instead (see `sql-output`).

#### Scenario: Post-commit pre-offset-commit crash produces duplicates
- **WHEN** the producer commits the transaction and the process crashes before the source offset is committed by auto-commit
- **THEN** on recovery the source redelivers the range, a new producer writes it again, and a `read_committed` downstream consumer observes duplicate rows — which downstream idempotency MUST absorb

### Requirement: Explicit stable transactional identity
A Kafka output configured with `exactly_once: true` SHALL require a non-empty `transactional_id`. The `transactional_id` SHALL be stable across restarts (the user is responsible for this) and unique per stream producer, so the broker can fence prior producer epochs on restart. The WAL's `node_id` (object-store namespace) and the Kafka `transactional_id` SHALL remain independent configuration values — neither is derived from the other.

#### Scenario: transactional_id required when exactly_once is enabled
- **WHEN** a Kafka output is configured with `exactly_once: true` but no `transactional_id`
- **THEN** configuration validation fails with a clear error

#### Scenario: transactional_id stability is the user's responsibility
- **WHEN** the process restarts and the user supplies the same `transactional_id`
- **THEN** the broker fences the prior producer epoch and aborts its in-flight transaction

#### Scenario: transactional_id is independent of WAL node_id
- **WHEN** a stream uses the object-store WAL backend with its own `node_id` and a Kafka output with a separate `transactional_id`
- **THEN** the two values are used independently — WAL namespace isolation and Kafka zombie fencing respectively

### Requirement: Kafka 输出 SHALL 支持事务内源位点提交（L3）

配置 `offset_commit_group`（要求 `exactly_once`）的 Kafka 输出 SHALL 在 `write_batch` 事务的 commit 前把覆盖的源位点折入同一事务：位点从各批次的 `__meta_partition`/`__meta_offset` 列推导（每分区取最大消费位点 +1），经进程内组注册表取得配对输入的消费者组元数据，调用 `send_offsets_to_transaction`。被指名的组 SHALL 有一个声明了 `transactional_offsets` 的同进程 Kafka 输入；该输入的 ack SHALL 推进内存 frontier 但不执行本地 `store_offset`。事务回滚时 broker 位点不得前进。

位点推导 SHALL 逐行校验来源 topic：批次携带行级 topic 元数据（`__meta_ext` map 的 `topic` 键，由 Kafka input 逐消息产出）时，任何携带 Kafka 位点元数据（`__meta_partition`/`__meta_offset` 非空）的行其 topic SHALL 等于配对输入订阅的 group topic；不满足时 `write_batch` SHALL 以显式数据错误失败（fail-closed：事务不提交、位点不推进），MUST NOT 把异源行的位点折入 group topic（否则该分区位点被事务性跳前、中间记录静默丢失）。批次整体不携带 `__meta_ext` 列时维持既有推导（兼容旧元数据形态，partition/offset 列同缺时沿用"无元数据批次贡献空位点"）。

#### Scenario: L3 提交后无重投递

- **WHEN** 一条 Kafka→Kafka 消息经 L3 输出事务写出并提交
- **THEN** 同 consumer group 的新消费者不重投递该消息（位点已随事务提交）

#### Scenario: 事务回滚不推进位点

- **WHEN** 一个 L3 write_batch 在 commit 前失败回滚
- **THEN** broker 组位点不前进，恢复后从上一已提交事务之后重放

#### Scenario: 无配对输入显式失败

- **WHEN** `offset_commit_group` 指名的组在本进程无声明 `transactional_offsets` 的活输入
- **THEN** write_batch 以配置错误失败

#### Scenario: 非事务配置拒绝

- **WHEN** `offset_commit_group` 未搭配 `exactly_once`
- **THEN** build 期以配置错误拒绝

#### Scenario: 无元数据批次贡献空位点

- **WHEN** write_batch 的批次不含 Kafka 源元数据列
- **THEN** 事务不携带额外位点（L3 只覆盖 Kafka→Kafka 流），写入本身照常

#### Scenario: 混入异源 topic 的行显式失败

- **WHEN** L3 write_batch 的批次携带 `__meta_topic` 列，且存在 Kafka 位点元数据的行其 topic 不等于配对输入订阅的 group topic（多源/扇入图混入的另一个 Kafka 源）
- **THEN** write_batch 以明确的错误失败，事务不提交、位点不推进——不得把这些行的位点折入 group topic 导致中间记录被跳过

声明 `transactional_offsets` 的输入不得进入分区指派（assign）模式：显式指派的消费者不加入组、无法提供组元数据，`assign_partition` SHALL 以明确配置错误拒绝该组合。

#### Scenario: 分区指派与 L3 组合被拒绝

- **WHEN** 一个声明 `transactional_offsets` 的 Kafka 输入被图构建期分区指派
- **THEN** assign_partition 以明确的配置错误失败，指明 L3 需要 subscribe 模式的单读者输入