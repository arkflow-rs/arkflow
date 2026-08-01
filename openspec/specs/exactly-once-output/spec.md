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
When a buffer (memory, tumbling/sliding/session window, or join) aggregates multiple input messages into one output batch, that batch's composite ack (e.g. `VecAck` / `ArrayAck`) SHALL be delivered to a single `write_batch` call. A transactional output SHALL treat one `write_batch` call as one atomic transaction unit covering all constituent input acks. No buffer SHALL drop, split, or silently merge acks in a way that breaks the one-`write_batch`-per-ack-range invariant.

#### Scenario: Window aggregation is one transaction unit
- **WHEN** a tumbling window aggregates messages from three input reads whose acks are combined into a single composite ack
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
The Kafka transactional output (L2) SHALL eliminate in-transaction partial writes and zombie duplicates. It SHALL NOT guarantee the absence of duplicates when a crash occurs after the producer transaction is committed and before the source offset is committed. Such residual duplicates MUST be absorbed by downstream idempotency (dedup key, business idempotency) or by a future L3 mechanism that commits the source offset inside the producer transaction. This limitation SHALL be documented.

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

