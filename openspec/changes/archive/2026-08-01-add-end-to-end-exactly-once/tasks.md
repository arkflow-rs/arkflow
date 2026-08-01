## 1. Core trait refactor (zero behavior change)

- [x] 1.1 Add `write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error>` default method to the `Output` trait in `crates/arkflow-core/src/output/mod.rs` (continue-on-error, collecting last error). — verify: `cargo build -p arkflow-core`
- [x] 1.2 Rewrite `Stream::do_output` / `output` (`crates/arkflow-core/src/stream/mod.rs:401-486`) to call `write_batch` once per ack range; the err_output path uses `write_batch(&[msg])`. Ordering (`BTreeMap` + `next_seq`) and ack chain stay untouched. — verify: `cargo test -p arkflow-core`
- [x] 1.3 Add a unit test asserting the output worker calls `write_batch` exactly once per ack range (not N times `write`). — verify: `cargo test -p arkflow-core stream`
- [x] 1.4 Add a unit test asserting a `write_batch` `Err` withholds the ack (WAL cursor does not advance). — verify: `cargo test -p arkflow-core`

## 2. Kafka transactional output (L2)

- [x] 2.1 Add `exactly_once: bool` (default false) and `transactional_id: Option<String>` to the Kafka output config; `OutputBuilder::build` SHALL reject `exactly_once: true` without a non-empty `transactional_id`. — verify: `cargo test -p arkflow-plugin kafka`
- [x] 2.2 When `exactly_once` is on, create a `FutureProducer` with `transactional.id` + `enable.idempotence` set and call `init_transactions` at `connect()` (in `spawn_blocking`). When off, keep today's non-transactional producer. — verify: `cargo build -p arkflow-plugin`
- [x] 2.3 Override `write_batch` for the transactional path: `begin_transaction → send_result × N → commit_transaction`, with the blocking transaction calls inside `spawn_blocking`. — verify: `cargo build -p arkflow-plugin`
- [x] 2.4 Map rdkafka transaction errors to the three states: `is_retriable` → retry the op, `txn_requires_abort` → `abort_transaction` then re-begin, `is_fatal` → propagate (producer discarded). — verify: `cargo clippy -p arkflow-plugin`
- [x] 2.5 Preserve the non-transactional default path; existing Kafka output behavior is unchanged when `exactly_once` is off (its `write_batch` falls through to the default). — verify: existing kafka output unit test passes

## 3. Integration tests

- [x] 3.1 Add a testcontainers broker fixture for Kafka transaction tests (`tests/kafka_eos.rs`). Uses `confluentinc/cp-kafka:7.5.0` (KRaft single-node) instead of redpanda: cp-kafka is the Kafka transaction reference implementation, is reliably pullable in CI, and redpanda is wire-compatible so the EOS semantics under test are identical (the redpanda-vs-mock fallback was explicitly permitted). — verify: `cargo test -p arkflow-plugin --test kafka_eos --no-run`
- [x] 3.2 Multi-message atomic commit: a `read_committed` consumer observes all messages of a `write_batch`. — verify: `cargo test -p arkflow-plugin --test kafka_eos atomic_commit`
- [x] 3.3 Zombie fencing across a simulated restart (same `transactional_id`). — verify: `cargo test -p arkflow-plugin --test kafka_eos zombie_fenced`
- [x] 3.4 The L2 post-commit boundary: crash after producer commit but before source offset commit yields duplicates (documents the honest L2 scope). — verify: `cargo test -p arkflow-plugin --test kafka_eos post_commit`

## 4. Docs, examples, wrap-up

- [x] 4.1 Add an example config `examples/eos-kafka.yaml` (Kafka input → Kafka transactional output with `exactly_once: true` + `transactional_id`). — verify: `--validate`
- [x] 4.2 Update CLAUDE.md "Codec Components" / output sections to note Kafka transactional EOS support. — verify: doc review
- [x] 4.3 Update `openspec/PLANNING.md` progress table: Change 3 EOS status + decision note. — verify: manual
