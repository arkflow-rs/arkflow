## Why

ArkFlow delivers **at-least-once** today. After a crash, in-flight messages are replayed and MAY be redelivered to outputs (`openspec/specs/input-durability/spec.md`, requirement "At-least-once delivery" → scenario "Duplicate delivery after recovery"). Two code-level facts widen the duplicate exposure:

- The output worker writes messages **one-by-one, non-atomically** within an ack range (`crates/arkflow-core/src/stream/mod.rs:469-483`). A partial write failure leaves already-written rows to be duplicated on replay.
- The Kafka source commits offsets **asynchronously** via `store_offset` + periodic auto-commit (`crates/arkflow-plugin/src/input/kafka.rs:285-294`), so the duplicate window extends to the auto-commit interval (default 5s), not just the crash instant.

For duplicate-intolerant sinks (Kafka transactional, JDBC upsert) these duplicates yield wrong results and block enterprise production use. This is precisely Benthos's stated weak spot (stateless, no native EOS), and closing it is the core of direction ② (see `openspec/PLANNING.md` §1.3 "生态位" and §3.2 Change 3).

## What Changes

- **Add `Output::write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error>`** with a default implementation equivalent to today's per-message loop (zero behavior change for outputs that don't override it). One ack range = one `write_batch` call = one transaction unit.
- **Refactor `do_output`** to call `write_batch` once per ack range instead of looping `write` in the stream layer (`stream/mod.rs:465-484`). The ordering logic (`BTreeMap` + `next_seq` watermark) and the ack chain (`WalAck` → `advance` → source commit) are untouched.
- **Kafka output gains transactional production (L2)**: opt-in via `exactly_once: true` + explicit `transactional_id`; `init_transactions` + `begin/send/commit_transaction`. Confirmed feasible — rdkafka 0.38's async `FutureProducer` implements the full transactional `Producer` trait (`rdkafka-0.38.0/src/producer/future_producer.rs:388,410-434`).
- **Kafka output gains explicit transactional config**: `exactly_once: true` + `transactional_id` (required when exactly_once is on). The user owns the id and keeps it stable across restarts. Decoupled from the WAL's `node_id` (different identity concepts) — no core or WAL config change, no migration.
- **Honest EOS scope**: L2 (Kafka transactional) = *effectively-once*. L3 (true end-to-end via `send_offsets_to_transaction`, Kafka→Kafka only) is explicitly a non-goal.

## Capabilities

### New Capabilities

- `exactly-once-output`: the transactional output contract (`write_batch` transaction unit), Kafka transactional-producer semantics, the transaction-boundary = buffer-aggregation-unit rule, and the honest effectively-once boundary (what L2 does and does not cover).

### Modified Capabilities

<!-- Exploration confirmed the Ack contract (message-acknowledgment) and the at-least-once
     guarantee (input-durability) are UNCHANGED — EOS is layered on top via the new output
     contract, not by extending ack with an epoch. This is simpler than PLANNING.md's initial
     guess of modifying both. No delta specs needed. -->

(none)

## Non-goals

- **L3 true end-to-end EOS** via `send_offsets_to_transaction` (Kafka→Kafka consume-transform-produce). It requires reworking `KafkaAck` to commit offsets inside the producer transaction with `ConsumerGroupMetadata`, and works only when both source and sink are Kafka. Tracked as a future extension in `design.md`.
- **Stateful processor checkpoint/recovery** — that is Change 4 in the roadmap; depends on this change's ack chain but is separately scoped.
- **Idempotent adapters for outputs other than Kafka and SQL** (Redis / Pulsar / NATS / MQTT / InfluxDB / HTTP / Stdout / Drop) — they keep today's default at-least-once behavior. Can be added incrementally later by overriding `write_batch`.
- **Distributed / HA** — stays single-node, per direction ②'s explicit boundary against RisingWave/Arroyo territory.
- **SQL idempotent upsert** — the SQL output's metadata *advertises* `upsert`/`upsert_keys`, but the implementation does not support them (`SqlOutputConfig` has only `output_type` + `table_name`; `execute_insert` does plain INSERT). Real `ON CONFLICT` / `ON DUPLICATE KEY` support — and fixing the schema-vs-implementation drift — is a separate change. This change does not touch the SQL output.
- **Changing the `Ack` trait** — no epoch/sequence is added to ack. Transaction identity lives entirely in the output (explicit `transactional_id`), keeping the ack contract stable.

## Impact

- **arkflow-core**:
  - `output/mod.rs` — `Output` trait gains the `write_batch` default method.
  - `stream/mod.rs:401-486` — `do_output` / `output` rewritten to call `write_batch` once per ack range.
- **arkflow-plugin**:
  - `output/kafka.rs` — transactional producer path + `exactly_once`/`transactional_id` config (the bulk of the implementation work).
  - The other 9 outputs — **unchanged** (inherit the `write_batch` default).
- **Dependencies**: none new. rdkafka 0.38 already supports transactions under the `cmake-build` feature currently in use.
- **Config**: optional `exactly_once` + `transactional_id` on the Kafka output. No core or WAL config change, no migration.
