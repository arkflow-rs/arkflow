## Context

Today's pipeline is at-least-once. The ack chain is sound — `input.read() → wal.append → WalAck(wal, seq, source_ack)`, and `do_output` only calls `ack.ack()` after the output confirms the write (`crates/arkflow-core/src/stream/mod.rs:465-484`), so a failed write withholds cursor advancement and triggers replay. The duplicate exposure comes from two places:

1. **Non-atomic per-message write within one ack range** — `do_output` loops `output.write(msg)` per message (`stream/mod.rs:469-477`). A partial failure already writes some rows, then the whole range is replayed.
2. **Asynchronous source offset commit** — `KafkaAck::ack()` only calls `store_offset`; the real commit is librdkafka's periodic auto-commit (`input/kafka.rs:285-294`), so the duplicate window is the auto-commit interval (default 5s), not just the crash instant.

**Spike result (rdkafka 0.38 async transactions):** confirmed feasible. `FutureProducer` implements the full transactional `Producer` trait — `init_transactions` / `begin_transaction` / `send_offsets_to_transaction` / `commit_transaction` / `abort_transaction` are all wired (`rdkafka-0.38.0/src/producer/future_producer.rs:388,410-434`), and the docs state "All records sent after starting a transaction and before committing… will automatically be associated with that transaction" (`producer/mod.rs:92-93`) — so the existing `send_result` path needs no special transactional send method. The earlier assumption that async transaction support was incomplete was wrong.

This change adds an effectively-once layer on top of the unchanged at-least-once base, by giving outputs a transaction-unit boundary and implementing it for Kafka (L2) and SQL (L1).

## Goals / Non-Goals

**Goals:**

- One unified output write path (`write_batch`) where one ack range = one transaction unit.
- Kafka output reaches L2 (transactional producer, atomic multi-message writes, zombie fencing).
- SQL output reaches L1 (idempotent upsert) reusing the existing config.
- Zero behavior change for the 8 outputs that don't need EOS (they inherit the default).
- An honest, spec-grade statement of what L2 does and does not guarantee.

**Non-Goals:** (see proposal.md) L3 true end-to-end EOS; stateful checkpoint (Change 4); idempotent adapters for other outputs; distributed/HA; modifying the `Ack` trait.

## Decisions

### D1 — `write_batch` as a default trait method (not a new trait, not a signature change)

**Choice:** Keep `Output::write(msg)` and add `write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error>` with a default implementation. `do_output` calls only `write_batch`.

**Alternatives considered:**

| Alt | Rejected because |
|---|---|
| Replace `write` with `write(msgs)` (pure batch) | Forces all 10 outputs to change; gains nothing over a default method. |
| `write(msg, ctx)` with a transaction context | Doesn't itself solve atomicity (still per-message); ctx is dead weight for non-EOS outputs. |
| Separate `ExactlyOnceOutput` trait + downcast dispatch | Two stream-level write paths and runtime downcast — long-term maintenance burden. |

A default method gets the single-path benefit of a rewrite, the zero-breakage of an opt-in trait, and needs no dispatch — strictly better than the alternatives. This is the core simplification versus PLANNING.md's initial instinct.

### D2 — Default implementation is continue-on-error

The default `write_batch` loops `write` and collects the last error, returning `Err` if any failed — **byte-for-byte equivalent to today's** `do_output` loop (`stream/mod.rs:469-483`). This is the guarantee that the 8 inheriting outputs have zero behavior change. (Fail-fast `?` was rejected because it would change which messages get written before a failure surfaces.)

### D3 — Transaction boundary = buffer aggregation unit

`ProcessorData::Ok(Vec<MessageBatchRef>)` is already ack-aligned (one `ProcessResult` carries one ack). Window/join buffers aggregate acks into `VecAck` / `ArrayAck` (`buffer/window.rs:135,145`, `buffer/memory.rs:134`), so a buffered batch's ack already represents all its constituent input acks. Therefore **one `write_batch` call ⇄ one (possibly composite) ack ⇄ one transaction** holds in *all* buffer modes — no buffer loses acks (verified for memory/window/sliding/tumbling/session/join). Consequence: transaction granularity equals the buffer's aggregation unit, so window size bounds transaction size.

### D4 — Kafka transactional producer; blocking calls off the async worker

`write_batch` override: `begin_transaction → send_result × N → commit_transaction`. `commit_transaction` / `init_transactions` are synchronous broker round-trips (`base_producer.rs:590`), so they run inside `spawn_blocking` to avoid stalling the tokio worker. Errors map to rdkafka's three transactional states (`producer/mod.rs:104-114`): `is_retriable` → retry the op; `txn_requires_abort` → `abort_transaction` then re-begin; `is_fatal` → propagate (producer must be discarded). `init_transactions` runs once at `connect()`.

### D5 — `transactional_id` configured explicitly on the Kafka output

**Choice:** the Kafka output gains `exactly_once: bool` (default `false`) and `transactional_id: String` (required when `exactly_once: true`). The user owns the id and MUST keep it stable across restarts so the broker can fence prior producer epochs.

**Rationale:** `transactional.id` (Kafka producer zombie-fencing identity) and `node_id` (the WAL's namespace inside a shared object-store bucket) are different identity concepts. Coupling them would require the output to read the durability config, but output and durability are peer configs under a stream and `Output::build()` has no access to `WalConfig` — `Resource` carries only `temporary` and `input_names` (`lib.rs:115`), and there is no global node identity on `EngineConfig`. An explicit id decouples the two, mirrors how Kafka Streams / Benthos configure `transactional.id`, and avoids any BREAKING change to the WAL config or core.

**Alternatives rejected:** (A) promote a global `EngineConfig.node_id` into `Resource` so wal + output share it — too invasive, BREAKING the WAL config; (C) have `StreamConfig::build` inject `durability.node_id` into the output — requires a setter on the output trait and only works when durability is enabled.

**Consequence:** the former `node_id` promotion task is dropped. The spec requirement becomes "explicit stable transactional identity", not "node_id at durability top-level".

### D6 — SQL idempotent upsert is out of scope (separate change)

**Discovery during apply:** the SQL output's metadata *advertises* `upsert`/`upsert_keys` (`output/sql.rs:449-450`), but the implementation does not support them — `SqlOutputConfig` has only `output_type` + `table_name`, and `DatabaseConnection::execute_insert` does plain INSERT (no `ON CONFLICT` / `ON DUPLICATE KEY`). The schema is aspirational and drifts from the implementation; `upsert: true` is silently ignored by serde.

**Decision:** this change does not touch the SQL output. Real idempotent upsert (plus fixing the schema-vs-implementation drift) is a separate change. The earlier assumption "SQL L1 reuses existing upsert" was wrong — it was based on the metadata, not the struct.

### D7 — Honest L2 boundary; L3 path recorded as future

L2 covers: in-transaction atomicity, zombie fencing (`transactional.id` + read_committed downstream), same-instance dedup (idempotent producer PID). L2 does **not** cover the **already-committed-then-crash window**: if the producer commits the transaction but the process crashes before the source offset auto-commits, replay redelivers and a *new* producer (new PID) writes again — `read_committed` downstream sees both. This window is bounded by the auto-commit interval. Mitigations: downstream business idempotency / dedup key, or future L3.

**L3 future path (Kafka→Kafka only):** `send_offsets_to_transaction(consumer.offset, consumer.group_metadata)` folds the source offset commit into the same producer transaction, eliminating the window. This requires reworking `KafkaAck` from `store_offset` to in-transaction offset commit and needs `ConsumerGroupMetadata`; out of scope here.

## Risks / Trade-offs

- **[commit_transaction blocks the async worker]** → wrap `init/commit/abort` in `spawn_blocking`. The `send_result` path stays async (delivery futures are awaited as today).
- **[Large transactions when windows are big]** → transaction size is bounded by the buffer aggregation unit (D3); documented as a knob controlled by processor/window config, not a new mechanism.
- **[Spike was source-level only; runtime behavior unverified in this repo]** → an integration test against a real broker (or testcontainers redpanda) is required in tasks, covering commit, abort, and the post-commit-crash duplicate window.
- **[L2 post-commit-crash duplicate window]** → explicitly declared in specs as outside L2's guarantee; downstream idempotency or future L3 required. Not silently hidden.
- **[`node_id` config move is BREAKING]** → migration plan below; `--validate` emits a clear error pointing to the new location.
- **[Transactional producer cannot send outside a transaction once initialized]** → the Kafka output's transactional mode is opt-in; the default non-transactional `FutureProducer` path is preserved for users who don't enable EOS.

## Migration Plan

1. **`node_id` relocation**: accept `durability.node_id`. During a deprecation window, also read `durability.backend.object_store.node_id` with a warn-level deprecation log if the top-level is absent. `--validate` documents the move. After one release, drop the fallback.
2. **`write_batch` default**: lands first as a pure refactor (default impl + `do_output` switch) with no output overriding it — provably zero behavior change via existing stream tests. Kafka/SQL overrides land as a follow-up within the same change.
3. **Kafka transactional mode**: opt-in via `transactional.id` (explicit) or an `exactly_once: true` flag (derives the id). Default stays the current idempotent producer.
4. **Rollback**: disabling `exactly_once` / removing `transactional.id` reverts Kafka to today's behavior; the `write_batch` refactor is internally reversible and carries no semantic risk.

## Open Questions

- **Auto-commit interval in EOS mode**: should `auto.commit.interval.ms` be tightened (or source commit made synchronous) to shrink the L2 post-commit duplicate window? Trade-off is throughput vs. duplicate exposure. Defer to implementation profiling.
- **Default for Kafka `exactly_once`**: opt-in (explicit flag, safest) vs. on-when-`transactional.id`-present. Leaning opt-in; confirm during implementation.
- **Integration test broker**: RESOLVED — use `confluentinc/cp-kafka:7.5.0` (KRaft single-node) via testcontainers, not redpanda. cp-kafka is the Kafka transaction reference implementation, is reliably pullable in CI (and was already cached on the dev machine), and redpanda is wire-compatible so the EOS semantics under test (atomic commit, zombie fencing, the post-commit duplicate window) are identical; `tasks.md` 3.1 explicitly permitted a broker fallback. A mock was rejected because it cannot validate fencing/atomicity. See `crates/arkflow-plugin/tests/kafka_eos.rs`.
