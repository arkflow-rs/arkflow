## 1. Phase 0 — Source ack-gating audit (no breaking changes)

- [x] 1.1 Audit Kafka input: confirm `enable.auto.commit=false` by default and that `store_offset` fires only inside `ack()`; document the requirement
- [x] 1.2 Audit MQTT / NATS / Pulsar inputs: confirm manual ack on `ack()` only (no auto-ack before output); fix defaults if needed
- [x] 1.3 Add integration test proving a replayable source → durable output survives a simulated mid-flight crash with auto-commit off
- [x] 1.4 Document which inputs are replayable vs non-replayable (HTTP/File/Generate/Modbus/Redis/SQL/WebSocket = non-replayable)

## 2. Fallible Ack trait (D2 — BREAKING)

- [x] 2.1 Change `Ack::ack()` to `async fn ack(&self) -> Result<(), Error>` in `arkflow-core/src/input/mod.rs`
- [x] 2.2 Update `NoopAck` → `Ok(())`; update `VecAck` to propagate the first `Err`
- [x] 2.3 Update Kafka / MQTT / NATS / Pulsar `Ack` implementors to the new signature
- [x] 2.4 Update all `ack.ack().await` call sites in `Stream` (`do_processor`, `output`, error paths) to handle `Result` (log + backpressure/stop on `Err`)
- [x] 2.5 `cargo test --workspace` green after the signature change

## 3. WAL storage layer (D4)

- [x] 3.1 Add `redb` workspace dependency in root `Cargo.toml`
- [x] 3.2 Implement a WAL module in `arkflow-core`: open at path, write `(seq → Arrow IPC bytes)`, advance committed cursor, read entries past cursor in order
- [x] 3.3 Verify Arrow `RecordBatch` ↔ IPC bytes round-trip preserves schema and metadata columns (`__meta_*`)
- [x] 3.4 Unit tests: write/read/advance-cursor, reopen after drop simulates recovery, corrupted-store open surfaces `Err`

## 4. Stream-owned ingest stage + config (D3)

- [x] 4.1 Add `DurabilityConfig` (`enabled`, `path`, `sync`, `flush_interval`) to `StreamConfig` and wire into `Stream::new`
- [x] 4.2 Add the ingest stage to `Stream::run`: on `input.read()`, persist `(msg, seq)` + flush, then emit `(msg, WalAck{seq, inner_ack})`
- [x] 4.3 Implement `WalAck`: on `ack()`, advance WAL cursor past `seq`, then call inner source ack; return `Err` if cursor advance fails
- [x] 4.4 Ensure durability is orthogonal to the optional windowing `buffer` (both active when configured)
- [x] 4.5 Default opt-in: stream without `durability` keeps today's in-memory behavior

## 5. Sync policy (D5)

- [x] 5.1 Implement `per-entry` / `group-commit` / `periodic` flush modes in the WAL
- [x] 5.2 Default to `group-commit` with the configured `flush_interval`
- [x] 5.3 Benchmark ingest throughput per mode; record before/after numbers

## 6. Crash recovery (Engine startup)

- [x] 6.1 Add a recovery phase to Engine startup: open each durability-enabled stream's WAL before `run()`
- [x] 6.2 Replay entries past the committed cursor into the stream in sequence order before reading new input
- [x] 6.3 Coordinate recovery ordering with `temporary` storage connect in `Stream::run` (resolve Open Question)
- [x] 6.4 Integration test: inject crash after read, restart, assert replay delivers the message exactly (no loss) and tolerates duplicates (at-least-once)

## 7. Documentation & contracts

- [x] 7.1 Document the at-least-once contract and duplicate-tolerance requirement for outputs (especially HTTP/Kafka)
- [x] 7.2 Add a `durability:` config example to `examples/`
- [x] 7.3 Note the single-node boundary (process crash only, not node loss) in docs
