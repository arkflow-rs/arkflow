## Context

Today every message in flight in ArkFlow lives only in memory:

```
input.read() → [flume channel] → [optional in-memory/window buffer] → processor → [flume channel] → output.write() → ack.ack()
```

The `Ack` plumbing is already correct in shape — `ack.ack()` fires only after a successful `output.write()` (`stream/mod.rs`). But nothing between `input.read()` and `ack.ack()` is durable:

- `memory` / `tumbling_window` / `sliding_window` / `session_window` / `join` buffers are all in-memory `VecDeque`s.
- The two flume channels in `Stream::run` are in-memory.
- `Ack::ack()` returns `()` — failures are invisible.

Source-side acks are only partially crash-safe:
- **Kafka**: `store_offset` on ack (`input/kafka.rs`); crash-safe only if `enable.auto.commit=false`.
- **MQTT / NATS / Pulsar**: manual ack on ack(); crash-safe if not auto-acked.
- **HTTP / File / Generate / Modbus / Redis / SQL / WebSocket**: `NoopAck` — non-replayable, no durability whatsoever.

So the crash-loss window is the entire gap between `input.read()` and `ack.ack()`, and it is fatal for non-replayable sources.

## Goals / Non-Goals

**Goals:**
- No data entering from any input is lost across a process crash (at-least-once).
- Crash recovery: on restart, replay any WAL entries past the committed cursor before streams resume.
- Durability is **orthogonal** to windowing — a stream can have both a window buffer and durable ingest.
- Reuse the existing ack-after-output plumbing; minimize changes to the `Stream` control flow.

**Non-Goals:**
- Exactly-once semantics (requires transactional/idempotent sinks — separate, much larger effort).
- Multi-node HA / replicated WAL (single-node crash recovery only).
- Full-pipeline persistent backbone (durability at every inter-stage hop).
- State checkpointing for stateful processors (window/join accumulated state).
- Splitting or restructuring the existing `Buffer` trait.

## Decisions

### D1 — Core shape (UNCHANGED): durability rides the ack path, it is not a buffer type

The WAL persists on `input.read()` and advances its cursor when the downstream ack fires. It reuses the existing "ack-after-output" pipe with no change to the `Stream` control flow. Durability is a property of the ingest boundary, conceptually separate from the windowing buffers (which transform timing, pop-on-read, and are in-memory by design).

```
input.read() → [ WAL ingest: persist msg+seq, fsync ] → buffer(window) → processor → output → [ commit: advance WAL cursor, then source ack ]
```

**Alternatives considered:**
- *Make durability a new `Buffer` type.* Rejected — `buffer.type` is single-select, forcing "window OR durable" to be mutually exclusive, and the Buffer trait semantics (pop-on-read) conflict with ack-tied cursor advancement. Conflating windowing and durability yields a leaky abstraction.

### D2 — BREAKING: `Ack::ack()` becomes fallible

```rust
// before
async fn ack(&self);
// after
async fn ack(&self) -> Result<(), Error>;
```

**Why.** A WAL whose cursor-advance / source-commit can fail silently cannot be called crash-safe. Today `ack()` returns `()`, so a disk-full or store error during cursor advance is invisible: either it is swallowed and the source still commits (loss risk on the next crash), or it is withheld but the stream keeps reading until the WAL grows unbounded with no signal. A fallible ack surfaces these errors so the stream can apply backpressure or stop.

**Nuance (honest).** A failed cursor advance is *not* data loss — it is *duplicate* data (the un-advanced entry replays on the next restart), which already falls inside the at-least-once contract. So fallible ack's primary value is **observability + backpressure**, not loss-prevention. It is still required: a silent-failure durability layer is unacceptable, and backpressure on WAL growth is the only thing keeping a cursor-advance failure from becoming an unbounded resource leak.

**Migration.** Every `Ack` implementor must change signature: `KafkaAck`, `MQTT`/`NATS`/`Pulsar` acks, plus `NoopAck` (returns `Ok(())`) and `VecAck` (propagates first error). Every `ack.ack().await` call site in `Stream` (e.g. `do_processor`, `output`) must handle the `Result`.

### D3 — BREAKING: the WAL is a first-class ingest stage owned by `Stream`, not an `Input` decorator

Instead of a `WalInput` wrapping an `Input`, the `Stream` owns the WAL directly. Config gains a top-level per-stream `durability:` section; the `Stream` struct and its run loop gain an explicit ingest stage.

```
stream:
  durability:        # NEW
    enabled: true
    path: ./data/wal/<stream>
    sync: group-commit   # per-entry | group-commit | periodic
    flush_interval: 50ms
  input: ...
  pipeline: ...
  output: ...
```

**Why.** A decorator was a workaround to avoid touching `Stream`. Owning the WAL in the stream is more honest about what is happening, removes a layer of indirection, and lets recovery be coordinated at the `Engine`/`Stream` level. Notably this does **not** require changing the `Input` trait — the stream takes `(msg, ack)` from `input.read()`, runs it through its WAL, and produces a `WalAck` wrapping the source ack.

### D4 — Storage engine: embedded transactional store (`redb`)

Persist each entry as `(seq → Arrow IPC bytes)`; the committed cursor is the highest contiguous acked `seq`. `redb` is pure-Rust, transactional, and ACID — it handles the durability primitives we do not want to hand-roll.

**Alternatives considered:**
- *Custom append-only segment log + cursor.* Rejected for v1 — hand-rolling correct durability is a known minefield (fsync the file *and* the directory on create/rename; recover gracefully from a corrupted half-written tail via checksums + length-prefix; durably persist the cursor itself; segment rotation/reclaim). Higher risk, more code, for no v1 benefit.
- *`sled` / `rocksdb` / SQLite.* Viable; `redb` chosen for the smallest pure-Rust footprint and transactional API that maps cleanly to "advance cursor atomically with reclaim." Open to revisiting (see Open Questions).

### D5 — fsync policy: group-commit by default

Per-entry fsync would cap throughput at single-digit ms latency. Default to **group-commit** on the natural `MessageBatch` granularity; expose `sync` as configurable (`per-entry` | `group-commit` | `periodic`). This is the single real throughput-vs-durability trade-off and is made explicit.

**Semantics** (important):
- `per-entry` — every `append()` commits a transaction before returning. Fully durable; fsync-bound.
- `group-commit` and `periodic` — `append()` stages the entry in an in-memory queue and returns immediately (NOT durable yet). A background flusher commits batches (group-commit: as soon as pending is non-empty; periodic: on a fixed interval). A crash before the next flush loses staged entries — the documented **small loss window** in exchange for much higher throughput.
- This means `group-commit`/`periodic` accept a bounded loss window for speed; `per-entry` accepts the fsync cost for full durability. The tradeoff is real and configurable.

**Measured throughput** (`cargo test -p arkflow-core --release wal::tests::bench_append_throughput -- --ignored`, 5000 sequential appends of a small batch):

| Policy | appends/s | µs/append |
|---|---|---|
| `per-entry` | ~184 | ~5445 |
| `group-commit` | ~19,450 | ~51 |
| `periodic(1ms)` | ~47,569 | ~21 |

`group-commit` is ~106× faster than `per-entry` by amortizing fsync across staged entries.

### D6 — Semantics: at-least-once, duplicate-tolerance as an output contract

Recovery replays in-flight entries → outputs may receive duplicates. This is stated as a contract: outputs MUST tolerate duplicates (UPSERT-friendly sinks like SQL/InfluxDB are natural; HTTP/Kafka outputs must document possible duplicates). Exactly-once is explicitly a Non-Goal.

### D7 — Phase 1 stores body uniformly for all inputs

Every enabled input persists the full message body, including replayable sources (Kafka/MQTT/...). This accepts a known double-write for replayable sources in exchange for a uniform mental model and fewer edge cases. Per-source optimization (offset-only WAL for replayable sources) is deferred.

### Staged delivery

- **Phase 0 (no breaking changes):** Audit and correct source-side ack gating — Kafka/MQTT/NATS/Pulsar default to manual commit; ack fires only after output success. Alone, this makes replayable-source → durable-output crash-safe *today*.
- **Phase 1 (the breaking changes):** `WalInput`-free first-class ingest stage (D3), fallible ack (D2), `redb` body WAL (D4/D7), recovery replay, group-commit (D5).

## Risks / Trade-offs

- **fsync throughput cliff** → group-commit default + configurable sync policy (D5). Benchmark before/after.
- **New dependency (`redb`)** → pure-Rust, transactional; validate its fsync guarantees on the target platforms. Fallback to a custom segment log if unacceptable (D4).
- **Duplicate delivery after recovery** → documented as the at-least-once contract; outputs MUST be duplicate-tolerant (D6).
- **WAL unbounded growth on cursor-advance failure** → fallible ack (D2) surfaces the error so the stream can backpressure/stop instead of leaking.
- **Corrupted WAL tail after mid-write crash** → `redb` transactions handle atomicity; if we ever move to a custom log, checksums + length-prefix on recovery are mandatory.
- **Single-node boundary** → this design does not survive node loss, only process crash. HA is a Non-Goal.
- **Behavioral change for existing configs** → durability is opt-in (`enabled: true`); streams without `durability:` keep today's in-memory behavior, so the blast radius is limited to the ack signature break (D2).

## Migration Plan

1. **Phase 0 first** — no public breaks; ship source ack-gating fixes. Safe to release independently.
2. **D2 (fallible ack)** — update `Ack` trait and all implementors in one coordinated change (core + plugins). Update all `ack.ack().await` call sites in `Stream` to handle `Result`.
3. **D3 + D4 + D5 + D7** — add the `durability:` config, the stream-owned WAL ingest stage, `redb` persistence, and group-commit.
4. **Recovery** — Engine startup phase: open WAL, replay entries past the committed cursor into the stream before `run()`.
5. **Rollback** — durability is opt-in per stream; disabling `durability:` reverts to today's behavior. The only non-opt-out break is the ack signature, which is compile-time (no silent runtime regression).

## Open Questions

- **redb vs alternatives** — final storage-engine call once we validate `redb` fsync behavior and Arrow IPC round-trip cost. (D4)
- **Default fsync policy** — confirm `group-commit` with a representative flush interval as the shipped default. (D5)
- **WAL placement** — per-stream subdirectory under a configurable root; naming/clash rules for unnamed streams.
- **Uniform source classification** — do we need an explicit `replayable: bool` marker on inputs for future offset-only optimization (D7), or infer it? Deferred, but worth reserving.
- **Recovery ordering vs. temporary storage** — does WAL replay need to coordinate with `temporary` storage connect/ordering in `Stream::run`?
