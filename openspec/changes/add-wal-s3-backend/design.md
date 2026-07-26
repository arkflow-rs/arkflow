## Context

The durable ingest WAL shipped by `add-input-durability` is a single-node artifact: it persists to an embedded `redb` database on the local filesystem (`Database::create(path.join("wal.redb"))`, an mmap'd B-tree). That design explicitly listed **"Multi-node HA / replicated WAL"** as a Non-Goal and stated the risk plainly: *"Single-node boundary → this design does not survive node loss, only process crash."* `examples/durability_example.yaml` repeats this at the top: *"Single-node only ... the WAL lives on local disk."*

This matters most in Kubernetes, where the most likely deployment target lives. A pod's local storage (`emptyDir`) is destroyed on every restart — rolling update, node drain, OOM-kill, scheduler migration. Persisting a WAL to local disk in that world means either accepting total loss of in-flight data on every pod restart, or bolting on a PersistentVolumeClaim (cost, `StorageClass` dependency, cross-AZ latency, `StatefulSet`-only stable binding, harder scaling). Neither is attractive.

The ask is: **let the WAL live in S3 (object storage) instead of local disk, and keep multiple arkflow nodes from colliding when they share one bucket.** The recovery contract (replay entries past the committed cursor → at-least-once) stays exactly as `add-input-durability` defined it; only the storage medium and the multi-node namespace change.

## Goals / Non-Goals

**Goals:**
- A WAL storage backend that writes to S3-compatible object storage, so a pod can be killed/migrated and its in-flight data is recoverable from object storage on the next start.
- Multiple arkflow nodes sharing one bucket are isolated by node identity — they never read or overwrite each other's WAL.
- The recovery semantics from `add-input-durability` (ack-gated cursor, replay-after-cursor, at-least-once) are preserved unchanged.
- The existing local (`redb`) backend stays the default; S3 is opt-in per stream.

**Non-Goals:**
- **HA failover / replicated WAL.** A dead node's WAL is *not* taken over by another live node. Each node only ever recovers its own WAL from object storage. No distributed lock, leader election, fencing token, or shared-cursor coordination is introduced. (This is the same boundary as before, just lifted from "process crash" to "node loss" via durable remote storage.)
- Exactly-once semantics (still at-least-once; unchanged).
- A generic cross-component object-store abstraction beyond the WAL.
- State checkpointing for stateful processors (window/join accumulated state).

## Decisions

### D1 — Extract a `WalStore` trait; `Wal` becomes a coordinator over a pluggable store

Today `Wal` is concrete and hard-wired to `redb` — every method (`open`/`append`/`advance`/`read_after_cursor`/`cursor`) reaches directly into `redb::Database`. To put a second medium underneath it we extract the persistence surface into a trait and keep `Wal` as the staging/flushing coordinator:

```rust
#[async_trait]
pub trait WalStore: Send + Sync {
    async fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error>;
    async fn advance_cursor(&self, seq: u64) -> Result<(), Error>;
    fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error>;
    fn cursor(&self) -> u64;
    async fn close(&self) -> Result<(), Error>;
}
```

`Wal` keeps `next_seq`, the in-memory `pending` queue, the `Notify`-driven flusher, and the `SyncPolicy` staging logic — exactly the group-commit machinery it already has. The only change is that `flush_pending` calls `store.append_batch(...)` instead of opening a `redb` transaction, and `advance` calls `store.advance_cursor(...)`.

**Why at this level.** The trait is shaped by the *operations the coordinator needs* (append a batch, advance a watermark, read the tail past the watermark), not by the storage engine's primitives. A KV `get/put` trait would fit `redb` but not S3 (no cheap point writes, no random write, expensive list); a "log of immutable segments" fits S3 and can be emulated over `redb`. The operation-level trait lets both backends be honest about their shape.

**Alternatives considered:**
- *Don't abstract; fork the WAL into `Wal` and `S3Wal`.* Rejected — duplicates the staging/flusher/sync-policy logic, which is medium-agnostic and the bulk of the tricky code.
- *KV-level trait (`get/put/range`).* Rejected — forces S3 to fake random writes it can't do cheaply.

### D2 — Per-node namespace isolation via an explicit `node_id`

Object storage is shared, so two nodes pointing at the same bucket must not collide. Isolation is purely a **key-prefix** concern:

```
{root_prefix}/{node_id}/{stream_id}/segments/NNNNNNNN.wal
{root_prefix}/{node_id}/{stream_id}/manifest.json
```

`node_id` is an **explicit configuration value** (no auto-generation, no hostname derivation). Rationale:
- **Predictable and stable across restarts** — recovery requires a node to find *its own* previous WAL, so the id must survive pod restarts. In K8s this is an ops contract: inject a fixed value per logical node (ConfigMap / env / StatefulSet ordinal). An auto-generated UUID would have to be persisted somewhere, and the obvious place (local disk) is exactly what we are trying to avoid.
- **No uniqueness enforcement.** Because there is no HA failover and no shared cursor, two nodes accidentally sharing an id simply corrupt each other's WAL — a misconfiguration with an obvious symptom, no different in kind from today's "two streams with the same `durability.path`." Enforcing uniqueness would require coordination (a lock/lease), which is a Non-Goal.

`stream_id` disambiguates multiple streams within a node. Today streams are distinguished only by `durability.path`; for object storage we need a stable identifier (see Open Questions for the exact source — proposed: an explicit optional `name` on the stream, falling back to a hash of the config).

**Alternative considered:** *Auto-generate a UUID and persist it to object storage.* Rejected — bootstrapping is circular (the id selects the prefix that would store the id), and a stolen/reused id is worse than a configured one because it is invisible.

### D3 — Object-store backend is pure remote storage; no local buffer/cache

`append()` stages the entry in the in-memory `pending` queue and returns the sequence immediately (same as today's `group-commit`); the background flusher batches entries into an immutable segment object and `PUT`s it to object storage. **There is no intermediate local file.**

This is forced by the K8s deployment target: a local buffer + async-upload design would depend on the very ephemeral local disk we are trying to eliminate, so it would survive process crashes but **lose data on node/pod loss** — i.e. it would not actually solve the problem. A node/pod that disappears takes only its in-memory `pending` queue with it; everything already flushed to a segment object is safe.

```
append() ──► in-memory pending ──[flusher: batch]──► PUT segment object (durable)
                 ▲ crash window: entries not yet flushed to an object are lost on loss
```

The trade-off — a crash/loss window sized by the flush trigger — is explicit and configurable (D4). This is the same loss-window semantics as the existing `group-commit`/`periodic` policies, just with a slower durable medium.

**Alternative considered:** *Local `redb` buffer + background upload to S3.* Rejected for the object-store backend (above). The local `redb` backend remains available for deployments with a reliable local volume that want maximum hot-path throughput and accept "process-crash-only" durability.

### D4 — Object layout: immutable segment objects + a small manifest object

```
{root_prefix}/{node_id}/{stream_id}/
    segments/
        0000000001.wal     ← sealed: [seq_a..seq_b], fully written
        0000000002.wal     ← sealed
        0000000003.wal     ← active: being appended, may be un-sealed/torn
    manifest.json          ← { node_id, stream_id, cursor, max_sealed_seq,
                              active_segment, sealed_segments: [...] }
```

**Segment object format** (a sequence of length-prefixed, checksummed entries):
```
[ seq(u64 BE) | payload_len(u32 BE) | payload(serialize() bytes) | crc32(u32 BE) ] × N
```
`serialize()` is reused verbatim from today's WAL (length-prefixed optional input name + Arrow IPC stream of the record batch, preserving schema and `__meta_*` columns). The trailing `crc32` covers `payload` and lets recovery detect a **torn tail** — a segment object whose last entry was truncated by a crash mid-`PUT` or a network interruption.

**Segment flush triggers** (any one seals the active segment and `PUT`s a new one):
- `segment.max_entries` (default `1000`)
- `segment.max_bytes` (default `1 MiB`)
- `segment.flush_interval` (default `1s`)

These three are the single throughput-vs-loss-window knob. Defaults target a ~1s loss window; operators wanting smaller windows lower the triggers at the cost of more `PUT` calls (each tens of ms).

**Manifest object** records the watermark and the segment index. It is rewritten (overwritten `PUT`) whenever the cursor advances past the batch threshold (D6) or a segment is sealed/rotated/truncated (D7). Object-store `PUT` is atomic per-object, so a reader always sees either the old or the new manifest, never a torn one.

### D5 — Recovery is consistent under partial writes: manifest + LIST fallback + checksum

Recovery cannot trust the manifest alone, because a crash can leave the store in any of: manifest updated but a segment `PUT` in flight; segment `PUT` complete but manifest not yet updated; or an active segment with a torn tail. The recovery procedure is defensive at every step:

```
1. GET manifest.json           → cursor, sealed_segments, active_segment, max_sealed_seq
2. LIST segments/              → actual objects on the store (fallback)
   segment set = manifest ∪ LIST    (trust the union; manifest may lag)
3. for the active segment (may be un-sealed):
       iterate entries, verify each crc32; on a bad crc, truncate at the last good entry
4. replay every entry with seq > cursor, across (sealed ∪ active), in ascending order
```

- **LIST fallback** handles "segment `PUT`'d but manifest not updated" — the segment is real and must be replayed.
- **crc truncation** handles the torn tail — a half-written final entry is discarded, not replayed as garbage.
- **Union, not intersection** — a segment listed in the manifest but missing from `LIST` (deleted between manifest write and recovery, e.g. by truncation) is skipped; a segment in `LIST` but not the manifest is included.

This mirrors the "checksums + length-prefix on recovery are mandatory" note from `add-input-durability`'s custom-log alternative (its design D4), applied to object storage. S3 itself is read-after-write strongly consistent since 2020, but S3-compatible implementations (MinIO, Alibaba OSS, Ceph) are not uniformly so — the LIST fallback is what makes recovery correct across them.

### D6 — Cursor (watermark) updates are batched, accepting bounded replay duplication

`advance()` is called once per acked message; `PUT`-ing the manifest on every ack would be prohibitively expensive on object storage. The cursor is therefore advanced in memory immediately and flushed to the manifest object in batches (every `cursor_flush_entries` acks, default `1000`, or every `cursor_flush_interval`, default `1s`).

Cost: on loss, the persisted manifest cursor may lag the true high-water mark by up to one flush interval, so recovery replays some entries that were already acked. This is **additional at-least-once duplication**, squarely inside the existing contract (outputs MUST tolerate duplicates) — not a correctness regression.

**Alternative considered:** *A separate cursor object updated independently of segment rotation.* Equivalent in effect; folding the cursor into the manifest keeps the number of objects small and the write path single-target. Revisit if cursor-update frequency diverges far from segment-rotation frequency.

### D7 — Segment truncation/reclaim (new; the `redb` backend has none)

Today the `redb` WAL grows monotonically — there is no reclaim, and `add-input-durability` lists "WAL unbounded growth on cursor-advance failure" as a known risk. Object storage bills per object-month, so unbounded growth is not just a local-disk concern anymore. The object-store backend **shall truncate**:

- When the manifest cursor advances past the last seq of a sealed segment, that segment object is `DELETE`'d and removed from `sealed_segments` in the next manifest write.
- Truncation is best-effort and never blocks the ingest path: a failed `DELETE` leaves a stale segment that is simply ignored on recovery (its seqs are all `<= cursor`).

The `redb` backend is left as-is for this change (its growth behavior is pre-existing and out of scope here); only the object-store backend gains reclaim.

### D8 — `per-entry` sync policy is rejected for the object-store backend

`per-entry` semantics ("commit/fsync before `append()` returns") would mean one `PUT` per message on object storage — throughput collapses to single-digit messages/second given per-request latency. The object-store backend **rejects `sync: per_entry` at config-load time** with an error, and only honors `group-commit` / `periodic` (which map naturally onto the segment flusher). The local `redb` backend keeps all three policies.

### D9 — Dependency placement: trait + registry in `arkflow-core`; S3 impl in `arkflow-plugin`

`Wal` lives in `arkflow-core` and is held directly by `Stream`. Two constraints shape where the S3 implementation lives: (a) `arkflow-core` must not gain a hard dependency on `object_store` (it is currently clean of it; `object_store` lives in `arkflow-plugin`); (b) the construction path (`StreamConfig::build`) is in `arkflow-core`, so it must be able to build the store without naming a plugin type.

Resolution, consistent with arkflow's existing plugin-registration architecture:
- `arkflow-core` defines `WalStore`, `WalStoreBuilder`, and a `register_wal_store_builder()` registry (same `lazy_static` + `RwLock<HashMap>` pattern as input/output/processor/buffer/codec). `Wal::open` takes the resolved `Arc<dyn WalStore>`; `StreamConfig::build` looks up the builder by `backend` name.
- `arkflow-core` ships the `redb` (default `local`) builder.
- `arkflow-plugin` ships the `s3` builder, implemented over `object_store::aws::AmazonS3Builder` (and the same `Store` enum dispatch already used by `input/file.rs`), registered in its `init()`. `arkflow-plugin` already depends on `object_store 0.12` with `aws`/`azure`/`gcp` features, so no new dependency is introduced.

This keeps `arkflow-core` free of `object_store`, makes GCS/Azure/HDFS backends a future plugin-only addition, and matches the registration pattern every other component type already uses.

**Alternatives considered:**
- *`S3Store` in `arkflow-core` behind a `wal-s3` cargo feature.* Simpler (no registry), but moves an `object_store` dependency (even if feature-gated) into core and breaks the "core stays storage-backend-free" layering. Rejected in favor of the registry to stay consistent with the rest of the codebase.
- *Two concrete types `Wal`/`S3Wal` with no trait.* Rejected (see D1).

## Risks / Trade-offs

- **Hot-path latency.** The durable medium is now object storage (tens of ms per `PUT`) instead of local `fsync` (µs–ms). Mitigated by segment batching (D4); the in-memory staging means `append()` itself stays fast, only the loss window grows. Operators needing sub-second windows pay for more `PUT`s.
- **Loss window on node loss.** Unlike the local backend (loss window = not-yet-`fsync`'d entries, microseconds under `per-entry`), the object-store backend loses up to one flush interval of entries on pod disappearance. This is the explicit trade for surviving node loss at all. Documented in the spec and configurable via the segment triggers.
- **`node_id` misuse.** Two nodes sharing an id corrupt each other's WAL. Not enforced (D2) — it is an ops contract, like unique `durability.path` today. Documented prominently.
- **S3-compatible consistency variance.** Mitigated by manifest+LIST+checksum recovery (D5); cannot be "fixed," only defended against.
- **Manifest rewrite cost.** Folding cursor + segment index into one manifest means every cursor-flush rewrites the whole manifest object (small JSON). Acceptable given batching (D6); revisit if manifest grows large (many sealed segments — bounded by truncation, D7).
- **More moving parts than the local backend** (trait, registry, segmenter, manifest, recovery). Justified by the K8s/node-loss goal; the local backend path is unchanged for users who do not opt in.

## Migration Plan

1. **No breaking config change for existing users.** Streams with `durability:` and no `backend` field default to `local` and behave exactly as today (`redb`, single-node, process-crash recovery).
2. **Extract `WalStore` + `redb` builder in core** (D1, D9) — pure refactor; all existing WAL tests stay green unchanged.
3. **Add the registry** (`register_wal_store_builder`) and route `StreamConfig::build` through it; `local` is the only registered builder so far.
4. **Implement the `s3` builder in `arkflow-plugin`** (segments, manifest, batched cursor, truncation — D4/D6/D7) and register it in `init()`.
5. **Recovery** (D5) and **`per-entry` rejection** (D8).
6. **Docs + example**: a `durability_example_s3.yaml`, and a prominent note on `node_id` stability in K8s.
7. **Rollback**: setting `backend: local` (or omitting `backend`) reverts entirely to today's behavior. No runtime regression for non-S3 users.

## Open Questions

- **`stream_id` source.** Today streams are identified only by `durability.path`. For object storage we need a stable per-stream id in the key prefix. Proposed: an explicit optional `name` field on the stream (or on the `durability` section), falling back to a deterministic hash of the config if absent. Needs a decision — does `StreamConfig` gain a top-level `name`?
- **Default loss window.** Is ~1s (`flush_interval: 1s`, `max_entries: 1000`) the right shipped default for the object-store backend, or should it be tighter/looser? Confirm with a representative workload.
- **`stream_id` collision across redeployments.** If `stream_id` is derived from config and the config is edited, a node could fail to find its prior WAL. Decide whether `name` must be explicitly set when `backend: s3` (fail closed) rather than silently hashing.
- **Object-store auth/endpoint config shape.** Reuse the exact `object_store` config block already parsed by `input/file.rs` (so users share one schema), or define a WAL-specific subset? Leaning toward reuse.
- **GCS/Azure in the same change or follow-up?** The `object_store` crate already supports them and `arkflow-plugin` already enables the features; the `s3` builder is the only one this change delivers. Decide whether to register `gcs`/`azure` builders now or as separate changes.
