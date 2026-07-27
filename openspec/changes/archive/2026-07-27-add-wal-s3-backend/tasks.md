## 1. `WalStore` trait + local backend refactor (D1, D9)

- [x] 1.1 Define the `WalStore` trait (`append_batch` / `advance_cursor` / `read_after_cursor` / `cursor` / `close`) in `arkflow-core/src/wal/`
- [x] 1.2 Refactor `Wal` into a staging/flushing coordinator over `Arc<dyn WalStore>`; move the existing `redb` logic into a `RedbStore` that implements the trait
- [x] 1.3 Add a `WalStoreBuilder` trait + `register_wal_store_builder()` registry (`lazy_static` + `RwLock<HashMap>`, matching the existing component pattern)
- [x] 1.4 Register the `local` (`redb`) builder; route `StreamConfig::build` / `Wal::open` through the registry by `backend` name
- [x] 1.5 `WalConfig` gains `backend` (default `local`); all existing WAL tests stay green unchanged

## 2. S3 / object-store store implementation (D3, D4)

- [x] 2.1 Implement `S3Store` in `arkflow-plugin` over `object_store::aws::AmazonS3Builder`, reusing the store config + dispatch already in `input/file.rs`
- [x] 2.2 Segment object format: `[seq(u64 BE) | len(u32 BE) | payload | crc32(u32 BE)] × N`; reuse the existing `serialize()` / `deserialize()`
- [x] 2.3 Segment flusher: seal + `PUT` a new segment on `max_entries` / `max_bytes` / `flush_interval`
- [x] 2.4 Manifest object: `{ node_id, stream_id, cursor, max_sealed_seq, active_segment, sealed_segments }`; atomic overwrite `PUT` on rotation / cursor-flush / truncation
- [x] 2.5 Object key layout: `{root}/{node_id}/{stream_id}/segments/NNNNNNNN.wal` + `manifest.json`
- [x] 2.6 Register the `s3` builder in `arkflow-plugin`'s `init()`

## 3. Per-node isolation (D2)

- [x] 3.1 `node_id` config field (explicit string; required when `backend: s3`)
- [x] 3.2 Resolve `stream_id` (explicit `name` vs config hash — resolve the Open Question)
- [x] 3.3 Document the `node_id` stability contract for K8s (ConfigMap / env / StatefulSet ordinal) and the no-uniqueness-enforcement stance

## 4. Defensive recovery (D5)

- [x] 4.1 Recovery: GET manifest → LIST segments (union fallback) → replay `seq > cursor` in ascending order
- [x] 4.2 Active-segment torn-tail handling: verify `crc32` per entry, truncate at the last good entry
- [x] 4.3 Tests: clean restart replays nothing; mid-`PUT` crash → torn tail truncated; manifest-lagging-but-segment-present → LIST fallback replays it
- [ ] 4.4 Test against an S3-compatible implementation (MinIO in CI) to exercise consistency variance

## 5. Batched cursor + truncation (D6, D7)

- [x] 5.1 Batched manifest cursor flush (`cursor_flush_entries` / `cursor_flush_interval`)
- [x] 5.2 Segment truncation: `DELETE` sealed segments fully behind the cursor; best-effort, never blocks ingestion
- [ ] 5.3 Tests: truncation removes reclaimed segments; a stale/missing segment is ignored on recovery

## 6. Config validation + docs (D8)

- [x] 6.1 Reject `sync: per_entry` when `backend: s3` at config-load time
- [x] 6.2 Add `examples/durability_example_s3.yaml`
- [x] 6.3 Docs: node-loss vs process-crash durability boundary, the loss window, `node_id` stability, and the at-least-once duplication introduced by batched cursor flushing

## 7. End-to-end

- [ ] 7.1 E2E: ingest → kill pod → restart with the same `node_id` → recovery replays unacked entries, no loss of flushed entries
- [ ] 7.2 E2E: two nodes, same bucket, different `node_id` → fully isolated, no cross-contamination
- [ ] 7.3 Benchmark object-store append throughput vs loss-window settings
