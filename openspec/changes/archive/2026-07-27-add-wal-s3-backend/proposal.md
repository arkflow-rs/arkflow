## Why

`add-input-durability` shipped a durable ingest WAL that persists to a local embedded `redb` file and explicitly scoped out multi-node / remote storage (Non-Goal: "Multi-node HA / replicated WAL"; risk: "Single-node boundary → does not survive node loss, only process crash"). In Kubernetes — the expected deployment target — a pod's local storage (`emptyDir`) is destroyed on every restart (rolling update, node drain, OOM-kill, scheduler migration). A local-disk WAL there means either losing all in-flight data on each pod restart, or bolting on a PVC (cost, `StorageClass` dependency, cross-AZ latency, `StatefulSet`-only stable binding, harder scaling). Neither is attractive.

We need the WAL to live in **S3-compatible object storage** so a killed or migrated pod recovers its in-flight data on restart, and we need **multiple arkflow nodes sharing one bucket to be isolated** from each other. The recovery contract (ack-gated cursor, replay-after-cursor, at-least-once) from `input-durability` is preserved unchanged; only the storage medium and the multi-node namespace change.

## What Changes

- **NEW**: Pluggable WAL storage backend — opt-in `backend: s3` writes WAL segments + a manifest to S3-compatible object storage; `backend: local` (default) preserves today's `redb` behavior with zero change for existing users.
- **NEW**: Per-node namespace isolation — an explicit `node_id` plus a stream id prefix the object key namespace, so multiple nodes sharing a bucket never collide.
- **NEW**: Object-store WAL survives node/pod loss — already-flushed segment objects remain recoverable after the node disappears. This lifts the durability boundary from "process crash" to "node loss."
- **NEW**: Segment-based batching with a bounded, configurable loss window (segment flush triggers).
- **NEW**: Defensive recovery — manifest + LIST fallback + per-entry checksum (handles torn tails and manifest lag).
- **NEW**: Segment truncation/reclaim (the `redb` backend has none; object storage bills per object).
- **NEW**: `WalStore` trait + builder registry in `arkflow-core`; the `s3` builder lives in `arkflow-plugin`, reusing the existing `object_store` dependency and the store-dispatch logic already used by `input/file.rs`. No new dependency is introduced.
- **NEW (validation)**: The `per-entry` sync policy is rejected for the object-store backend (one `PUT` per message is not viable).

## Capabilities

### Modified Capabilities

- `input-durability`: extends durable ingestion to support an opt-in object-storage backend with per-node isolation, segment-based durability, and node-loss recovery, while keeping the existing local backend and the ack-gated recovery contract unchanged.

## Impact

- **`arkflow-core`**: extract a `WalStore` trait + a `WalStoreBuilder` registry; refactor `Wal` from a `redb` owner into a staging/flushing coordinator over `Arc<dyn WalStore>`; ship the `local` (`redb`) builder; route `StreamConfig::build` / `Wal::open` through the registry by `backend` name. `WalConfig` gains `backend`, and (for `s3`) `node_id`, `stream_id`, an `object_store` config block, and `segment` tuning.
- **`arkflow-plugin`**: new `s3` `WalStore` builder (segments, manifest, batched cursor, truncation, defensive recovery) implemented over `object_store`; registered in `init()`. Reuses the existing `object_store 0.12` dependency already enabled with `aws`/`azure`/`gcp` features — **no new dependency**.
- **Behavioral**: the object-store backend accepts a bounded loss window on node loss (configurable via segment triggers) in exchange for surviving node loss, and additional at-least-once duplication from batched cursor flushing. `per-entry` sync is rejected for `s3`.
- **Out of scope**: HA failover / replicated WAL (each node recovers only its own WAL; no distributed lock, leader election, or shared cursor), exactly-once semantics, GCS/Azure builders (follow-up — the crate already supports them), state checkpointing for stateful processors.
