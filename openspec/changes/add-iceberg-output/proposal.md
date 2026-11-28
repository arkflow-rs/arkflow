## Why

ArkFlow cannot land stream data in a lakehouse table: all fifteen registered outputs (`crates/arkflow-plugin/src/output/mod.rs:22-48`, wired at `:57-77`) target brokers, databases, HTTP endpoints, or vector stores, so a "Kafka → Iceberg" pipeline needs a second system between ArkFlow and the warehouse. The kernel already exposes the contract such a sink needs — one `write_batch` call is one acknowledgement range and therefore one transaction unit (`crates/arkflow-core/src/output/mod.rs:41-49`, `crates/arkflow-core/src/executor/task.rs:2970-3012`) — and `iceberg` 0.11 (released 2026-11-03) depends on the same `arrow ^58.4` line the workspace pins. Exactly-once delivery today covers only Kafka→Kafka (L2/L3) and SQL (`openspec/specs/exactly-once-output/spec.md:28`, `:73`), so a new sink has to state where it stands against that line instead of implying it.

One constraint shapes the whole design: the S3 WAL reclaims acknowledged segments by default (`openspec/specs/input-durability/spec.md:149`, `openspec/changes/add-s3-wal-retention/design.md:48`). Acknowledged data is not a staging area — whatever the sink accepts must already be committed to the table when `write_batch` returns.

## What Changes

- Add an `iceberg` output (type name `iceberg`): append-only writes to an existing Apache Iceberg table through a REST catalog, Parquet data files written with the table's own storage configuration.
- Gate it behind a new default-on cargo feature `connectors-iceberg` (dependencies `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal`), forwarded by the `arkflow` and `arkflow-server` crates like the other families.
- Transaction model: one `write_batch` call is one atomic Iceberg append. The output keeps no buffer, open file, or commit timer across calls; commit cadence and file size are set by the batches the upstream pipeline delivers, in practice the existing `batch` processor (`count` + `timeout_ms`).
- Optional exactly-once delivery (`exactly_once: true` + `writer_id`): a per-`(topic, partition)` Kafka offset watermark is stored atomically with each commit and replayed rows below it are dropped. Default remains at-least-once.
- Schema policy: the table must already exist; columns are matched by name with strict, checked type conversion; `__meta_*` columns are never written; no schema evolution.
- Partitioning: data files follow the table's partition spec; a spec the output cannot evaluate is rejected at `connect()`.
- Error reporting follows the connector contract (transient / deterministic / connection) and transient failures retry through the shared retry implementation.
- Component metadata, CI gates, example, README lists, and EN + zh-Hans documentation for the new output; a gated integration and crash-point test suite (MinIO + Lakekeeper).

## Non-goals

- No in-sink buffering or timed flush across `write_batch` calls, and no parallel commit pipeline — both would let the source acknowledgement outrun the table commit (or need a kernel-level asynchronous sink acknowledgement).
- No table creation, schema evolution, or partition-spec evolution by the output; tables are provisioned outside ArkFlow.
- No Hive, Glue, JDBC, or other catalog implementations — REST only.
- No custom `FileIO` adapter over the workspace's `object_store`; storage goes through the upstream OpenDAL-backed implementation.
- No delete, update, upsert, or merge semantics — append only.
- No exactly-once for sources without replayable Kafka positions, no cross-writer deduplication, and no ordering guarantees for topologies that are not an ordered single chain from the Kafka input to this output.
- No new Prometheus metrics (structured `tracing` only).
- No table maintenance (compaction, orphan-file cleanup, snapshot expiry) — documented as the operator's responsibility.
- No benchmark, chaos, or quickstart deliverables in this change; they follow as separate work once the output is mergeable.
- No change to `connector-recovery-contract`: its allowlist text already extends as components adopt the contract.

## Capabilities

### New Capabilities

- `iceberg-output`: Behavior of the `iceberg` output — the one-`write_batch`-one-transaction commit model, side-effect-free build, optional exactly-once by source-position watermark, schema and partition policy, and error classification.

### Modified Capabilities

- `build-feature-gating`: Add `connectors-iceberg` to the feature layer and to the components and dependencies a minimal build must exclude. The same edit records `connectors-amqp09`, which exists in all three `Cargo.toml` files and in `feature_hints.rs` but is missing from the spec's enumerations.
- `exactly-once-output`: Add the effectively-once boundary of the `iceberg` output — what the source-position watermark eliminates and the three preconditions under which it holds.

## Impact

- **Dependencies**: `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` (which brings `opendal` 0.58 and `reqsign` 3 for S3) in `[workspace.dependencies]`, optional in `arkflow-plugin`. `arrow`/`parquet` `^58.4` already match; `reqwest` 0.12 is already in the lock file through `object_store 0.13.2`. Adds a second pin on `arrow` 58 and `zstd` 0.13 for default builds, and increases default cold-build time and binary size (baseline delta to be measured and recorded).
- **Code**: new `crates/arkflow-plugin/src/output/iceberg.rs`; `crates/arkflow-plugin/src/output/mod.rs` (cfg gate + init step); `crates/arkflow-plugin/src/feature_hints.rs` (hint rows); `Cargo.toml` of the workspace and the three crates; `.github/workflows/rust.yml` (minimal-build `FORBIDDEN` list).
- **Tests**: new gated integration test (`ARKFLOW_REQUIRE_ICEBERG=1`); `crates/arkflow-plugin/tests/metadata_conformance.rs` (config row), `crates/arkflow-plugin/tests/connector_error_contract.rs` (allowlist + behavioral pin); regenerated `docs/reference/component-inventory.json` and `docs/static/config-schema.json`.
- **Docs**: `docs/docs/components/3-outputs/iceberg.md` and its zh-Hans counterpart, the feature table in `docs/docs/get-started/1-install.md` and its zh-Hans counterpart, output bullets in `README.md` and `README_zh.md`, a new example under `examples/` registered in `docs/reference/example-manifest.json`, and `CHANGELOG.md`.
- **Compatibility**: additive. Existing components and configurations are unchanged; a default build registers one additional output type; minimal builds are unchanged.
