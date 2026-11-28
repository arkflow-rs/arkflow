## Context

ArkFlow has no lakehouse sink. The kernel contract that governs sinks is narrow and strict:

- `Output::write_batch(&[MessageBatchRef])` is one transaction unit (`crates/arkflow-core/src/output/mod.rs:41-49`). The output worker awaits the call under a five-minute sink write timeout and settles the source acknowledgements only after it returns `Ok` (`crates/arkflow-core/src/executor/task.rs:64`, `:2970-3012`).
- The sink receives only the batches. `Ack` exposes `ack`/`undo`/`abort` and carries no batch identity (`crates/arkflow-core/src/input/mod.rs:58`).
- The S3 WAL keeps only the unacknowledged replay window: under the default retention, acknowledged sealed segments are reclaimed immediately (`openspec/changes/add-s3-wal-retention/design.md`, D1).
- The Kafka input never waits to fill a batch (`openspec/specs/kafka-input-batching/spec.md`), so at low traffic a batch reaching the sink can hold a single row.
- The `batch` processor buffers deliveries by `count` and `timeout_ms` and holds their acknowledgements until the merged batch is written downstream (`crates/arkflow-plugin/src/processor/batch.rs:163-221`, `message-acknowledgment`).
- The SQL processor treats `__meta_*` columns as ordinary columns: a projection that does not select them drops them.
- Output errors follow the transient / deterministic / connection discipline and are pinned by a file-keyed CI test whose `rust_files` helper is a non-recursive `read_dir` (`crates/arkflow-plugin/tests/connector_error_contract.rs:82`).
- Configuration strings may reference external values (`${env:VAR}`, `${file:/path}`, `${secret:...}`); references are resolved once when the document is materialized (`crates/arkflow-core/src/config.rs:465-473`), so any field of the new output inherits this without extra code.

External facts (crates.io and docs.rs, 2026-11-28):

- `iceberg` 0.11.0 is core-only and has no cargo features. The REST catalog lives in `iceberg-catalog-rest` 0.11.0 and storage in `iceberg-storage-opendal` 0.11.0 (`opendal` 0.58; its `opendal-s3` feature adds `reqsign-aws-v4` and `reqsign-core` 3).
- `iceberg` depends on `arrow-* ^58.4` and `parquet ^58.4`, matching the workspace (`58.4.2`), and on `reqwest ^0.12.12`, which is already in the lock file through `object_store 0.13.2`.
- `FastAppendAction` exposes `add_data_files`, `set_commit_uuid`, `set_snapshot_properties(HashMap<String, String>)`, and `with_check_duplicate`; `Transaction::commit(&dyn Catalog)` commits.
- Schema and partition helpers exist: `schema_to_arrow_schema`, `RecordBatchProjector`, `RecordBatchPartitionSplitter`, `FanoutWriter`.

The 0.11 API is a generation newer than most published examples, so the decisions that depend on it are marked spike-gated and resolved in Phase 1 (see Open Questions).

## Goals / Non-Goals

**Goals:**

- An append-only Iceberg sink whose failure semantics fit the kernel's acknowledgement contract without kernel changes.
- Exactly-once delivery for Kafka sources across crash and replay, opt-in, with an honest and documented boundary.
- A default-on feature that changes no other component and leaves minimal builds untouched.
- Every existing CI gate covering the new component.

**Non-Goals:**

- In-sink buffering, timed flush, or a parallel commit pipeline.
- Table creation, schema evolution, partition-spec evolution.
- Catalogs other than REST; a custom storage adapter over the workspace's `object_store`.
- Delete / update / upsert / merge semantics.
- Exactly-once for sources without replayable Kafka positions, cross-writer deduplication, or ordering on non-single-chain topologies.
- Prometheus metrics, table maintenance, benchmarks, chaos suite, quickstart package.

## Decisions

### D1. One `write_batch` is one Iceberg transaction; the sink is stateless across calls

```
Kafka ─▶ MessageBatch (rows carry __meta_partition / __meta_offset / __meta_ext)
            │
            ▼   durability.backend = object_store   (S3 WAL keeps only the unacknowledged window)
        [processors]   e.g. sql — the SELECT must keep the __meta_* columns
            │
            ▼
        batch { count, timeout_ms }                 (commit cadence; acknowledgements held while buffering)
            │
            ▼
        iceberg.write_batch(batches)
            │   filter ─▶ encode Parquet ─▶ upload ─▶ FastAppend (+ watermark)
            └─Ok─▶ kernel settles acks ─▶ WAL cursor advances + Kafka offsets commit
```

Every input batch of the call is concatenated, encoded, uploaded, and committed with a single fast-append snapshot. `Ok` is returned only after the catalog accepts the commit. A call that carries no rows, or whose rows are all filtered as already committed, returns `Ok` without creating a snapshot — exactly one append per call with new rows, zero appends otherwise. The output holds no row, open file, or timer between calls, so `close()` has nothing to flush and a crash before `Ok` leaves nothing acknowledged.

Alternatives considered:

- *In-sink timed/size buffering with an early `Ok`*: the acknowledgement outruns the table commit, and under the default WAL retention the segments are reclaimed — a crash loses the buffered rows. Rejected.
- *In-sink buffering with a delayed `Ok`*: the kernel awaits the call in the chain loop, so no further rows arrive while the sink waits. Impossible without an asynchronous sink acknowledgement in the kernel. Rejected (non-goal).
- *Two-phase commit relying on WAL retention to cover the commit lag*: couples correctness to `retention.max_age`, needs replay from below the acknowledged cursor, and breaks when a stream restarts under a different `node_id`. Rejected.

### D2. Commit cadence belongs to the upstream `batch` processor

The number of commits equals the number of `write_batch` calls. The existing `batch` processor already aggregates by row count and elapsed time and holds the source acknowledgements until the merged batch is written — the same role a checkpoint interval plays in Flink's Iceberg sink. No new timing logic is needed in the sink.

```
throughput ceiling   ≈ batch.count ÷ commit latency (encode + upload + catalog commit, sequential)
latency floor        ≈ batch.timeout_ms + commit latency
file size            ≈ rows per commit × row width   (target_file_size_bytes only splits within one commit)
peak memory          ≈ Arrow size of one commit + one Parquet buffer
```

Because the Kafka input does not wait to fill batches, a pipeline that puts the output directly behind the input produces one tiny commit per read. The output therefore emits a rate-limited warning when its commit rate exceeds 10 commits in 2 seconds (at most one warning per minute), recommending a `batch` processor. Alternatives considered: a `flush_interval` in the sink (rejected for the D1 reasons) and rejecting undersized batches (rejected — it would stall low-traffic streams).

### D3. Replay idempotency by source position, not by batch fingerprint

With `exactly_once: true` the output keeps a committed-through offset watermark per `(topic, partition)`:

```
write_batch:  1 row filter ─▶ 2 upload data files ─▶ 3 catalog commit (append + watermark) ─▶ 4 return Ok ─▶ ack
crash at:     during 1–2        after 2, before 3         after 3, before ack                     after ack
recovery:     whole call redone  orphan files, not in     replayed rows are all below the          nothing to
                                the table; replay          watermark ─▶ filtered ─▶ Ok, no commit  replay
                                re-uploads and commits
```

- `connect()` recovers the watermark from the most recent snapshot in the ancestry that carries this writer's property (D4).
- `write_batch` drops rows whose offset is below the watermark of their `(topic, partition)`; writes the rest; advances each present group to `max(offset) + 1`; stores the merged watermark atomically with the append.
- A row's position comes from `__meta_ext.topic`, `__meta_partition`, and `__meta_offset`. Rows expanded from one Kafka record share a position and are kept or dropped together.
- After any failed commit attempt the in-memory watermark is discarded and table metadata is reloaded before the next attempt: a commit whose outcome is unknown (timeout) may have been accepted, and the catalog is the only source of truth.
- The upstream duplicate-data-file check stays enabled as a last line of defence against re-appending a committed file.

Alternatives considered:

- *Batch fingerprint in the snapshot summary*: the sink has no batch identity, and replay re-runs the `batch` processor, whose time-based flush makes the grouping non-deterministic. A changed grouping breaks the fingerprint. Rejected.
- *Deterministic file names plus the duplicate-file check*: catches re-appending the same path but not a regrouped replay, which produces different files with the same rows. Rejected as the primary mechanism.
- *Writing position columns into the table and deduplicating at read time*: moves the cost to every consumer. Rejected.

### D4. Watermark carrier: a snapshot summary property

Each commit sets `arkflow.writer.<writer_id>.offsets` to a JSON object mapping `"<topic>#<partition>"` to the exclusive next offset. The API is confirmed (`FastAppendAction::set_snapshot_properties`) and writers never contend on it. Recovery walks the snapshot ancestry from the current snapshot, bounded to 1000 snapshots; if no carrying snapshot is found, the output starts with an empty watermark and logs a warning. That degradation can only duplicate rows inside the replay window — it can never drop rows.

Spike-gated alternative: *table properties* are cumulative (no ancestry walk, no per-snapshot growth with many partitions) but concurrent writers contend on them, and the watermark is only safe if one `Transaction` carrying both the append and the property update commits atomically. S5 confirmed that atomicity holds live, so the table property `arkflow.writer.<writer_id>.offsets` remains a viable alternative; the snapshot summary stays the default carrier. A non-atomic marker file was rejected: behind the real commit it duplicates, ahead of it it loses data.

### D5. `exactly_once` is opt-in and fails closed

- `exactly_once` defaults to `false` (mirrors the Kafka output's `exactly_once`), giving at-least-once.
- `exactly_once: true` requires a non-empty `writer_id` (build-time configuration error). `writer_id` must identify exactly one consumer group; using the group id is the documented convention.
- At run time the guarantee holds only when every row reaches the output with its position columns and the path from the Kafka input to the output is an ordered single chain per partition. A `write_batch` whose rows lack a position column fails with `Error::Process` naming the column; the output never silently degrades to at-least-once when the user asked for exactly-once.

Cost accepted: with a SQL processor the query must select `__meta_partition`, `__meta_offset`, and `__meta_ext`. Making the SQL processor carry `__meta_*` implicitly would change SQL semantics and is left to a follow-up.

### D6. Schema policy

The table must already exist and its schema is the only contract. Columns are matched by name. `__meta_*` columns are never written. A batch column that is not a table column is an error (silent drops hide pipeline mistakes). A table column missing from the batch is allowed when the column is optional (Iceberg lets a file omit later-added optional columns) and an error when it is required. Values are converted with checked Arrow casts — lossless or overflow-checked — and any unsupported conversion is an error naming the column and both types. Scalars, structs, and lists are in scope; decimals and maps depend on S3. No schema evolution. Timestamp mapping follows S3: `Timestamp(µs)` is the universal case; `Timestamp(ns)` maps to the format-v3-only `timestamp_ns` type and is rejected against v2 tables (ns→µs is neither lossless nor overflow-checked).

Alternative considered: create the table from the first batch. Convenient for a quickstart, but it adds type-inference rules, partition-spec creation, and a create race between writers. Deferred to a follow-up.

### D7. Partition policy

Data files must follow the table's partition spec, otherwise partition pruning reads past them. The output evaluates the spec with the upstream partition splitter; S4 confirmed the supported transforms are identity, void, year, month, day, hour, bucket(N), and truncate(W) — only `Unknown` is unsupported, and a spec containing it is rejected at `connect()` with an error naming the field and transform — never written as if unpartitioned.

### D8. Dependencies: upstream crates behind `connectors-iceberg`, no custom storage adapter

`iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` (`default-features = false`, features `opendal-s3`, `opendal-fs`, `opendal-memory` for offline tests) are optional dependencies of `arkflow-plugin`, pulled in by `connectors-iceberg`, which is in `default` and forwarded by `arkflow` and `arkflow-server`. Minimal builds do not compile any of it.

Alternatives considered:

- *A custom `FileIO` adapter over the workspace's `object_store 0.13`*: would reuse the S3 WAL's storage stack and avoid OpenDAL and reqsign. The `FileIO`/`FileIOBuilder` shape suggests a registered implementation is possible, but it would make ArkFlow the owner of storage correctness. Deferred; revisit if S1 shows an unacceptable cost.
- *Writing Parquet with the workspace's `parquet` crate and calling the REST catalog directly*: re-implements field-id mapping, partition routing, and snapshot metadata that the upstream writer already gets right. Rejected.

Accepted costs: a second pin on `arrow` 58 and `zstd` 0.13 for default builds, plus OpenDAL and reqsign in the dependency graph. The build-baseline delta is measured in Phase 1 and recorded.

### D9. Single-file module `output/iceberg.rs`

Every existing output is a single file, and the CI gates that key on source paths — the `CONFIG_TYPES` rows, `OUTPUT_DISCONNECTION_ALLOW`, the hand-rolled-backoff pin, and the retry-adopter pin — enumerate `src/output/` non-recursively. A subdirectory would be invisible to them. The component is therefore one file, `crates/arkflow-plugin/src/output/iceberg.rs`, following the largest existing outputs (`kafka.rs`, `sql.rs`). Rule: every `arkflow_core::Error` variant is constructed in this file; internal helpers return a private typed error that this file maps. If non-test code grows past roughly 2,000 lines, helpers move to flat sibling files (`output/iceberg_*.rs`), never into a subdirectory. As with other large Rust files in this repository, the file is written in several pieces and assembled.

### D10. Error classification and retry

| Condition | Variant |
| --- | --- |
| Upload or catalog transport failure, timeout, HTTP 429 or 5xx | `Error::Disconnection` |
| Catalog unreachable or credentials rejected while connecting (run time) | `Error::Connection` |
| Write attempted before `connect()` succeeded | `Error::Connection` with a message |
| Table missing, schema mismatch, unsupported partition transform, position column missing under `exactly_once` | `Error::Process` |
| Invalid configuration detected at build time | `Error::Config` |

`Error::EOF` never appears. Transient failures retry through `RetryUtils::retry_with_backoff_classified` with `RetryConfig::default()`; deterministic failures abort the loop immediately. The whole retry budget stays far below the kernel's five-minute sink write timeout. Exposing a `retry:` block is out of scope.

### D11. Build is side-effect free

`build()` only deserializes and validates configuration; it opens no connection and contacts no catalog or storage. All remote checks run in `connect()`: catalog reachability, table existence, schema and partition-spec compatibility, watermark recovery. This keeps `arkflow --config <file> --validate` offline and lets the example validate without a `"validate": false` exemption. A configured `codec` is rejected at build time, as the SQL output does.

### D12. Observability

Structured `tracing` only: per-commit debug event (duration, rows, files), info event with the duplicate-skipped row count, the commit-rate warning (D2), and the watermark-recovery-miss warning (D4). Prometheus metrics are out of scope, matching the S3 WAL retention change.

## Risks / Trade-offs

- [Throughput ceiling is rows per commit divided by commit latency, because commits are sequential] → Document the capacity model and `batch.count` sizing; a parallel commit pipeline needs a kernel-level asynchronous sink acknowledgement (non-goal).
- [One snapshot per `write_batch`; a pipeline without `batch` produces many tiny snapshots and files] → Commit-rate warning; documentation makes `batch` mandatory in front of the output; table maintenance is documented as the operator's job.
- [Large commits raise memory: the peak is about the Arrow size of one commit plus one Parquet buffer] → Sizing guidance comes from S6; `target_file_size_bytes` only splits within a commit and does not drive memory.
- [The watermark property is unreachable after a long pause while other writers commit and snapshot history is expired] → Bounded ancestry walk; degradation duplicates at most the replay window and never loses rows; a warning is logged.
- [Many partitions enlarge every snapshot summary] → Compact encoding; switch to the table-property carrier if S5 proves an atomic multi-action commit.
- [Orphan data files after a failed commit] → Standard `remove_orphan_files` maintenance, documented.
- [Dependency coupling: second pin on `arrow` 58 and `zstd` 0.13; OpenDAL and reqsign added] → Feature-gated, so minimal builds are unaffected; the new pins are stated in the PR description so they enter the dependency-roadmap blocker list; cost measured in S1.
- [`https://` catalog URIs may lack a TLS backend because `iceberg-catalog-rest` declares `reqwest` without default features] → S2 verifies; fallback is an explicit `rustls-tls` feature edge under `connectors-iceberg`.
- [Loopback requests are captured by the system proxy on developer machines] → Integration tests and local runs set `NO_PROXY`.
- [Archive CLI drops all but the last requirement when a capability has several `## MODIFIED Requirements` blocks] → Both modified requirements of `build-feature-gating` sit in one block, and the archive step compares the delta with the main spec requirement by requirement.
- [Exactly-once silently drops rows on an unordered topology] → Stated as a precondition in `exactly-once-output`; missing position columns fail closed; documented.

## Migration Plan

Additive: no existing component, configuration, or spec behavior changes. Rollback is building without `connectors-iceberg` or removing the output from the configuration; data already committed is ordinary Iceberg and stays readable.

First deployment: create the table, put a `batch` processor in front of the output, enable `exactly_once` with the consumer group as `writer_id`, and verify the table's row count against the source.

## Open Questions

Resolved by the Phase 1 spike (2026-11-28, Lakekeeper `latest-main` + Silo + Postgres stack, `/tmp/iceberg-spike/writer`). **Go decision: proceed to Phase 2.**

| # | Question | Outcome |
| --- | --- | --- |
| S1 | Dependency resolution and cost | **PASS.** Lock diff 775 insertions / 27 deletions, zero version changes (deletion lines are cargo's two-version disambiguation suffixes). Single `arrow`/`parquet` 58.4, single `reqwest` 0.12 (0.12/0.13 coexistence predates this change). New structural duplicates: `apache-avro` 0.21+0.23, `itertools` 0.13–0.15, `rand` 0.8–0.10. Stack compiles in the workspace (debug, 4m41s incremental). Release cold-build and binary-size delta (task 1.2) still to measure before the PR. |
| S2 | HTTPS catalog URIs | **PASS (graph level).** `rustls-tls`, `rustls-tls-native-roots`, and `rustls-tls-webpki-roots` already reach the single `reqwest` 0.12 via oauth2/openidconnect chains; feature unification makes them global. No feature edge needed. Runtime TLS smoke test rides along with Phase 3 integration tests. |
| S3 | External Arrow batch write | **PASS.** `ParquetWriterBuilder::new(props, table_schema)` + `schema_to_arrow_schema` re-seating works; field ids ride on the table schema. **`timestamp_ns` is format-v3-only** — a v2 table rejects it (Lakekeeper 400). MVP type set: `Timestamp(µs)` universally; `Timestamp(ns)` only against v3 tables (else rejected — ns→µs is neither lossless nor overflow-checked). Decimals and maps: untested this spike → rejected in MVP per the D6 fallback, revisited in Phase 3 tests. |
| S4 | Partition transforms | **PASS, broader than assumed.** `create_transform_function` supports identity, void, year, month, day, hour, bucket(N), truncate(W) — only `Unknown` errors. The end-to-end chain works: `PartitionValueCalculator::try_new` → `RecordBatchPartitionSplitter::try_new(..).split(batch) -> Vec<(PartitionKey, RecordBatch)>` → `DataFileWriterBuilder.build(Some(key))` per partition. D7's supported set is all eight transforms. |
| S5 | Watermark carrier | **PASS.** Custom snapshot summary property round-trips through Lakekeeper and is recoverable by walking `parent_snapshot_id()` (1 hop observed). **Also**: one `Transaction` combining `update_table_properties` + `fast_append` commits atomically (marker and rows landed together) — the table-property carrier is a viable alternative; snapshot summary stays the default (no ancestry walk, no cross-writer contention). |
| S6 | Commit latency | **PASS (preliminary).** 5 commits × 50k rows: avg **161 ms per commit ≈ 311k rows/s**, debug build, local Silo + Lakekeeper. Sequential commits are not the practical bottleneck at realistic batch sizes; the D2 capacity model stands, with real-S3 latency (300 ms–1 s) the ceiling drops accordingly. Peak RSS not measured in the spike — Phase 3 integration tests record it. |
| S7 | REST auth surface | **PASS.** `rest.auth.type` (`none`/`oauth2`) plus the `credential` prop (`client_id:secret` or bare secret) and `oauth2-server-uri` exist in `iceberg-catalog-rest`; live OAuth2 test not needed (local Lakekeeper runs auth-free, `AUTHZ_BACKEND=allowall`). Storage credentials flow through the factory, not the catalog auth (see D-note below). |
| S8 | `with_check_duplicate` | **PASS.** Default is `true` (`FastAppendAction::new`); the net fired live — re-adding an already-referenced file was rejected with `DataInvalid`. Implementation must generate unique file names per attempt: `DefaultFileNameGenerator` numbering is deterministic per prefix, so each commit attempt needs a unique prefix (uuid or writer-id + attempt id). |

Spike implementation notes that feed Phase 3 (learned the hard way, all verified live):

- The output must NOT build its own FileIO: pass `OpenDalStorageFactory::S3 { customized_credential_load: Some(CustomAwsCredentialLoader::new(config-driven provider)) }` to `RestCatalogBuilder::with_storage_factory` and write through `table.file_io()`. A standalone FileIO never participates in table writes.
- Client catalog props **override** server defaults (`defaults < client < overrides` in `merge_with_config`): `s3.endpoint`/`s3.region` set per output config fix NAT'd endpoints — this is how the configured S3 endpoint reaches the write path.
- Credentials must be config-driven (`ProvideCredential` impl wrapped in `CustomAwsCredentialLoader`); without it the factory falls back to env vars / instance profile, which the docs must not imply.
- A loaded `Table` is a metadata snapshot: never scan a stale handle after committing (tests must reload). The output never scans, so this only affects tests and the quickstart verification.
- Table creation with `format-version` 3 accepts `timestamp_ns`; v2 does not.
