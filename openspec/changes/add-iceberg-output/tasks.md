## 1. Spike (go / no-go gate)

- [x] 1.1 S1 dependencies: on a scratch branch add `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` (`opendal-s3`, `opendal-fs`, `opendal-memory`) as optional dependencies; confirm a single `arrow`/`parquet` 58.4 and a single `reqwest` 0.12 in `cargo tree`, and that the `Cargo.lock` diff only adds entries without changing an existing version
- [x] 1.2 Record the default cold-build time and release binary size delta against `main` in the PR description
- [x] 1.3 S2 TLS: load a table through an `https://` catalog URI; if it fails, add an explicit `reqwest` 0.12 `rustls-tls` feature edge and record it
- [x] 1.4 S3 schema: write an Arrow `RecordBatch` cast to `schema_to_arrow_schema(table.schema())` and read it back through a table scan; record whether field ids survive and whether decimals and maps are writable
- [x] 1.5 S4 partitions: drive the upstream partition splitter and fan-out writer with identity and temporal transforms; record the supported transform list
- [x] 1.6 S5 watermark carrier: commit with `FastAppendAction::set_snapshot_properties` against Lakekeeper and read the property back; test whether one `Transaction` with `fast_append` and `update_table_properties` commits atomically
- [x] 1.7 S6 capacity: measure commit latency and peak RSS for commits of 50k and 500k rows on MinIO + Lakekeeper
- [x] 1.8 S7 auth: confirm bearer-token auth and note whether OAuth2 client credentials and SigV4 are available in `iceberg-catalog-rest`
- [x] 1.9 S8 duplicate check: confirm the default and the exact semantics of `with_check_duplicate`
- [x] 1.10 Record every outcome under "Open Questions" in `design.md` and make the go / no-go call; a no-go on S1 stops the change before phase 2, and a switch of the watermark carrier updates the watermark requirement in the spec delta before phase 3

基线实测（2026-11-28，本机 release + strip，两次全新冷构建）：

| 口径 | default（含 iceberg） | 全家族减 iceberg | 变化 |
| --- | --- | --- | --- |
| 冷构建时间 | 1474s | 1274s | +200s（+15.7%） |
| 二进制体积 | 159,410,000 B（152.0 MB） | 153,035,408 B（145.9 MB） | +6.1 MiB（+4.2%） |
| cargo tree 行数 | 4070 | 3795 | +275（+7.2%） |

## 2. Feature and dependency layer

- [x] 2.1 Add `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` to `[workspace.dependencies]` in the root `Cargo.toml` (versions live only there)
- [x] 2.2 In `crates/arkflow-plugin/Cargo.toml` declare the three as optional dependencies and add `connectors-iceberg` with a comment naming its dependencies and the `iceberg` output; add it to `default` right after `connectors-amqp09`
- [x] 2.3 Add the forwarding feature `connectors-iceberg = ["arkflow-plugin/connectors-iceberg"]` and the `default` entry to `crates/arkflow/Cargo.toml` and `crates/arkflow-server/Cargo.toml`
- [x] 2.4 In `crates/arkflow-plugin/src/feature_hints.rs` add `connectors-iceberg` to `FAMILY_FEATURES` at the same position as in `default` and add the `("iceberg", "requires the 'connectors-iceberg' feature")` row to `HINTS`; `cargo test -p arkflow-plugin --locked feature_hints` passes
- [x] 2.5 In `.github/workflows/rust.yml` extend the minimal-build `FORBIDDEN` pattern with `iceberg v|opendal v|reqsign`; `cargo tree --locked -p arkflow --no-default-features --features engine-minimal | grep -E "iceberg v|opendal v|reqsign"` prints nothing
- [x] 2.6 `cargo check --workspace --locked --no-default-features --features engine-minimal` and the same with `--features engine-minimal,connectors-kafka` both succeed

## 3. Output implementation (`crates/arkflow-plugin/src/output/iceberg.rs`, written in pieces and assembled)

- [x] 3.1 `IcebergOutputConfig` with serde defaults and build-time validation (non-empty `catalog.uri`, `catalog.warehouse`, `namespace`, `table`; `writer_id` required exactly when `exactly_once` is true; `target_file_size_bytes` greater than zero; a supported compression codec), with a unit test for every rejected shape
- [x] 3.2 `IcebergOutputBuilder`, `init()`, and the docs metadata schema that mirrors the serde fields exactly; `build()` rejects a configured `codec` with `Error::Config` and performs no I/O
- [x] 3.3 `connect()`: build the REST catalog client, load the table, reject an unsupported partition spec, recover the watermark; the not-connected guard maps to `Error::Connection` with a message
- [x] 3.4 Schema mapping as a pure function (name matching, `__meta_*` exclusion, optional and required absence, checked casts) with a unit-test matrix covering every scenario of the schema requirement
- [x] 3.5 Position filter and watermark encode / merge / decode as pure functions, with a unit-test matrix covering replay, regrouping, multi-row expansion, partial coverage, and a missing column
- [x] 3.6 `write_batch`: concatenate, filter, encode, upload, and commit with `FastAppendAction` plus the watermark property; `write` delegates to `write_batch` with a single batch
- [x] 3.7 Error mapping (a private typed error mapped to `arkflow_core::Error` variants, all in this file), transient retry through `RetryUtils::retry_with_backoff_classified`, and the reload-after-failed-commit rule
- [x] 3.8 Commit-rate warning helper with tests for the burst case and the silent case
- [x] 3.9 `close()` releases the catalog and storage clients with no pending work; `crates/arkflow-plugin/src/output/mod.rs` gains the `#[cfg(feature = "connectors-iceberg")]` module and its `init` step

## 4. Integration and crash-point tests (gated by `ARKFLOW_REQUIRE_ICEBERG=1`)

- [x] 4.1 Harness in `crates/arkflow-plugin/tests/iceberg_output.rs`: testcontainers with MinIO, PostgreSQL, and Lakekeeper following the existing `ARKFLOW_REQUIRE_*` pattern (skipped when the variable is unset), with `NO_PROXY` set for loopback
- [x] 4.2 Happy path: three input batches in one `write_batch` produce one snapshot and the expected row count through a table scan
- [x] 4.3 Crash point after upload and before commit: only orphan files remain and the replay commits exactly once
- [x] 4.4 Crash point after commit and before `Ok`: the replay adds zero rows under `exactly_once`
- [x] 4.5 Regrouped replay (same records, different batch boundaries): zero duplicates
- [x] 4.6 At-least-once contrast: the same crash with `exactly_once: false` reports the duplicate count
- [x] 4.7 Fail-closed cases: missing position column, unsupported partition transform, missing table, unknown batch column, missing required column, unsupported type conversion
- [ ] 4.8 Ambiguous commit: a timeout injected after the catalog accepted the commit leads to a reload and zero duplicates — deferred: the no-proxy prefix of this window is exercised by the replay tests through the reload-refilter path (tasks 4.4/4.5); a fault-injecting catalog proxy is a follow-up

## 5. CI gates

- [x] 5.1 Add the `CONFIG_TYPES` row `("output", "iceberg", "output/iceberg.rs", "IcebergOutputConfig")` to `crates/arkflow-plugin/tests/metadata_conformance.rs`
- [x] 5.2 In `crates/arkflow-plugin/tests/connector_error_contract.rs` add `"iceberg.rs"` to `OUTPUT_DISCONNECTION_ALLOW` and a behavioral pin `iceberg_output_write_before_connect_is_connection_error`; add the file to the retry-adopter list if that pin enumerates adopters explicitly
- [x] 5.3 Regenerate the inventory and schema with `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`
- [x] 5.4 `cargo test -p arkflow-plugin --locked --test metadata_conformance --test registry_consistency --test connector_error_contract --test docs_inventory_snapshot` passes

## 6. Documentation (EN and zh-Hans in lockstep)

- [x] 6.1 Add `examples/iceberg_output_example.yaml` (Kafka → SQL that keeps `__meta_partition`, `__meta_offset`, `__meta_ext` → `batch` → `iceberg`, with the object-store WAL) and register it in `docs/reference/example-manifest.json`; it validates offline
- [x] 6.2 Write `docs/docs/components/3-outputs/iceberg.md` (`components: [output/iceberg]`, snippets marked `validate=fragment wrap=output`): configuration table, prerequisites, the mandatory `batch` processor, the capacity model, the exactly-once boundary, and the table-maintenance note
- [x] 6.3 Write the zh-Hans counterpart under `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/3-outputs/`
- [x] 6.4 Add the `connectors-iceberg` row to the feature table in `docs/docs/get-started/1-install.md` and its zh-Hans counterpart
- [x] 6.5 Add the output bullet with the `iceberg` type name inside the `README_COMPONENTS:output` markers of `README.md` and `README_zh.md`
- [x] 6.6 Add an `[Unreleased]` entry to `CHANGELOG.md`
- [x] 6.7 `pnpm --dir docs run docs:check` and `cargo test -p arkflow --locked --test examples_validate --test docs_snippets_validate` pass

## 7. Final verification

- [x] 7.1 `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` are clean
- [x] 7.2 `cargo test --workspace --all-targets --locked` passes under default features
- [x] 7.3 `openspec validate add-iceberg-output` passes
- [ ] 7.4 At archive time, compare both modified requirements of `build-feature-gating` in the delta with the archived main spec, requirement by requirement
