## Why

ArkFlow currently has relational and key/value database outputs, but no native MongoDB output for persisting Arrow record batches as BSON documents. The output registry exposes only the existing builders in `crates/arkflow-plugin/src/output/mod.rs:21-38`, while the `Output` contract accepts a batch through `write` in `crates/arkflow-core/src/output/mod.rs:29-52`; adding MongoDB support fills this storage integration gap without changing the pipeline contract.

## What Changes

- Add a MongoDB output plugin registered under the `mongodb` output type.
- Connect to a MongoDB URI and target a configured database and collection.
- Convert each Arrow row to a BSON document, preserving supported scalar values and representing nulls as BSON null.
- Insert an output batch with MongoDB bulk insertion and return connection/write failures through ArkFlow errors.
- Expose component metadata/schema and a configuration example, plus user-facing output documentation.
- Add unit tests for BSON conversion, configuration validation, registration, and batch behavior.

## Non-goals

- No MongoDB change-stream input or CDC source.
- No update/upsert/delete semantics, generated `_id` customization, or per-record routing in the initial plugin.
- No exactly-once guarantee beyond the existing output acknowledgement contract.

## Capabilities

### New Capabilities

- `mongodb-output`: Configure a MongoDB destination and write Arrow batches as BSON documents.

### Modified Capabilities

- None.

## Impact

- `crates/arkflow-plugin`: new MongoDB output module, registration, dependency, and tests.
- `docs/docs/components/3-outputs`: MongoDB configuration documentation.
- `examples`: MongoDB output example.
- Cargo dependency resolution gains the official Rust MongoDB driver.
