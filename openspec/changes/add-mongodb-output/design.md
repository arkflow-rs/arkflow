## Context

ArkFlow outputs receive columnar Apache Arrow `MessageBatch` values and are initialized through the plugin output registry. MongoDB's Rust driver accepts BSON `Document` values and provides an async `insert_many` operation, so the integration needs an explicit row-to-document conversion boundary and a connection lifecycle matching `Output::connect`/`close`.

## Goals / Non-Goals

**Goals:**

- Add a small native MongoDB output with URI/database/collection configuration.
- Preserve Arrow scalar values as BSON values, including nulls, and insert one document per row in each output batch.
- Keep connection setup in `connect`, serialize writes through a shared collection handle, and classify MongoDB driver errors: network/lifecycle failures as `Error::Connection`, authentication failures as `Error::Authentication`, and server/data failures as `Error::Process`.
- Make configuration discoverable through component metadata, examples, and documentation.

**Non-Goals:**

- Transactions, retries, upserts, deletes, sharding-aware routing, or change streams.
- Converting arbitrary Arrow nested structures in the initial implementation.
- Supporting encoded binary payloads through the MongoDB output codec slot; a configured codec is rejected as unsupported rather than silently discarded.

## Decisions

1. **Use the official `mongodb` Rust driver and its BSON types.** This avoids hand-built BSON serialization and gives async pooling and URI parsing. A custom HTTP/JSON implementation was rejected because it would require reimplementing authentication, pooling, and BSON semantics.
2. **Use `uri`, `database`, and `collection` as the required configuration.** The URI owns server authentication and driver options; separate database and collection names keep destination selection explicit. A single database URI convention was rejected because it makes YAML validation and UI metadata less clear.
3. **Map each supported Arrow scalar column to a same-named BSON field.** This preserves the batch schema without a second mapping language. Configurable field mappings were deferred because they are not required for a usable first output and would duplicate processor functionality.
4. **Call `insert_many` once per `write` batch.** This preserves batch throughput and gives the output a single driver operation per acknowledgement unit. Per-row `insert_one` was rejected for unnecessary round trips; transactions were excluded because they require session handling and a replica-set deployment.
5. **Reject configured codecs.** MongoDB output writes structured BSON documents, while the codec interface produces encoded bytes or a transformed batch. Rejecting an incompatible codec at build time prevents accidental insertion of opaque payloads.

## Risks / Trade-offs

- [Unsupported Arrow types] → Return a descriptive configuration/process error naming the field and type; cover supported scalar mappings with unit tests.
- [Partial bulk insertion] → Surface the driver's `insert_many` error so the engine does not acknowledge the failed output batch; document that initial semantics are not transactional.
- [Driver dependency footprint] → Keep the dependency limited to the async Tokio runtime and default TLS behavior, and run package-level tests/build checks.
- [Concurrent writes] → Guard the optional collection handle with Tokio's mutex so lifecycle and writes cannot race.

## Migration Plan

Add the dependency and register the new output during normal plugin initialization. Existing configurations are unchanged. Deployments that use MongoDB add an `output` with `type: mongodb`; rollback is deleting that output and reverting the dependency/code change.

## Open Questions

None for the initial implementation.
