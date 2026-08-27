## 1. Dependency and registration

- [x] 1.1 Add the official MongoDB Rust driver dependency with Tokio support to the workspace/plugin manifests.
- [x] 1.2 Add the MongoDB output module and register it from the output initializer under `mongodb`.

## 2. MongoDB output implementation

- [x] 2.1 Implement strict URI/database/collection configuration parsing and reject configured codecs.
- [x] 2.2 Implement connect/close lifecycle with a shared MongoDB collection handle.
- [x] 2.3 Implement Arrow scalar/null/binary-to-BSON row conversion with descriptive unsupported-type errors.
- [x] 2.4 Implement empty-batch handling and one-operation-per-batch `insert_many` writes with error propagation.
- [x] 2.5 Register component schema metadata and a valid configuration example.

## 3. Tests and documentation

- [x] 3.1 Add unit tests for configuration, BSON conversion, null handling, unsupported types, and empty batches.
- [x] 3.2 Add a MongoDB output YAML example and user-facing output documentation.
- [x] 3.3 Run formatting, OpenSpec validation, and package/workspace tests; fix any failures.
