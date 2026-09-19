## 1. Configuration and startup policy

- [x] 1.1 Add explicit `insecure_local`/storage configuration with serde/env wiring and loopback-aware startup validation.
- [x] 1.2 Make the standalone Hub open durable storage by default for secure startup and restore persisted state before binding/listening.
- [x] 1.3 Make Hub operator/node authorization fail closed unless explicit local-insecure mode is active.

## 2. Persistence and authentication tests

- [x] 2.1 Add startup matrix tests for loopback, external bind, missing storage, missing tokens, and invalid insecure-local combinations.
- [x] 2.2 Add Hub restart tests proving Jobs, operations, and checkpoint pointers survive storage-backed recovery.
- [x] 2.3 Add secure authorization tests for missing/wrong/valid operator and node credentials, including no mutation on rejection.

## 3. Verification

- [x] 3.1 Update generated configuration/schema/docs snapshots and run server workspace tests.
- [x] 3.2 Run clippy and confirm readiness never reports healthy before durable recovery completes.
