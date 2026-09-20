## 1. Job contract validation

- [x] 1.1 Reject unsupported distributed `OperatorKind::Join` plans with a stable migration error and add validation tests.
- [x] 1.2 Add `StateSpec.max_bytes`, schema metadata, positive-bound validation, and documentation.

## 2. Runtime state semantics

- [x] 2.1 Add the effective namespace helper and use it for generic stateful and window operators.
- [x] 2.2 Wire `max_bytes` into Redb backend construction and preserve replayable mutation failures.
- [x] 2.3 Add namespace isolation, exact budget-boundary, and over-budget rollback tests.

## 3. Verification

- [x] 3.1 Update generated docs/config snapshots and reject stale Join claims in examples or validation fixtures.
- [x] 3.2 Run focused core state/Job tests, workspace tests, and clippy.
