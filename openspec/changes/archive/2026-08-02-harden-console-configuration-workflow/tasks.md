## 1. Core and API validation contracts

- [x] 1.1 Normalize configuration parser errors into useful line/column paths while preserving semantic paths, with Rust unit coverage.
- [x] 1.2 Add/adjust API types and helpers needed to represent exact candidate identity and terminal configuration operation outcomes.

## 2. Console configuration workflow

- [x] 2.1 Add the YAML parser/serializer dependency and format conversion helpers for the UI's YAML/JSON formats; make format/content changes invalidate validation atomically.
- [x] 2.2 Separate redacted active configuration display from editable draft state and gate Save draft/Publish accordingly.
- [x] 2.3 Poll apply and rollback operations to terminal state, refresh only after success, and retain failure/timeout feedback.
- [x] 2.4 Render path-aware validation/conversion issues and enforce the exact validated candidate publish gate.

## 3. Regression coverage and verification

- [x] 3.1 Add console tests for format conversion, stale validation, redacted display-only behavior, and async publish success/failure.
- [x] 3.2 Run console typecheck/tests/build and focused Rust tests; validate the OpenSpec change.
