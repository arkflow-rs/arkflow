## 1. Domain model and workspace setup

- [x] 1.1 Add `id` to `StreamConfig`, define validation rules for non-empty unique IDs, and preserve deterministic IDs for legacy configurations.
- [x] 1.2 Extend the generated Engine JSON Schema with the Stream ID field and its constraints.
- [x] 1.3 Add the `arkflow-server` crate to the workspace with minimal library and binary integration points.
- [x] 1.4 Define shared lifecycle states, status snapshots, operation results, standard API errors, and event payloads.
- [x] 1.5 Add unit tests covering Stream ID parsing, duplicate detection, legacy ID assignment, and status serialization.

## 2. Runtime manager foundation

- [x] 2.1 Define the runtime record containing StreamConfig, lifecycle state, cancellation token, task handle, metrics handle, and recent errors.
- [x] 2.2 Implement a process-local RuntimeManager registry keyed by stable Stream ID with lock boundaries that never await while holding the registry write lock.
- [x] 2.3 Move Engine Stream construction and task supervision behind RuntimeManager without changing existing Stream processing behavior.
- [x] 2.4 Implement Engine-wide shutdown that requests all Stream shutdowns and awaits their task completion with bounded error reporting.
- [x] 2.5 Add runtime manager tests for registration, lookup, missing IDs, task completion, and Engine shutdown.
- [x] 2.6 Add a regression test proving an individual Stream runtime failure does not stop unrelated Stream runtimes.

## 3. Unified HTTP server and read-only API

- [x] 3.1 Replace the detached health server startup with a listener-owned unified Axum Router and preserve existing health/readiness/liveness paths and response shapes.
- [x] 3.2 Add server configuration for control API enablement, address, and route prefix with safe local defaults.
- [x] 3.3 Implement `GET /api/v1/system` and `GET /api/v1/status` using runtime snapshots.
- [x] 3.4 Implement `GET /api/v1/streams` and `GET /api/v1/streams/{id}` with 404 handling for unknown IDs.
- [x] 3.5 Implement component catalogue and component-detail endpoints from the existing registry.
- [x] 3.6 Implement the full configuration JSON Schema endpoint using `build_config_schema()`.
- [x] 3.7 Implement a consistent JSON error response and map validation, not-found, conflict, and internal errors to HTTP statuses.
- [x] 3.8 Add Axum integration tests for health routes, API routes, error envelopes, and listener bind failures.

## 4. Stream lifecycle control

- [x] 4.1 Implement per-Stream cancellation tokens and ensure `Stream::run()` reaches its existing resource close path on cancellation.
- [x] 4.2 Implement start command handling with state transitions and duplicate-start protection.
- [x] 4.3 Implement stop command handling that awaits the Stream task and reports bounded shutdown failures.
- [x] 4.4 Implement restart command handling as stop, fresh build, and start using the retained StreamConfig.
- [x] 4.5 Add `POST /api/v1/streams/{id}/start`, `/stop`, and `/restart` endpoints with conflict responses for concurrent operations.
- [x] 4.6 Add lifecycle event recording for start, stop, restart, failure, and recovery outcomes.
- [x] 4.7 Add tests proving stop/restart of one Stream does not affect another Stream.
- [x] 4.8 Add WAL-backed restart tests proving the existing replay and cursor semantics remain intact.

## 5. Metrics and recent errors

- [x] 5.1 Add a low-cardinality metrics registry with Stream and stage labels only.
- [x] 5.2 Instrument input receipt, input errors, reconnects, processing, processing errors, output writes, and output errors.
- [x] 5.3 Instrument Stream state, uptime, restart count, and lifecycle operation outcomes.
- [x] 5.4 Implement Prometheus exposition at `/metrics` and include it in the unified server.
- [x] 5.5 Expose metrics and bounded recent errors in Stream status responses.
- [x] 5.6 Add tests for counter updates, gauge transitions, Prometheus output, and bounded error retention.

## 6. Configuration validation and versioning

- [x] 6.1 Define candidate configuration request/response types and structured validation error paths.
- [x] 6.2 Implement parsing for YAML, JSON, and TOML request payloads using the existing EngineConfig loaders or shared parser utilities.
- [x] 6.3 Implement candidate validation for schema constraints, duplicate IDs, component lookup, and Stream construction without starting candidates.
- [x] 6.4 Add recursive secret redaction for password, token, secret, credential, and auth fields in read responses and diagnostics.
- [x] 6.5 Implement file-backed configuration version metadata and atomic version file writes.
- [x] 6.6 Implement `GET /api/v1/config` and `POST /api/v1/config/validate` with redaction and structured errors.
- [x] 6.7 Implement configuration apply with affected-Stream detection, candidate build-before-stop, and unchanged-Stream preservation.
- [x] 6.8 Implement failed-apply recovery and report partial restoration failures explicitly.
- [x] 6.9 Implement configuration version listing, diff metadata, and rollback through the same validation/apply pipeline.
- [x] 6.10 Add tests for invalid candidates, add/change/remove Stream operations, failed startup recovery, redaction, versioning, and rollback.

## 7. Control API events and operational behavior

- [x] 7.1 Add bounded in-memory event storage with ordering, timestamps, event type, Stream ID, and outcome.
- [x] 7.2 Implement a read-only recent-events endpoint for console polling.
- [x] 7.3 Add graceful API shutdown tied to the Engine cancellation token.
- [x] 7.4 Add request logging and ensure secrets and request bodies are not logged by default.
- [x] 7.5 Add API contract tests covering status changes observed after lifecycle commands.

## 8. Web Console foundation

- [x] 8.1 Create the independent `console/` React application with development API base URL configuration.
- [x] 8.2 Add typed API client models for system, Stream, metrics, configuration, components, errors, and operations.
- [x] 8.3 Add application shell, navigation, loading states, empty states, and API error handling.
- [x] 8.4 Implement the Dashboard with Engine state, Stream counts, aggregate metrics, and recent errors.
- [x] 8.5 Implement Stream list and detail views with topology summary, state, metrics, recent errors, and lifecycle actions.
- [x] 8.6 Add confirmation and operation-progress feedback for start, stop, and restart commands.
- [x] 8.7 Implement YAML/JSON configuration editing with schema validation results and path-aware error display.
- [x] 8.8 Implement configuration version listing, publish, and rollback flows.
- [x] 8.9 Implement component catalogue and schema/example display.
- [x] 8.10 Ensure redacted values are preserved in the UI and never written to URLs, browser logs, or error text.
- [x] 8.11 Add frontend unit/component tests for dashboard states, lifecycle actions, validation errors, and redaction.

## 9. Security and deployment baseline

- [x] 9.1 Default the control server to a local or explicitly configured bind address and document exposure risks.
- [x] 9.2 Add an optional bearer-token middleware for write operations and configuration reads.
- [x] 9.3 Add CORS configuration with deny-by-default behavior for production settings.
- [x] 9.4 Add console build and API proxy instructions for local development and reverse-proxy deployment.
- [x] 9.5 Add Docker or deployment documentation for running the API and console together through a protected endpoint.
- [x] 9.6 Add security tests covering unauthenticated write rejection, redaction, CORS behavior, and token comparison.

## 10. Verification and documentation

- [x] 10.1 Add an example configuration using named Streams and control-plane settings.
- [x] 10.2 Document API endpoints, lifecycle semantics, at-least-once restart behavior, and configuration rollback.
- [x] 10.3 Run `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features`, and `cargo test --workspace`.
- [x] 10.4 Run the console typecheck, unit tests, and production build.
- [x] 10.5 Run an end-to-end smoke test covering Engine startup, API discovery, Stream restart, config validation, and shutdown.
- [x] 10.6 Verify no existing health endpoint, WAL recovery test, or unrelated OpenSpec change is regressed.
