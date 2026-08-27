## Context

ArkFlow currently starts a health-check Axum server and then builds all configured streams inside `Engine::run()`. The engine keeps only global readiness/running flags, while `Stream::run()` owns the stream task and receives one shared cancellation token. `StreamConfig` is serializable but streams have no stable identity. The component registry already exposes metadata and JSON Schema, so it can be reused by an API and a schema-driven console.

The control plane is scoped to one ArkFlow process. It must preserve existing input WAL recovery and at-least-once behavior. It must also avoid blocking processing tasks on control-plane requests.

## Goals / Non-Goals

**Goals:**

- Introduce a supervised runtime registry with one independently addressable entry per Stream.
- Serve health, control API, metrics, and lifecycle events from one HTTP server.
- Support read-only inspection first, then safe per-Stream start/stop/restart.
- Validate and version configurations before applying them.
- Reuse component metadata and the generated configuration schema.
- Provide an independent React console for single-node operation.

**Non-Goals:**

- Multi-process or multi-node orchestration.
- In-place mutation of already-connected component objects.
- Exactly-once guarantees for restart or configuration operations.
- Full identity, RBAC, OIDC, and multi-tenant management in the initial implementation.
- A graphical arbitrary DAG editor.

## Decisions

### 1. Add a runtime manager instead of extending the existing health state

Introduce a runtime manager owned by the Engine. It maps a validated Stream ID to a runtime record containing the immutable StreamConfig, lifecycle state, per-Stream CancellationToken, supervised JoinHandle, counters, and recent error information. The manager exposes command methods and snapshots; handlers never access Stream internals directly.

This is preferred over putting mutable fields directly into `Engine` because lifecycle, status, and command serialization are cross-cutting concerns. A single process-local manager is sufficient for the current single-node scope.

### 2. Use per-Stream cancellation and rebuild for restart

Each Stream receives a child cancellation token. Stop cancels that token and awaits the task so the existing Stream close path releases input, buffer, pipeline, output, error output, and WAL resources. Restart stops the old instance, builds a fresh instance from the candidate configuration, then starts it.

The implementation MUST NOT mutate a live Input/Output/Processor object. This keeps connector-specific connection semantics isolated and makes failed rebuilds recoverable.

### 3. Add stable Stream IDs with compatibility for legacy configurations

Add an optional-to-required-in-new-config `id` field to StreamConfig. New configurations must contain unique IDs. Legacy configurations without IDs are assigned deterministic `stream-<index>` IDs during loading and produce a migration warning. The generated full JSON Schema will document the field.

This is preferred over array indexes because API resources and metrics need stable identity across configuration versions.

### 4. Merge health and control routes into one server

Build one Axum Router containing health routes, `/metrics`, `/api/v1/*`, and a later event stream route. Bind the listener before reporting server startup success, and use the Engine cancellation token for graceful shutdown. The existing separate background health server is replaced by this unified server.

This avoids port conflicts and ensures consistent startup failure, shutdown, and middleware behavior.

### 5. Use versioned REST APIs and polling-first events

Expose REST under `/api/v1`. The initial console polls snapshots. The backend records recent lifecycle/error events and may expose them through a bounded SSE endpoint after the snapshot APIs are stable. REST is preferred over WebSocket for the first release because commands are request/response and the console primarily needs server-to-client state updates.

### 6. Apply configuration transactionally at the Stream level

Configuration application parses and validates the candidate, builds all affected new Streams without starting them, persists a version, then replaces affected runtime instances. If parsing, validation, build, or startup fails, the previous configuration and unaffected Streams remain intact; the system attempts to restore stopped old Streams.

Configuration versions are file-backed in the first release, adjacent to the configured file, and contain metadata plus the redacted or original operator-controlled configuration according to deployment policy. A database is deferred until multi-user or multi-node requirements exist.

### 7. Keep metrics low-cardinality and non-blocking

Use process-local counters/gauges for input, processing, output, errors, restarts, state, and uptime. Labels are limited to stable Stream IDs and component stage/type. Prometheus exposition is provided through `/metrics`; control API snapshots use the same registry. Metrics updates must not await network or connector operations.

### 8. Keep the console independent from Docusaurus

Create a separate `console/` React application. During development it runs independently against the API; deployment can initially use a reverse proxy. Static asset embedding into the Rust binary is deferred until the API and frontend contracts stabilize. The console uses the component/schema endpoints instead of duplicating component-specific configuration definitions.

## Risks / Trade-offs

- [Restart can redeliver messages] → Document that restart retains existing at-least-once semantics; rely on WAL and connector behavior rather than claiming exactly-once.
- [A connector may not stop promptly] → Stop operations have a timeout and report `stopping`/`failed`; connector close remains responsible for unblocking reads.
- [A failed new configuration can leave a stopped old Stream] → Build candidates before stopping where possible, retain old configs, and perform explicit rollback/start recovery with an operation result describing partial failure.
- [Secrets can leak through configuration APIs or logs] → Redact known secret fields recursively, never return live credentials by default, and bind the unauthenticated initial server to localhost or an operator-protected network.
- [High-cardinality metrics can exhaust memory] → Restrict labels and reject dynamic labels derived from URLs, topics, SQL, or error text.
- [Existing health endpoint behavior may change during server unification] → Preserve paths and response shapes and add compatibility tests before removing the old server implementation.
- [A large all-in-one change is difficult to review] → Implement in dependency order: API/runtime foundation, lifecycle control, configuration management, then console.
