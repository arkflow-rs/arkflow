## 1. Contract and server boundary

- [x] 1.1 Define versioned resource, page, problem, correlation, capability, and operation DTOs in the control-plane domain module.
- [x] 1.2 Define a cloneable core `ControlPlane` facade that exposes system, node, Stream, configuration, component, event, metrics, and operation services without Axum types.
- [x] 1.3 Move HTTP Router construction and handlers from `arkflow-core::Engine` into `arkflow-server` while preserving `/health`, `/readiness`, `/liveness`, and `/metrics` compatibility behavior.
- [x] 1.4 Refactor binary startup so Engine domain lifecycle and server lifecycle are composed explicitly by `arkflow`.
- [x] 1.5 Add API contract tests for resource envelopes, problem responses, correlation IDs, pagination, and compatibility health routes.

## 2. Runtime and operation model

- [x] 2.1 Extend runtime records with desired state, observed state, transition timestamps, active operation ID, and node identity.
- [x] 2.2 Implement bounded operation storage with queued/running/succeeded/failed/cancelled states, progress, timestamps, and affected resource IDs.
- [x] 2.3 Connect RuntimeManager supervisor completion to operation completion and ordered control events.
- [x] 2.4 Make lifecycle commands idempotent and prevent duplicate equivalent operations under concurrent requests.
- [ ] 2.5 Implement operation query and cancellation/reconciliation behavior with explicit timeout outcomes.
- [ ] 2.6 Add tests for operation success, failure, duplicate commands, concurrent commands, timeout, and unrelated Stream isolation.

## 3. Resource-oriented backend API

- [x] 3.1 Implement `/api/v1/system` and `/api/v1/nodes` with identity, version, capabilities, health, uptime, and runtime summary.
- [x] 3.2 Implement paginated/filterable `/api/v1/streams` and detailed Stream resources with desired/observed state and operation links.
- [x] 3.3 Implement `/api/v1/operations` and `/api/v1/events` query filters, ordering, pagination, and correlation metadata.
- [x] 3.4 Implement `/api/v1/configuration` draft, active version, validation, publish, diff, and rollback resources; retain compatibility aliases.
- [x] 3.5 Implement component catalogue, component detail, schema, and example endpoints through the server boundary.
- [x] 3.6 Add metrics resource/query support while retaining Prometheus exposition.
- [x] 3.7 Add request correlation middleware, structured access logging, request-size limits, CORS policy, and centralized authentication/problem handling.
- [ ] 3.8 Add integration tests covering every resource route, invalid filters, unknown resources, auth failures, redaction, and compatibility aliases.

## 4. Console application architecture

- [x] 4.1 Split the Console into application shell, typed API client, query/cache, shared UI state, and feature modules.
- [x] 4.2 Add route-aware navigation for Overview, Runtime, Configuration, Components, Events, and Settings.
- [x] 4.3 Implement centralized request/cache refresh with stale-data indicators, retry, permission, conflict, empty, and loading states.
- [ ] 4.4 Implement Overview with system identity, node health, runtime totals, active operations, recent events, and aggregate metrics.
- [ ] 4.5 Implement Runtime list/detail views with filters, pagination, topology summary, desired/observed state, errors, metrics, and operation history.
- [x] 4.6 Implement operation confirmation, progress, polling, terminal result, and conflict feedback for lifecycle commands.
- [ ] 4.7 Implement Configuration draft editor with YAML/JSON modes, schema-aware validation, path errors, publish, version diff, and rollback.
- [x] 4.8 Implement Components catalogue/schema/examples, Events audit filters/details, and Settings security/capability status.
- [x] 4.9 Enforce frontend secret/redaction rules and add component tests for stale data, permissions, validation errors, operations, and redaction.

## 5. Deployment and compatibility

- [x] 5.1 Add explicit backend and frontend startup commands for local control-plane development.
- [x] 5.2 Update Docker/Nginx/reverse-proxy configuration for same-origin `/api`, `/metrics`, static assets, and protected credentials.
- [x] 5.3 Document API resource model, operation semantics, compatibility aliases, security posture, and single-node limitations.
- [x] 5.4 Add a migration note from the health-centric API/console to the resource-oriented control plane.

## 6. Verification and rollout

- [ ] 6.1 Run Rust formatting, clippy, core/server tests, and workspace tests; distinguish pre-existing external integration failures.
- [x] 6.2 Run Console typecheck, unit/component tests, and production build.
- [ ] 6.3 Run an end-to-end smoke test for startup, resource discovery, lifecycle operation polling, configuration validation/publish, events, and graceful shutdown.
- [ ] 6.4 Verify WAL replay, health compatibility, redaction, and unrelated OpenSpec changes are not regressed.
- [ ] 6.5 Review the API/design/spec/task audit and record remaining warnings before archive.
