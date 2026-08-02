## 1. Operational state model and persistence

- [x] 1.1 Add typed operational health, reconciliation health, and node maintenance-mode models with stable snake_case serialization.
- [x] 1.2 Extend `cp_nodes` with additive maintenance-state/timestamp migration and repository read/write methods defaulting legacy nodes to `active`.
- [x] 1.3 Add bounded repository aggregate queries for node states, Intent/Attempt states, outbox backlog, and oldest pending age.
- [x] 1.4 Persist maintenance transitions and operational audit events atomically with actor/correlation metadata.

## 2. Hub reconciliation and operational controls

- [x] 2.1 Add Hub lifecycle markers for startup recovery, reconciliation success/failure, duration, and last error class.
- [x] 2.2 Prevent new Attempt dispatch to draining or maintenance nodes while allowing already dispatched Attempts to settle.
- [x] 2.3 Implement authenticated drain, maintenance, and resume mutations with idempotent state transitions and stable errors.
- [x] 2.4 Make node maintenance state visible in node resources, operation status, and event queries.

## 3. Health and metrics API

- [x] 3.1 Make liveness process-only and readiness depend on successful recovery/storage readiness with compatible JSON responses and correct status codes.
- [x] 3.2 Implement bounded operational status at `/api/v1/operations/status` with component, lease, reconciliation, outbox, and convergence summaries.
- [x] 3.3 Implement Prometheus exposition with fixed low-cardinality names/labels and no resource IDs, secrets, or error text labels.
- [x] 3.4 Add correlation-aware problem responses and authorization checks for operational endpoints while preserving scrape/deployment compatibility.

## 4. Tests and production documentation

- [x] 4.1 Add migration, maintenance transition, audit, and dispatch-fencing repository tests.
- [x] 4.2 Add health/readiness tests for startup recovery, storage failure, stale nodes, and reconciliation degradation.
- [x] 4.3 Add metrics contract tests for stable names, labels, bounded output, and degraded values.
- [x] 4.4 Add HTTP integration tests for drain/maintenance/resume, unauthorized access, status, and audit correlation.
- [x] 4.5 Document scrape configuration, alert signals, readiness semantics, drain rollout, rollback, and maintenance runbook.
- [x] 4.6 Run formatting, workspace tests, strict OpenSpec validation, and diff checks.
