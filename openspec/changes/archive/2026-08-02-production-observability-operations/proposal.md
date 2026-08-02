## Why

The control plane currently exposes health and metrics routes, but the Hub health handler always reports ready/healthy and the metrics endpoint only aggregates Agent-reported stream values (`crates/arkflow-server/src/lib.rs:1050-1125`). The reconciliation worker and node lease transitions run in background tasks without durable operational counters or latency/error visibility (`crates/arkflow-server/src/lib.rs:324-347`, `crates/arkflow-server/src/hub.rs:1133-1168`), making it difficult to distinguish a healthy process from a converging or degraded fleet in production.

Now that desired state, durable Intents, Attempts, events, and node leases exist, operators need production-grade signals and safe operational controls built on those same sources of truth.

## What Changes

- Add a typed control-plane health snapshot that distinguishes process liveness, dependency/storage readiness, node lease health, and reconciliation degradation.
- Expose Prometheus-compatible metrics for reconciliation queue depth, Intent/Attempt outcomes, convergence latency, stale nodes, outbox recovery, API failures, and Agent report freshness.
- Add a protected operational status endpoint with bounded summaries and diagnostic correlation, without exposing secrets or arbitrary configuration payloads.
- Persist and expose audit events for operational actions such as drain, maintenance, retry, and cancellation, including actor and correlation identity.
- Add node drain/maintenance semantics that stop new dispatch, allow in-flight work to settle, and make the state visible to operators and Agents.
- Add rollout guidance, metric naming/cardinality rules, readiness behavior, and tests for degraded dependencies and stale-node conditions.

## Capabilities

### New Capabilities

- `control-plane-observability`: Operational health snapshots, Prometheus metrics, diagnostic summaries, and bounded cardinality rules.

### Modified Capabilities

- `control-plane-service`: Extend health, metrics, audit, and operational action contracts with production semantics.
- `control-plane-deployment`: Define readiness, protected metrics exposure, drain, and maintenance behavior for production deployment.

## Impact

- Affected Rust code: `arkflow-server` Hub/reconciler/storage/router and `arkflow-core` health/configuration types.
- Affected HTTP contract: versioned operational status, metrics, node maintenance/drain actions, and event representations.
- Affected persistence: operational counters/snapshots and audit records may be stored alongside existing SQLite control-plane tables.
- Affected deployment/docs: reverse-proxy guidance, scrape configuration, alert examples, and safe rollout/rollback procedures.
- No change to stream data-plane semantics, plugin protocols, or the existing operator authentication model.

## Non-goals

- Building a full external alerting system, notification router, or incident-management integration.
- Replacing Prometheus/OpenTelemetry collectors or requiring a remote metrics database.
- Automatically draining or restarting production nodes without an explicit operator action or deployment policy.
- Exposing secrets, raw configuration contents, or unbounded per-request/per-stream metric labels.
