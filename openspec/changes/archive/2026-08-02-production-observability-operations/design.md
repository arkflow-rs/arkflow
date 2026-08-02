## Context

The Hub already has durable node, Intent, Attempt, event, and outbox records, while the HTTP layer exposes liveness, readiness, health, and a basic metrics response. Those endpoints currently report process-level facts or Agent-provided stream counters, so an operator cannot tell whether the Hub has recovered storage, whether reconciliation is making progress, or whether a node is safe to receive new work.

This change adds a small operational plane on top of the existing sources of truth. It must remain cheap to scrape, safe to expose behind the existing authentication/reverse-proxy boundary, and useful during partial failure.

## Goals / Non-Goals

**Goals:**

- Make liveness, readiness, health, and reconciliation degradation distinct and machine-readable.
- Export stable Prometheus text metrics with bounded labels and no secret/configuration payloads.
- Make node drain/maintenance an explicit, durable, auditable operator action.
- Expose enough diagnostic state to identify storage, outbox, lease, retry, and convergence problems without querying SQLite directly.
- Preserve compatibility with existing JSON health and metrics consumers.

**Non-Goals:**

- A distributed metrics store, alert manager, or tracing backend.
- Per-message or per-request metric labels.
- Automatic remediation based solely on thresholds.
- Replacing the existing operator/Agent authentication model.

## Decisions

### 1. Derive operational health from durable state and explicit lifecycle markers

The Hub will maintain a small in-memory lifecycle snapshot (`started`, `recovered`, `last_reconcile_at`, `last_reconcile_error`) and query durable storage for counts of pending outbox rows, active Attempts, and non-terminal Intents. Readiness is true only after startup recovery succeeds and storage checks pass. Liveness remains process-only and must not require SQLite or an Agent. Health returns `200` for healthy/degraded-but-serving and `503` when the Hub cannot serve control-plane writes.

### 2. Use a fixed Prometheus metric vocabulary

The metrics endpoint will emit text exposition with fixed state/failure/operation labels only. Node IDs, stream IDs, correlation IDs, and error messages will not be labels. Primary signals include readiness, reconciler activity, node states, Intent/Attempt states, outbox backlog, reconcile runs/failures, stale nodes, and oldest pending age.

### 3. Persist node operational mode and audit transitions in the existing event log

`cp_nodes` will gain a backward-compatible `maintenance_state` (`active`, `draining`, `maintenance`) and timestamps through the existing migration mechanism. A drain or maintenance mutation updates the node mode and appends a `node_maintenance_changed` event. Reconciliation checks the mode before dispatching new Attempts; already dispatched Attempts are allowed to finish.

### 4. Add explicit operational endpoints without changing lifecycle routes

The versioned Hub API will add `GET /api/v1/operations/status`, node drain/maintenance actions, and a resume action. Existing health and metrics paths remain available; JSON status is protected like other operator resources and Prometheus scraping uses the configured reverse-proxy policy.

### 5. Make failure and rollout behavior observable

Every reconciliation tick records success/failure and duration in the lifecycle snapshot; durable event records remain the audit history. Rollout starts with metrics and read-only status, then enables drain/maintenance actions, and finally enables readiness enforcement. Rollback disables actions or readiness policy without deleting durable audit or desired-state history.

## Risks / Trade-offs

- [Risk] Metrics computation adds read load to SQLite. → Mitigation: one short aggregate query path, a small cache interval, and a bounded scrape result.
- [Risk] A stale in-memory lifecycle marker misstates health after a task panic. → Mitigation: expose last-success age and let periodic recovery refresh it; readiness remains false until startup recovery completes.
- [Risk] Drain strands desired Intents while a node is intentionally offline. → Mitigation: preserve desired state, expose mode and pending counts, and resume automatically when mode returns to active.
- [Risk] Existing probes expect `200` JSON responses. → Mitigation: keep response fields stable and only use `503` when serving is genuinely unsafe; add compatibility tests.

## Migration Plan

1. Apply additive SQLite columns for node maintenance state with `active` as the legacy default.
2. Deploy read-only status and metrics; validate scrape cardinality and alert thresholds.
3. Enable drain/maintenance actions and verify audit events plus Agent reconnect behavior.
4. Enable readiness enforcement in the production reverse proxy/orchestrator.
5. Roll back by disabling operational mutations or readiness enforcement; retain stored events and desired state.
