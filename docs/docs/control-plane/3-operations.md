---
description: ArkFlow documentation page.
---

# Control-plane operations

The Hub exposes process liveness at `/liveness`, storage/recovery readiness at
`/readiness`, bounded diagnostics at `/api/v1/operations/status`, and
Prometheus exposition at `/api/v1/metrics`. Put the operational routes behind
the configured operator-token boundary or an authenticated monitoring proxy;
never publish bearer tokens, configuration payloads, node IDs as metric
labels, or error text as labels.

Command dispatch metrics cover enqueue-to-acknowledgement latency
(`arkflow_command_duration_bucket`/`_count`/`_sum`) and per-outcome counters
(`arkflow_command_total`) with fixed `command` and `outcome` label
vocabularies. These counters reset on Hub restart, consistent with Prometheus
counter semantics. Job lifecycle mutations (`job_start`, `job_stop`,
`job_checkpoint`, `job_savepoint`) are additionally audited with actor,
correlation, outcome, and failure-code metadata; audit history is retained
within a bounded window (30 days, 100k records) and queryable at
`/api/v1/audit`.

Readiness is deliberately stricter than liveness. A live process can return
`200` from `/liveness` while `/readiness` returns `503` during startup recovery
or storage failure. Alert on readiness, reconciliation failures, stale nodes,
and growing outbox age rather than restarting solely on a transient scrape
failure.

For a rolling deployment, an authorized operator should POST
`/api/v1/nodes/{node_id}/drain`, wait for active Attempts to settle, deploy the
Agent, and resume with DELETE
`/api/v1/nodes/{node_id}/maintenance`. Use POST to the maintenance route for a
longer planned outage. These transitions preserve desired state and produce
`node_maintenance_changed` audit events containing actor and correlation
metadata. Reconciliation suppresses new dispatch while draining or in
maintenance, but does not cancel in-flight work.

Rollback disables the operational mutation or readiness policy at the proxy
or deployment layer. It does not delete desired state, event history, or audit
records, and the Hub performs no automatic destructive remediation.
