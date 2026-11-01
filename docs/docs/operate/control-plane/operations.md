---
sidebar_position: 4
description: ArkFlow documentation page.
---

# Control-plane operations

The Hub exposes process liveness at `/liveness`, storage/recovery readiness at
`/readiness`, bounded diagnostics at `/api/v1/operations/status`, and
Prometheus exposition at `/api/v1/metrics`. Put the operational routes behind
the configured operator-token boundary or an authenticated monitoring proxy;
never publish bearer tokens, configuration payloads, node IDs as metric
labels, or error text as labels.

`/api/v1/metrics` is content-negotiated: the default response stays the
Prometheus text exposition above, while an explicit `Accept: application/json`
(or `?format=json`) returns the JSON aggregate `{"items": [{"node_id",
"metrics"}], "aggregate"}` that the web console renders; the `node_id` query
filter is honored on both branches. The Hub also serves `GET /api/v1/status` —
a fleet-aggregated engine status (stream totals summed over registered nodes,
plus Hub version and uptime) — so console clients see one overview contract in
local and Hub mode. Job detail diagnostics (`GET /api/v1/jobs/{id}/detail`)
carry only measured, job-scoped gauges (`watermark_lag_ms`,
`checkpoint_duration_ms`, `checkpoint_failures`) and merge observed task
state — reported by the nodes executing the job — over the desired placement,
marking not-yet-observed tasks explicitly instead of presenting placement
state as runtime state. Read-only configuration reports are reachable through
the Hub at `POST /api/v1/nodes/{node_id}/configuration/validate` and
`GET /api/v1/nodes/{node_id}/configuration/diff?from&to`: both dispatch a
read-only node command (no version or rollout side effects) and deliver the
report on the tracked operation's `result` field.

Command dispatch metrics cover enqueue-to-acknowledgement latency
(`arkflow_command_duration_bucket`/`_count`/`_sum`) and per-outcome counters
(`arkflow_command_total`) with fixed `command` and `outcome` label
vocabularies. These counters reset on Hub restart, consistent with Prometheus
counter semantics. Job lifecycle mutations (`job_start`, `job_stop`,
`job_checkpoint`, `job_savepoint`) are additionally audited with actor,
correlation, outcome, and failure-code metadata; audit history is retained
within a bounded window (30 days, 100k records) and queryable at
`/api/v1/audit`.

Job pause and resume are part of the same audit and metrics surface. Both
mutations are audited as `job.pause` / `job.resume` with the actor, target Job,
outcome (`accepted`, `rejected`, or `failed`), and the stable problem code on
rejection; a stop that settles a paused Job is audited as `job.stop`. Every
attempt also feeds `arkflow_job_pause_total{outcome}`, whose label vocabulary
is closed: `requested` (pause accepted), `paused` (quiesce converged),
`resumed` (resume accepted), `failed` (capture aborted or the capture deadline
expired), and `rejected` (mutation refused). Phase transitions are broadcast on
the event stream (`GET /api/v1/events/stream`) as `job.pause_requested`,
`job.pause_retrying`, `job.paused`, `job.resume_requested`, `job.resumed`, and
`job.pause_failed`, and the pause phase, artifact reference, timestamp, and
actor are visible on the Job record and detail. Because the pause phase and its
retention pin are durable state, both mutations are unavailable on a volatile
Hub, which answers `503 pause_requires_durable_storage`.

Identity administration participates in the same audit surface: `user.create`,
`user.update`, `user.delete`, `user.revoke_tokens`, `credential.create`,
`credential.rotate`, and `credential.revoke` record the actor, target
user/credential, and outcome (`accepted`/`rejected`) with scalar metadata
only — token material and secret hashes never enter audit records. The
60-second maintenance task also prunes expired and revoked `cp_sessions` rows,
keeping the browser-session table bounded.

Durable reconciliation history is bounded the same way:
terminal operation records, processed outbox rows, and terminal attempt
records are reclaimed after 24 hours or beyond a 4096-row count bound;
pending/failed checkpoint records are reclaimed after 24 hours; events are
kept to the newest 2048 rows. Unprocessed outbox rows and active attempts are
never reclaimed; a dedicated 60-second maintenance task runs these retention
sweeps so they do not contend with the per-second reconciliation tick.

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
