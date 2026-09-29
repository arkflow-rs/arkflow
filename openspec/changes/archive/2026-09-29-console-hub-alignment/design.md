# Design — console-hub-alignment

## Context

The console speaks one API client (`console/src/api.ts`) against two historically different routers: the agent-local `router()` (`lib.rs:262-321`, Engine-centric: `/status`, JSON-era `/metrics` assumptions, global `/configuration*`) and the Hub `hub_router()` (`lib.rs:388-536`). In Hub deployments the console cannot reach agent-local routes at all — agents hold only outbound sessions to the Hub — so every local-only endpoint the console still calls is a guaranteed 404, and every idealized frontend type without a backend producer renders fabricated data. The misalignments are enumerated with file:line evidence in `proposal.md`; this design fixes them with additive server surface plus surgical console changes.

Constraints that shape the design:

- Prometheus scrapers point at Hub `/metrics` today; their config must keep working unchanged.
- The Hub's node view is built from agent reports (`/agent/report`, `/agent/job-observations`) — the Hub cannot query nodes synchronously; it only dispatches commands and reads the last report.
- Wire types shared by handlers and contract tests live in `api_contract.rs` ("stable v1 operator HTTP wire shapes").
- Console gating: `pnpm docs:check` + i18n (en/zh) strings for any new UI state.

## Goals / Non-Goals

**Goals:**

- Every endpoint the console calls works identically in local and Hub mode.
- Job-detail diagnostics are scoped to the displayed Job and contain only measured values.
- HA topology (leader/standby) is visible and standby connections explain themselves.
- The distributed-runtime spec fields `rescale` and `resources` are editable without raw JSON.

**Non-Goals:**

- Measuring `state_bytes`/`recovery_progress`/`task_pressure`/`partition_health` (removed, not implemented).
- Console IA/visual redesign; agent-local route changes; OIDC/TLS changes; metrics-over-SSE.
- Exposing `rebalance` policy in the editor (follow-up; same pattern as `resources`).

## Decisions

### D1 — Hub serves `/status` as a fleet aggregate (not console-side synthesis)

Add `GET /system`-adjacent `GET /status` on `hub_router()` returning the existing `EngineStatus` shape: `streams_total/running/failed` summed from the node registry gauges (`HubNode.streams_*`, already maintained by report ingestion), `state` from leadership (`leader` → `running`, standby serves 503 before the handler anyway), `uptime_seconds` from Hub process start, `version` from `CARGO_PKG_VERSION`.

*Alternative considered*: console derives overview totals from `/system` + `/streams`. Rejected — the shell spec already promises "the same node collection contract in local and Hub mode", a server-side aggregate benefits non-console consumers, and it removes the failing-query class entirely instead of masking it.

### D2 — Content-negotiated `/metrics`; Prometheus stays the default

`hub_metrics` inspects `Accept: application/json` (with `?format=json` as an explicit override) before encoding. JSON clients get `{items: [{node_id, metrics}], aggregate}` serialized from the existing `Hub::metrics_by_node()` / `Hub::metrics()` (`hub/observability.rs:307-353`) — no new aggregation logic. The `node_id` query filter is honored on both branches (today it is parsed and ignored, `lib.rs:2882`). Default (no Accept, scraper UA) behavior is byte-identical Prometheus text.

*Alternative considered*: a separate `/metrics/json` route. Rejected — route proliferation and a second thing for scrapers to accidentally hit; negotiation keeps every existing scraper config untouched.

### D3 — Hub-proxied configuration validate/diff, node-scoped

Add `POST /nodes/{node_id}/configuration/validate` and `GET /nodes/{node_id}/configuration/diff?from&to` to `hub_router()`. Both dispatch a bounded read-only node command through the existing command channel (same path `apply` already uses) and return the node's `ConfigValidationReport` / `ConfigDiff`; unknown or offline node → the standard `node_unavailable` problem. Console `api.validateConfig`/`api.diff` gain the `nodeId` parameter and always use the node-scoped route in Hub mode; draft remains local-mode-only (`enabled: !nodeId`, unchanged semantics — Hub has no fleet draft store and should not grow one).

*Alternative considered*: console requires node selection and talks to agent-local endpoints directly. Rejected — architecturally impossible in Hub mode (no inbound path to agents); validation must execute where the component registry lives.

### D4 — Job-scoped metrics via the existing per-job report path, keys limited to measured ones

Implementation outcome (simpler than the original sketch, which proposed extending the `/agent/job-observations` wire type): the Agent already reports per-job kernel metric snapshots on every `NodeReport.jobs` (`BTreeMap<job_id, KernelMetricsSnapshot>`, `agent.rs` `job_snapshots()`), and the Hub already stores them per node. `hub_job_detail` therefore aggregates that existing map across live-lease nodes — max for `watermark_lag_ms`/`checkpoint_duration_ms` (a max is never summed across nodes), sum for `checkpoint_failures` — and emits **only keys with a measurement source** (`Hub::job_detail_metrics`, `hub/observability.rs`; the fixed 7-key `unwrap_or_default()` list at `lib.rs` is deleted). The four unmeasured keys disappear from the wire and from the Jobs detail; `JobMetricsSnapshot` (`core/job.rs`, never constructed) is removed.

*Alternative considered*: keep the keys with `null`. Rejected — a null/0 split is unexplainable in a gauge card; the keys have never held a real value. *Alternative considered*: filter `hub.metrics(None)` to the job's `selected_nodes`. Rejected — still node-wide, mixing other jobs' kernels on shared nodes.

### D5 — Observed task state rides the node report

The Agent reports the task ids each running Job kernel executes (`JobRuntime::job_tasks()` → `NodeReport.job_tasks: BTreeMap<job_id, Vec<task_id>>`, serde-defaulted so older Agents simply omit it; presence of a task id is the node's observation that the task runs there). `hub_job_detail` merges observed state over `assignments_for_nodes` output (desired placement stays the fallback for unobserved tasks, and a boolean `observed` flag distinguishes them). Console Tasks tab renders `state` (now real) and drops the never-populated `attempt_id` column.

*Alternative considered*: extend `/agent/job-observations` (`{job_id, generation, state, error}`) instead. Rejected during implementation — that path fires only when a job task finishes, so it cannot carry liveness; the periodic node report is the right vehicle. *Alternative considered*: a dedicated `/jobs/{id}/tasks` endpoint. Rejected — new polling surface for data that already flows on the report path.

### D6 — HA surfaced, standby explained

`SystemResource` gains `online_nodes` and `ha?: {enabled, role, epoch, transitions}` (Hub-only fields; local mode leaves them absent and the UI hides the badge). Overview renders a leader/standby badge with epoch. `request()` in `api.ts` recognizes the 503 `hub_standby` problem code and the app shell renders a dedicated banner ("connected to a standby Hub — retrying; point the console at the elected leader if it persists") instead of the generic stale-data banner; polling continues unchanged so the banner self-clears when this Hub wins the lease.

### D7 — Editor fields for `rescale` / `resources`

`job-editor.tsx` adds: a `rescale` checkbox (rendered with its actual semantics: opt-in to state redistribution when parallelism/task set changes) and a `resources` group with `cpu_millicores` / `memory_bytes` numeric inputs matching `JobResourceSpec` (`core/job.rs:428+`, both optional, `skip_serializing_if` — empty inputs serialize nothing). Values participate in the existing server-side validation (`/jobs/validate` already deep-validates the spec).

### D8 — Rollout actor from the authenticated principal

`hub_create_rollout` / `rollout_action` (`lib.rs:2775`, `2865`) already authorize through the operator principal; capture its id into `RolloutRecord.actor`, replacing the literal `"operator"`.

## Risks / Trade-offs

- [Prometheus scrapers get JSON after the change] → Mitigation: JSON only on explicit `Accept: application/json` / `?format=json`; default branch untouched; contract test pins both branches.
- [Job metrics become per-job and "smaller" than before (cluster sums)] → Mitigation: that is the correctness fix; fleet totals remain available from the D2 JSON aggregate the overview already renders.
- [Removing the four metrics keys is a wire change] → Mitigation: keys were structurally always `0`; called out in the proposal; released in one step since no information can be lost.
- [Observation payload growth (per-job metrics + task states)] → Mitigation: bounded allowlist (3 metric keys), task list bounded by plan size, map-not-list encoding for metrics; observation cadence unchanged.
- [Standby banner noise during normal failover] → Mitigation: banner is advisory, polling continues, self-clears on lease acquisition; 10s OIDC-style redirect guard not involved (no navigation).
- [Validate/diff dispatch adds Hub→agent command traffic] → Mitigation: read-only, operator-triggered, bounded payloads, routed through the existing expiry/retry command machinery.

## Migration Plan

Single release, ordered so the tree stays green at every step:

1. Hub: `/status`, metrics negotiation, node-scoped validate/diff, observation extensions, job-detail scoping, rollout actor (+ contract tests).
2. Console: switch data sources, add HA/standby states, editor fields, type cleanup (+ vitest).
3. Docs: en + zh-Hans control-plane pages for the changed endpoints and console states.

Rollback: all server surface is additive except the job-detail metrics key removal and actor semantics; reverting the console commit alone restores the previous rendering (zeros included) against either server version.

## Open Questions

- None blocking. (Editor `rebalance` exposure and real measurement of the removed metric keys are recorded as follow-ups in the proposal's Non-goals.)
