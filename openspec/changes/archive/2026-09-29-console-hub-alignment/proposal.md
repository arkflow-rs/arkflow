# console-hub-alignment

## Why

The web console grew out of the original node-local control plane (#1200) and was later retargeted at the Hub, but a field- and data-source-level audit on `main` (ff5182a) shows the alignment was never completed. Concretely, against a healthy Hub the console today:

- polls `GET /api/v1/status`, which exists only on the agent-local router (`crates/arkflow-server/src/lib.rs:266`); the Hub router (`lib.rs:400-493`) has no such route, so the overview's live query errors every cycle (`console/src/queries.ts:45-47`) and the shell permanently shows the stale-data banner (`console/src/app.tsx:127,258`);
- parses `GET /api/v1/metrics` as JSON (`console/src/api.ts:316,321-322`) while the Hub returns Prometheus text exposition (`text/plain; version=0.0.4`, `lib.rs:2974`) and ignores the `node_id` filter (`lib.rs:2882`), leaving the overview metrics card permanently empty (`console/src/features/overview.tsx:30`);
- offers Configuration-page Validate and Compare actions that always hit global `/configuration/validate` and `/configuration/diff` (`api.ts:345-351`) — routes the Hub does not serve (only node-scoped proxies exist, `lib.rs:435-447`) — so both are unconditional 404s in Hub mode;
- renders Job-detail metrics of which four (`state_bytes`, `recovery_progress`, `task_pressure`, `partition_health`) are structurally always `0`: the agent never emits them (`crates/arkflow-server/src/agent.rs:619-641`), the Hub allowlist would strip them (`crates/arkflow-server/src/hub/nodes.rs:5-38`), and the emitting struct `JobMetricsSnapshot` is never constructed (`crates/arkflow-core/src/job.rs:84-93`); the remaining three are cluster-wide sums over all nodes and jobs (`hub.metrics(None)`, `lib.rs:1259`; `hub/observability.rs:307-319`), not values of the displayed Job;
- renders a Tasks tab whose `state` is hardcoded `queued` (`job.rs:1088,1135`) and whose `attempt_id` column never exists on `TaskAttempt`, so it always shows `—` (`console/src/features/jobs.tsx:429`);
- ignores the HA leadership block the Hub already returns (`"ha": {enabled, role, epoch, transitions}`, `lib.rs:818-830`) and has no handling for the standby 503 `hub_standby` error (`lib.rs:393-399,500-521`) beyond a generic failure banner;
- provides no editor fields for the distributed-runtime spec fields `rescale` (`job.rs:415`) and `resources` quota declarations (`job.rs:428`), leaving #1264's distributed rescale and resource-quota features reachable only by hand-writing JSON.

## What Changes

- **Hub fleet status endpoint**: the Hub serves the EngineStatus-shaped aggregate at `/status` (fleet-wide stream totals from the node registry), so the console overview works unchanged in Hub mode.
- **Hub JSON metrics for the console**: `GET /metrics` on the Hub negotiates content — Prometheus text remains the default for scrapers; JSON `{items, aggregate}` (honoring `node_id`) is returned when the client requests it (Accept header or `format=json`), backed by the existing `Hub::metrics()` aggregation.
- **Hub configuration validate/diff**: the Hub gains node-scoped `/nodes/{node_id}/configuration/validate` and `/nodes/{node_id}/configuration/diff` proxies, and the console routes Validate/Compare through the selected node in Hub mode.
- **Honest Job-detail metrics**: the Job-detail metrics object carries only keys with a real measurement source and reflects the displayed Job's scope (per-job aggregation from job-scoped observation, not a fleet-wide sum); the console stops rendering fabricated zeros.
- **Observed task state in Job detail**: job-detail tasks reflect observed attempt state reported by agents (not the always-`queued` desired placement), and the console drops the nonexistent `attempt_id` column.
- **HA-aware console**: the console types and renders the `/system` `ha` block (role, epoch, transitions, online nodes) and maps the 503 `hub_standby` error to an explicit "connected to a standby Hub — retry against the elected leader" state instead of the generic stale banner.
- **Editor coverage for distributed spec fields**: the Job editor exposes `rescale` and `resources` (per-task request quotas) fields with validation.
- **Wire-type hygiene**: console types drop fields no Hub endpoint ever produces (`ControlNode.role`, `ControlNode.uptime_seconds`, `Operation.resource_type`, `ControlEvent.failure_class`, `ControlEvent.generation`, `Job.spec`); action provenance records the authenticated operator principal instead of the literal `"operator"` — rollout create/action, job upgrades and upgrade actions, node-targeted commands, desired-state mutations, and node maintenance.

Note: removing the four unmeasured keys from the Job-detail metrics object is a wire-shape change for that endpoint; the keys have never carried a non-default value, so no consumer can lose information.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `control-plane-service`: the Hub SHALL serve a fleet-aggregated `/status` resource with the EngineStatus contract, in addition to `/system`.
- `control-plane-observability`: the Hub SHALL serve its fleet metrics aggregate in JSON to console clients (content-negotiated, `node_id`-filtered) while keeping Prometheus text as the scrape default; Job-detail diagnostics SHALL be job-scoped and SHALL NOT include keys without a measurement source.
- `configuration-management`: configuration validation and version diff SHALL be reachable through the Hub (node-scoped proxy), so the console Configuration workflow is complete in Hub mode.
- `distributed-job-runtime`: job-detail task listings SHALL reflect observed task-attempt state reported by compute nodes rather than desired placement state.
- `control-plane-console`: the console SHALL operate correctly against a Hub (overview without local-only endpoints, node-scoped configuration actions, HA role/standby presentation, editor fields for `rescale`/`resources`, no fabricated metric values).

## Impact

- `crates/arkflow-server/src/lib.rs` (hub router: new `/status`, metrics content negotiation, node-scoped config validate/diff proxies, job-detail metrics scoping, rollout actor), `hub/observability.rs`, `hub/jobs.rs`, `hub/nodes.rs` (allowlist), `api_contract.rs` (new wire shapes).
- `console/src/api.ts`, `queries.ts`, `app.tsx`, `features/{overview,configuration,jobs,job-editor,runtime}.tsx`, `i18n/{en,zh}.ts`.
- Docs: `docs/docs/operate/control-plane/` pages for the new/changed endpoints, both en and zh-Hans trees (per documentation workflow gates).
- No changes to agent-local control-plane routes (backward compatible), no auth/TLS changes, no console redesign.

## Non-goals

- Implementing full measurement pipelines for `state_bytes`, `recovery_progress`, `task_pressure`, `partition_health` (state-backend instrumentation) — this change removes the fabricated values; real measurement is future work.
- Redesigning console information architecture, theming, or i18n beyond the strings the new states require.
- Changing agent-local (`router()`) endpoints, OIDC flows, or the standby allowlist semantics.
- Streaming job metrics over SSE or adding new event types.
- Reconciling the legacy `control-console` / `fleet-control-console` spec stubs (separate cleanup).
