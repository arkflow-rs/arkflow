# Tasks — console-hub-alignment

## 1. Hub: fleet status and JSON metrics

- [x] 1.1 Add `GET /status` to `hub_router()` (`crates/arkflow-server/src/lib.rs`): EngineStatus shape with stream totals summed from `HubNode.streams_*` gauges, Hub version/uptime; standby path already 503s via the lease middleware. Add the wire shape to `api_contract.rs`.
- [x] 1.2 Add a hub contract test asserting `/status` totals equal the registry sum and that a standby Hub returns 503 `hub_standby`.
- [x] 1.3 Teach `hub_metrics` content negotiation: `Accept: application/json` or `?format=json` returns `{items, aggregate}` from `Hub::metrics_by_node()`/`Hub::metrics()`; default stays Prometheus text byte-identical; honor `node_id` on both branches (currently parsed and ignored).
- [x] 1.4 Add contract tests: scraper-style request (no Accept / Prometheus Accept) keeps `text/plain; version=0.0.4`; JSON request returns items/aggregate; `node_id` filters items in both branches.

## 2. Hub: node-scoped configuration validate/diff

- [x] 2.1 Add `POST /nodes/{node_id}/configuration/validate` and `GET /nodes/{node_id}/configuration/diff` to `hub_router()`, dispatching read-only node commands through the existing channel used by apply; unknown/offline node → `node_unavailable` problem; no version/intent side effects.
- [x] 2.2 Add hub tests covering validate success/failure passthrough, diff passthrough, and the offline-node rejection.

## 3. Job-scoped diagnostics (agent report + hub detail)

- [x] 3.1 Extend the job observation wire type (`hub/wire.rs`) with an optional allowlisted `metrics` map (`watermark_lag_ms`, `checkpoint_duration_ms`, `checkpoint_failures`) and a bounded `tasks: [{task_id, state}]` list; keep both optional and backward compatible.
- [x] 3.2 Agent side (`agent.rs`): per-job kernel metrics and running attempt states populate the observation; per-job aggregation replaces the node-wide `JobRuntime::metrics()` keys for these three gauges.
- [x] 3.3 Hub side: store per-job observation metrics/task states; `hub_job_detail` emits per-job metrics with only measured keys (delete the fixed 7-key `unwrap_or_default` block at `lib.rs:1280-1288`) and merges observed state over `assignments_for_nodes` with an `observed` flag.
- [x] 3.4 Remove the never-constructed `JobMetricsSnapshot` (`crates/arkflow-core/src/job.rs:84-93`) and any references.
- [x] 3.5 Add tests: two jobs on one node report disjoint gauges; unobserved task falls back to placement state marked not observed; metrics object omits unmeasured keys.

## 4. Hub: rollout actor provenance

- [x] 4.1 Capture the authenticated operator principal id into `RolloutRecord.actor` for create and action (`lib.rs` create_rollout / rollout_action), replacing the literal `"operator"`; update affected tests.
- [x] 4.2 Extend principal capture to the remaining audited actions: job upgrade create/action, node-targeted stream commands, desired-state mutations, and node drain/maintenance/resume (CR follow-up).

## 5. Console: Hub-mode data sources and HA states

- [x] 5.1 Type `SystemResource.online_nodes` and `ha?: {enabled, role, epoch, transitions}` in `console/src/api.ts`; render a leader/standby badge with epoch on the overview when `ha` is present.
- [x] 5.2 Handle the 503 `hub_standby` problem code in `request()`/app shell: dedicated standby banner (names the condition, advises the elected leader), polling continues and the banner self-clears; en + zh i18n strings.
- [x] 5.3 Verify the overview against a Hub has zero failing live queries (no `/status`-class 404s, metrics card populated from JSON); extend `console/src/api.test.ts` / `app.test.tsx` with a Hub-mode fixture.
- [x] 5.4 `api.validateConfig`/`api.diff` gain a `nodeId` parameter and use node-scoped routes; the Configuration page addresses compare/rollback (and any validation/publish request) to the selected node in Hub mode, keeps the draft-editing workflow local-only (node snapshots render read-only, as before), and presents the draft workflow as explicitly local-only instead of a load error; tests for both modes.

## 6. Console: honest job detail and editor fields

- [x] 6.1 Job detail renders only the metric keys present in the API response; remove the four fabricated gauges from `features/jobs.tsx` and their i18n labels.
- [x] 6.2 Tasks tab: drop the never-populated `attempt_id` column; render observed state with a distinct not-observed treatment; tests with an observed/unobserved fixture.
- [x] 6.3 Job editor: add `rescale` checkbox and `resources.cpu_millicores`/`memory_bytes` inputs (absent-when-empty serialization, round-trip from persisted spec, included in validation payload); en + zh labels; editor tests.

## 7. Console: wire-type hygiene

- [x] 7.1 Remove dead fields from `console/src/api.ts` types (`ControlNode.role`, `ControlNode.uptime_seconds`, `Operation.resource_type`, `Operation.result`, `ControlEvent.failure_class`, `ControlEvent.generation`, `Job.spec`) and the dead `intent_state === 'cancelled'` branch in `waitForOperation`; confirm no component references remain.

## 8. Documentation

- [x] 8.1 Update `docs/docs/operate/control-plane/` API pages (en) for `/status`, metrics content negotiation, node-scoped validate/diff, job-detail metrics/task semantics, rollout actor; classify any new ```yaml/http fences per the docs snippet rules.
- [x] 8.2 Mirror every change in the zh-Hans tree (`docs/i18n/.../operate/control-plane/`).
- [x] 8.3 Note the removed job-detail metrics keys and standby banner in release/upgrade notes if the project's release-notes convention requires it.

## 9. Verification

- [x] 9.1 `cargo test --workspace --all-targets` and `cargo clippy --workspace --all-targets` clean.
- [x] 9.2 Console checks from `console/`: `npm test` (vitest) and build clean.
- [x] 9.3 `pnpm docs:check` passes (en + zh pages, anchors, snippet classification).
