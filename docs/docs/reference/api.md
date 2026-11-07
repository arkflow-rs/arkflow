---
sidebar_position: 20
title: HTTP API reference
description: Complete route reference for the node API and the Hub operator/agent API (/api/v1).
---

# HTTP API reference

The control plane exposes three API surfaces from one binary role-dependent
service:

| Surface | Base path | Audience |
|---------|-----------|----------|
| Node API | `/` | A single ArkFlow node: local stream lifecycle, configuration, events, metrics. |
| Hub operator API | `/api/v1` | Fleet operators and the web console: jobs, nodes, rollouts, audit. |
| Hub agent API | `/api/v1/agent/*` | Compute nodes (Agents): registration, heartbeats, reports, command pull. |

Health, readiness, and liveness probes (configurable paths) plus `GET
/metrics` (Prometheus text format) are served on both node and Hub services.

The Hub operator API is a **desired-state API**: a successful HTTP mutation
means an Intent was durably accepted — it does not mean a node has executed
the command or that a stream has converged. Poll the resource or subscribe to
events to observe convergence.

## Conventions

### Principal echo

`GET /system` on both the Hub plane and the local plane includes an optional
`principal` object describing the principal authenticated for the current
request:

```json
{
  "id": "ops",
  "roles": ["operator"],
  "actions": ["read", "operate", "configure", "manage_nodes", "manage_rollouts", "manage_webhooks"]
}
```

`actions` is derived from the same predicate that enforces mutations — the
echo cannot claim an action the enforcing check would deny. Requests without
valid credentials are rejected (401) before any identity is echoed;
`principal: null` is a defensive shape for a response produced without a
resolvable principal. The local standalone plane always echoes its single
all-powerful operator as `operator (admin)`. The field is additive: consumers
that do not know it ignore it without behavior change. The `manage_users`
action appears only for `admin` principals and gates the
[user and credential administration](#users-and-credentials) routes.

### Resource model

A stream has three independent views:

```text
desired       operator target: state, generation, config_version
observed      latest node report: state, generation, config_version
convergence   comparison: unknown, pending, applying, in_sync, degraded, blocked
```

Operation records additionally expose `intent_id`, `attempt_id`, generation,
retry metadata, failure classification, and the latest observed result.

### Optimistic concurrency and idempotency

Canonical lifecycle mutation:

```http
PUT /api/v1/nodes/{node_id}/streams/{stream_id}/desired-state
Authorization: Bearer <operator-token>
Content-Type: application/json
If-Match: "generation-3"
Idempotency-Key: orders-desired-4
X-Correlation-ID: request-123

{"state":"running","config_version":"cfg-17"}
```

The Hub returns `202 Accepted` with `Location` pointing to the operation and
an `ETag` for the new generation:

```json
{
  "operation_id": "intent-4-17",
  "intent_id": "intent-4-17",
  "node_id": "node-a",
  "stream_id": "orders",
  "generation": 4,
  "desired_state": "running",
  "config_version": "cfg-17",
  "convergence": "pending"
}
```

- `If-Match` is a compare-and-swap guard: an old generation returns `412` with
  `generation_conflict`.
- `Idempotency-Key` deduplicates retries for the same principal, resource, and
  request body; reusing a key with another body returns `409` with
  `idempotency_key_reused`.
- A node being offline does **not** reject a desired-state write; the Intent is
  dispatched when the node reconnects.

### Pagination

Paginated collection endpoints (`/nodes`, `/streams`, `/operations`,
`/events`, `/audit`) return:

```json
{"items": [], "page": 1, "page_size": 50, "total": 0}
```

`page_size` is bounded to 100. The remaining collection endpoints (`/jobs`,
`/rollouts`, `/config-approvals`, `/users`, and `/users/{id}/credentials`)
return plain arrays, and `GET /webhooks`
wraps its list in `{"subscriptions": [...]}`. Operation listing supports `node_id`,
`resource_id`, `operation`, `state`, and `correlation_id` filters. Use
`intent_id` and `generation` as stable reconciliation references instead of
assuming that a command ID represents final success.

### Problem envelope

Errors use a stable `code`, human-readable `message`, echoed `correlation_id`,
and optional machine-readable `details`:

```json
{
  "code": "generation_conflict",
  "message": "Expected generation 3, current generation 4",
  "correlation_id": "request-123",
  "details": {
    "expected_generation": 3,
    "current_generation": 4,
    "resource": {"node_id": "node-a", "stream_id": "orders"}
  }
}
```

### Rate limiting

Both control planes enforce a per-credential request rate limit over the
versioned API; health probes, `GET /metrics`, and the OIDC login flow are
exempt. Buckets are keyed on a truncated hash of the presented credential —
static/structured operator credentials per credential, OIDC browser sessions
per session, agent requests per node (the `Authorization: Bearer` header and
the legacy `session_token` query parameter resolve to the same bucket). A
request that exceeds its bucket is rejected before reaching any handler:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json

{"code":"rate_limited","message":"Control-plane request rate limit exceeded; retry after the indicated interval"}
```

`Retry-After` carries the whole-second refill estimate. Rejections create no
audit events and no operation records, and are exported as the
`arkflow_control_plane_rate_limit_rejections_total{plane,key_kind}` counter
family (`plane` is `local` or `hub`; `key_kind` is `principal`, `node`, or
`catch_all` — the shared bucket that absorbs traffic beyond the bounded
bucket table). Configuration lives under `health_check.rate_limit` on the node
plane (the control-API section is flattened into `health_check`) and the `ARKFLOW_RATE_LIMIT_*` environment variables on the Hub (see
[deployment](../operate/control-plane/deploy.md#rate-limiting)). Defaults are
enabled with limits far above legitimate traffic; `enabled: false` restores
the unlimited behavior exactly.

## Node API

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/system` | Static system descriptor (node identity, capabilities). |
| `GET` | `/status` | Runtime status of the node. |
| `GET` | `/nodes` | Node view (single-node deployment alias). |
| `GET` | `/node` | Node view (singular alias). |
| `GET` | `/streams` | List streams with observed state. |
| `GET` | `/streams/{id}` | One stream's resource view. |
| `POST` | `/streams/{id}/start` | Start a stream. |
| `POST` | `/streams/{id}/stop` | Stop a stream. |
| `POST` | `/streams/{id}/restart` | Restart a stream. |
| `GET` | `/operations` | List operations (`node_id`, `resource_id`, `operation`, `state`, `correlation_id` filters). |
| `GET` | `/operations/{id}` | One operation record. |
| `DELETE` | `/operations/{id}` | Cancel a pending Intent. |
| `GET` | `/events` | Query the local event log. |
| `GET` | `/configuration` (`/config`) | Current configuration. |
| `POST` | `/configuration/validate` (`/config/validate`) | Validate a configuration body. |
| `GET`/`PUT` | `/configuration/draft` | Read/save a working draft configuration. |
| `GET` | `/configuration/diff` | Diff draft against the live configuration. |
| `GET` | `/configuration/versions` (`/config/versions`) | Configuration version history. |
| `POST` | `/configuration/apply` (`/config/apply`) | Apply a configuration version. |
| `POST` | `/configuration/rollback/{id}` (`/config/rollback/{id}`) | Roll back to a previous configuration version. |
| `GET` | `/components` | Registered components. |
| `GET` | `/components/{kind}/{name}` | One component's schema and example. |
| `GET` | `/schema` | Engine configuration JSON Schema. |
| `GET` | `/metrics` | Prometheus metrics. |

## Hub operator API (`/api/v1`)

### Jobs

Jobs are managed with the same desired-state semantics as streams. A mutation
returns the updated Job record: creation and pause/resume answer
`202 Accepted`, the other lifecycle mutations answer `200 OK`. There is no
separate operation record for a Job lifecycle mutation — observe the returned
Job and its `convergence`.

| Method | Route | Purpose |
|--------|-------|---------|
| `POST` | `/jobs` | Create a job: `{"spec": ..., "desired_state": "stopped" \| "running"}`. Creation does **not** accept `paused`. |
| `POST` | `/jobs/validate` | Deep-validate a spec against the target `node_ids` before creating anything. |
| `GET` | `/jobs` | List jobs. |
| `GET` | `/jobs/{id}` (`/jobs/{id}/status`) | Observed job state. |
| `GET` | `/jobs/{id}/detail` | Assignment and convergence detail. |
| `GET` | `/jobs/{id}/versions` | Version history. |
| `GET` | `/jobs/{id}/plan` | Explain the plan: operator boundaries, partition routes, stateful operators, checkpoint policy. |
| `PUT` | `/jobs/{id}/desired-state` | Lifecycle mutation: `{"state": "running" \| "stopped" \| "paused"}`. Applies a generation compare-and-swap internally; no `If-Match` / `Idempotency-Key` headers (see [Job pause and resume](#job-pause-and-resume)). |
| `GET`/`POST` | `/jobs/{id}/checkpoints` | List recovery artifacts / trigger a barrier checkpoint. |
| `GET`/`POST` | `/jobs/{id}/savepoints` | List artifacts / trigger a savepoint. |
| `POST` | `/jobs/{id}/actions/pause` | Pause a converged running Job: capture a recovery artifact, then stop it. `202` + Job record. |
| `POST` | `/jobs/{id}/actions/resume` | Resume a paused Job from the pause artifact (generation bump + recovery start). `202` + Job record. |
| `POST` | `/jobs/{id}/upgrades` | Upgrade to a new version (job must be stopped and converged). |
| `GET` | `/jobs/{id}/upgrades/{upgrade_id}` | Upgrade status: phase, savepoint, deadline, last error. |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/actions` | Upgrade actions: `pause`, `resume`, `cancel`, `rollback`. |
| `POST` | `/jobs/{id}/upgrades/{upgrade_id}/rollback` | Roll back to a compatible savepoint. |
| `POST` | `/jobs/{id}/actions/{action}` | One-shot action: `start`, `stop`, `restart`, `pause`, or `resume`. `restart` is a generation bump that restores from the recovery artifact — not a stream-style restart. On a paused Job, `start`/`restart` mean resume and `stop` clears the pause bookkeeping. |

Recommended submit flow: `validate` first, submit with
`desired_state: "stopped"`, inspect `detail` and `plan`, then switch to
`running`. Recovery artifacts are bound to the job version and state format
version; an incompatible artifact is rejected before restore instead of
corrupting state.

#### Job pause and resume {#job-pause-and-resume}

A Job pause is a **durable quiesce**: the Hub captures a recovery artifact at
the pause generation (a checkpoint for `recovery: latest_checkpoint`, a
savepoint for `latest_savepoint`; a Job whose spec has no `checkpoint`
configuration stops directly), then stops the Job. The mutation answers `202`
with the Job record, and the Job is not reported paused until every target
assignment has been observed `stopped`.

```http
POST /api/v1/jobs/{job_id}/actions/pause
POST /api/v1/jobs/{job_id}/actions/resume

PUT /api/v1/jobs/{job_id}/desired-state
{"state": "paused"}
```

`desired_state` has three values — `running`, `stopped`, and `paused` — but Job
creation still accepts only `running` and `stopped`. On a paused Job,
`{"state": "running"}` means resume and `{"state": "stopped"}` stops it and
clears the pause bookkeeping. The desired-state route applies an internal
generation compare-and-swap and returns the updated Job record (`200`); unlike
the stream desired-state route above, it does **not** implement the
`If-Match` / `Idempotency-Key` headers.

Job responses are additive: a Job that was never paused carries none of these
fields. `GET /jobs` and `GET /jobs/{id}` return the Job record, and
`GET /jobs/{id}/detail` nests it under `job`.

| Field | Type | Meaning |
| --- | --- | --- |
| `pause_phase` | `capturing` \| `stopping` \| `paused` \| `resuming` | Durable pause state-machine phase; absent when the Job is not paused. |
| `pause_artifact_id` | string | Recovery artifact captured at the pause generation. |
| `pause_prev_checkpoint_id` | string | Recovery pointer before the pause captured its artifact; restored if the capture aborts. |
| `pause_retry_count` | integer | Bounded capture re-dispatch counter (`0` when none). |
| `paused_at_ms` | integer | When the pause was requested. |
| `paused_by` | string | Principal id that requested the pause. |

Convergence for a paused Job is phase-defined: it converges only when
`pause_phase` is `paused` and every target assignment is observed `stopped`;
`capturing`, `stopping`, and `resuming` report `reconciling`, and an offline
node keeps the pause reconciling instead of falsely converging. Resume bumps
the generation, keeps the recovery pointer on the pause artifact, and runs the
ordinary recovery start; events processed between the artifact barrier and
teardown completion are replayed (exactly-once for transactional sinks,
at-least-once for non-transactional sinks), and a Job without checkpoint
configuration resumes with a cold start. Atomic and stopped-mode upgrades are
rejected while a pause phase is present, and pause/resume are rejected while an
upgrade orchestration owns the Job (see
[Atomic job upgrades](../operate/control-plane/job-upgrades.md#pause-orchestration-vs-job)).

| Status | Code | When |
| --- | --- | --- |
| `409` | `job_not_running` | Pause requires desired and observed `running` and a converged Job. |
| `409` | `pause_in_progress` | A pause phase is already in flight, or resume arrived before the phase settled to `paused`. |
| `409` | `job_already_paused` | Pause requested for a Job already in the `paused` phase. |
| `409` | `pause_not_paused` | Resume (or a stop routed through the pause state machine) with no active pause phase. |
| `409` | `job_paused` | Upgrade rejected because a pause phase is present. |
| `409` | `pause_requires_recovery_policy` | Pause rejected because `recovery: fail` can never select a recovery artifact. |
| `409` | `pause_conflict` | A concurrent pause-state write won the phase compare-and-swap; retry. |
| `409` | `orchestration_in_progress` | Pause or resume rejected while a non-terminal upgrade orchestration owns the Job. |
| `503` | `pause_requires_durable_storage` | The Hub runs without durable storage and cannot persist a pause phase. |

Rejections are audited (`job.pause` / `job.resume`, outcome `rejected`, with
the failure code above) and counted in
`arkflow_job_pause_total{outcome="rejected"}`; phase transitions emit
`job.pause_requested`, `job.paused`, `job.resume_requested`, `job.resumed`,
and `job.pause_failed` on `GET /events/stream`. A capture failure or the
capture deadline aborts the pause, restores the pre-pause recovery pointer, and
records `pause_savepoint_failed` / `pause_savepoint_timeout` in the Job's
`last_error`.

### Nodes and streams

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/system` | Hub system descriptor, including `principal: {id, roles, actions}` echoing the authenticated caller's identity and authorized actions (additive; see [Principal echo](#principal-echo)). |
| `GET` | `/status` | Fleet-aggregated engine status: streams total/running/failed summed across registered nodes. |
| `GET` | `/nodes` | Fleet node registry. Supports a label filter: `?label=env%3Dprod%2Ctier%3Dedge` (comma-joined `key=value` terms, AND semantics). |
| `GET` | `/streams` | Streams across the fleet. |
| `GET` | `/nodes/{node_id}/streams/{id}` | Authoritative point-in-time stream view on a node. |
| `PUT` | `/nodes/{node_id}/streams/{id}/desired-state` | Canonical lifecycle mutation (see [Conventions](#conventions)). |
| `POST` | `/nodes/{node_id}/streams/{id}/{action}` | Targeted one-shot command (`start`, `stop`, ...). |
| `POST` | `/nodes/{node_id}/streams/{id}/actions/restart` | Restart action; converges only after the Agent reports the matching `action_id`. |
| `GET` | `/nodes/{node_id}/configuration` | Node configuration view. |
| `GET` | `/nodes/{node_id}/configuration/versions` | Node configuration history. |
| `POST` | `/nodes/{node_id}/configuration/validate` | Validate a configuration candidate against one node before applying. |
| `GET` | `/nodes/{node_id}/configuration/diff` | Diff two configuration versions on a node (`from`/`to` query parameters). |
| `POST` | `/nodes/{node_id}/configuration/apply` | Apply configuration to one node. |
| `POST` | `/nodes/{node_id}/configuration/rollback/{version}` | Roll node configuration back. |
| `POST` | `/nodes/{node_id}/drain` | Drain a node (evacuate assignments before maintenance). |
| `POST`/`DELETE` | `/nodes/{node_id}/maintenance` | Enter/exit maintenance mode. |
| `PUT` | `/nodes/{node_id}/labels` | Replace the node's complete label map (see [Node labels](#node-labels)). |

#### Node labels

`PUT /nodes/{node_id}/labels` takes `{"labels": {"env": "prod", "tier": "edge"}}` with
full-replacement semantics: the submitted map becomes the node's complete label
set. It requires the `ManageNodes` action (operator and admin roles); accepted
replacements and denials are audited (`node.labels`).

- Labels are Hub-managed: Agents never advertise labels, and re-registration
  or a Hub restart never changes them.
- Bounds: at most 16 labels per node; keys and values are 1-63 characters,
  alphanumeric start and end, with `-`, `_`, `.` allowed inside. Violations
  are rejected with `invalid_labels` without partially applying.
- Rollout targeting: `POST /rollouts` (and approval submission) accept
  `target_selector` — an equality map, mutually exclusive with `node_ids`. The
  Hub resolves it to an explicit node set **once, at creation time** (or
  submission time under the approval policy); an empty resolution is rejected
  with `no_matching_nodes`. Later label changes never re-target an existing
  rollout or a pending approval request. Matching reads the durable node
  records (`cp_nodes`), not the live registration map: a node that has not
  re-registered since a Hub restart still resolves, and unreachable nodes
  stay selectable — the rollout health gates handle delivery.

The legacy un-targeted stream mutation routes remain as adapters that create
the same durable Intent pipeline.

### Operations, events, audit

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/operations` | List operations fleet-wide. |
| `GET` | `/operations/{id}` | One operation record. |
| `DELETE` | `/operations/{id}` | Cancel a pending Intent (never an executed side effect). |
| `GET` | `/operations/status` | Operational status summary for dashboards. |
| `GET` | `/events` | Query the fleet event log. |
| `GET` | `/events/stream` | Server-Sent Events stream of live events. |
| `GET` | `/audit` | Bounded audit history (`?resource_id={id}`), accepted and rejected mutations. |
| `GET`/`PUT` | `/webhooks` | List / full-replace the webhook alerting subscription set (see [Webhook alerting](#webhook-alerting)). |
| `POST` | `/webhooks/{id}/test` | One synchronous test delivery of a synthetic `webhook.test` event. |

#### Webhook alerting

`GET/PUT /webhooks` manage the webhook subscription set with full-replace
semantics (at most 16 entries). Both routes (and `POST /webhooks/{id}/test`)
require the `ManageWebhooks` action — admin and operator roles — and every
accepted replacement or denial is audited (`webhook.replace`). Each entry
carries `id`, `url` (`http`/`https` only), `event_types` (prefix filters),
`enabled`, and an optional write-only `secret`: responses never echo it, an
omitted secret retains the stored one for the same id, and an explicitly
empty string clears it.

Delivery is best-effort (leader-only forwarding, bounded drop-oldest queues,
3 attempts with backoff, auto-pause after 5 consecutive failed deliveries
publishing `webhook.paused`/`webhook.resumed` lifecycle events). Signed
deliveries carry `X-Arkflow-Signature` (hex HMAC-SHA256 of the body). See
[Webhook alerting](/docs/operate/control-plane/webhook-alerting) for the
full contract.

`DELETE /operations/{intent_id}` cancels an Intent, not an already executed
side effect: pending work is suppressed before dispatch; after dispatch, the
Attempt outcome remains visible.

Audit records carry actor, target resource, node, correlation ID, outcome
(`accepted`/`rejected`), and a stable failure code (`node_unavailable`,
`capacity`, `incompatible_capability`, `expired`). Messages contain scalar
operation metadata only — never credentials or job configuration bodies.

### Users and credentials {#users-and-credentials}

The Hub serves a managed user directory in Hub mode when it has durable
storage. Every route below requires the **`manage_users`** action — held by
`admin` only — and answers `401` without a valid credential and `403` for
everyone else. On a volatile Hub (no `ARKFLOW_HUB_STORAGE`) the whole group
answers `503 identity_store_unavailable`.

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/users` | List users (plain array). |
| `POST` | `/users` | Create a user: `{user_id, display_name?, role, scopes?, oidc_subject?}` → `201` User. |
| `GET` | `/users/{id}` | One user (`404 user_not_found` when absent). |
| `PATCH` | `/users/{id}` | Partial update: `display_name`, `role`, `scopes`, `disabled`, `oidc_subject` (an empty string clears the OIDC binding). |
| `DELETE` | `/users/{id}` | `204`; cascades the user's credentials and browser sessions. |
| `GET` | `/users/{id}/credentials` | Credential metadata only (plain array) — never the token or hash. `404 user_not_found` when the user does not exist. |
| `POST` | `/users/{id}/credentials` | Issue: `{label?, expires_at_ms?}` → `201 {credential, token}`; the plaintext `token` is returned **only** here. |
| `POST` | `/credentials/{id}/rotate` | `200 {credential, token}`; the old credential is revoked in the same storage mutation. |
| `DELETE` | `/credentials/{id}` | Idempotent revoke → `200 {"revoked": true\|false}`. |
| `POST` | `/users/{id}/revoke-tokens` | `200 {revoked_sessions}`; raises the user's JWT revocation threshold and deletes their browser sessions. |

A user record:

```json
{
  "user_id": "alice",
  "display_name": "Alice",
  "role": "admin",
  "scopes": [{"resource_type": "node", "resource_id": "node-a"}],
  "disabled": false,
  "oidc_subject": "00u1abc",
  "tokens_valid_after_ms": 0,
  "created_at_ms": 1767225600000,
  "updated_at_ms": 1767225600000
}
```

`role` is one of `admin`, `operator`, `viewer` (one role per user); `scopes`
narrow it with the same `{resource_type, resource_id?}` grammar used by
static credentials. User ids are 1–64 characters of `[a-z0-9._-]` starting
with a lowercase letter or digit. The directory holds at most 1024 users and
at most 16 active credentials per user, and an `oidc_subject` can be bound to
only one user.

A credential view and an issuance response:

```json
{"credential_id":"3f6a1c9e2b7d4a80","label":"alice-laptop","created_at_ms":1767225600000,"expires_at_ms":null,"last_used_at_ms":null,"revoked_at_ms":null}
```

```json
{"credential":{"credential_id":"3f6a1c9e2b7d4a80","label":"alice-laptop","created_at_ms":1767225600000,"expires_at_ms":null,"last_used_at_ms":null,"revoked_at_ms":null},"token":"ark_3f6a1c9e2b7d4a80_<secret>"}
```

Use the issued token as `Authorization: Bearer <token>`. The Hub stores only
`SHA-256(credential_id ‖ 0x00 ‖ secret)` and cannot display the plaintext
again; `last_used_at_ms` is `null` on this release (usage timestamps are
reserved in the shape but not yet recorded). Revocation takes effect on the
credential's next request. `tokens_valid_after_ms` is the JWT revocation
threshold: federated tokens for a bound subject with an older `iat` are
rejected (60-second clock-skew tolerance).

Behavioral invariants:

- The credentials resolve in this order: static operator credential → managed
  `ark_` credential → browser session → OIDC JWT. A managed token that fails
  any check is a hard `401` and never falls back to OIDC; identity storage
  errors deny the request.
- Disabling, deleting, or `revoke-tokens` on a user invalidates their
  sessions immediately. `revoke-tokens` does **not** revoke managed
  credentials — revoke or rotate them individually.
- The last enabled admin cannot be disabled, deleted, or demoted
  (`409 last_admin_required`) unless a static operator credential is
  configured as break-glass.
- Accepted and rejected identity mutations are audited as `user.create`,
  `user.update`, `user.delete`, `user.revoke_tokens`, `credential.create`,
  `credential.rotate`, and `credential.revoke`, with scalar metadata only.
  A rejection writes the same action with `outcome=rejected`, the real actor,
  and the stable problem code as `failure_code` (for example
  `last_admin_required`, `credential_limit`, `user_not_found`).

| Status | Code | When |
|--------|------|------|
| `400` | `invalid_user` | Malformed user id, unknown role, oversized name/label, unparsable scopes, or an empty `PATCH`. |
| `400` | `invalid_credential` | Rotating an already-revoked credential. |
| `403` | `forbidden` | Authenticated principal without `manage_users`. |
| `404` | `user_not_found` / `credential_not_found` | Unknown user or credential — including a credentials listing for a user that does not exist. |
| `409` | `last_admin_required` | The last enabled admin would be disabled, deleted, or demoted with no static break-glass credential. |
| `409` | `user_limit` / `credential_limit` | 1024 users exist / the user has 16 active credentials. |
| `409` | `oidc_subject_conflict` | The OIDC subject is already bound to another user. |
| `503` | `identity_store_unavailable` | The Hub has no durable storage. |

See [Users and credentials](../operate/control-plane/users-and-credentials.md)
for the operational lifecycle.

### Rollouts

| Method | Route | Purpose |
|--------|-------|---------|
| `GET`/`POST` | `/rollouts` | List rollouts / create a rollout plan. Target selection is `node_ids` **or** `target_selector` (label equality map resolved once at creation; see [Node labels](#node-labels)). |
| `GET` | `/rollouts/{id}` | Rollout state and per-node progress. |
| `POST` | `/rollouts/{id}/actions` | Rollout actions: pause, resume, cancel, rollback. |

When `control_api.approval_required` is enabled on the Hub, the creation
routes (`POST /rollouts` and the node-level apply/rollback routes) reject
direct creation with an `approval_required` problem and publishing goes
through approval requests instead:

| Method | Route | Purpose |
|--------|-------|---------|
| `GET`/`POST` | `/config-approvals` | List approval requests (`?status=pending\|approving\|approved\|rejected\|expired`) / submit one pinning a version, targets, and batch size. Accepts `target_selector` like `POST /rollouts`; the request pins the selector resolved to its explicit node set at submission time. |
| `GET` | `/config-approvals/{id}` | One approval request and its pinned snapshot. |
| `POST` | `/config-approvals/{id}/decide` | Approve or reject (`{"decision":"approve"\|"reject","note":"..."}`). Requires the `ApproveRollouts` permission (admin role only) and a principal different from the requester. |

Approval creates the rollout with the approver as actor; a rollback is
exempt only when its target version was previously applied to the affected
nodes.

See [Reconciliation rollout and recovery](../operate/control-plane/reconciliation.md)
for the state machine behind these routes.

### Login sessions (OIDC)

When the Hub is configured with an OIDC provider (see
[deployment](../operate/control-plane/deploy.md#oidc-jwt-federation)), the
operator API also serves a browser login flow:

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/auth/oidc/status` | Public probe reporting `login_enabled`, `authenticated`, and the current principal. |
| `GET` | `/auth/oidc/login` | Redirect to the identity provider (PKCE S256 and `nonce` bound to an HttpOnly cookie). |
| `GET` | `/auth/oidc/callback` | Exchange the code for an id_token and create the server-side session. |
| `GET` | `/auth/oidc/logout` | Delete the server-side session and clear the cookie. |

With durable storage the session is persisted (`cp_sessions`, schema v5): it
survives a Hub restart and is invalidated by logout, by disabling/deleting the
user, or by `POST /users/{id}/revoke-tokens`. Backup and restore exclude
`cp_sessions` deliberately, so a restored Hub never resurrects logins. On a
volatile Hub the session lives in memory and a restart signs everyone out.

### Discovery and metrics

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/components` | Registered components across the fleet image. |
| `GET` | `/components/{kind}/{name}` | One component's schema and example. |
| `GET` | `/schema` | Engine configuration JSON Schema. |
| `GET` | `/metrics` | Prometheus text exposition by default; `?format=json` (or an `Accept: application/json` request) returns `{"items": ..., "aggregate": ...}` — per-node metrics plus the fleet aggregate, optionally narrowed with `?node_id=`. |

Beyond readiness, reconciliation, outbox, and fleet-state gauges, command
dispatch exposes `arkflow_command_duration_bucket{command,le}` (enqueue-to-
acknowledgement latency with `_count`/`_sum` companions) and
`arkflow_command_total{command,outcome}`. Labels come from fixed vocabularies
— command types (`job_start`, `job_stop`, `job_checkpoint`, `job_savepoint`,
stream actions) and outcome classes (`enqueued`, `acknowledged`, `succeeded`,
`failed`, `timed_out`, `node_unavailable`, `capacity`, `rejected`); unknown
command names collapse into `other`, and resource IDs, correlation IDs, and
error text never become labels. Counters reset on Hub restart.

## Hub agent API (`/api/v1/agent/*`)

The Agent contract is separate from the operator contract: agents authenticate
with node session credentials, pull commands, and push observations.

| Method | Route | Purpose |
|--------|-------|---------|
| `POST` | `/agent/register` | Register a node; establishes a session. The payload declares node `capabilities` (e.g. `network_shuffle`) and, when the data plane is enabled, the routable `data_address` peers use for remote edges. |
| `POST` | `/agent/heartbeat` | Heartbeat with lease renewal; re-asserts capabilities and data address. |
| `POST` | `/agent/report` | Observed stream/node state; carries `boot_id` and monotonic `report_seq`. |
| `POST` | `/agent/job-observations` | Report job-level observations from the co-located kernel runtime. |
| `GET` | `/agent/commands` | Pull pending commands. |
| `POST` | `/agent/commands/{id}/result` | Report a command result. |

Commands carry generation, Attempt ID, configuration version, and expiry. A
`job_start` for a `split` placement additionally carries the full
`task_nodes` map and `node_data_ports`, so every node can derive its remote
edges without extra lookups; the Hub only dispatches such placements to nodes
that advertised the `network_shuffle` capability and a data address.
Command acknowledgement is transport state only — convergence is always
derived from reports, never from acks.

## Related pages

- [Control plane overview](../operate/control-plane/overview.md) — architecture behind these routes.
- [Control-plane operations](../operate/control-plane/operations.md) — operator workflows using this API.
- [CLI reference](./cli.md) — the local single-binary alternative to the node API.
