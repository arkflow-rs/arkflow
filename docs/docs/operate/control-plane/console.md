---
sidebar_position: 3
title: Web console
description: Tour of the ArkFlow web console — fleet overview, job DAG editor, rollouts, and runtime views.
---

# Web console

The console is the graphical front end to the Hub operator API. It ships in
the repository under `console/` as a Vite + React single-page application and
is served from a reverse proxy next to the Hub — see
[deployment](./deploy.md) for build and serving options.

## Views

| View | What it shows |
|------|---------------|
| **Overview** | Fleet health first: node up/down summaries, convergence state, and pointers into problem areas. |
| **Jobs** | The job catalog with desired vs. observed state, versions, pause phase, and lifecycle actions (start/stop, pause/resume, checkpoint, upgrade). |
| **Job editor** | A form-based editor for job specs with spec validation before submit. |
| **Job DAG** | A directed-graph orchestrator view of a job's operator graph — operator boundaries, partition routes, and stateful operators, mirroring `GET /api/v1/jobs/{id}/plan`. |
| **Rollouts** | Per-node rollout progress with pause, resume, cancel, and rollback actions. |
| **Runtime** | Live per-node stream state and convergence, backed by node reports. |
| **Components** | The component registry browser: every registered kind/type with its config schema and example, mirroring `arkflow components show`. |
| **Configuration** | Node configuration history, diffs, apply, and rollback. In Hub mode every action is routed through the selected node; drafts are a local-control-plane feature. |
| **Events** | The fleet event log (`GET /api/v1/events`, with the SSE stream for live updates), filtered and paginated server-side. |
| **Audit** | The fleet audit history (`GET /api/v1/audit`): accepted and rejected mutations with actor, target, node, and outcome, filterable by resource and paginated server-side. |
| **Users** | The managed user directory and credentials page (`/users`): users, roles, scopes, OIDC bindings, one-time credential issuance, rotation, revocation, and token revocation. Visible only in Hub mode to principals holding `manage_users` (admin). |
| **Settings** | Console preferences, connection settings, and the runtime bearer credential panel. |

## Reading the console the way the API works

The console is a thin client over the desired-state API, and its views make
the three resource views explicit:

- **Desired** — what you asked for (the intent you just saved).
- **Observed** — what the node last reported.
- **Convergence** — whether they agree (`in_sync`, `pending`, `applying`,
  `degraded`, `blocked`).

A green badge means converged, not "command succeeded": after you click an
action the console shows the pending operation until the matching node report
arrives. If a node is offline, actions are accepted (the Intent is durable)
and the view shows the operation as blocked until the node reconnects.

The console reports the fleet's High-Availability posture when the Hub
advertises it: the overview card shows the leadership role (leader or
standby) with the lease epoch and transition count. When every request is
rejected because the connected Hub is a standby (503 `hub_standby`), the
console replaces the generic stale banner with an explicit standby notice and
keeps polling, so the notice clears itself when that Hub wins the lease.
Diagnostics stay honest: the job detail view renders only gauges the API
actually measures, and the tasks tab distinguishes observed runtime state
from desired placement that has not been reported yet. The job editor exposes
the distributed-runtime spec fields — the `rescale` opt-in for redistributing
keyed state across a changed parallelism, and per-task `resources` requests
(`cpu_millicores`, `memory_bytes`) that participate in placement accounting —
as form controls, serializing both back to their absent form when cleared.

## Job pause and resume

The Jobs list and detail panel expose **Pause** on a converged running Job and
**Resume** on a paused one, together with a phase badge that keeps the
transition visible (`capturing`, `stopping`, `paused`, `resuming`) instead of
folding it into "stopped". While a pause or resume transition is in flight the
actions are disabled, and a paused row's task list shows tasks as not observed
rather than reporting a stale running state. The controls require the
`operate` action like start and stop. The confirmation explains that pausing
captures a recovery artifact before stopping and that resume replays events
processed after the artifact, and a rejected transition shows the server's
problem code (`pause_in_progress`, `job_already_paused`, `job_paused`,
`orchestration_in_progress`, …) verbatim. On a paused Job, `start` and
`restart` act as resume, and `stop` settles it as stopped. The Jobs filter
includes a `paused` option. See
[Streaming jobs → Pause and resume](../../build/jobs.md#pause-and-resume) for the
server-side semantics and the replay boundary.

## Node maintenance actions

The overview cards expose three node actions for fleet maintenance. Each one
asks for confirmation before it fires:

- **Drain** (`POST /api/v1/nodes/{id}/drain`) evacuates the node's
  placements to other nodes — use it before taking a node offline.
- **Maintain** (`POST /api/v1/nodes/{id}/maintenance`) marks the node as in
  maintenance so placement stops assigning new work to it.
- **Resume** (`DELETE /api/v1/nodes/{id}/maintenance`) returns the node to
  active duty.

While a node is not `active`, its overview card shows the current state
(`draining` or `maintenance`) as a badge next to the node name.

## Navigation and URLs

Every view has its own URL path (`/`, `/runtime`, `/jobs`, `/configuration`,
`/rollouts`, `/components`, `/events`, `/audit`, `/users`, `/settings`), so views can be
bookmarked, shared, and reached with the browser's back and forward buttons.
The selected compute node is stored as a `node_id` query parameter on the
current path and carries over when you switch views. Legacy links that used
the old `?page=` query parameter (for example `/?page=jobs`) redirect
automatically to the corresponding path and keep the remaining query
parameters.

Navigation reflects what the connected control plane actually serves. The
Jobs, Rollouts, Approvals, Audit, and Users views depend on Hub-only APIs; when
the console is connected to a local engine (`/system` capabilities lack
`fleet_aggregation`) those entries disappear from the navigation, their
direct URLs render an explainer naming the Hub requirement instead of an
error, and Settings hides its webhook management section. The Users entry is
additionally gated on the `manage_users` action, so non-admin principals never
see it. While the system
identity is still loading, the full navigation renders so nothing flashes
away at startup.

On the Events and Audit views, the filter values and page position live in
the URL query (`event_type`, `stream_id`, `correlation_id`, `node_id`,
`resource_id`, `page`, `page_size`): a copied link reproduces the exact
view, filtering and pagination execute on the server, and changing any
filter resets to the first page. The page size selector offers 25 / 50 /
100 rows.

## Identity and role-aware controls

`GET /api/v1/system` echoes the authenticated principal
(`principal: {id, roles, actions}` on both the Hub and the local plane), and
the console uses it directly: the header shows who you are (for example
`ops (operator)`), and every mutating control — stream lifecycle, publish and
rollback, job actions, rollout creation, approval decisions, node
maintenance, webhook management, and identity administration — is gated on the echoed action list. A
principal without the required action sees the control disabled with a
tooltip naming the missing permission, so a viewer session reads everything
but changes nothing. The echo comes from the same predicate the server uses
to authorize mutations; if a mutation is still rejected with 403 (for
example the role changed mid-session), the console shows a permission error,
never retries the mutation, and refreshes the identity. Against an older
control plane that does not echo a principal, the console keeps controls
enabled and marks the session identity as unknown rather than locking
everything.

## Users and credentials {#users-and-credentials}

On a durable-storage Hub the console exposes a standalone **Users** page
(`/users`), listed in the navigation only for principals holding the
`manage_users` action (admin). It is the console-side equivalent of the
[user and credential API](../../reference/api.md#users-and-credentials):

- **Users**: list the directory, create a user (id, display name, role, scopes
  edited item by item, optional OIDC subject), edit it, disable or enable it,
  and delete it. Every mutating action goes through the standard confirmation
  dialog.
- **Credentials**: inspect credential metadata (id, label, created/expiry/
  revocation timestamps) without ever seeing stored hashes. Issuing a
  credential shows the plaintext token exactly once, with a copy action and an
  explicit "shown only once" warning — the console cannot retrieve it again,
  and the only recovery is rotation. Rotation shows the replacement token the
  same way; a `DELETE` revocation is idempotent (the `200` body carries
  `{"revoked": bool}` but is not needed by the UI).
- **Revoke tokens**: invalidates a user's federated JWTs and browser sessions;
  managed credentials stay valid and are revoked or rotated individually.
- Rejected operations display the server's problem code verbatim
  (`last_admin_required`, `credential_limit`, `oidc_subject_conflict`,
  `identity_store_unavailable`, …); a `403` follows the usual
  permission-downgrade path.

The page is gated twice: Hub mode (the local engine has no user directory) and
a `/system` principal echo that includes `manage_users`. Everyone else sees no
navigation entry; a direct visit renders the Hub-requirement explainer, and a
request from a lower-privileged principal fails with the server's
`403 forbidden`.

### Runtime bearer credentials

The console no longer requires a token at build time. When the API answers
`401` and no OIDC login flow is configured, the shell prompts for a bearer
credential; the **Settings** page keeps a runtime credential panel with
**switch credential** / **clear credential** controls. The entered token is
kept in `sessionStorage` — never `localStorage` — so it disappears when the
tab closes and is not shared across tabs. `VITE_API_TOKEN`, when present, only
acts as the initial default; clearing the runtime credential falls back to it.
When OIDC login is enabled the normal redirect-to-login flow takes precedence,
and the runtime prompt is not used.

## When to use the console vs. the API

- **Console**: day-to-day observation, single job lifecycle changes, rollout
  monitoring, and incident triage (events + runtime + audit in one place).
- **API**: automation, GitOps-style configuration delivery, and anything you
  need to reproduce — every console action is a documented route in the
  [HTTP API reference](../../reference/api.md).

## Theme

The console ships with a dark theme (default) and a light theme built on the
same design tokens. The theme selector in the top bar offers **Dark / Light /
System**; **System** follows your operating system preference. The choice is
persisted in `localStorage` (key `arkflow.console.theme`) and applied before
first paint, so reloading never flashes the wrong theme.

Destructive and lifecycle-changing actions are confirmed with an in-app
dialog (focus is contained, `Esc` cancels, `Enter` confirms), outcomes arrive
as toast notifications, and tables show skeleton rows while data is loading.

## Interface language

The console user interface ships with English and Simplified Chinese
(简体中文) translations. On first load the console picks the language from
your browser (`navigator.language`; any `zh*` locale selects Chinese),
falling back to English. The language selector in the top bar
(**中文 / English**) switches immediately and persists the choice in
`localStorage`, so it survives reloads. Date and number formatting follow
the selected language.

Backend-returned messages (API errors, event payloads) are displayed as
received and are not translated by the console.

## Security notes

The console is a static bundle that holds no secrets beyond what you enter;
it authenticates to the Hub with operator tokens over your reverse proxy. A
bearer token entered at runtime lives in `sessionStorage` for the tab's
lifetime and is never written to `localStorage` or the URL. The one-time
credential reveal renders the plaintext from that single issuance response —
the console cannot fetch it again. Keep both the Hub API and the console behind
an authenticated, protected network boundary — do not expose token-bearing
deployments to the public internet (see [deployment](./deploy.md)).
