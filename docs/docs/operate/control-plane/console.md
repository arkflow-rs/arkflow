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
| **Jobs** | The job catalog with desired vs. observed state, versions, and lifecycle actions (start/stop, checkpoint, upgrade). |
| **Job editor** | A form-based editor for job specs with spec validation before submit. |
| **Job DAG** | A directed-graph orchestrator view of a job's operator graph — operator boundaries, partition routes, and stateful operators, mirroring `GET /api/v1/jobs/{id}/plan`. |
| **Rollouts** | Per-node rollout progress with pause, resume, cancel, and rollback actions. |
| **Runtime** | Live per-node stream state and convergence, backed by node reports. |
| **Components** | The component registry browser: every registered kind/type with its config schema and example, mirroring `arkflow components show`. |
| **Configuration** | Node configuration history, diffs, apply, and rollback. In Hub mode every action is routed through the selected node; drafts are a local-control-plane feature. |
| **Events** | The fleet event log (`GET /api/v1/events`, with the SSE stream for live updates), filtered and paginated server-side. |
| **Audit** | The fleet audit history (`GET /api/v1/audit`): accepted and rejected mutations with actor, target, node, and outcome, filterable by resource and paginated server-side. |
| **Settings** | Console preferences and connection settings. |

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
`/rollouts`, `/components`, `/events`, `/audit`, `/settings`), so views can be
bookmarked, shared, and reached with the browser's back and forward buttons.
The selected compute node is stored as a `node_id` query parameter on the
current path and carries over when you switch views. Legacy links that used
the old `?page=` query parameter (for example `/?page=jobs`) redirect
automatically to the corresponding path and keep the remaining query
parameters.

Navigation reflects what the connected control plane actually serves. The
Jobs, Rollouts, Approvals, and Audit views depend on Hub-only APIs; when the
console is connected to a local engine (`/system` capabilities lack
`fleet_aggregation`) those entries disappear from the navigation, their
direct URLs render an explainer naming the Hub requirement instead of an
error, and Settings hides its webhook management section. While the system
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
maintenance, webhook management — is gated on the echoed action list. A
principal without the required action sees the control disabled with a
tooltip naming the missing permission, so a viewer session reads everything
but changes nothing. The echo comes from the same predicate the server uses
to authorize mutations; if a mutation is still rejected with 403 (for
example the role changed mid-session), the console shows a permission error,
never retries the mutation, and refreshes the identity. Against an older
control plane that does not echo a principal, the console keeps controls
enabled and marks the session identity as unknown rather than locking
everything.

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
it authenticates to the Hub with operator tokens over your reverse proxy.
Keep both the Hub API and the console behind an authenticated, protected
network boundary — do not expose token-bearing deployments to the public
internet (see [deployment](./deploy.md)).
