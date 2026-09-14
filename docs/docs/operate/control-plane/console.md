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
| **Configuration** | Node configuration history, drafts, diffs, apply, and rollback. |
| **Events** | The fleet event log (same data as `GET /api/v1/events`, with the SSE stream for live updates). |
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

## When to use the console vs. the API

- **Console**: day-to-day observation, single job lifecycle changes, rollout
  monitoring, and incident triage (events + runtime + audit in one place).
- **API**: automation, GitOps-style configuration delivery, and anything you
  need to reproduce — every console action is a documented route in the
  [HTTP API reference](../../reference/api.md).

## Security notes

The console is a static bundle that holds no secrets beyond what you enter;
it authenticates to the Hub with operator tokens over your reverse proxy.
Keep both the Hub API and the console behind an authenticated, protected
network boundary — do not expose token-bearing deployments to the public
internet (see [deployment](./deploy.md)).
