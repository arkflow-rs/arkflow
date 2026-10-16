---
sidebar_position: 6
title: Atomic job upgrades
---

# Atomic job upgrades

The classic job upgrade flow is a six-step manual pipeline: stop the Job, wait
for convergence, take a savepoint, wait for it to complete, submit the
upgrade, start the Job again. Every step is operator latency, and the Job sits
stopped between them.

The **atomic mode** performs the same sequence behind one API call:

```text
POST /api/v1/jobs/{id}/upgrades
{
  "mode": "atomic",
  "spec": { ...new JobSpec with a strictly greater version... },
  "expected_generation": 7
}
→ 202 { "upgrade_id": "job-upgrade-12", "state": "saving_savepoint", ... }
```

The Hub orchestrates the phases below and returns `202` immediately. The Job
keeps processing during the savepoint; the stop-the-world window shrinks to
the savepoint-plus-restore interval. The stopped mode remains available and
unchanged for planned maintenance.

## Phase machine

```text
saving_savepoint ──▶ committing_version ──▶ verifying ──▶ succeeded
   │ (retries/d deadline)                      │ (deadline / no convergence)
   ▼                                           ▼
 aborted                                   rolling_back ──▶ rolled_back
                                                │ (restore fails / no convergence)
                                                ▼
                                              failed
```

| Phase | What happens | Default deadline |
| --- | --- | --- |
| `saving_savepoint` | Dispatches a savepoint at the current generation through the ordinary checkpoint path. Failed rounds are retried up to 2 times; exhaustion or deadline aborts with the Job untouched and still running. | 5 minutes |
| `committing_version` | ONE generation-fenced write commits the new spec with `desired_state = running`. The recovery pointer is not written — it already references the just-completed savepoint, and the fenced write preserves it. | 1 minute |
| `verifying` | Ordinary reconciliation starts the new generation (which also stops the old one). The orchestration observes until the Job is running at the target version. | 10 minutes (`verify_timeout_ms` request override) |
| `rolling_back` | The previous version is restored from the SAME savepoint through the same fenced write; reconciliation restarts it. Terminal `failed` leaves the Job stopped with its recovery pointer intact for manual action. | 10 minutes |

Phase transitions are broadcast as `job.upgrade` events on
`GET /api/v1/events/stream`, and operator actions are audited
(`job.upgrade.atomic.initiate/pause/resume/cancel/rollback`).

## Replay semantics

A checkpoint-mediated cutover is **not** lossless duplication-free for every
sink: events processed by the old generation *after* the savepoint barrier are
replayed by the new generation from the savepoint's source positions.

- Transactional sinks: exactly-once (the replayed events are deduplicated by
  the sink transaction).
- Non-transactional sinks: at-least-once. The duplication window is bounded by
  the savepoint-to-cutover interval, which the orchestrator minimizes by
  committing in the same reconcile tick in which the savepoint completes.

## Orchestration ownership and conflicts

While an orchestration is non-terminal, the Job is fenced: the general
reconciler defers re-placement during the savepoint, commit, and pause phases
(a re-placement would bump the generation the savepoint dispatch is keyed
to), and job-level mutations are rejected with
`409 orchestration_in_progress`:

- `PUT /jobs/{id}/desired-state`
- `POST /jobs/{id}/actions/{start|stop|restart}`
- further upgrades (atomic or stopped mode) and manual version rollback

The observation phases (`verifying`, `rolling_back`) deliberately let ordinary
reconciliation run — it is the start mechanism there.

Checkpoint retention never deletes an artifact an active orchestration
references; the pin is released when the orchestration reaches a terminal
state.

## Status and actions

```text
GET  /api/v1/jobs/{id}/upgrades/{upgrade_id}      → phase, savepoint, deadline, last error
POST /api/v1/jobs/{id}/upgrades/{upgrade_id}/actions  {"action": "pause"|"resume"|"cancel"|"rollback"}
```

- `pause` freezes the orchestration; `resume` re-arms the phase deadline.
  Pausing during the savepoint phase leaves the old version running, but
  pausing mid-verification can freeze a Job in the cutover gap (old version
  already stopped, new one not yet started) until `resume`. A long pause also
  stretches the replay window — prefer cancelling and starting a fresh
  upgrade instead.
- `cancel` releases the Job (a verification-phase cancel leaves the new
  version converging on its own; ordinary reconciliation continues it).
- `rollback` is available once the orchestration is verifying.

## Hub restart and failover

Orchestration phase, deadline, and savepoint reference are durable
(`cp_job_upgrades`). A Hub that starts or gains leadership with a non-terminal
orchestration resumes it idempotently: a savepoint phase dispatches a fresh
round after the lost command settles, a commit phase applies the
conflict-interpretation rule (an already-applied commit advances; anything
else aborts), and the observation phases continue watching. Nothing is
re-applied blindly — idempotency is judged against the Job's observable
state, never the phase row.

## Console

The Jobs page exposes an **Atomic upgrade** action on running Jobs (the DAG
editor, no savepoint selection required) and renders the active
orchestration's phase, savepoint, and actions in the detail panel. The replay
semantics above are shown in the confirmation dialog.
