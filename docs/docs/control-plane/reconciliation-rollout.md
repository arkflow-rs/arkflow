---
sidebar_position: 21
title: Reconciliation rollout and recovery
---

# Reconciliation rollout and recovery

Reconciliation should be enabled in stages. The durable Hub state is the
operator source of truth; a node report is observation and must not rewrite a
desired target.

## Rollout stages

1. **Observe-only**: persist reports and compare desired versus observed, but
   do not dispatch new Attempts. Inspect generation mismatches, stale nodes,
   and configuration-version drift.
2. **One node**: enable lifecycle reconciliation for a node with idempotent
   start/stop commands. Confirm that offline desired writes remain durable and
   reconnects converge the current generation.
3. **Configuration**: enable configuration apply/rollback after lifecycle
   convergence is understood. A configuration Intent is terminal only after
   the node reports the target version and every affected Stream satisfies its
   desired generation and state.
4. **Fleet**: expand gradually while watching blocked Intents, retry counts,
   lease expiries, and reconciliation latency.

## Failure and restart behavior

Temporary transport/node failures preserve desired state and schedule a
backoff retry. Permanent execution failures become `blocked` and require a
new generation, such as a corrected configuration or rollback. A dispatched
Attempt whose result is lost becomes `ambiguous`; the Hub does not blindly
replay it. A newer full report is required before a divergent Intent receives
another Attempt.

On Hub restart, persisted Intents and outbox rows are recovered. Queued work
may resume. An expired dispatched Attempt remains ambiguous until a fresh
Agent report establishes whether the target already took effect. This avoids
duplicating restart or other one-shot actions.

## Planned node drain

Draining a node changes availability, not operator intent. The Hub should stop
dispatching new work to a draining node while retaining desired Stream state.
When the node returns online, registration/report events resume reconciliation
for the current generation. Operators who want Streams to remain stopped must
write an explicit stopped desired state.

## Rollback

Rollback is a new configuration Intent and generation. It does not delete the
failed version or rewrite the previous observed report. The operation remains
visible with its failure class and correlation ID, while the new version is
tracked independently until observed convergence.
