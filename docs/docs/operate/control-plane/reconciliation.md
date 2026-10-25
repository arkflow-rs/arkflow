---
sidebar_position: 5
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
new generation, such as a corrected configuration or rollback. Placement
rejections fail closed the same way: a `split` placement whose target nodes
do not all advertise the `network_shuffle` capability and a data address is
rejected before dispatch, with no partial `job_start` sent. A dispatched
Attempt whose result is lost becomes `ambiguous`; the Hub does not blindly
replay it. A newer full report is required before a divergent Intent receives
another Attempt.

On Hub restart, persisted Intents and outbox rows are recovered. Queued work
may resume. An expired dispatched Attempt remains ambiguous until a fresh
Agent report establishes whether the target already took effect. This avoids
duplicating restart or other one-shot actions.

## Node labels and selector targeting

Nodes carry an optional operator-managed label map (`PUT /api/v1/nodes/{id}/labels`,
full replacement, at most 16 labels of 1-63 characters each). Labels are durable
Hub state: Agent re-registrations and Hub restarts never change them, and every
replacement is audited. The console exposes label editing on the fleet overview
and label-based rollout creation.

When you create a rollout (or submit one for approval), the target selection can
be an explicit node list **or** a label selector such as
`{"env": "prod", "tier": "edge"}`. Two operational rules follow from the
resolution semantics:

- The selector resolves **once, at creation or submission time**. Nodes that
  gain the label afterwards are not picked up; to include them, create a new
  rollout. Under the approval policy the pending request pins the resolved node
  set, so approving later cannot swap targets.
- Matching reads the durable node records, not the live registration map: a
  drained or stale node that still carries the label stays selected, and so
  does a node that has not yet re-registered after a Hub restart — exactly as
  if you had listed their ids explicitly. The rollout health gates decide
  whether and when that batch converges.

An empty resolution (`no_matching_nodes`) rejects the request outright.

## Planned node drain

Draining a node changes availability, not operator intent. The Hub should stop
dispatching new work to a draining node while retaining desired Stream state.
When the node returns online, registration/report events resume reconciliation
for the current generation. Operators who want Streams to remain stopped must
write an explicit stopped desired state.

## Configuration approval gate

Setting `control_api.approval_required: true` (with `approval_ttl_ms`,
default 24h) requires a second principal's approval before any configuration
rollout is created. Every creation path is gated — the batch creation route,
node-level apply, and rollback — and direct creation is rejected with an
explicit `approval_required` error pointing at the approval flow:

1. An operator submits `POST /config-approvals` pinning the configuration
   version, target nodes, and batch size. The submission validates the
   version and nodes but creates nothing.
2. A different principal (admin role — the `ApproveRollouts` action is not
   granted to the operator role) approves or rejects the request.
3. Approval creates the rollout with the approver as the recorded actor. A
   crash between the rollout creation and the approval marking is recovered
   by adoption: at most one rollout ever exists per request.

Rollback is exempt only when its target version was previously applied to
the affected nodes: rolling back to a version that was never applied is new
content riding the rollback path and is rejected.

Two operational requirements follow from the design. A single static
operator credential resolves to one principal identity, so it **cannot
approve its own submission** — enabling the policy on a one-identity
deployment freezes publishing (the Hub logs a startup warning when it
detects this). Plan for at least two operator identities (structured
credentials or OIDC) before enabling the gate. Rejected and expired requests
are terminal; republishing submits a new request, which the console can
pre-fill from the rejected one.

## Rollback

Rollback is a new configuration Intent and generation. It does not delete the
failed version or rewrite the previous observed report. The operation remains
visible with its failure class and correlation ID, while the new version is
tracked independently until observed convergence.
