## ADDED Requirements

### Requirement: Approval requests are immutable snapshots

The Hub SHALL accept a configuration approval request consisting of an existing immutable configuration version id, an explicit target node selection, and rollout batch configuration, and SHALL persist it with a unique id, the requesting principal, a pending status, and an expiry timestamp (`approval_ttl_ms` after creation, default 24h). The request SHALL be immutable after creation; only its status and decision metadata (approver, decided_at, rollout_id, failure_note) may change. Submission SHALL validate that the referenced version exists, every target node exists, and the batch configuration is valid, without creating a rollout or dispatching work.

#### Scenario: Valid submission is accepted without side effects

- **WHEN** an operator with rollout management permission submits a request naming an existing version, existing nodes, and a valid batch configuration while approval policy is enabled
- **THEN** the Hub persists a pending request, and no rollout is created and no node receives work

#### Scenario: Invalid submission is rejected

- **WHEN** a request names a configuration version that does not exist, or a node that is not registered
- **THEN** submission fails with an error naming the offending reference and nothing is persisted

### Requirement: Approval policy gates every forward creation channel

The Hub SHALL provide a configuration flag `approval_required` (default disabled). While enabled, every path that creates a configuration rollout — the batch creation route, the create-with-content route, and node-level legacy apply and rollback routes — SHALL reject direct creation with an explicit error directing the operator to the approval request flow, with one exception: a rollback whose target version was previously applied to the affected nodes SHALL proceed without approval, and a rollback naming a version never applied to those nodes SHALL be rejected as unapproved content. While disabled, creation behavior SHALL be identical to before this capability, and approval request submission SHALL be rejected with an explicit disabled error; already-pending requests MAY still be decided while the policy is disabled. The Hub SHALL warn at startup when the policy is enabled and fewer than two distinct statically configured operator identities exist. Pause, resume, and cancel on already-created rollouts SHALL NOT be gated.

#### Scenario: Direct creation is rejected while enabled

- **WHEN** approval policy is enabled and an authorized operator posts directly to a rollout creation route
- **THEN** the Hub rejects the request with an explicit error naming the approval flow, and no rollout or version is created

#### Scenario: Legacy single-node apply is gated

- **WHEN** approval policy is enabled and a client posts configuration to a node-level apply route
- **THEN** the request is rejected with the approval-required error

#### Scenario: Rollback to a previously applied version proceeds

- **WHEN** approval policy is enabled and an authorized operator rolls back a non-terminal rollout to a version that was previously applied to the affected nodes
- **THEN** the rollback proceeds without an approval request

#### Scenario: Rollback to a never-applied version is rejected

- **WHEN** approval policy is enabled and a rollback names a configuration version that exists but was never applied to the affected nodes
- **THEN** the rollback is rejected with an error directing the operator to the approval flow, and no rollout is created

#### Scenario: Disabled policy changes nothing

- **WHEN** approval policy is disabled
- **THEN** rollout creation and rollback routes behave exactly as before and no approval state is consulted

#### Scenario: Submitting while disabled is rejected

- **WHEN** approval policy is disabled and an operator submits an approval request
- **THEN** submission fails with an explicit approval-disabled error and nothing is persisted

#### Scenario: Pending requests remain decidable after the policy is disabled

- **WHEN** approval policy is switched from enabled to disabled while a request is pending
- **THEN** the pending request can still be decided, and its expiry continues to advance

#### Scenario: Startup warns on a single operator identity

- **WHEN** the Hub starts with approval policy enabled and the statically configured operator credentials resolve to fewer than two distinct principal identities
- **THEN** the Hub emits a startup warning explaining that no second approver identity exists and approvals cannot be granted

### Requirement: Approval creates the rollout as the approver, at most once

Approving a pending request SHALL transition the request through an intermediate deciding state that reserves the decision (only one decision per request takes effect), and SHALL associate at most one rollout with the request: the approval first adopts an existing rollout linked by the request's correlation id if one already exists, otherwise creates the rollout from the pinned snapshot with the approving principal as actor and the correlation id linking the request, and only then marks the request approved with the created rollout id. If rollout creation fails, the request SHALL return to pending with a recorded failure reason and the decision MAY be retried. Requests left in the deciding state by a crash SHALL be resolved at startup and by the periodic sweep using the same adoption rule. An approved-without-rollout state MUST NOT exist.

#### Scenario: Approval executes the pinned snapshot

- **WHEN** an authorized approver approves a pending request whose snapshot is (version V, nodes N1..Nk, batch size B)
- **THEN** the Hub creates a rollout targeting exactly V, N1..Nk, and B with the approver as actor, and the request becomes approved referencing that rollout

#### Scenario: Concurrent decisions are serialized

- **WHEN** two approvers decide the same pending request concurrently
- **THEN** exactly one decision takes effect and the other fails with a conflict error

#### Scenario: Crash between rollout creation and approval marking recovers by adoption

- **WHEN** the Hub crashes after creating the rollout for a request being approved but before marking it approved, and the decision is retried or recovery runs
- **THEN** the request adopts the already-created rollout by its correlation id instead of creating a second rollout, and is marked approved referencing it

#### Scenario: Creation failure keeps the request retryable

- **WHEN** approval is granted but rollout creation fails (for example a target node disappeared after submission)
- **THEN** the request remains pending with a recorded failure reason

### Requirement: Rejection is terminal

Rejecting a pending request SHALL mark it rejected with the rejecting principal and a timestamp, and SHALL NOT delete or modify the pinned snapshot. A rejected request MUST NOT be re-opened; publishing the same content afterwards requires a new request.

#### Scenario: Rejected request cannot be decided again

- **WHEN** an approver attempts to approve or reject an already-rejected request
- **THEN** the attempt fails with an explicit terminal-state error

#### Scenario: Content is republished through a new request

- **WHEN** an operator resubmits the same version and targets after a rejection
- **THEN** a new pending request is created and the old rejection record is unchanged

### Requirement: Approver SHALL differ from the requester

While approval policy is enabled, the Hub SHALL grant the approve action only to principals holding the dedicated `ApproveRollouts` permission (held by the admin role, not the operator role) and SHALL reject an approval whose principal is the same as the request's requester. Rejecting one's own submission is the conservative direction and MUST remain available: the requester MAY withdraw (reject) a pending request they submitted.

#### Scenario: Self-approval is rejected

- **WHEN** the principal that submitted a pending request approves it
- **THEN** the decision fails with an error naming the segregation rule

#### Scenario: The requester may withdraw their own submission

- **WHEN** the principal that submitted a pending request rejects it
- **THEN** the request becomes rejected with that principal recorded, and no rollout is created

#### Scenario: Operator role cannot approve

- **WHEN** a principal holding only the operator role attempts to approve a pending request
- **THEN** the decision is not authorized

### Requirement: Pending requests expire and decided requests are retained bounded

A pending request whose expiry timestamp has passed SHALL be presented and stored as expired and SHALL NOT be approvable. The Hub SHALL sweep expired and decided requests under bounded retention (age and count bounds) in its periodic maintenance task; sweeping MUST NOT remove pending requests and MUST NOT be recorded as an operator mutation in the audit trail.

#### Scenario: Expired request is not approvable

- **WHEN** an approval decision targets a pending request past its expiry timestamp
- **THEN** the decision fails as expired and the request is stored as expired

#### Scenario: Terminal requests are pruned bounded

- **WHEN** decided and expired requests exceed the retention bounds during the periodic sweep
- **THEN** the sweep removes only terminal requests within the bounds and pending requests are never removed

### Requirement: Approval decisions are durable and audited

Approval requests and their decisions SHALL survive a Hub restart on every supported storage backend. Submit, approve, and reject SHALL each record an actor-aware audit event with the actor, action, request id, target version and nodes, correlation id, and outcome; audit records MUST NOT contain configuration bodies or credentials.

#### Scenario: Decisions survive restart

- **WHEN** the Hub restarts while a request is pending
- **THEN** the request is still pending after recovery and can still be approved or rejected

#### Scenario: Decisions are audited

- **WHEN** an approver rejects a pending request
- **THEN** an audit event records the approver, the rejection, the request id and its pinned version and targets, and no configuration body
