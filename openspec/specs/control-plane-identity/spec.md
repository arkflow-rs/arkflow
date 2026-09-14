## Purpose

Define operator identity, resource-scoped authorization, and actor-aware control-plane auditing.

## Requirements

### Requirement: Authenticated operator principal

The control plane SHALL resolve every operator request to an authenticated principal, except explicitly configured health and liveness routes, and SHALL keep Agent authentication separate from operator authentication.

#### Scenario: Reject an unauthenticated mutation
- **WHEN** an unauthenticated request attempts to change a Stream, configuration, node mode, or rollout
- **THEN** the service returns 401 and does not create or mutate a control-plane resource

### Requirement: Resource-scoped RBAC

The service SHALL authorize actions by principal, role, action, and resource scope, including fleet, node, Stream, configuration, rollout, and audit read scopes.

#### Scenario: Deny an out-of-scope action
- **WHEN** an authenticated principal lacks permission for the target node or requested action
- **THEN** the service returns 403 and leaves desired state, operations, and audit state unchanged

### Requirement: Actor-aware audit

Every accepted or rejected control-plane mutation SHALL record the principal, action, target resource, correlation ID, outcome, and stable failure code when applicable. Audit records MUST NOT contain credentials, authorization headers, or secret configuration values.

#### Scenario: Audit a denied request
- **WHEN** a principal submits a validly authenticated but unauthorized mutation
- **THEN** an audit record identifies the principal, target, denied action, correlation ID, and reason without exposing secrets

### Requirement: Job lifecycle mutations are audited

The Hub SHALL record an audit event for every accepted or rejected distributed-Job lifecycle mutation — the `job_start`, `job_stop`, `job_checkpoint`, and `job_savepoint` operations — with the actor, action, target Job resource, node targets, correlation ID, outcome, and stable failure code where applicable. Recovery IS part of the start mutation and SHALL appear in its audit record rather than as a separate event; command-result operations (`job_checkpoint_commit`, `job_savepoint_commit`) are Agent reports, not operator mutations, and are not covered by this requirement. Audit messages MUST NOT contain credentials or Job configuration bodies; they SHALL be limited to scalar operation metadata.

#### Scenario: A Job start is accepted

- **WHEN** the Hub accepts a Job start mutation for placement
- **THEN** an audit record with action `job.start`, resource type `job`, the target Job ID, actor, correlation ID, and outcome `accepted` is persisted

#### Scenario: A Job stop is rejected

- **WHEN** a Job stop mutation is rejected (for example by fencing or capacity)
- **THEN** an audit record with action `job.stop` and a stable failure code is persisted, and the record exposes the rejection reason without embedding the Job configuration

#### Scenario: Audit queries resolve Job history

- **WHEN** an authorized operator lists audit events filtered by the Job resource ID
- **THEN** the response includes the Job lifecycle audit records within the retention bound
