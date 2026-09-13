## ADDED Requirements

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
