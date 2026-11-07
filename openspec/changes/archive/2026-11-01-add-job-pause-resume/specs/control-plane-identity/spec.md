# Delta Spec: control-plane-identity

## ADDED Requirements

### Requirement: Job pause and resume mutations are audited

The Hub SHALL record an audit event for every accepted or rejected Job pause and resume mutation, with the actor, action (`job.pause` / `job.resume`), target Job resource, target nodes where applicable, correlation id, outcome, and a stable failure code where applicable. Phase-transition failures that abort a pause SHALL be recorded with a stable failure code on the same lifecycle action. Audit messages MUST NOT contain credentials, Job configuration bodies, or secret values; they SHALL be limited to scalar operation metadata.

#### Scenario: A Job pause is accepted

- **WHEN** the Hub accepts a Job pause mutation
- **THEN** an audit record with action `job.pause`, resource type `job`, target Job id, actor, correlation id, and outcome `accepted` is persisted

#### Scenario: A Job pause is rejected

- **WHEN** a Job pause mutation is rejected (for example because the Job is not running or an orchestration owns it)
- **THEN** an audit record with action `job.pause`, outcome `rejected`, and a stable failure code is persisted without embedding the Job spec

#### Scenario: A failed pause is recorded

- **WHEN** a pause aborts because its savepoint failed or timed out
- **THEN** an audit record carrying the stable abort failure code is persisted for the Job

#### Scenario: A Job resume is audited

- **WHEN** an authorized operator resumes a paused Job or the resume is rejected because the Job is not paused
- **THEN** an audit record with action `job.resume` is persisted with the matching outcome
