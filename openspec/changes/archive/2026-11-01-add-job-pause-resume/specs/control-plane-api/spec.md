# Delta Spec: control-plane-api

## ADDED Requirements

### Requirement: Job pause and resume SHALL be addressable API mutations

Job pause and resume SHALL be exposed as authorized lifecycle mutations returning the updated Job record, and the Job desired-state mutation SHALL accept `paused` in addition to `running` and `stopped` (Job creation SHALL continue to accept only `running` and `stopped`). While a pause phase is in flight or paused, upgrade creation SHALL be rejected with a stable `job_paused` problem code for both the atomic and stopped modes, and while a non-terminal upgrade orchestration owns the Job, pause and resume SHALL be rejected with the existing `orchestration_in_progress` problem code. Rejections SHALL use stable problem codes so clients can distinguish `job_not_running`, `pause_in_progress`, `job_already_paused`, `pause_not_paused`, `pause_requires_recovery_policy`, `job_paused`, and `orchestration_in_progress`.

#### Scenario: Pause is accepted for a running Job

- **WHEN** an authorized operator submits a pause mutation for a converged running Job
- **THEN** the API returns `202` with the updated Job record carrying the pause phase

#### Scenario: Resume requires the paused phase

- **WHEN** an operator submits a resume mutation for a Job that is not paused
- **THEN** the API returns `409 pause_not_paused` and the Job is unchanged

#### Scenario: Desired-state mutation accepts paused

- **WHEN** an authorized operator writes `{"state":"paused"}` to the Job desired-state resource
- **THEN** the mutation behaves exactly like the pause mutation

#### Scenario: Paused Job cannot be upgraded

- **WHEN** an operator submits an atomic or stopped-mode upgrade for a paused or pausing Job
- **THEN** the API returns `409 job_paused` and neither the Job nor its pause artifact changes
