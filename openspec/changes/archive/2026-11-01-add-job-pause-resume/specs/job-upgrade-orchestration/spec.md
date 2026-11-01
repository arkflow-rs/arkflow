# Delta Spec: job-upgrade-orchestration

## ADDED Requirements

### Requirement: Pause and upgrade orchestration SHALL be mutually exclusive

The Hub SHALL reject an atomic or stopped-mode upgrade for a Job while a pause phase is present (in flight, paused, or resuming), with a stable `job_paused` problem code, and SHALL reject a pause request while a non-terminal upgrade orchestration owns the Job, with the existing `orchestration_in_progress` problem code. A rejected request SHALL leave the Job, the pause bookkeeping, and any existing orchestration unchanged.

#### Scenario: Upgrade rejected while paused

- **WHEN** an operator requests an upgrade for a paused or pausing Job
- **THEN** the Hub returns `409 job_paused`, the Job's version, generation, desired state, and pause phase are unchanged, and the pause artifact stays pinned

#### Scenario: Pause rejected during orchestration

- **WHEN** an operator requests a pause while a non-terminal upgrade orchestration owns the Job
- **THEN** the Hub returns the existing `orchestration_in_progress` problem code and neither the Job nor the orchestration changes

#### Scenario: Upgrade accepted after pause completes or is cleared

- **WHEN** a previously paused Job has been resumed and converged or has been stopped with its pause bookkeeping cleared
- **THEN** the existing upgrade preconditions apply exactly as before this change
