# Spec Delta: control-plane-api

## ADDED Requirements

### Requirement: Atomic job upgrade mode and orchestration conflicts
The job upgrade endpoint SHALL accept an atomic mode that performs savepoint, version commit, and recovery start as one supervised orchestration, returning 202 with an orchestration id and phase, plus endpoints to read one orchestration's status and to invoke its actions (pause, resume, cancel, rollback). While an orchestration is non-terminal for a Job, job-level mutations — desired-state changes, job actions, and further upgrades — SHALL be rejected with `orchestration_in_progress`, without affecting the running orchestration. The existing stopped-mode upgrade and rollback contracts SHALL remain available and unchanged.

#### Scenario: Atomic upgrade is initiated
- **WHEN** the operator POSTs an atomic-mode upgrade for a running Job that passes validation and exclusivity
- **THEN** the response is 202 carrying the orchestration id and current phase, and the stop-the-world preconditions do not apply to this mode

#### Scenario: Desired-state change conflicts with an active orchestration
- **WHEN** the operator PUTs a desired-state change for a Job with a non-terminal orchestration
- **THEN** the request fails with `orchestration_in_progress` and the orchestration continues unaffected

#### Scenario: Orchestration status and actions are addressable
- **WHEN** the operator GETs the orchestration status or POSTs an action for it
- **THEN** the status exposes phase, savepoint reference, deadline, and last error, and actions follow terminal-state rejection and paused-only resume rules

#### Scenario: Stopped-mode upgrade is unchanged
- **WHEN** the operator POSTs a stopped-mode upgrade without an active orchestration
- **THEN** the request is governed by the existing stopped-and-converged preconditions and fencing behavior
