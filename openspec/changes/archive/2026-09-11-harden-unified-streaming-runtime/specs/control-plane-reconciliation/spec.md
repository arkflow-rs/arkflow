## MODIFIED Requirements

### Requirement: Intent, attempt, and convergence state
The Hub SHALL expose separate state for a durable reconciliation Intent, each command Attempt, and the computed resource Convergence condition. For a multi-node Job, the observed Job state SHALL be derived from the aggregate of all expected assignment operations for the same generation and action, not from the first command result received.

#### Scenario: Command acknowledgement is not final success
- **WHEN** an Agent acknowledges a start command but has not reported the target generation as running
- **THEN** the Attempt MAY be acknowledged or running while the Intent remains converging and Convergence is not in_sync

#### Scenario: Observed state reaches the target
- **WHEN** every expected assignment report matches the desired state, target configuration version, and generation
- **THEN** the Hub marks Convergence in_sync and the associated Intent converged

#### Scenario: One assignment is still pending
- **WHEN** one Job assignment reports running and another expected assignment remains pending or unavailable
- **THEN** the Job remains converging or degraded and is not reported as fully running

#### Scenario: One peer has a transient error
- **WHEN** one assignment has a retryable transport error while other assignments are healthy
- **THEN** the Job remains retrying or degraded, the desired state is preserved, and the healthy assignment observations are not overwritten as failed

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness. Job checkpoint commit SHALL use the full expected assignment set.

#### Scenario: Agent reconnects with stale observed state
- **WHEN** a node reconnects and its full report does not match the persisted desired Stream or Job state
- **THEN** the Hub creates or resumes one eligible Attempt for the current generation and does not dispatch a stale task assignment

#### Scenario: Hub restarts with an unfinished rollout
- **WHEN** the Hub loads a persisted rollout that is not converged, cancelled, or rolled back
- **THEN** it restores the current batch and resumes only eligible current-generation intents after storage recovery

#### Scenario: Job checkpoint completes
- **WHEN** a Job checkpoint is durably committed and all expected assignment observations report the checkpoint generation
- **THEN** the Hub records the checkpoint as the latest valid recovery point and may advance Job convergence or rollout health gates

#### Scenario: Checkpoint has an offline assignment
- **WHEN** an expected assignment is offline or has not produced a manifest for the checkpoint generation
- **THEN** the Hub leaves the checkpoint attempt incomplete or failed and does not dispatch a partial commit that could become a recoverable artifact

## ADDED Requirements

### Requirement: Job operation state SHALL be aggregated by assignment
The Hub SHALL persist or derive one operation result per expected assignment and SHALL calculate the Job-level observed state using all results for the current generation/action. Terminal Job states SHALL not be written until the aggregate has enough information to distinguish success, retryable degradation, and permanent failure.

#### Scenario: All assignments succeed
- **WHEN** every expected assignment reaches `running` or `succeeded` for a start operation
- **THEN** the Job-level observed state becomes `running`

#### Scenario: A permanent failure is confirmed
- **WHEN** the complete assignment set has been evaluated and an assignment reports a non-retryable execution failure
- **THEN** the Job-level state becomes failed/blocked according to the operation policy and retains the assignment failure details
