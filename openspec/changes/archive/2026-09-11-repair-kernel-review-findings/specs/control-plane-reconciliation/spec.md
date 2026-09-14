## MODIFIED Requirements

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness.

#### Scenario: Agent reconnects with stale observed state
- **WHEN** a node reconnects and its full report does not match the persisted desired Stream or Job state
- **THEN** the Hub creates or resumes one eligible Attempt for the current generation and does not dispatch a stale task assignment

#### Scenario: Hub restarts with an unfinished rollout
- **WHEN** the Hub loads a persisted rollout that is not converged, cancelled, or rolled back
- **THEN** it restores the current batch and resumes only eligible current-generation intents after storage recovery, including their persisted operations

#### Scenario: Job checkpoint completes
- **WHEN** a Job checkpoint is durably committed and all expected assignment observations report the checkpoint generation
- **THEN** the Hub records the checkpoint as the latest valid recovery point and may advance Job convergence or rollout health gates

#### Scenario: Checkpoint has an offline assignment
- **WHEN** an expected assignment is offline or has not produced a manifest for the checkpoint generation
- **THEN** the Hub leaves the checkpoint attempt incomplete or failed and does not dispatch a partial commit that could become a recoverable artifact

#### Scenario: Job task fails during recovery
- **WHEN** a Job task cannot restore from the selected checkpoint
- **THEN** the Hub records a bounded recovery failure, preserves the last valid checkpoint, and does not report the Job as healthy
