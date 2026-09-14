## MODIFIED Requirements

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness. A lifecycle operation whose terminal result already satisfies the desired state at the current generation SHALL be remembered as a dispatch skip condition: the periodic scan SHALL NOT re-enqueue that operation while the desired state, generation, and operation are unchanged. A failure to enqueue commands for one Job (node unavailable, queue capacity, or a lost race with node expiry) SHALL be recorded and SHALL skip only that Job for the tick instead of aborting reconciliation of the remaining Jobs. Operation records, checkpoint attempt records, processed reconciliation outbox rows, and terminal Attempt records SHALL be retained under a bounded retention policy that also reclaims non-completed and processed records, so the durable operation, checkpoint, outbox, and attempt stores cannot grow without bound. Unprocessed outbox rows and active Attempt records SHALL NOT be reclaimed by retention.

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

#### Scenario: A stopped Job is not re-commanded every tick
- **WHEN** a Job's desired state is stopped and the current generation already has a succeeded stop operation for every target node, and neither the desired state nor the generation changed
- **THEN** subsequent reconcile ticks dispatch no new stop commands and create no new operation records for that Job

#### Scenario: One Job's dispatch failure does not stall the scan
- **WHEN** enqueueing commands for one Job fails because the target node expired between the pre-check and the enqueue or the command queue is at capacity
- **THEN** the Hub records the failure for that Job and continues reconciling the remaining Jobs in the same scan

#### Scenario: Non-completed checkpoint records are reclaimed
- **WHEN** checkpoint attempt records remain pending or failed beyond the retention bound
- **THEN** the retention sweep reclaims them and the durable checkpoint store does not grow without bound

#### Scenario: Processed outbox rows converge under steady churn
- **WHEN** reconciliation continuously inserts outbox rows that are claimed and marked processed, and the retention sweep runs
- **THEN** processed rows older than the retention window or beyond the count bound are reclaimed, the unprocessed backlog is untouched, and the outbox store does not grow without bound

#### Scenario: Terminal attempt records are reclaimed without touching active attempts
- **WHEN** Attempt records have reached a terminal state with a completion time beyond the retention window, while an Attempt for another (node, stream, generation) remains active
- **THEN** the retention sweep reclaims the terminal records and the active Attempt row is preserved unchanged
