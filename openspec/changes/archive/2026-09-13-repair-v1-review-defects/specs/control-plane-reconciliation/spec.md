## MODIFIED Requirements

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness. A lifecycle operation whose terminal result already satisfies the desired state at the current generation SHALL be remembered as a dispatch skip condition: the periodic scan SHALL NOT re-enqueue that operation while the desired state, generation, and operation are unchanged. A failure to enqueue commands for one Job (node unavailable, queue capacity, or a lost race with node expiry) SHALL be recorded and SHALL skip only that Job for the tick instead of aborting reconciliation of the remaining Jobs. Operation records and checkpoint attempt records SHALL be retained under a bounded retention policy that also reclaims non-completed records, so the durable operation and checkpoint stores cannot grow without bound.

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

### Requirement: Idempotent one-shot actions

The Agent and Hub SHALL support idempotent lifecycle commands and SHALL identify non-stable actions such as restart with a unique action ID that is reported after completion. The Agent's completed-command dedup cache SHALL evict entries one at a time when the cache is full and SHALL NOT clear the cache wholesale, so recently completed commands remain deduplicated across Hub redelivery.

#### Scenario: Duplicate lifecycle delivery
- **WHEN** an Agent receives the same command ID more than once
- **THEN** it executes the command at most once and returns the existing result

#### Scenario: Restart is confirmed
- **WHEN** a restart action completes and the Agent reports its action ID with the Stream observed as running
- **THEN** the Hub marks the restart Intent converged even though desired and observed stable state are both running

#### Scenario: A full dedup cache does not forget recent completions wholesale
- **WHEN** the Agent's completed-command cache reaches its bound and a new command completes
- **THEN** only the oldest entry is evicted, a redelivered recently completed command still returns its existing result, and no running Job is restarted by a re-executed lifecycle command
