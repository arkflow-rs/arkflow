## Purpose

Define durable desired-state reconciliation between the Hub, Agents, and observed runtime resources.
## Requirements
### Requirement: Durable desired state

The Hub SHALL persist desired state independently from observed node and Stream state, including a monotonically increasing generation, optional target configuration version, update time, and update correlation metadata.

#### Scenario: Set a Stream target while the node is offline
- **WHEN** an authorized operator requests a Stream to run on an offline node
- **THEN** the Hub persists the running target and generation without claiming execution, and marks convergence as unknown or degraded until the node reconnects

#### Scenario: Node report conflicts with intent
- **WHEN** a node reports a stopped Stream while the Hub desired state is running
- **THEN** the Hub retains the running desired state and schedules reconciliation instead of overwriting the intent

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

### Requirement: Generation fencing and supersession
The Hub SHALL increment a resource generation for every desired-state mutation, include it in commands, include it in active-operation deduplication, and prevent an older generation from superseding a newer generation. Reconciliation SHALL stop placements belonging to prior generations when the desired placement changes.

#### Scenario: New intent supersedes an old command
- **WHEN** generation 43 requests stopped after generation 42 requested running and the Agent receives the generation 42 command late
- **THEN** the Agent does not apply the stale command, reports it as superseded, and the Hub continues reconciling generation 43

#### Scenario: Placement moves between nodes
- **WHEN** a running Job changes generation and its explicit placement moves from node A to node B
- **THEN** reconciliation sends a stop for the prior-generation placement on node A and a start for the current generation on node B

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness, and SHALL also restore the durable lifecycle outcomes that drive dispatch-skip decisions, so a restart does not re-dispatch lifecycle commands for (node, Job, generation) pairs whose terminal result already satisfies the desired state. A lifecycle operation whose terminal result already satisfies the desired state at the current generation SHALL be remembered as a dispatch skip condition: the periodic scan SHALL NOT re-enqueue that operation while the desired state, generation, and operation are unchanged. A failure to enqueue commands for one Job (node unavailable, queue capacity, or a lost race with node expiry) SHALL be recorded and SHALL skip only that Job for the tick instead of aborting reconciliation of the remaining Jobs. Operation records, checkpoint attempt records, processed reconciliation outbox rows, and terminal Attempt records SHALL be retained under a bounded retention policy that also reclaims non-completed and processed records, so the durable operation, checkpoint, outbox, and attempt stores cannot grow without bound. Unprocessed outbox rows and active Attempt records SHALL NOT be reclaimed by retention.

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

#### Scenario: Restart does not re-dispatch satisfied lifecycle starts
- **WHEN** the Hub restarts while Agents keep running their assigned Jobs at the current generation, and the durable operation history contains succeeded starts for those (node, Job, generation) pairs
- **THEN** the recovered dispatch-skip memory suppresses re-enqueueing `job_start` for those pairs, and the Jobs keep running without a lifecycle command


### Requirement: Failure classification and retry
The Hub SHALL distinguish rejected requests, transient failures, unavailable nodes, permanent execution failures, ambiguous results, and superseded commands, and SHALL apply bounded retry with recorded attempt count and next retry time to retryable failures. An expired command lease is a retryable or ambiguous attempt, not a silently discarded operation. A retryable failure SHALL always converge to a fresh queued command after its backoff expires, even when the prior operation record for the intent is terminal.

#### Scenario: Temporary node failure
- **WHEN** a command fails because the node or transport is temporarily unavailable
- **THEN** the desired state remains unchanged, the Intent enters retrying or degraded, and the Hub schedules a backoff retry

#### Scenario: Ambiguous command result
- **WHEN** the Agent disconnects after receiving a command but before reporting its result
- **THEN** the Hub marks the Attempt outcome ambiguous or expired, requests a fresh report, and does not retry until the observed state is evaluated

#### Scenario: A retried intent produces a fresh command
- **WHEN** the retry backoff of an intent whose last attempt failed transiently expires and reconciliation runs
- **THEN** the Hub replaces the terminal operation record with a fresh queued operation carrying the new attempt's command id, and the next command poll delivers that command

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

### Requirement: Configuration convergence

The Hub SHALL treat a target configuration version as part of desired state and SHALL mark a node-level configuration publication converged only after the target node reports that version applied and affected Streams satisfy their desired lifecycle states. A Fleet rollout SHALL additionally require completion of its batch health gates before advancing or converging.

#### Scenario: Publish configuration to a connected node
- **WHEN** an authorized operator publishes a validated configuration version
- **THEN** the Hub records the version as the desired configuration, schedules application, and returns a non-terminal converging result until the node reports the version and affected state

#### Scenario: Configuration application is blocked
- **WHEN** a node cannot apply a validly stored configuration because of a permanent component error
- **THEN** the Hub preserves the previous observed version, marks the configuration Intent blocked with the failure reason, and leaves rollback or a new version available

#### Scenario: Advance after batch health gates
- **WHEN** every node in a rollout batch reports the target version and passes its configured health gates
- **THEN** the Hub marks the batch complete and dispatches no more than the configured next batch

### Requirement: Job operation state SHALL be aggregated by assignment
The Hub SHALL persist or derive one operation result per expected assignment and SHALL calculate the Job-level observed state using all results for the current generation/action. Terminal Job states SHALL not be written until the aggregate has enough information to distinguish success, retryable degradation, and permanent failure. The observation write SHALL be a compare-and-set on the generation the caller read: a report whose generation no longer matches SHALL be ignored without regressing the Job's generation, desired state, or convergence.

#### Scenario: All assignments succeed
- **WHEN** every expected assignment reaches `running` or `succeeded` for a start operation
- **THEN** the Job-level observed state becomes `running`

#### Scenario: A permanent failure is confirmed
- **WHEN** the complete assignment set has been evaluated and an assignment reports a non-retryable execution failure
- **THEN** the Job-level state becomes failed/blocked according to the operation policy and retains the assignment failure details

#### Scenario: A stale observation cannot regress a newer generation
- **WHEN** a desired-state change bumps the Job generation between the observation's read and write, and a report captured at the older generation is then applied
- **THEN** the storage rejects the write with a generation conflict, the Hub returns the current record, and the newer generation with its desired state survives

