## MODIFIED Requirements

### Requirement: Generation fencing and supersession
The Hub SHALL increment a resource generation for every desired-state mutation, include it in commands, include it in active-operation deduplication, and prevent an older generation from superseding a newer generation. Reconciliation SHALL stop placements belonging to prior generations when the desired placement changes.

#### Scenario: New intent supersedes an old command
- **WHEN** generation 43 requests stopped after generation 42 requested running and the Agent receives the generation 42 command late
- **THEN** the Agent does not apply the stale command, reports it as superseded, and the Hub continues reconciling generation 43

#### Scenario: Placement moves between nodes
- **WHEN** a running Job changes generation and its explicit placement moves from node A to node B
- **THEN** reconciliation sends a stop for the prior-generation placement on node A and a start for the current generation on node B

### Requirement: Reconciliation triggers and recovery
The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, Job deployment or recovery changes, node registration, valid reports, lease recovery, checkpoint completion or failure, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations, Rollouts, and Job deployments from durable state before reporting readiness.

#### Scenario: Hub restarts with an unfinished rollout
- **WHEN** the Hub loads a persisted rollout that is not converged, cancelled, or rolled back
- **THEN** it restores the current batch and resumes only eligible current-generation intents after storage recovery, including their persisted operations

#### Scenario: Job task fails during recovery
- **WHEN** a Job task cannot restore from the selected checkpoint
- **THEN** the Hub records a bounded recovery failure, preserves the last valid checkpoint, and does not report the Job as healthy

### Requirement: Failure classification and retry
The Hub SHALL distinguish rejected requests, transient failures, unavailable nodes, permanent execution failures, ambiguous results, and superseded commands, and SHALL apply bounded retry with recorded attempt count and next retry time to retryable failures. An expired command lease is a retryable or ambiguous attempt, not a silently discarded operation.

#### Scenario: Temporary node failure
- **WHEN** a command fails because the node or transport is temporarily unavailable
- **THEN** the desired state remains unchanged, the Intent enters retrying or degraded, and the Hub schedules a backoff retry

#### Scenario: Ambiguous command result
- **WHEN** the Agent disconnects after receiving a command but before reporting its result
- **THEN** the Hub marks the Attempt outcome ambiguous or expired, requests a fresh report, and does not retry until the observed state is evaluated
