# Purpose

Define durable desired-state reconciliation between the Hub, Agents, and observed runtime resources.

# Requirements

### Requirement: Durable desired state

The Hub SHALL persist desired state independently from observed node and Stream state, including a monotonically increasing generation, optional target configuration version, update time, and update correlation metadata.

#### Scenario: Set a Stream target while the node is offline
- **WHEN** an authorized operator requests a Stream to run on an offline node
- **THEN** the Hub persists the running target and generation without claiming execution, and marks convergence as unknown or degraded until the node reconnects

#### Scenario: Node report conflicts with intent
- **WHEN** a node reports a stopped Stream while the Hub desired state is running
- **THEN** the Hub retains the running desired state and schedules reconciliation instead of overwriting the intent

### Requirement: Intent, attempt, and convergence state

The Hub SHALL expose separate state for a durable reconciliation Intent, each command Attempt, and the computed resource Convergence condition.

#### Scenario: Command acknowledgement is not final success
- **WHEN** an Agent acknowledges a start command but has not reported the target generation as running
- **THEN** the Attempt MAY be acknowledged or running while the Intent remains converging and Convergence is not in_sync

#### Scenario: Observed state reaches the target
- **WHEN** a node report matches the desired state, target configuration version, and generation
- **THEN** the Hub marks Convergence in_sync and the associated Intent converged

### Requirement: Generation fencing and supersession

The Hub SHALL increment a resource generation for every desired-state mutation, include it in commands, and prevent an older generation from superseding a newer generation.

#### Scenario: New intent supersedes an old command
- **WHEN** generation 43 requests stopped after generation 42 requested running and the Agent receives the generation 42 command late
- **THEN** the Agent does not apply the stale command, reports it as superseded, and the Hub continues reconciling generation 43

#### Scenario: Concurrent writes to one resource
- **WHEN** two desired-state writes target the same node and Stream
- **THEN** the Hub serializes them, returns the resulting generation for each accepted write, and marks the older intent superseded when the newer write commits

### Requirement: Reconciliation triggers and recovery

The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, node registration, valid reports, lease recovery, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations and Rollouts from durable state before reporting readiness.

#### Scenario: Agent reconnects with stale observed state
- **WHEN** a node reconnects and its full report does not match the persisted desired state
- **THEN** the Hub creates or resumes one eligible Attempt for the current generation

#### Scenario: Hub restarts with an unfinished rollout
- **WHEN** the Hub loads a persisted rollout that is not converged, cancelled, or rolled back
- **THEN** it restores the current batch and resumes only eligible current-generation intents after storage recovery

#### Scenario: Hub restarts with an unfinished intent
- **WHEN** the Hub loads a persisted Intent that is not converged or superseded
- **THEN** it restores the Intent and resumes reconciliation after the target node is known and authenticated

### Requirement: Failure classification and retry

The Hub SHALL distinguish rejected requests, transient failures, unavailable nodes, permanent execution failures, ambiguous results, and superseded commands, and SHALL apply bounded retry with recorded attempt count and next retry time to retryable failures.

#### Scenario: Temporary node failure
- **WHEN** a command fails because the node or transport is temporarily unavailable
- **THEN** the desired state remains unchanged, the Intent enters retrying or degraded, and the Hub schedules a backoff retry

#### Scenario: Permanent execution failure
- **WHEN** the Agent reports a validated non-retryable configuration or component failure
- **THEN** the Attempt is failed, the Intent becomes blocked, and the Hub does not hot-loop retries or change the desired state

#### Scenario: Ambiguous command result
- **WHEN** the Agent disconnects after receiving a command but before reporting its result
- **THEN** the Hub marks the Attempt outcome ambiguous or expired, requests a fresh report, and does not retry until the observed state is evaluated

### Requirement: Idempotent one-shot actions

The Agent and Hub SHALL support idempotent lifecycle commands and SHALL identify non-stable actions such as restart with a unique action ID that is reported after completion.

#### Scenario: Duplicate lifecycle delivery
- **WHEN** an Agent receives the same command ID more than once
- **THEN** it executes the command at most once and returns the existing result

#### Scenario: Restart is confirmed
- **WHEN** a restart action completes and the Agent reports its action ID with the Stream observed as running
- **THEN** the Hub marks the restart Intent converged even though desired and observed stable state are both running

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
