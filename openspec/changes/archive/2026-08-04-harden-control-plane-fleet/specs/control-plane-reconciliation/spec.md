## MODIFIED Requirements

### Requirement: Reconciliation triggers and recovery

The Hub SHALL trigger reconciliation after desired-state changes, rollout batch changes, node registration, valid reports, lease recovery, and expired Attempts, and SHALL provide a periodic bounded scan as a recovery mechanism. Recovery SHALL restore unfinished Operations and Rollouts from durable state before reporting readiness.

#### Scenario: Hub restarts with an unfinished rollout
- **WHEN** the Hub loads a persisted rollout that is not converged, cancelled, or rolled back
- **THEN** it restores the current batch and resumes only eligible current-generation intents after storage recovery

#### Scenario: Agent reconnects with stale observed state
- **WHEN** a node reconnects and its full report does not match persisted desired state or rollout target
- **THEN** the Hub creates or resumes one eligible Attempt for the current generation and preserves the rollout batch position

### Requirement: Configuration convergence

The Hub SHALL treat a target configuration version as part of desired state and SHALL mark a node-level configuration publication converged only after the target node reports that version applied and affected Streams satisfy desired lifecycle states. A Fleet rollout SHALL additionally require completion of its batch health gates before advancing or converging.

#### Scenario: Publish configuration to a connected node
- **WHEN** an authorized operator publishes a validated configuration version
- **THEN** the Hub records the version as desired, schedules application, and returns a non-terminal result until the node reports the version and affected state

#### Scenario: Advance after batch health gates
- **WHEN** every node in a rollout batch reports the target version and passes its configured health gates
- **THEN** the Hub marks the batch complete and dispatches no more than the configured next batch

#### Scenario: Configuration application is blocked
- **WHEN** a node cannot apply a validly stored configuration because of a permanent component error
- **THEN** the Hub preserves the previous observed version, marks the node intent and rollout batch blocked or paused with the failure reason, and leaves rollback available
