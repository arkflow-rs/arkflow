## Purpose

Define immutable configuration rollouts, bounded batch scheduling, health gates, and durable rollout actions.

## Requirements

### Requirement: Create an immutable configuration rollout

The service SHALL validate and persist an immutable configuration version before accepting a rollout and SHALL associate the rollout with an explicit target selection, actor, correlation ID, and desired generation.

#### Scenario: Accept a valid rollout
- **WHEN** an authorized operator submits a valid configuration and target node selection
- **THEN** the service persists the version and rollout atomically and returns an accepted non-terminal rollout representation

### Requirement: Bounded rollout batches

The Hub SHALL apply a rollout to no more than the configured batch size at once and SHALL expose per-node state, target version, attempt identity, and outcome.

#### Scenario: Advance to the next batch
- **WHEN** every node in the current batch reaches the target version and passes health gates
- **THEN** the Hub marks that batch complete and makes only the next eligible batch dispatchable

### Requirement: Health-gated rollout convergence

The service SHALL mark a rollout converged only after every selected node is compatible, the target configuration is observed, affected Streams satisfy desired lifecycle state, and configured health gates pass.

#### Scenario: Do not converge on command acknowledgement
- **WHEN** a node acknowledges configuration application but has not reported the target version and healthy affected Streams
- **THEN** the rollout remains applying or paused and is not reported as converged

### Requirement: Pause, resume, cancel, and rollback

An authorized operator SHALL be able to pause, resume, cancel, or create a rollback from a non-terminal rollout. These actions SHALL be durable, generation-fenced, and auditable.

#### Scenario: Pause after a failed gate
- **WHEN** a node fails a retryable or health gate during rollout
- **THEN** the rollout enters paused or degraded state, prevents the next batch from dispatching, and records the failure

#### Scenario: Resume a paused rollout
- **WHEN** an authorized operator resumes a paused rollout and its current targets are eligible
- **THEN** the Hub continues from the durable batch position without duplicating completed node work

### Requirement: Single-node compatibility rollout

Existing node-level configuration apply and rollback routes SHALL create or reuse a single-node rollout and SHALL return the canonical rollout/operation convergence representation.

#### Scenario: Use a legacy apply route
- **WHEN** a client posts configuration to an existing node-level apply route
- **THEN** the service creates a single-node rollout with equivalent validation, audit, and convergence semantics
