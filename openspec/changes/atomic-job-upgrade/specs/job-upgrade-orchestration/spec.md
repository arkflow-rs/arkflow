# Spec Delta: job-upgrade-orchestration (new capability)

## ADDED Requirements

### Requirement: Atomic upgrade initiation SHALL validate and hold per-job exclusivity
The Hub SHALL accept an atomic upgrade request for a running Job only when the target spec version is strictly newer than the current version, the target's state format matches the current recovery artifact's format, the target spec deep-validates, and no non-terminal orchestration exists for that Job. A rejected request SHALL leave the Job and any existing orchestration unchanged.

#### Scenario: Valid atomic upgrade is accepted
- **WHEN** the operator submits an atomic upgrade with a strictly newer spec version, a compatible state format, and a valid spec for a Job with no active orchestration
- **THEN** the Hub returns 202 with an orchestration id and the current phase, and the orchestration enters its savepoint phase

#### Scenario: Concurrent orchestration is rejected
- **WHEN** an atomic upgrade is requested for a Job that already has a non-terminal orchestration
- **THEN** the request fails with `orchestration_in_progress` and neither the Job nor the existing orchestration changes

#### Scenario: Invalid target version or format is rejected before any effect
- **WHEN** the submitted spec version is not newer than the Job's current version, or its declared state format is incompatible with the Job's current recovery artifact
- **THEN** the request is rejected with the existing version-monotonicity or state-format errors and no savepoint is dispatched

### Requirement: The savepoint phase SHALL preserve the running old generation
During the savepoint phase the old generation SHALL continue processing, and the orchestration SHALL dispatch its savepoint at the Job's current generation through the ordinary checkpoint dispatch path. A failed savepoint round SHALL be retried under a bounded orchestration-level retry count, and exhaustion or phase deadline SHALL terminate the orchestration as `aborted` with the old generation still running and unchanged.

#### Scenario: Old generation keeps processing during the savepoint
- **WHEN** the orchestration's savepoint round is in flight
- **THEN** the old generation continues to process and emit data, and a barrier failure on one node fails the round without stopping the Job

#### Scenario: Savepoint failure exhausts retries
- **WHEN** savepoint rounds fail until the orchestration retry bound is reached
- **THEN** the orchestration terminates as `aborted`, an event and audit record are emitted, and the Job remains running at its current version and generation

#### Scenario: Node goes stale during the savepoint phase
- **WHEN** a placement node becomes stale while the savepoint phase is active
- **THEN** the general reconciler does not re-place the Job (see the reconciliation fence), and the phase deadline eventually aborts the orchestration, after which normal reconciliation handles the degraded placement

### Requirement: The commit phase SHALL be a single generation-fenced write without a recovery-pointer write
The version commit SHALL be one conditional Job write carrying the new spec, `desired_state = running`, and the bumped generation, and SHALL NOT write the recovery pointer: the pointer SHALL already reference the completed savepoint because the savepoint completed while the Job was still at the old version. On a generation conflict the orchestration SHALL re-read the Job and interpret observable state: if the Job is already at the target version with `desired_state = running`, the conflict SHALL be treated as an already-applied commit and the orchestration SHALL advance to verification; otherwise the orchestration SHALL abort.

#### Scenario: Commit succeeds and preserves the pointer
- **WHEN** the savepoint has completed and the commit write applies
- **THEN** the Job record holds the new spec and `desired_state = running` at a bumped generation, its recovery pointer still references the savepoint, and a version-history record for the new spec exists

#### Scenario: Hub restarts between the commit write and the phase update
- **WHEN** the recovered orchestration re-attempts the commit and the write conflicts because the Job is already at the target version with `desired_state = running`
- **THEN** the orchestration interprets the conflict as already-committed and proceeds to verification without issuing a second effective write

#### Scenario: Commit conflicts with an unrelated concurrent change
- **WHEN** the commit write conflicts and the re-read Job is not at the target version with `desired_state = running`
- **THEN** the orchestration aborts with an explanatory error and the newer Job state is preserved

### Requirement: The verification phase SHALL converge through normal reconciliation and roll back on deadline
During verification the general reconciler SHALL be permitted to reconcile the Job (it is the start mechanism), and the orchestration SHALL observe the Job until `observed_state` is running at the target version, or the phase deadline expires, or the Job reports a failure that reconciliation cannot clear within the deadline. On deadline or unresolved failure the orchestration SHALL initiate a rollback.

#### Scenario: New generation converges
- **WHEN** reconciliation starts the new generation and every expected node observes the Job running at the target version
- **THEN** the orchestration terminates as `succeeded` and emits a completion event and audit record

#### Scenario: Verification deadline expires
- **WHEN** the Job has not been observed running at the target version when the phase deadline expires
- **THEN** the orchestration initiates a rollback to the previous version instead of leaving the Job in an ambiguous state

#### Scenario: Old generation is fenced by the new generation's start
- **WHEN** the new generation starts during verification
- **THEN** the reconciler's existing historical-node fencing stops the old generation without any orchestration-issued stop command

### Requirement: Rollback SHALL restore the previous version from the same savepoint
A rollback SHALL be executed as a new orchestration-like operation that writes the previous Job version through the same generation-fenced path with `desired_state = running` and the original savepoint as the recovery artifact, after re-validating artifact compatibility for the restored version. If the rollback itself fails, the orchestration SHALL terminate as `failed`, emit an event, and leave the Job stopped with its recovery pointer intact for operator action; the referenced savepoint SHALL remain pinned until a terminal state.

#### Scenario: Rollback restores the previous version
- **WHEN** verification fails and the rollback applies
- **THEN** the Job runs again at the previous version, restoring its state from the same savepoint artifact

#### Scenario: Rollback fails terminally
- **WHEN** the rollback write or the restored version's recovery start fails irrecoverably
- **THEN** the orchestration terminates as `failed`, an event makes the failure visible, the Job is left stopped with `pending_recovery` and its pointer intact, and the savepoint is not garbage-collected

### Requirement: Orchestration SHALL survive Hub restart and leadership change
Orchestration phase, deadline, and savepoint reference SHALL be durable, and a Hub that starts or gains leadership with a non-terminal orchestration SHALL resume it idempotently: a savepoint phase SHALL dispatch a fresh savepoint round, a commit phase SHALL apply the conflict-interpretation rule, and a verification phase SHALL continue observing. In-flight commands lost with the previous Hub process SHALL be settled by the existing non-terminal operation recovery (TimedOut) before the orchestration proceeds.

#### Scenario: Restart during the savepoint phase
- **WHEN** the Hub restarts while the savepoint phase is active and the in-flight savepoint command settles as TimedOut
- **THEN** the recovered orchestration dispatches a fresh savepoint round at the current generation and continues

#### Scenario: Standby promotion with a non-terminal orchestration
- **WHEN** a standby Hub is promoted while an orchestration is non-terminal
- **THEN** the promoted leader recovers the orchestration from durable state and resumes it without re-applying completed phases

### Requirement: Checkpoint retention SHALL pin orchestration-referenced artifacts
Retention enforcement SHALL NOT delete a completed artifact that a non-terminal orchestration references as its savepoint, including while a rollback is in flight, and SHALL release the pin when the orchestration reaches a terminal state.

#### Scenario: Retention skips the orchestration's savepoint
- **WHEN** the retention sweep would delete the oldest completed artifacts and one of them is referenced by an active orchestration
- **THEN** the sweep deletes only unreferenced artifacts and the orchestration's savepoint remains recoverable

#### Scenario: Pin is released on termination
- **WHEN** an orchestration reaches a terminal state
- **THEN** subsequent retention sweeps treat its formerly referenced artifacts normally

### Requirement: Atomic upgrade cutover semantics SHALL be explicit
The API and documentation SHALL state the cutover consistency semantics: events processed by the old generation after the savepoint barrier are replayed by the new generation, yielding exactly-once behavior for transactional sinks and at-least-once behavior otherwise, and the orchestration SHALL minimize the window by proceeding to commit in the same reconcile tick in which the savepoint completes.

#### Scenario: Non-transactional sink sees a bounded replay window
- **WHEN** an atomic upgrade completes on a Job with a non-transactional sink
- **THEN** events processed by the old generation after the savepoint barrier may appear again from the new generation, the bound is the savepoint-to-cutover interval, and the documentation states this behavior

### Requirement: Orchestration SHALL be observable and operator-controllable
Phase transitions SHALL emit events through the Hub's event broadcast (SSE-visible) and audit records for operator actions (initiate, pause, resume, cancel, rollback). The orchestration status SHALL be queryable, and actions SHALL be rejected on terminal orchestrations; resume SHALL be accepted only from `paused`.

#### Scenario: Phase transitions are visible on the event stream
- **WHEN** an orchestration transitions between phases
- **THEN** an event carrying the orchestration id, Job id, and phase is broadcast to SSE subscribers

#### Scenario: Operator cancels a non-terminal orchestration
- **WHEN** the operator cancels an orchestration that has not reached a terminal state
- **THEN** the orchestration terminates as `cancelled`, the pin on its savepoint is released, and subsequent mutations of the Job are accepted again
