# Delta Spec: job-pause-resume

## ADDED Requirements

### Requirement: Pause SHALL durably quiesce a Job

The Hub SHALL accept a pause mutation only for a Job whose desired state is `running`, whose observed state is `running` and converged at the current generation, which has no active upgrade orchestration, no pause phase in flight, and whose recovery policy permits selecting a recovery artifact (`recovery: fail` Jobs SHALL be rejected with a stable problem code). Acceptance SHALL persist the pause intent (`desired_state = paused` plus a durable pause phase, the pre-pause recovery pointer, the pause actor, and the pause timestamp) in one generation-fenced write, and SHALL return `202` with the updated Job record; the Job SHALL NOT be reported as paused until quiesce completes. When the Job can produce a recovery artifact, pause SHALL first produce a completed artifact of the kind its recovery policy selects (a `checkpoint` for `latest_checkpoint`, a `savepoint` for `latest_savepoint`) at the pause generation and only then dispatch the stop; when the Job declares no checkpoint configuration, pause SHALL skip the artifact phase and dispatch the stop directly. A rejected pause SHALL leave desired state, phase, placement, and generation unchanged and SHALL return a stable problem code.

#### Scenario: Pause a converged running Job with a checkpoint policy

- **WHEN** an operator pauses a Job whose desired and observed state are both `running` at the current generation and whose recovery policy is `latest_checkpoint`
- **THEN** the Hub returns `202`, records the pause intent, phase, and pre-pause pointer, dispatches a checkpoint artifact at that generation, and the Job is not marked paused while the artifact is in flight

#### Scenario: Artifact kind follows the recovery policy

- **WHEN** a Job whose recovery policy is `latest_savepoint` is paused
- **THEN** the pause artifact is recorded with the savepoint kind so the ordinary recovery selection accepts it on resume

#### Scenario: Pause waits for the artifact before stopping

- **WHEN** the pause artifact has not yet completed
- **THEN** no stop command is dispatched for the Job and the pause phase remains the artifact phase

#### Scenario: Pause of a Job without checkpoint configuration stops directly

- **WHEN** an operator pauses a running Job whose spec declares no checkpoint configuration
- **THEN** the Hub skips the artifact phase, dispatches the stop, and the resume path starts the Job fresh

#### Scenario: Pause requires a selectable recovery policy

- **WHEN** an operator pauses a running Job whose recovery policy is `fail`
- **THEN** the Hub returns `409 pause_requires_recovery_policy` and the Job is unchanged

#### Scenario: Rejected pause does not mutate the Job

- **WHEN** a pause is requested for a Job that is stopped, already paused, pausing, owned by an active orchestration, or whose recovery policy is `fail`
- **THEN** the Hub returns `409` with a stable problem code and the Job's desired state, phase, placement, and generation are unchanged

### Requirement: Pause SHALL converge only after the Job stops and the artifact is durable

The Job SHALL be marked paused only when the pause phase is the paused phase, the pause artifact reference (when one was produced) is durable in the Job record, and every target node's observation at the pause generation reports the Job stopped. Pause convergence SHALL be defined by the pause state machine rather than by equality between `desired_state` and the observed state; while the phase is in flight the Job SHALL report `reconciling`. A target node that is offline SHALL keep the pause pending (reconciling) rather than being reported as converged, and dispatch SHALL resume when the node returns. An artifact failure or the artifact phase deadline SHALL abort the pause, restore the Job to its pre-pause running state, **restore the recovery pointer to the pre-pause pointer**, retain a stable failure code in the Job's last error, and leave the Job processing.

#### Scenario: Observed stopped advances the pause to converged

- **WHEN** every target node reports the Job stopped at the pause generation and the phase is the stopped-waiting phase
- **THEN** the Hub records the paused phase and the Job reports converged

#### Scenario: Artifact failure aborts the pause and restores the pointer

- **WHEN** the pause artifact fails or its phase deadline expires
- **THEN** the Job returns to `desired_state = running`, the pause phase and artifact reference are cleared, the recovery pointer is restored to the pre-pause pointer, the old generation keeps running, and a stable `pause_savepoint_failed` failure code is recorded

#### Scenario: Offline node keeps the pause pending

- **WHEN** a target node is offline while the pause is in the stopped-waiting phase
- **THEN** the Job remains reconciling rather than converged, and the stop is dispatched again once the node returns

#### Scenario: In-flight pause is not converged

- **WHEN** the Job's desired state is `paused` and its observed state is `stopped` but the pause phase is still in flight
- **THEN** the Job reports `reconciling`, not converged

### Requirement: Resume SHALL restart from the pause artifact without operator-provided input

The Hub SHALL accept a resume mutation only when the pause phase is the paused phase; acceptance SHALL persist `desired_state = running`, the resuming phase, and a bumped generation while keeping the recovery pointer on the pause artifact, and SHALL dispatch the ordinary recovery start. The Job SHALL clear the pause phase, pause artifact pin, and pause actor/timestamp only after the start converges with the observed state `running`. A resume whose start fails SHALL leave the Job in the existing Job failure semantics with the pause artifact still pinned, and SHALL NOT silently fall back to the paused state.

#### Scenario: Resume restores from the pause artifact

- **WHEN** an operator resumes a paused Job
- **THEN** the Hub dispatches a recovery start whose recovery artifact is the pause artifact (of the kind the recovery policy selects), without the operator supplying a checkpoint or savepoint id

#### Scenario: Resume clears pause state only on convergence

- **WHEN** the resume start has not yet converged to observed `running`
- **THEN** the pause phase remains the resuming phase and the pause artifact remains pinned

#### Scenario: Resume rejected outside the paused phase

- **WHEN** an operator resumes a Job whose pause phase is in flight or absent
- **THEN** the Hub returns `409` with a stable problem code and does not change desired state or generation

### Requirement: Pause and resume SHALL survive Hub restart and leadership change

Pause phases, artifact references, retry counts, and the pause generation SHALL be durable. After a Hub restart or a leadership change, the reconcile tick SHALL resume a non-terminal pause or resume idempotently: an artifact phase SHALL dispatch a fresh artifact round with a new artifact id, a stopped-waiting phase SHALL re-dispatch the stop command, a paused phase SHALL dispatch nothing, and a resuming phase SHALL continue observing the start. Lost in-flight commands SHALL be settled through the existing non-terminal operation recovery before the pause state machine advances. The number of artifact re-dispatches SHALL be bounded; exhausting the bound SHALL abort the pause with the stable failure code and restore the pre-pause recovery pointer.

#### Scenario: Restart during the artifact phase

- **WHEN** the Hub restarts while the artifact phase is in flight and the in-flight artifact command settles as timed out
- **THEN** the recovered Hub dispatches a fresh artifact round at the pause generation and the pause continues

#### Scenario: Restart during the stop phase

- **WHEN** the Hub restarts while the pause phase is the stopped-waiting phase
- **THEN** the recovered Hub re-dispatches the stop command and the pause converges when the nodes report stopped

#### Scenario: Retry exhaustion aborts the pause

- **WHEN** the bounded artifact re-dispatch count is exhausted
- **THEN** the pause aborts with `pause_savepoint_failed`, the recovery pointer is restored, and the Job returns to running

### Requirement: A paused Job SHALL be inert and exclusive

While a pause phase is in flight or paused, reconciliation SHALL NOT start the Job, re-place it, supersede its start claims, or subject it to pressure rebalancing; the recorded placement SHALL be retained. Upgrades (atomic and stopped modes) SHALL be rejected while a pause phase is present, with a stable `job_paused` problem code. A `stop` on a paused or pausing Job SHALL settle it as stopped and clear the pause phase and artifact pin; a `start` or `restart` SHALL be treated as a resume. Deleting a paused Job SHALL release the pause artifact pin.

#### Scenario: Reconciler leaves a paused Job alone

- **WHEN** a reconcile tick runs over a paused Job
- **THEN** no start, stop, or re-placement command is dispatched for it beyond the pause state machine's own convergence step

#### Scenario: Upgrade rejected while paused

- **WHEN** an operator requests an atomic or stopped-mode upgrade for a paused or pausing Job
- **THEN** the Hub returns `409 job_paused` and the Job and its pause artifact are unchanged

#### Scenario: Stop clears pause bookkeeping

- **WHEN** an operator stops a paused Job
- **THEN** the Job is recorded as stopped with no pause phase, and the pause artifact is no longer pinned

#### Scenario: Start or restart resumes a paused Job

- **WHEN** an operator issues start or restart for a paused Job
- **THEN** the Job follows the resume path and restores from the pause artifact

### Requirement: The pause artifact SHALL be retention-protected

While a pause phase is present, checkpoint retention SHALL NOT delete the pause artifact or the recovery pointer it references: checkpoint-kind artifacts are eligible for retention garbage collection and SHALL be excluded through the pin mechanism, and savepoint-kind artifacts SHALL additionally be excluded defensively even though retention does not collect savepoints. The pin SHALL be released when the pause phase is cleared by resume convergence, stop, or delete.

#### Scenario: Retention skips a checkpoint-kind pause artifact

- **WHEN** the retention sweep would delete the oldest completed artifacts and one is a paused Job's checkpoint-kind pause artifact
- **THEN** that artifact is skipped and remains recoverable for resume

#### Scenario: Pin released after resume converges

- **WHEN** a resumed Job reaches observed `running` and the pause phase is cleared
- **THEN** later retention sweeps treat the former pause artifact normally

### Requirement: Pause and resume SHALL be operator-visible and audited

Pause and resume mutations SHALL be auditable lifecycle mutations recording actor, action (`job.pause` / `job.resume`), target Job, correlation id, outcome, and a stable failure code where applicable, without Job configuration bodies or credentials. Phase transitions SHALL emit events on the Hub event stream, and the Job record and detail SHALL expose the pause phase, artifact reference, pause timestamp, and pause actor. The Hub SHALL export a closed-label counter of pause outcomes.

#### Scenario: Audited pause rejection

- **WHEN** a pause is rejected because the Job is not running
- **THEN** an audit record with action `job.pause`, outcome `rejected`, and a stable failure code is persisted

#### Scenario: Paused state visible on Job detail

- **WHEN** an operator reads the paused Job's record or detail after convergence
- **THEN** the response exposes the paused phase, pause artifact reference, pause timestamp, and pause actor, and no credential or spec body

#### Scenario: Phase transitions reach the event stream

- **WHEN** a pause or resume phase transition commits
- **THEN** an event carrying the Job id and the phase is broadcast to event-stream subscribers

### Requirement: Pause SHALL preserve existing Job behavior when unused

Jobs that are never paused SHALL behave byte-identically to the pre-change runtime: the pause columns and response fields are additive and empty, no additional commands or events are emitted, and agent and kernel protocol are unchanged. Job creation SHALL continue to accept only `running` and `stopped` as initial desired states.

#### Scenario: Never-paused Job is unchanged

- **WHEN** a Job is created and run through start, stop, restart, checkpoint, and upgrade flows without any pause
- **THEN** its desired/observed states, generations, dispatched commands, and audit records match the pre-change behavior, and the pause fields read as empty

#### Scenario: Pause is not an initial desired state

- **WHEN** a Job creation request declares `desired_state = paused`
- **THEN** the request is rejected with a validation error and no Job is created
