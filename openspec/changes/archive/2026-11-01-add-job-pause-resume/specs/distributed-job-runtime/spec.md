# Delta Spec: distributed-job-runtime

## ADDED Requirements

### Requirement: Paused Jobs SHALL be inert under reconciliation

While a pause phase is present, the reconciler SHALL treat the Job as fenced: it SHALL NOT dispatch start commands, SHALL NOT supersede or re-create start claims, SHALL NOT re-place the Job, and SHALL NOT subject it to pressure rebalancing; only the pause state machine's own convergence steps (an artifact round, a stop dispatch, or observation of a resumed start) may act. Job convergence for a paused Job SHALL be defined by the pause state machine — a paused desired state converges only when the pause phase is the paused phase and the observed state is stopped, regardless of string equality between desired and observed state.

#### Scenario: Reconciler does not start a paused Job

- **WHEN** a reconcile tick observes a Job whose desired state is `paused`
- **THEN** no `JobStart` is dispatched and no stale start claim is superseded for it

#### Scenario: Reconciler does not relocate a paused Job

- **WHEN** a paused Job's recorded nodes remain online but its placement would otherwise be reconsidered
- **THEN** the recorded placement is retained and no stop or start command is issued by placement logic

#### Scenario: Paused convergence is phase-defined

- **WHEN** a paused Job's observed state is stopped and its pause phase has reached the paused phase
- **THEN** the Job reports converged even though the desired state string is `paused`

#### Scenario: In-flight pause is not converged

- **WHEN** a paused Job's observed state is stopped but the pause phase is still in flight
- **THEN** the Job reports reconciling
