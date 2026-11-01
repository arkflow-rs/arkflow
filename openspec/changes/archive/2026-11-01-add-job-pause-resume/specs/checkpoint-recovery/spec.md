# Delta Spec: checkpoint-recovery

## ADDED Requirements

### Requirement: Pause artifacts SHALL remain recoverable until resume completes

A pause artifact SHALL be produced through the ordinary recovery-artifact path with the kind selected by the Job's recovery policy, and SHALL remain selectable by the ordinary recovery selection while the pause phase is present. Checkpoint retention SHALL NOT garbage-collect a checkpoint-kind pause artifact referenced by an active pause phase; savepoint-kind pause artifacts are naturally excluded from retention collection and SHALL also be treated as pinned defensively. The protection SHALL end when the pause phase is cleared by resume convergence, a stop, or a delete.

#### Scenario: Recovery selects the pause artifact on resume

- **WHEN** a paused Job is resumed and the ordinary recovery selection runs
- **THEN** it selects the pause artifact as the recovery cut and restores source positions, watermarks, and keyed state from it

#### Scenario: Retention does not remove a paused Job's checkpoint-kind artifact

- **WHEN** retention enforcement runs while a Job with a checkpoint-kind pause artifact is paused
- **THEN** the pause artifact is not deleted and remains available to the resume path
