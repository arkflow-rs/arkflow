# Delta Spec: control-plane-console

## ADDED Requirements

### Requirement: Job pause and resume SHALL be controllable from the console

The Job views SHALL expose pause and resume actions gated by the caller's authorized actions, render the pause phase (in flight, paused, resuming) distinctly from stopped and running, keep actions disabled while a pause or resume transition is in flight, and surface rejected transitions with the problem code instead of silently ignoring them. The strings SHALL be translated in the English and Simplified Chinese dictionaries.

#### Scenario: Pause a running Job from the console

- **WHEN** an operator with the operate action opens the Jobs page for a converged running Job
- **THEN** a pause control is offered, invoking it sends the pause mutation, and the Job row shows the pause in-flight phase until it converges to paused

#### Scenario: Resume a paused Job from the console

- **WHEN** an operator opens the Jobs page for a paused Job
- **THEN** a resume control is offered and the row shows the resuming phase until the Job converges back to running

#### Scenario: Unauthorized or conflicting action feedback

- **WHEN** a user without the operate action, or against a Job with an active orchestration, invokes pause
- **THEN** the console disables the action for unauthorized callers, and for a rejected mutation surfaces the returned problem code
