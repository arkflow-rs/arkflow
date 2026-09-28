# Spec Delta: control-plane-reconciliation

## ADDED Requirements

### Requirement: Reconciliation defers to an active Job orchestration
While a non-terminal orchestration exists for a Job, the general reconciler SHALL defer placement decisions for that Job: during phases that own the Job's dispatch state (savepoint and commit) it SHALL skip reconciling the Job entirely — in particular it SHALL NOT re-place the Job or bump its generation in response to node staleness or placement churn — and during the observation phases (verification and rollback) it SHALL reconcile the Job normally, because ordinary reconciliation is the start mechanism there — for the new generation and for a restored previous one alike. When no orchestration is active for a Job, reconciliation SHALL behave exactly as before this change.

#### Scenario: Node goes stale while the savepoint phase owns the Job
- **WHEN** a placement node becomes stale while an orchestration is in its savepoint phase for a Job
- **THEN** the reconciler skips that Job for the tick without re-placing it or bumping its generation, preserving the generation-keyed savepoint dispatch targets

#### Scenario: Observation phases are reconciled normally
- **WHEN** an orchestration is in its verification or rollback phase and the Job's desired state is running
- **THEN** the reconciler reconciles the Job like any unorchestrated Job, dispatching the recovery start and fencing historical generations

#### Scenario: Terminal orchestration restores ordinary reconciliation
- **WHEN** an orchestration reaches a terminal state (succeeded, aborted, failed, cancelled, rolled back)
- **THEN** subsequent ticks reconcile the Job with no orchestration-aware behavior
