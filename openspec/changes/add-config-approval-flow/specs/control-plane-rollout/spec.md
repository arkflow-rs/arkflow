## MODIFIED Requirements

### Requirement: Create an immutable configuration rollout

The service SHALL validate and persist an immutable configuration version before accepting a rollout and SHALL associate the rollout with an explicit target selection, actor, correlation ID, and desired generation. When the approval policy (`approval_required`, see `configuration-approval`) is enabled, direct rollout creation by any route — including node-level legacy apply routes — SHALL be rejected with an explicit error directing the operator to the configuration approval flow, and a rollout SHALL be created only by the system when a pending approval request pinning that version, target selection, and batch configuration is approved, with the approving principal as actor. Rollback of an existing non-terminal rollout SHALL NOT require approval when its target version was previously applied to the affected nodes; while the approval policy is enabled, a rollback naming a never-applied version SHALL be rejected (see `configuration-approval`).

#### Scenario: Accept a valid rollout

- **WHEN** an authorized operator submits a valid configuration and target node selection and the approval policy is disabled
- **THEN** the service persists the version and rollout atomically and returns an accepted non-terminal rollout representation

#### Scenario: Direct creation is rejected under the approval policy

- **WHEN** the approval policy is enabled and an authorized operator submits a valid configuration and target node selection to a rollout creation route
- **THEN** the service rejects creation with an explicit approval-required error and persists no rollout

#### Scenario: Approved request creates the rollout

- **WHEN** a pending approval request pinning this configuration version, target selection, and batch configuration is approved
- **THEN** the service creates the rollout with the approver as actor and a correlation ID linking the approval request

## MODIFIED Requirements

### Requirement: Single-node compatibility rollout

Existing node-level configuration apply and rollback routes SHALL create or reuse a single-node rollout and SHALL return the canonical rollout/operation convergence representation. While the approval policy (`approval_required`, see `configuration-approval`) is enabled, these routes SHALL be rejected with the approval-required error instead of creating a rollout, except a rollback whose target version was previously applied to the node, which SHALL proceed without approval.

#### Scenario: Use a legacy apply route

- **WHEN** a client posts configuration to an existing node-level apply route and the approval policy is disabled
- **THEN** the service creates a single-node rollout with equivalent validation, audit, and convergence semantics

#### Scenario: Legacy apply route is gated under the approval policy

- **WHEN** a client posts configuration to an existing node-level apply route and the approval policy is enabled
- **THEN** the service rejects the request with the approval-required error and creates no rollout
