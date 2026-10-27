## MODIFIED Requirements

### Requirement: Create an immutable configuration rollout

The service SHALL validate and persist an immutable configuration version before accepting a rollout and SHALL associate the rollout with an explicit target selection, actor, correlation ID, and desired generation. A target selection SHALL be either an explicit node identifier list or a label selector resolved to an explicit node set at creation time per `node-labels`; submitting both or neither SHALL be rejected, and a selector resolving to no nodes SHALL be rejected without persisting a rollout. When the approval policy (`approval_required`, see `configuration-approval`) is enabled, direct rollout creation by any route — including node-level legacy apply routes — SHALL be rejected with an explicit error directing the operator to the configuration approval flow, and a rollout SHALL be created only by the system when a pending approval request pinning that version, target selection, and batch configuration is approved, with the approving principal as actor. Rollback of an existing non-terminal rollout SHALL NOT require approval when its target version was previously applied to the affected nodes; while the approval policy is enabled, a rollback naming a never-applied version SHALL be rejected (see `configuration-approval`).

#### Scenario: Accept a valid rollout

- **WHEN** an authorized operator submits a valid configuration and target node selection and the approval policy is disabled
- **THEN** the service persists the version and rollout atomically and returns an accepted non-terminal rollout representation

#### Scenario: Accept a rollout with a label selector

- **WHEN** an authorized operator submits a valid configuration and a label selector as the target selection, and the approval policy is disabled
- **THEN** the service persists the version and rollout atomically with the selector resolved to its node set at creation time and returns an accepted non-terminal rollout representation listing the resolved targets

#### Scenario: Direct creation is rejected under the approval policy

- **WHEN** the approval policy is enabled and an authorized operator submits a valid configuration and target node selection to a rollout creation route
- **THEN** the service rejects creation with an explicit approval-required error and persists no rollout

#### Scenario: Approved request creates the rollout

- **WHEN** a pending approval request pinning this configuration version, target selection, and batch configuration is approved
- **THEN** the service creates the rollout with the approver as actor and a correlation ID linking the approval request
