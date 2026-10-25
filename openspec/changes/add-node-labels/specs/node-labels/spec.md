## ADDED Requirements

### Requirement: Node labels are Hub-managed, durable, and bounded

The Hub SHALL store an optional `labels` map (`map<string, string>`) per node. Labels SHALL be managed exclusively through the Hub API by operators: Agent registration, heartbeat, and reports SHALL NOT create, modify, or remove labels. Labels SHALL persist across Hub restarts and Agent re-registrations, and SHALL be included in node representations returned by the fleet APIs. A label map SHALL contain at most 16 entries; keys and values SHALL each be at most 63 characters and match `[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?` (no leading or trailing `-`, `_`, `.`). Label maps that violate these bounds or the character set SHALL be rejected with an explicit validation error and SHALL NOT partially apply.

#### Scenario: Labels survive Agent re-registration

- **WHEN** a node with operator-set labels re-registers (same or new boot identity)
- **THEN** the stored labels are unchanged and the node representation still reports them

#### Scenario: Labels survive Hub restart

- **WHEN** the Hub restarts after labels were set on a node
- **THEN** the node representation reports the same labels that existed before restart

#### Scenario: Reject an out-of-bounds or malformed label map

- **WHEN** an operator submits a label map with 17 entries, an empty key, a 64-character value, or a value with a leading `-`
- **THEN** the Hub rejects the request with an explicit validation error and the node's existing labels are unchanged

### Requirement: Label management is authorized and audited

Replacing a node's labels SHALL require the `ManageNodes` action for that node's scope and SHALL use full-replace semantics (the submitted map becomes the node's complete label set). Every accepted label replacement and every authorization denial SHALL be recorded in the audit log with the actor and node identity. Setting labels on an unknown node SHALL be rejected with a not-found error.

#### Scenario: Operator replaces labels

- **WHEN** an operator holding `ManageNodes` for a node submits a valid label map to the label replacement route
- **THEN** the node's labels become exactly the submitted map and an audit event records the actor and node

#### Scenario: Unauthorized label replacement is denied and audited

- **WHEN** an operator without `ManageNodes` submits a label map to the label replacement route
- **THEN** the Hub rejects the request as forbidden, the node's labels are unchanged, and the denial is audited

### Requirement: Node listing filter by labels

The fleet node listing SHALL accept label filter parameters of the form `key=value` and SHALL return only nodes whose label map contains every filtered key with exactly the matching value. Nodes without any of the filtered keys SHALL be excluded. An invalid filter expression SHALL be rejected with an explicit error.

#### Scenario: Filter returns the matching subset

- **WHEN** nodes carry `{env=prod, tier=edge}`, `{env=staging}`, and no labels, and the operator lists nodes filtered by `env=prod`
- **THEN** only the node labeled `env=prod` is returned

#### Scenario: Multiple filter terms are ANDed

- **WHEN** the operator lists nodes filtered by `env=prod&tier=edge`
- **THEN** only nodes carrying both labels exactly are returned

### Requirement: Rollout target selection by label selector

Rollout creation SHALL accept a label selector (an equality map) as the target selection, mutually exclusive with an explicit node identifier list: submitting both SHALL be rejected, and submitting neither SHALL be rejected. The Hub SHALL resolve the selector to an explicit node set exactly once, at rollout creation time (or at approval-request creation time when the approval policy routes creation through approval), and SHALL persist that resolved node set as the rollout's targets. Resolution to the empty set SHALL be rejected with an explicit error and SHALL NOT persist a rollout. Subsequent label changes SHALL NOT alter the target set of an already-created rollout or the node set pinned by a pending approval request.

#### Scenario: Selector resolves to the matching node set

- **WHEN** an operator creates a rollout with target selector `{env=prod}` while three nodes carry `env=prod`
- **THEN** the rollout is created with exactly those three nodes as its persisted targets

#### Scenario: Later label changes do not re-target an existing rollout

- **WHEN** a rollout created from a selector is in progress and one of its target nodes has its labels changed or removed
- **THEN** the rollout's target set is unchanged and convergence continues to be evaluated against the persisted targets

#### Scenario: Selector resolving to no nodes is rejected

- **WHEN** an operator creates a rollout with a selector that matches no node
- **THEN** the Hub rejects creation with an explicit empty-target error and persists no rollout or configuration association for it

#### Scenario: Approval request pins the resolved node set

- **WHEN** the approval policy is enabled and an operator submits a rollout request with a label selector
- **THEN** the pending approval request pins the selector resolved to its explicit node set at submission time, and the rollout created on approval targets exactly that pinned set
