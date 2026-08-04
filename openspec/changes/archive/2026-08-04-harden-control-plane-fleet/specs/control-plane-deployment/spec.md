## MODIFIED Requirements

### Requirement: Safe node drain rollout

Production deployment documentation SHALL define how operators drain, maintain, resume, and roll back nodes. Drain operations MUST preserve desired state and audit history, deployment automation MUST NOT silently drain nodes without an explicit policy, and rollout dispatch MUST respect draining and maintenance modes.

#### Scenario: Drain before deployment
- **WHEN** an operator prepares a node for a rolling deployment
- **THEN** the node enters draining, new control-plane dispatch is suppressed, in-flight Attempts are observable, and the node can be resumed after deployment

#### Scenario: Block rollout dispatch during maintenance
- **WHEN** a rollout reaches a node in draining or maintenance mode
- **THEN** the node is recorded as deferred and no new Attempt is dispatched until the node becomes active

#### Scenario: Roll back an operational rollout
- **WHEN** the operational action or readiness policy is rolled back
- **THEN** desired state, audit events, and observed history remain intact and no automatic destructive remediation is triggered
