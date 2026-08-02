## ADDED Requirements

### Requirement: Production health and metrics exposure

Production deployment SHALL expose liveness, readiness, operational health, and Prometheus metrics through the protected control-plane listener or an authenticated reverse proxy. Liveness MUST remain independent of Agent availability; readiness SHALL fail when startup recovery or required storage dependencies are unavailable.

#### Scenario: Protect the metrics endpoint
- **WHEN** a production reverse proxy exposes metrics to a scraper
- **THEN** the route is restricted to the configured monitoring principal or network and does not expose bearer credentials or configuration data

#### Scenario: Restart after storage failure
- **WHEN** the Hub process is alive but its control-plane storage cannot be opened or recovered
- **THEN** liveness remains available for diagnosis while readiness returns a non-success status until storage recovery succeeds

### Requirement: Safe node drain rollout

Production deployment documentation SHALL define how operators drain, maintain, resume, and roll back nodes. Drain operations MUST preserve desired state and audit history, and deployment automation MUST NOT silently drain nodes without an explicit policy.

#### Scenario: Drain before deployment
- **WHEN** an operator prepares a node for a rolling deployment
- **THEN** the node enters draining, new control-plane dispatch is suppressed, in-flight Attempts are observable, and the node can be resumed after deployment

#### Scenario: Roll back an operational rollout
- **WHEN** the operational action or readiness policy is rolled back
- **THEN** desired state, audit events, and observed history remain intact and no automatic destructive remediation is triggered
