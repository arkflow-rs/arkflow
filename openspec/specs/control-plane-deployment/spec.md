## Purpose

Define repeatable local and production deployment paths for the ArkFlow control-plane backend and Console.

## Requirements

### Requirement: Unified local and production startup

The project SHALL document and provide a repeatable startup path that runs the backend control-plane service and static console against the same API prefix. Production packaging SHALL support a protected same-origin reverse-proxy deployment.

#### Scenario: Start locally
- **WHEN** an operator runs the documented backend and frontend commands
- **THEN** the console reaches the backend through the configured development proxy and can discover system resources

#### Scenario: Deploy behind a reverse proxy
- **WHEN** the console and API are served through a protected TLS reverse proxy
- **THEN** /api, /metrics, and static assets use the documented routes and credentials are not exposed to the public listener

### Requirement: Production health and metrics exposure

Production deployment SHALL expose liveness, readiness, operational health, and Prometheus metrics through the protected control-plane listener or an authenticated reverse proxy. Liveness MUST remain independent of Agent availability; readiness SHALL fail when startup recovery or required storage dependencies are unavailable.

#### Scenario: Protect the metrics endpoint
- **WHEN** a production reverse proxy exposes metrics to a scraper
- **THEN** the route is restricted to the configured monitoring principal or network and does not expose bearer credentials or configuration data

#### Scenario: Restart after storage failure
- **WHEN** the Hub process is alive but its control-plane storage cannot be opened or recovered
- **THEN** liveness remains available for diagnosis while readiness returns a non-success status until storage recovery succeeds

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
