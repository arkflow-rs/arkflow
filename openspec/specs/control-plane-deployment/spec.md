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

### Requirement: The Hub server CLI surface SHALL be documented in the CLI reference

The CLI reference page SHALL document the `arkflow-server` binary: its startup environment variables (`ARKFLOW_HUB_ADDRESS`, `ARKFLOW_NODE_TOKEN`, `ARKFLOW_HUB_INSECURE_LOCAL`, `ARKFLOW_HUB_STORAGE`, `ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`, `ARKFLOW_OPERATOR_TOKEN`, and the `ARKFLOW_HUB_HA_*` lease family) and the `migrate` subcommand contract. The documentation SHALL exist in both English and the zh-Hans tree and stay consistent with the binary's actual parsing behavior.

#### Scenario: An operator deploys the Hub from a binary

- **WHEN** an operator reads the CLI reference to configure a Hub deployment
- **THEN** every environment variable the binary reads is listed with its default and effect, including the HA lease variables

#### Scenario: A storage schema migration is needed

- **WHEN** an operator needs to run `arkflow-server migrate`
- **THEN** the CLI reference documents the subcommand's arguments and exit-code contract

#### Scenario: The binary's env surface changes

- **WHEN** a pull request adds or renames a startup environment variable in `arkflow-server`
- **THEN** the CLI reference (en and zh-Hans) is updated in the same pull request, keeping docs consistent with the binary
