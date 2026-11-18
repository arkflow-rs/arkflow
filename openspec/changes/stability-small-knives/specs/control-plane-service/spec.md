## MODIFIED Requirements

### Requirement: Resource-oriented system API

The API SHALL expose versioned resources for system identity, runtime nodes, Streams, Jobs, rollouts, configuration approvals, components, configuration, operations, events, and metrics. Collection responses SHALL provide stable envelopes and pagination metadata where applicable. `GET /api/v1/nodes` SHALL return a paginated node collection in both local and Hub mode, and every node-owned resource SHALL preserve `node_id`. The Job, rollout, and configuration-approval collections SHALL accept `page`/`page_size` and return the same envelope shape as the other collections. Metrics SHALL be reachable under the same configured versioned prefix as the other control resources.

#### Scenario: Discover the control-plane resources

- **WHEN** a client requests /api/v1/system and /api/v1/nodes
- **THEN** it receives node identity, version, capabilities, health state, and runtime summary

#### Scenario: List many Streams

- **WHEN** a client requests /api/v1/streams?page=2&page_size=20
- **THEN** it receives only the requested page plus total and page metadata

#### Scenario: Page through many Jobs

- **WHEN** a client requests /api/v1/jobs?page=2&page_size=20
- **THEN** it receives only the requested page plus total and page metadata, and omitting the parameters returns the first page with the default page size

#### Scenario: Page through rollouts and approvals

- **WHEN** a client requests the rollout or configuration-approval collection with pagination parameters
- **THEN** it receives the same envelope shape with total and page metadata
