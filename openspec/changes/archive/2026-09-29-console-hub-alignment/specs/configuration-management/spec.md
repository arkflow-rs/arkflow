# configuration-management Delta — console-hub-alignment

## ADDED Requirements

### Requirement: Hub-reachable configuration validation and diff

The Hub SHALL expose node-scoped configuration validation (`POST /api/v1/nodes/{node_id}/configuration/validate`) and version diff (`GET /api/v1/nodes/{node_id}/configuration/diff?from&to`) that execute on the selected node through the existing command channel and return that node's validation report and diff metadata. Requests targeting an unknown or offline node SHALL fail with the standard node-unavailable problem instead of a partial or stale result. These operations SHALL be read-only: they MUST NOT create versions, alter the active configuration, or advance any intent.

#### Scenario: Validate a draft through the Hub

- **WHEN** an operator validates a configuration candidate while connected to the Hub with node `n1` selected
- **THEN** the Hub dispatches validation to `n1` and returns the same path-aware report the node-local endpoint produces, and the node's active configuration is unchanged

#### Scenario: Compare two versions through the Hub

- **WHEN** an operator compares version `v3` against `v2` of a node's configuration through the Hub
- **THEN** the Hub returns the diff metadata (from, to, changed, formats) computed on the selected node

#### Scenario: Target an offline node

- **WHEN** a validation or diff request targets a node the Hub considers offline
- **THEN** the Hub responds with the node-unavailable problem and does not fabricate a report from stale state
