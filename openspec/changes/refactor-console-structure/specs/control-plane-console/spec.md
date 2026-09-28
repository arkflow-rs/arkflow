## MODIFIED Requirements

### Requirement: Operations application shell

The console SHALL provide persistent navigation and route-level pages for Overview, Runtime, Configuration, Components, Events, and Settings. It SHALL show global connection, permission, stale-data, loading, empty, and error states. The console SHALL use the same node collection contract in local and Hub mode, SHALL persist the selected `node_id` in the current URL state, and SHALL persist the current page in the URL state and restore it when the console is opened via a page deep link or after a page reload.

#### Scenario: Open the overview
- **WHEN** an operator opens the console
- **THEN** the console loads system identity, node health, runtime totals, active operations, recent events, and aggregate metrics in one overview

#### Scenario: API becomes unavailable
- **WHEN** the control API cannot be reached after data was loaded
- **THEN** the console marks data stale, preserves the last safe snapshot, and provides a retry action

#### Scenario: Select a node
- **WHEN** an operator selects a node
- **THEN** the URL contains the selected `node_id` and runtime, configuration, event, operation, and metric requests use that node context

#### Scenario: Reopen a page via deep link or reload
- **WHEN** an operator opens the console with a page query parameter (e.g. `?page=jobs`) or reloads the browser while viewing a non-default page
- **THEN** the console displays that page instead of the default overview, with the page reflected in the URL state
