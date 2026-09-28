## MODIFIED Requirements

### Requirement: Operations application shell

The console SHALL provide persistent navigation and route-level pages for Overview, Runtime, Configuration, Components, Events, and Settings. It SHALL show global connection, permission, stale-data, loading, empty, and error states. The console SHALL use the same node collection contract in local and Hub mode, SHALL persist the selected `node_id` as a query parameter in the current URL state, and SHALL address each page by URL path (e.g. `/jobs`), support browser back and forward navigation, and redirect legacy `?page=` links to the corresponding path. When live updates arrive over SSE, the console SHALL refresh live resources (system, nodes, streams, jobs, operations, events, metrics, tracked operation and rollout details) without discarding local editing state such as configuration drafts, and SHALL poll live resources on their configured intervals.

#### Scenario: Open the overview
- **WHEN** an operator opens the console
- **THEN** the console loads system identity, node health, runtime totals, active operations, recent events, and aggregate metrics in one overview

#### Scenario: API becomes unavailable
- **WHEN** the control API cannot be reached after data was loaded
- **THEN** the console marks data stale, preserves the last safe snapshot, and provides a retry action

#### Scenario: Select a node
- **WHEN** an operator selects a node
- **THEN** the URL contains the selected `node_id` as a query parameter on the current path, and runtime, configuration, event, operation, and metric requests use that node context

#### Scenario: Open a page by path
- **WHEN** an operator opens the console at a page path (e.g. `/jobs`) or navigates back and forward in browser history
- **THEN** the console displays the page addressed by the current path

#### Scenario: Redirect a legacy page link
- **WHEN** an operator opens the console with a legacy page query parameter (e.g. `/?page=jobs`)
- **THEN** the console redirects to the corresponding path (`/jobs`), preserving the remaining query parameters, and displays that page

#### Scenario: Live updates do not discard local edits
- **WHEN** an SSE event arrives or a polling interval elapses while an operator has unsaved content in the configuration editor
- **THEN** the editor content is unchanged, and only live resources are refreshed
