# Delta Spec: control-plane-console

## ADDED Requirements

### Requirement: Role-aware console surfaces

The console SHALL display the authenticated principal identity (id and roles) whenever the system identity echoes a principal. Every mutating entry point (stream lifecycle, configuration publish/rollback, job operations, rollout creation, approval decisions, node maintenance, webhook management) SHALL be gated on the echoed action list for its required operator action: entries the principal lacks SHALL be rendered disabled with an explanation naming the required permission, not hidden. When the system identity does not echo a principal (older control plane), the console SHALL fall back to ungated mutating entry points and display an unknown-identity indication. When a mutation is rejected with 403 despite being enabled, the console SHALL present a permission error, SHALL NOT retry automatically, and SHALL refresh the system identity.

#### Scenario: Viewer session

- **WHEN** the echoed principal holds only the viewer role
- **THEN** mutating controls across pages render disabled with the required action named, while read views remain fully usable

#### Scenario: Identity is visible

- **WHEN** the system identity echoes a principal
- **THEN** the console shows the principal id and roles on a persistent surface

#### Scenario: Older control plane without principal echo

- **WHEN** the system identity response carries no principal field
- **THEN** the console keeps mutating entry points enabled and marks the session identity as unknown

#### Scenario: Permission rejection at execution time

- **WHEN** a mutation is rejected with 403 despite the control being enabled
- **THEN** the console shows a permission error without retrying the mutation and refreshes the system identity

## MODIFIED Requirements

### Requirement: Operations application shell

The console SHALL provide persistent navigation and route-level pages for Overview, Runtime, Configuration, Components, Events, and Settings, plus Jobs, Rollouts, Approvals, and Audit when reachable. It SHALL show global connection, permission, stale-data, loading, empty, and error states. The console SHALL use the same node collection contract in local and Hub mode, SHALL persist the selected `node_id` as a query parameter in the current URL state, and SHALL address each page by URL path (e.g. `/jobs`), support browser back and forward navigation, and redirect legacy `?page=` links to the corresponding path. When live updates arrive over SSE, the console SHALL refresh live resources (system, nodes, streams, jobs, operations, events, metrics, tracked operation and rollout details) without discarding local editing state such as configuration drafts, and SHALL poll live resources on their configured intervals.

Every live resource the overview loads SHALL resolve against a healthy Hub exactly as against a local control plane: the console MUST NOT poll endpoints that exist only in local mode, and a fully healthy deployment MUST NOT produce a persistent stale-data banner. When the system identity advertises High-Availability information, the console SHALL present the leadership role (leader or standby) with its epoch; when the control plane rejects requests with the `hub_standby` problem code, the console SHALL show a dedicated standby state that names the condition and advises retrying against the elected leader, while continuing to poll so the state self-clears when the connected Hub acquires the lease.

Navigation SHALL reflect the reachability advertised by the system identity's capabilities: pages whose backing APIs exist only in Hub mode (Jobs, Rollouts, Approvals, Audit, and the webhook management area of Settings) SHALL NOT be offered as navigation entries in local mode, and opening such a page by URL in local mode SHALL render an explainer state naming the Hub-only requirement instead of an error. Until the system identity resolves, the console SHALL render the full navigation so capabilities-dependent hiding causes no startup flash.

#### Scenario: Open the overview

- **WHEN** an operator opens the console
- **THEN** the console loads system identity, node health, runtime totals, active operations, recent events, and aggregate metrics in one overview

#### Scenario: Overview against a healthy Hub

- **WHEN** an operator opens the console connected to a leader Hub holding the lease
- **THEN** every overview data source resolves without error and no stale-data banner is shown

#### Scenario: API becomes unavailable

- **WHEN** the control API cannot be reached after data was loaded
- **THEN** the console marks data stale, preserves the last safe snapshot, and provides a retry action

#### Scenario: Connected to a standby Hub

- **WHEN** the control plane rejects live requests with the `hub_standby` problem code
- **THEN** the console shows a standby-specific state naming the condition and pointing to the elected leader, and clears it automatically once the connected Hub serves requests again

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

#### Scenario: Leadership is visible

- **WHEN** the system identity reports HA enabled with role and epoch
- **THEN** the overview displays the leadership role and epoch alongside system identity

#### Scenario: Local mode hides Hub-only pages

- **WHEN** the console is connected to a local control plane whose capabilities lack the Hub fleet markers
- **THEN** navigation omits Jobs, Rollouts, Approvals, and Audit, Settings omits its webhook management area, and opening any of those paths renders an explainer state naming the Hub-only requirement

### Requirement: Runtime administration

The Runtime page SHALL support filtering and pagination over Streams, topology/resource summaries, desired/observed state, metrics, recent errors, operation history, and lifecycle actions. Actions SHALL display operation progress, target node, correlation ID, and terminal results. Mutations SHALL be disabled for stale or unavailable nodes or when the authenticated principal lacks the action required by the mutation, and SHALL not be retried automatically after a permission or availability error.

#### Scenario: Operate one Stream from the runtime page

- **WHEN** an operator starts, stops, or restarts a Stream
- **THEN** the UI confirms the action, tracks its operation ID, refreshes the affected resource, and reports success or failure without blocking other Streams

#### Scenario: Inspect a failed Stream

- **WHEN** an operator opens a failed Stream
- **THEN** the UI shows last error, transition timeline, metrics, recent related events, and available recovery actions

#### Scenario: Target a stale node

- **WHEN** an operator selects a stale or unavailable node
- **THEN** lifecycle controls are disabled and the UI explains that the node must reconnect before mutation

#### Scenario: Principal lacks the lifecycle action

- **WHEN** the echoed principal lacks the action required by a lifecycle control
- **THEN** the control is disabled and the UI names the required permission, independent of node availability

### Requirement: Components, events, and settings

The console SHALL provide component catalogue/schema display, server-side paginated and filtered event/audit history whose filter and page state is carried in the URL, and settings for API endpoint, authentication status, CORS/security posture, node identity, and capability information. Event and audit lists SHALL pass the control plane's pagination and filter query parameters instead of filtering client-side over a single fetched page, so records beyond the first page remain reachable. It SHALL preserve redaction and never put secrets in URLs, browser logs, or client-side error messages.

#### Scenario: Investigate an administrative event

- **WHEN** an operator filters events by type, stream, correlation ID, or node, or filters audit history by resource, or paginates either list
- **THEN** the console issues the corresponding server-side query parameters, reflects them in the URL, and shows ordered event details with links to the related resource/operation without exposing credentials

#### Scenario: Reach records beyond the first page

- **WHEN** more events or audit records exist than one page contains
- **THEN** pagination controls navigate the full result set on the server, and no client-side filter can silently hide unread pages

#### Scenario: View security settings

- **WHEN** an operator opens Settings
- **THEN** the console shows whether the session is authenticated and the server's effective security/capability status without rendering token values
