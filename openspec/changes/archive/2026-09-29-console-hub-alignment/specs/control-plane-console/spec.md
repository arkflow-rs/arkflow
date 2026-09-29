# control-plane-console Delta — console-hub-alignment

## MODIFIED Requirements

### Requirement: Operations application shell

The console SHALL provide persistent navigation and route-level pages for Overview, Runtime, Configuration, Components, Events, and Settings. It SHALL show global connection, permission, stale-data, loading, empty, and error states. The console SHALL use the same node collection contract in local and Hub mode, SHALL persist the selected `node_id` as a query parameter in the current URL state, and SHALL address each page by URL path (e.g. `/jobs`), support browser back and forward navigation, and redirect legacy `?page=` links to the corresponding path. When live updates arrive over SSE, the console SHALL refresh live resources (system, nodes, streams, jobs, operations, events, metrics, tracked operation and rollout details) without discarding local editing state such as configuration drafts, and SHALL poll live resources on their configured intervals.

Every live resource the overview loads SHALL resolve against a healthy Hub exactly as against a local control plane: the console MUST NOT poll endpoints that exist only in local mode, and a fully healthy deployment MUST NOT produce a persistent stale-data banner. When the system identity advertises High-Availability information, the console SHALL present the leadership role (leader or standby) with its epoch; when the control plane rejects requests with the `hub_standby` problem code, the console SHALL show a dedicated standby state that names the condition and advises retrying against the elected leader, while continuing to poll so the state self-clears when the connected Hub acquires the lease.

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

### Requirement: Configuration workflow

The Configuration page SHALL load redacted active configuration, support YAML/JSON editing, schema-aware validation with path locations, draft/publish separation, version listing, diff metadata, and rollback confirmation. A dirty or invalid draft MUST NOT be publishable; publish SHALL be enabled only after a successful validation of the current content and SHALL track the returned operation to terminal state.

In Hub mode the page's node-scoped actions — version comparison and rollback — SHALL be addressed to the selected node through Hub-proxied endpoints, and any node-scoped validation or publish request SHALL be addressed to the selected node; the page MUST NOT issue configuration requests to endpoints that exist only in local mode. The configuration draft workflow (editing with validation-gated publishing) remains available in local mode only — in Hub mode the page presents the node's redacted active snapshot read-only — and the page SHALL make the draft workflow's unavailability explicit in Hub mode rather than surfacing it as a load error.

#### Scenario: Validate an invalid draft

- **WHEN** an operator validates a malformed or semantically invalid draft
- **THEN** the UI displays structured path-aware errors and does not publish or alter runtime state

#### Scenario: Compare versions through the Hub

- **WHEN** an operator connected to a Hub with a node selected compares two reported versions
- **THEN** the request is routed through the Hub's node-scoped diff endpoint (a tracked read-only command) and the result renders identically to local mode

#### Scenario: Publish and rollback configuration

- **WHEN** an operator publishes a valid draft or rolls back a version
- **THEN** the UI creates/tracks an operation, shows affected resources, and refreshes the active version only after the server reports success

#### Scenario: Configuration permission failure

- **WHEN** a configuration mutation returns 401 or 403
- **THEN** the UI shows a permission error, does not retry the mutation, and does not expose the bearer token or configuration secret

### Requirement: Visual Job DAG orchestration

The Console SHALL provide one visual DAG editor for new Job creation and savepoint-based upgrades, SHALL render registered input components as sources, registered output components as sinks, and registered processor components as processors, and SHALL serialize its logical graph as the existing JobSpec without persisting layout state or exposing raw JSON authoring. When the editor's target or mode changes while the editor stays mounted — for example an upgrade opened from the Job detail panel while a create editor is already open — the editor SHALL reset its draft state (spec, graph, and validation) to the new target, so an upgrade SHALL submit the target Job's current spec and never a stale create draft. Generated component node identifiers SHALL remain unique across add and delete sequences.

The editor SHALL expose the JobSpec's distributed-runtime fields as first-class form controls: the `rescale` opt-in for state redistribution on parallelism/task-set changes, and the `resources` per-task requests (`cpu_millicores`, `memory_bytes`). Empty optional inputs SHALL serialize to their absent form so a spec that never declared them round-trips unchanged.

#### Scenario: Create a stopped Job from a graph

- **WHEN** an operator creates and validates a source-to-sink graph
- **THEN** the Console submits the existing create API with the derived JobSpec and `desired_state: stopped`

#### Scenario: Load an existing Job for upgrade

- **WHEN** an operator selects a completed savepoint for a Job upgrade
- **THEN** the Console reconstructs the persisted JobSpec as an editable graph and submits the selected savepoint and current expected generation to the existing upgrade API

#### Scenario: Upgrade leaves the Job stopped

- **WHEN** an upgrade is accepted and the Hub records the Job as stopped or pending recovery
- **THEN** the Console states that the Job is stopped and offers or executes the start action, instead of closing the editor as if the Job were running

#### Scenario: Switching from create to upgrade resets the draft

- **WHEN** an operator has a create editor open and then triggers an upgrade for an existing Job from the detail panel
- **THEN** the editor resets to the target Job's persisted spec as an upgrade draft, and the submitted upgrade contains that Job's spec rather than the earlier create draft

#### Scenario: Node ids stay unique after deletions

- **WHEN** an operator adds nodes, deletes some, and adds more in the same editing session
- **THEN** every node receives an identifier distinct from all existing nodes, and the derived JobSpec contains no duplicate operator ids

#### Scenario: Declare rescale and resource requests without raw JSON

- **WHEN** an operator enables `rescale` and enters per-task `cpu_millicores` and `memory_bytes` values, then submits
- **THEN** the derived JobSpec carries `rescale: true` and the declared `resources` fields, passes the existing validation API, and reloading the Job into the editor restores the same values; clearing the inputs serializes both to their absent form

## ADDED Requirements

### Requirement: Job detail diagnostics rendering

The Job detail view SHALL render only diagnostic values the API actually measures and SHALL NOT display fabricated or placeholder numbers for unimplemented metrics. The task listing SHALL render observed task state (with not-observed entries visibly distinguished) and SHALL NOT render columns for fields the API does not provide.

#### Scenario: Only measured gauges are shown

- **WHEN** an operator opens a Job detail whose API metrics contain only `watermark_lag_ms`, `checkpoint_duration_ms`, and `checkpoint_failures`
- **THEN** the detail view renders those three gauges and renders no zero-valued placeholder for state size, recovery progress, task pressure, or partition health

#### Scenario: Task states reflect observations

- **WHEN** a Job detail reports one task observed running and one not-yet-observed task
- **THEN** the tasks listing shows the running state for the observed task, shows the fallback placement state distinctly marked for the other, and displays no permanently-empty identifier column
