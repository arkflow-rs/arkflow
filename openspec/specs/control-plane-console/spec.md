## Purpose

Define the operator-facing Console for discovering, observing, and administering ArkFlow control-plane resources.
## Requirements
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

### Requirement: Graph and compatibility validation

The Console SHALL reject self-loops, duplicate edges, source input edges, sink output edges, and cyclic graphs, SHALL invalidate a previous validation after any change to the Job's graph, fields, configuration values, or target-node selection, and SHALL enable submission only after the current `/jobs/validate` result is valid. A validation response SHALL apply only to the exact spec it validated: a response that arrives after the graph changed SHALL NOT re-enable submission for the newer graph. Presentation-only interactions — selecting or focusing a node, panning or zooming the canvas, and repositioning a node without changing its configuration — SHALL NOT invalidate an existing validation result.

#### Scenario: Change a validated graph

- **WHEN** an operator changes a node, edge, configuration value, or target node after validation
- **THEN** the Console disables create or upgrade until it validates the new JobSpec and target selection

#### Scenario: Select a node after validation

- **WHEN** an operator selects or focuses a node, or repositions it without changing its configuration, after a successful validation
- **THEN** the Console keeps the validation result and leaves the create or upgrade action enabled

#### Scenario: A late validation response cannot unlock a newer graph

- **WHEN** a validate request is in flight, the operator edits the graph, and the response for the older spec arrives afterwards
- **THEN** the Console does not mark the newer graph valid, and submission stays disabled until a validation of the current spec succeeds

#### Scenario: Inspect incompatible nodes

- **WHEN** Hub validation reports warnings, a physical plan, required capabilities, or missing node capabilities
- **THEN** the Console displays those results next to the editor and does not submit an invalid graph

### Requirement: Hub component catalogue availability

The Console SHALL load the registered component catalogue from both local-server and external-Hub deployments. The external Hub SHALL expose the existing component and schema routes after initializing the shared plugin registry.

#### Scenario: Open the Job editor against a Hub

- **WHEN** an operator opens the Job editor through the external Hub
- **THEN** the source, processor, and sink palette loads registered component metadata, or displays a retryable request failure instead of an indefinite loading state

### Requirement: Compact component browsing

The Console SHALL provide search and Input/Processor/Output category filtering for both the Job palette and component catalogue. The catalogue SHALL show configuration metadata only for the selected component rather than expanding every registered component at once.

#### Scenario: Filter a component catalogue

- **WHEN** an operator selects a component category or enters a search term
- **THEN** the Console shows only matching components and details for the selected matching item

### Requirement: In-app destructive action confirmation

The console SHALL present destructive or lifecycle-changing actions (stream start/stop/restart, node drain/maintain/resume, job stop, configuration publish/rollback, rollout create/pause/resume/cancel/rollback, upgrade restore) behind an in-application confirmation dialog with focus containment, Escape-to-cancel, and Enter-to-confirm. The console SHALL NOT use native `window.confirm` or `window.prompt` dialogs. The outcome of such actions SHALL be reported through a transient toast notification in addition to the existing operation tracking.

#### Scenario: Confirm a node drain
- **WHEN** an operator triggers a node drain and confirms the dialog
- **THEN** the drain request is sent, a toast reports the accepted outcome, and the dialog closes

#### Scenario: Cancel a destructive action
- **WHEN** an operator dismisses the confirmation dialog with Escape or the cancel button
- **THEN** no request is sent and the current view is unchanged

### Requirement: Loading and empty states

The console SHALL render skeleton placeholders for tables and metric panels while their data is loading, and empty states SHALL pair a plain-language explanation with the next available action (such as a create button or filter reset) instead of bare text alone.

#### Scenario: Loading a table
- **WHEN** a list page is fetching its first page of data
- **THEN** skeleton rows are shown instead of an empty table or a blank panel

#### Scenario: Empty state offers the next action
- **WHEN** a filtered list matches nothing
- **THEN** the empty state explains the miss and offers the nearest action (for example clearing the filter or creating the first resource)

### Requirement: Job detail diagnostics rendering

The Job detail view SHALL render only diagnostic values the API actually measures and SHALL NOT display fabricated or placeholder numbers for unimplemented metrics. The task listing SHALL render observed task state (with not-observed entries visibly distinguished) and SHALL NOT render columns for fields the API does not provide.

#### Scenario: Only measured gauges are shown

- **WHEN** an operator opens a Job detail whose API metrics contain only `watermark_lag_ms`, `checkpoint_duration_ms`, and `checkpoint_failures`
- **THEN** the detail view renders those three gauges and renders no zero-valued placeholder for state size, recovery progress, task pressure, or partition health

#### Scenario: Task states reflect observations

- **WHEN** a Job detail reports one task observed running and one not-yet-observed task
- **THEN** the tasks listing shows the running state for the observed task, shows the fallback placement state distinctly marked for the other, and displays no permanently-empty identifier column

### Requirement: Console SHALL 有全局 Error Boundary

Console 的 app 路由出口 SHALL 被 Error Boundary 包裹：任何组件 render 异常（API 形状漂移、未预期的 null 等）SHALL 显示一个含错误摘要与重试按钮的错误页，MUST NOT 卸载整棵 React 树导致白屏。

#### Scenario: Render 错误不白屏

- **WHEN** 任意子组件在 render 阶段抛出异常
- **THEN** 用户看到错误页面（含错误信息摘要与 Reload 按钮），而非空白页

### Requirement: 操作等待 SHALL 诚实报告长时操作

`waitForOperation` 的轮询上限 SHALL 足以覆盖正常服务端操作时长（默认 60 秒）；超时错误 SHALL 提示操作可能仍在服务端执行并建议刷新查看结果，MUST NOT 暗示操作已失败。

#### Scenario: 超过旧上限的操作不误报失败

- **WHEN** 一个服务端操作执行 10 秒（旧 7.5 秒上限内未完成）
- **THEN** 等待不超时（新 60 秒上限内完成），用户看到真实结果

#### Scenario: 超时提示不暗示失败

- **WHEN** 等待超过 60 秒上限
- **THEN** 错误信息提示操作可能仍在执行并建议刷新查看（而非 "Operation timed out"）

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

### Requirement: Job pause and resume SHALL be controllable from the console

The Job views SHALL expose pause and resume actions gated by the caller's authorized actions, render the pause phase (in flight, paused, resuming) distinctly from stopped and running, keep actions disabled while a pause or resume transition is in flight, and surface rejected transitions with the problem code instead of silently ignoring them. The strings SHALL be translated in the English and Simplified Chinese dictionaries.

#### Scenario: Pause a running Job from the console

- **WHEN** an operator with the operate action opens the Jobs page for a converged running Job
- **THEN** a pause control is offered, invoking it sends the pause mutation, and the Job row shows the pause in-flight phase until it converges to paused

#### Scenario: Resume a paused Job from the console

- **WHEN** an operator opens the Jobs page for a paused Job
- **THEN** a resume control is offered and the row shows the resuming phase until the Job converges back to running

#### Scenario: Unauthorized or conflicting action feedback

- **WHEN** a user without the operate action, or against a Job with an active orchestration, invokes pause
- **THEN** the console disables the action for unauthorized callers, and for a rejected mutation surfaces the returned problem code

### Requirement: Users and credentials SHALL be administrable from the console

The console SHALL provide a user and credential administration area visible and usable only for principals authorized with `ManageUsers` in Hub mode. It SHALL list users with their role, scope summary, and disabled state; create and edit users; disable, enable, and delete users; list credentials by metadata only; issue a credential with a one-time plaintext display and copy affordance; rotate and revoke credentials; and revoke all of a user's tokens. Destructive actions SHALL use the existing in-app confirmation pattern, rejected mutations SHALL surface the returned problem code, and the strings SHALL be translated in the English and Simplified Chinese dictionaries.

#### Scenario: Administrator manages users

- **WHEN** an admin opens the console Settings area in Hub mode
- **THEN** the user and credential administration area is offered and lists users without any credential material

#### Scenario: Issued credential is shown once

- **WHEN** an admin issues a credential for a user
- **THEN** the console displays the plaintext token once with a copy action and does not display it again on later list views

#### Scenario: Non-administrators see no administration area

- **WHEN** a principal without `ManageUsers` opens the console
- **THEN** the administration area is not offered, matching the existing action-gated navigation pattern

#### Scenario: Rejected administration surfaces the problem code

- **WHEN** an administration mutation is rejected (for example `last_admin_required`)
- **THEN** the console displays the returned problem code instead of silently ignoring the failure

### Requirement: The console SHALL support runtime operator credential entry

The console SHALL allow an operator to enter or replace the bearer credential it uses at runtime, without requiring a build-time token, and SHALL keep that credential only for the browser session. When a request fails with 401 and no OIDC login flow is available, the console SHALL prompt for a credential and retry the failed navigation after a successful entry; it SHALL also offer an explicit sign-out that clears the stored credential. The build-time token SHALL remain an optional default for existing deployments.

#### Scenario: Runtime credential entry recovers from 401

- **WHEN** the console has no credential and a Hub returns 401 while no OIDC login flow is configured
- **THEN** the console prompts for a bearer credential, stores it for the browser session, and retries the request successfully

#### Scenario: Sign-out clears the runtime credential

- **WHEN** an operator signs out of the console
- **THEN** the stored credential is cleared and subsequent requests prompt again

#### Scenario: Build-time token keeps working

- **WHEN** the console is built with a default token and the operator does not enter a runtime credential
- **THEN** existing authentication behavior is unchanged

