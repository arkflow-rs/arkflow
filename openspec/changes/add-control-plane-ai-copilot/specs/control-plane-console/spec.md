## ADDED Requirements

### Requirement: Hub AI Copilot drawer and capability-gated visibility

When connected to a Hub control plane whose `GET /api/v1/system` response advertises `"ai_copilot"` in `capabilities`, the Console SHALL provide a persistent Copilot toggle in the header shell that opens a global slide-over assistant drawer preserving the active conversation across route navigation, with conversation history stored only in the browser session (`sessionStorage` / memory). When connected to a local standalone control plane or a Hub without `"ai_copilot"` in `capabilities`, the Console MUST NOT render the Copilot drawer trigger or any page-level AI action triggers.

#### Scenario: Copilot entry hidden in local mode or unconfigured Hub

- **WHEN** the Console connects to a local standalone control plane or a Hub whose `capabilities` lack `"ai_copilot"`
- **THEN** the header Copilot button and all contextual AI triggers on Configuration, Jobs, and Events are hidden

#### Scenario: Conversation persists across route navigation within browser session

- **WHEN** an operator converses with Copilot on the Configuration page and navigates to Jobs or Events in the same Hub deployment
- **THEN** the drawer preserves the conversation messages and proposals without calling any Hub conversation-storage API

### Requirement: Contextual AI triggers and proposal card workflows

In Hub deployments with `"ai_copilot"` enabled, the Console SHALL provide contextual triggers on the Configuration, Job Editor, Jobs, and Events views that open the Copilot drawer pre-populated with page context (such as current `node_id`, `job_id`, or error summary), subject to the principal's permissions (authoring triggers disabled or omitted for principals lacking `Configure`). The Console SHALL render structured proposal cards returned over SSE:
- Applying a `stream_config` proposal SHALL navigate to `/configuration` (preserving or selecting the target `node_id`), populate the editor into an editable local candidate state (without calling local-only `/configuration/draft`), allow client-side diff preview against the active snapshot, and keep Hub validation (`POST /nodes/{node_id}/configuration/validate`) gating publish.
- Applying a `job_spec` proposal SHALL navigate to `/jobs`, populate the visual ReactFlow DAG editor via `jobSpecToDag`, and clear validation status so the operator MUST run `/jobs/validate` before submitting.
- A `diagnostic_action` proposal SHALL render a deep-linked recommendation card navigating to the suggested Console route rather than executing any mutation directly.

#### Scenario: Apply a validated Stream config proposal in Hub mode

- **WHEN** an operator with `Configure` clicks "Apply to editor" on a `stream_config` proposal card with a selected `node_id`
- **THEN** the Console opens `/configuration?node_id=...`, loads the proposed YAML/JSON into an editable candidate state with diff preview, does not call `/configuration/draft`, and enables Publish only after node validation passes

#### Scenario: Import a validated JobSpec proposal into the DAG canvas

- **WHEN** an operator clicks "Import to DAG" on a `job_spec` proposal card
- **THEN** the Console opens the Job Editor, renders the corresponding source/processor/sink nodes and edges on the DAG canvas, and requires validation before Job creation or upgrade

#### Scenario: Diagnostic recommendation navigates without mutating

- **WHEN** an operator clicks the action button on a `diagnostic_action` proposal card
- **THEN** the Console navigates to the linked target page/resource view and sends no mutating API request
