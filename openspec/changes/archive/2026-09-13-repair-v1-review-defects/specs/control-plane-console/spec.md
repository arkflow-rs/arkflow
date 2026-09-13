## MODIFIED Requirements

### Requirement: Visual Job DAG orchestration

The Console SHALL provide one visual DAG editor for new Job creation and savepoint-based upgrades, SHALL render registered input components as sources, registered output components as sinks, and registered processor components as processors, and SHALL serialize its logical graph as the existing JobSpec without persisting layout state or exposing raw JSON authoring. When the editor's target or mode changes while the editor stays mounted — for example an upgrade opened from the Job detail panel while a create editor is already open — the editor SHALL reset its draft state (spec, graph, and validation) to the new target, so an upgrade SHALL submit the target Job's current spec and never a stale create draft. Generated component node identifiers SHALL remain unique across add and delete sequences.

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
