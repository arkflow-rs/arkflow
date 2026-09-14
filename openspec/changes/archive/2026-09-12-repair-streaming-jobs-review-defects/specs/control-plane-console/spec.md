## MODIFIED Requirements

### Requirement: Visual Job DAG orchestration

The Console SHALL provide one visual DAG editor for new Job creation and savepoint-based upgrades, SHALL render registered input components as sources, registered output components as sinks, and registered processor components as processors, and SHALL serialize its logical graph as the existing JobSpec without persisting layout state or exposing raw JSON authoring. After a successful upgrade the Console SHALL report the lifecycle state the Hub recorded for the Job and SHALL offer the action that starts it, so an operator is never left with a silently stopped Job.

#### Scenario: Create a stopped Job from a graph

- **WHEN** an operator creates and validates a source-to-sink graph
- **THEN** the Console submits the existing create API with the derived JobSpec and `desired_state: stopped`

#### Scenario: Load an existing Job for upgrade

- **WHEN** an operator selects a completed savepoint for a Job upgrade
- **THEN** the Console reconstructs the persisted JobSpec as an editable graph and submits the selected savepoint and current expected generation to the existing upgrade API

#### Scenario: Upgrade leaves the Job stopped

- **WHEN** an upgrade is accepted and the Hub records the Job as stopped or pending recovery
- **THEN** the Console states that the Job is stopped and offers or executes the start action, instead of closing the editor as if the Job were running

### Requirement: Graph and compatibility validation

The Console SHALL reject self-loops, duplicate edges, source input edges, sink output edges, and cyclic graphs, SHALL invalidate a previous validation after any change to the Job's graph, fields, configuration values, or target-node selection, and SHALL enable submission only after the current `/jobs/validate` result is valid. Presentation-only interactions — selecting or focusing a node, panning or zooming the canvas, and repositioning a node without changing its configuration — SHALL NOT invalidate an existing validation result.

#### Scenario: Change a validated graph

- **WHEN** an operator changes a node, edge, configuration value, or target node after validation
- **THEN** the Console disables create or upgrade until it validates the new JobSpec and target selection

#### Scenario: Select a node after validation

- **WHEN** an operator selects or focuses a node, or repositions it without changing its configuration, after a successful validation
- **THEN** the Console keeps the validation result and leaves the create or upgrade action enabled

#### Scenario: Inspect incompatible nodes

- **WHEN** Hub validation reports warnings, a physical plan, required capabilities, or missing node capabilities
- **THEN** the Console displays those results next to the editor and does not submit an invalid graph
