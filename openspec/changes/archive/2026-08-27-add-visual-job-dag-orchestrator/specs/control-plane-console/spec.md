## ADDED Requirements

### Requirement: Visual Job DAG orchestration

The Console SHALL provide one visual DAG editor for new Job creation and savepoint-based upgrades. The editor SHALL render registered input components as sources, registered output components as sinks, and registered processor components as processors. It SHALL serialize its logical graph as the existing JobSpec without persisting layout state or exposing raw JSON authoring.

#### Scenario: Create a stopped Job from a graph

- **WHEN** an operator creates and validates a source-to-sink graph
- **THEN** the Console submits the existing create API with the derived JobSpec and `desired_state: stopped`

#### Scenario: Load an existing Job for upgrade

- **WHEN** an operator selects a completed savepoint for a Job upgrade
- **THEN** the Console reconstructs the persisted JobSpec as an editable graph and submits the selected savepoint and current expected generation to the existing upgrade API

### Requirement: Graph and compatibility validation

The Console SHALL reject self-loops, duplicate edges, source input edges, sink output edges, and cyclic graphs. It SHALL invalidate a previous validation after any graph, field, or target-node change and enable submission only after the current `/jobs/validate` result is valid.

#### Scenario: Change a validated graph

- **WHEN** an operator changes a node, edge, configuration value, or target node after validation
- **THEN** the Console disables create or upgrade until it validates the new JobSpec and target selection

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
