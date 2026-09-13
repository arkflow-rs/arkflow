## ADDED Requirements

### Requirement: Command dispatch latency and failure metrics

The Hub SHALL expose Prometheus text metrics for command dispatch latency, bucketed from enqueue to acknowledgement per fixed command-type label, and for command outcomes as counters per fixed command-type and outcome-class labels. The label vocabulary SHALL be a fixed low-cardinality enumeration; resource IDs, correlation IDs, and error messages MUST NOT appear as labels. Counters MAY reset on Hub restart in line with Prometheus counter semantics.

#### Scenario: Scrape latency after acknowledged commands

- **WHEN** a scraper requests the metrics endpoint after commands have been acknowledged
- **THEN** the exposition contains per-command-type latency bucket counters covering the enqueue-to-acknowledgement durations

#### Scenario: A command fails with a node unavailable

- **WHEN** a command cannot be dispatched because its target node is unavailable
- **THEN** the failure counter for that command type and outcome class `node_unavailable` increments without changing other command series

#### Scenario: Labels stay low-cardinality

- **WHEN** many Jobs across many nodes produce command outcomes
- **THEN** metric series are bounded by the fixed command-type and outcome-class enumerations, never by Job or node identifiers
