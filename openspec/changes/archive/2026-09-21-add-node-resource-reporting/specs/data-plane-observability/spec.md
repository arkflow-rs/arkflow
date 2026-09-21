## ADDED Requirements

### Requirement: Node resource gauges SHALL flow to the Hub metrics export

The Hub metrics export SHALL emit node resource gauges as series carrying the node label, within the closed label vocabulary, using the reported values as-is (gauges, not derived rates). No resource series SHALL be emitted for a node that has not reported resource gauges.

#### Scenario: Resource gauges export with the node label

- **WHEN** the Hub metrics endpoint is scraped after a node reports `node_cpu_usage_percent` and memory gauges
- **THEN** series named for the resource keys are exported carrying that node's label and the reported values unchanged

#### Scenario: Absent gauges export nothing

- **WHEN** a node has not reported resource gauges (old Agent, sampler failure, or fresh Hub restart)
- **THEN** no resource series are emitted for that node, and no zero-valued placeholders appear
