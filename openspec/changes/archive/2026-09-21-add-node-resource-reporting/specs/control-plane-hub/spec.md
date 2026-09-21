## ADDED Requirements

### Requirement: Hub SHALL ingest bounded node resource gauges

The Hub SHALL accept the resource gauge keys `node_cpu_usage_percent`, `node_memory_used_bytes`, `node_memory_total_bytes`, `node_memory_available_bytes` in Agent reports, expose the latest values through the node registry view, and treat them as ephemeral state: resource gauges SHALL NOT be persisted as durable history and SHALL be cleared on Hub restart until the next report arrives. Unknown metric keys SHALL continue to be dropped.

#### Scenario: Whitelisted resource keys reach the node metrics view

- **WHEN** a report carries the four resource keys
- **THEN** the Hub accepts them and the node's metrics view (and the Hub metrics export) exposes the latest values

#### Scenario: Unknown keys are still dropped

- **WHEN** a report carries metric keys outside the whitelist
- **THEN** the unknown keys are dropped and the report is otherwise accepted unchanged

#### Scenario: Gauges are ephemeral across Hub restart

- **WHEN** a Hub restarts with durable storage
- **THEN** node resource gauges are empty until the affected node's next report, with no durable history retained or pruned
