## ADDED Requirements

### Requirement: Agent SHALL sample and report node resource gauges

The Agent SHALL sample host resource gauges (CPU usage, memory used/total/available) on a fixed interval independent of heartbeat and report ticks, and SHALL merge the latest fresh snapshot into every report's existing metrics map under the fixed key vocabulary `node_cpu_usage_percent`, `node_memory_used_bytes`, `node_memory_total_bytes`, `node_memory_available_bytes`. Sampling SHALL be best-effort: any sampler failure SHALL NOT block, fail, or delay reporting, heartbeat, or command execution.

#### Scenario: Gauges ride the regular report

- **WHEN** the report tick fires after the sampler has published a fresh snapshot
- **THEN** the posted report's metrics map contains the four `node_*` keys carrying the latest sampled values, alongside the existing flow counters

#### Scenario: Sampler failure never blocks reporting

- **WHEN** the sampler cannot read host metrics (unsupported platform, read error, or task exit)
- **THEN** reports continue to be posted without the resource keys, the node's session remains online, and heartbeats and command polling continue unaffected

#### Scenario: Stale samples are omitted

- **WHEN** the latest published snapshot is older than twice the sampler interval at report time
- **THEN** the report omits all resource keys rather than sending stale values

#### Scenario: Vocabulary stays bounded

- **WHEN** resource gauges are merged into a report
- **THEN** only the four fixed keys are added, with scalar values and no per-Job, per-Stream, or per-core cardinality
