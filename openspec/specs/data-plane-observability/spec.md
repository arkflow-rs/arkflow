# data-plane-observability Specification

## Purpose
Process-level observability for ArkFlow: Prometheus metrics and health endpoints served independently of the control-plane API server, covering legacy streams and kernel Jobs. Synced from change add-data-plane-observability (implemented in PR #1224).

## Requirements
### Requirement: Process observability endpoints independent of the API server

The process SHALL expose observability endpoints — a Prometheus metrics endpoint and process health endpoints (`/ready`, `/live`, paths configurable) — even when the control-plane API server is disabled (`server.enabled=false`). The observability listener SHALL bind a configurable address (default loopback), SHALL serve only metrics and health responses, and MUST NOT expose resource APIs, configuration content, or credentials. When the API server is enabled, the existing endpoint paths and semantics SHALL remain unchanged and compatible.

#### Scenario: Metrics and readiness without the API server

- **WHEN** the process runs with `server.enabled=false` and a scraper requests the observability metrics endpoint and `/ready`
- **THEN** both respond successfully: `/metrics` returns a valid Prometheus exposition and `/ready` returns the process readiness status

#### Scenario: API server endpoints remain compatible

- **WHEN** the process runs with the API server enabled
- **THEN** the existing `/metrics`, `/health`, `/readiness`, and `/liveness` endpoints continue to respond with their current paths and semantics, including the legacy `arkflow_stream_*` series

### Requirement: Kernel Job metrics export

The metrics endpoint SHALL export Prometheus series for each unified-kernel Job visible to the process: per-chain counters `batches_in`, `batches_out`, `rows`, and `errors`; per-chain gauges `in_flight` and mean processing latency; Job-level checkpoint duration and checkpoint failure counter; Job-level watermark lag and late-event counter. Series SHALL be labeled by Job identifier and chain (task) identifier. When no kernel Job is registered, the exposition SHALL remain valid with no Job series.

#### Scenario: Scrape a process running a kernel Job

- **WHEN** a kernel Job is running with an input chain and an SQL processor chain and a scraper requests the metrics endpoint
- **THEN** the exposition contains per-chain counters and gauges for each chain plus Job-level checkpoint, watermark-lag, and late-event series, all labeled with the Job and chain identifiers

#### Scenario: Scrape with no kernel Jobs

- **WHEN** only legacy Streams are configured and no kernel Job is registered
- **THEN** the metrics endpoint still returns a valid exposition containing the legacy Stream series and no kernel Job series

### Requirement: Prometheus exposition format

All data-plane series SHALL use the `arkflow_` prefix, monotonically increasing counters MUST be declared with `# TYPE ... counter` and named with the `_total` suffix, and gauges MUST be declared with `# TYPE ... gauge`. Every exported series family SHALL include HELP and TYPE metadata, and the exposition SHALL be parseable as Prometheus text format version 0.0.4.

#### Scenario: Type declarations match semantics

- **WHEN** a scraper parses the exposition of a process that has processed batches and experienced a checkpoint failure
- **THEN** batches/rows/errors/checkpoint-failure/late-event families are declared as counters with `_total` names, and in-flight, latency, checkpoint-duration, and watermark-lag families are declared as gauges

### Requirement: Legacy Stream series compatibility

The existing `arkflow_stream_*` series with the `stream_id` label SHALL continue to be exported with their current names, labels, and meanings after this change.

#### Scenario: Upgrade preserves stream series

- **WHEN** a process that previously exported `arkflow_stream_input_messages{stream_id=...}` runs the updated build with the same configuration
- **THEN** the same series name, label set, and meaning are still present in the exposition

### Requirement: Hub export of Agent-reported data-plane metrics

The Hub metrics endpoint SHALL export the data-plane metric vocabulary defined in this capability for metrics reported by Agents, using the same series names plus a `node` label identifying the reporting Agent. The export SHALL reflect the most recently reported values per (node, job) and SHALL be subject to the same authorization as the existing Hub metrics endpoint. When an Agent stops reporting (expired lease or deregistration), its series SHALL stop being exported.

#### Scenario: Scrape the Hub with two reporting Agents

- **WHEN** two Agents report Job chain metrics via heartbeat and an authorized scraper requests the Hub metrics endpoint
- **THEN** the exposition contains the reported data-plane series distinguished by the `node` label alongside the existing control-plane series

#### Scenario: Agent goes offline

- **WHEN** an Agent's lease expires and it stops reporting metrics
- **THEN** subsequent Hub scrapes no longer contain that node's data-plane series while other nodes' series remain

### Requirement: Bounded label vocabulary

Data-plane metric labels SHALL be limited to a fixed vocabulary: metric-name-defining identifiers (Job id, chain/task id, Stream id, node id). Message content, error messages, correlation IDs, SQL text, and credentials MUST NOT appear as metric labels or in metric names. The number of series per process SHALL be bounded by the configured workload (jobs, chains, streams), not by message volume or error counts.

#### Scenario: Errors do not create series

- **WHEN** a Job's processor fails repeatedly with distinct error messages while a scraper polls the metrics endpoint
- **THEN** only the corresponding error counter increments and no series is created or labeled by the error text

### Requirement: Process readiness and liveness semantics

The `/ready` endpoint SHALL report process readiness: it returns success once the engine runtime has finished starting the configured Streams and Jobs (and, when the control plane is enabled, startup recovery has completed), and failure with a stable machine-readable reason otherwise. The `/live` endpoint SHALL report process liveness and MUST NOT depend on external systems. The existing API-server `/readiness` and `/liveness` semantics SHALL remain unchanged.

#### Scenario: Ready after streams and jobs start

- **WHEN** the engine has finished starting all configured Streams and Jobs and a client requests `/ready`
- **THEN** the endpoint responds with success and a machine-readable ready status

#### Scenario: Not ready during startup

- **WHEN** a client requests `/ready` before the engine runtime has finished starting the configured workload
- **THEN** the endpoint responds with a failure status code and a stable not-ready reason
