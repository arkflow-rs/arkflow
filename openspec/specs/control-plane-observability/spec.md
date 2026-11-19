## Purpose

Define bounded operational health, diagnostics, and metrics for production control-plane operation.
## Requirements
### Requirement: Operational health snapshot

The Hub SHALL expose a bounded operational status that distinguishes process liveness, storage/recovery readiness, reconciliation activity, node lease health, outbox backlog, active Attempts, and non-terminal Intent counts. The status SHALL not include secrets, raw configuration content, or unbounded resource lists.

#### Scenario: Hub is live but not ready
- **WHEN** the Hub process is running but startup recovery or the storage actor has not completed successfully
- **THEN** the liveness endpoint remains alive while readiness reports `503` with `ready: false` and a stable dependency reason

#### Scenario: Agent is offline
- **WHEN** the Hub storage and reconciler are healthy but one or more Agents have expired leases
- **THEN** the operational status reports degraded node health and stale-node counts without marking the Hub process itself not live

#### Scenario: Reconciliation stops making progress
- **WHEN** no successful reconciliation tick has completed within the configured health window
- **THEN** the operational status reports degraded reconciliation with the last successful timestamp and bounded error classification

### Requirement: Prometheus metrics

The Hub SHALL expose Prometheus text metrics for readiness, reconciler activity, node connection states, Intent/Attempt states, outbox backlog, stale nodes, and pending age using a fixed vocabulary of low-cardinality labels. Resource IDs, correlation IDs, error messages, and secrets MUST NOT be metric labels.

#### Scenario: Scrape a healthy Hub
- **WHEN** a scraper requests the configured metrics endpoint
- **THEN** it receives valid Prometheus exposition containing readiness, reconciliation, lease, Intent, Attempt, and outbox metrics

#### Scenario: Scrape during degradation
- **WHEN** storage is ready but reconciliation has failures or stale node leases exist
- **THEN** the metrics preserve the same names and labels while exposing non-zero failure/degraded gauges and counters

### Requirement: Bounded operational diagnostics

The Hub SHALL provide an authenticated JSON diagnostic endpoint with stable schema fields for component status, last successful reconciliation, storage/outbox summaries, node state counts, and failure categories. Diagnostic responses SHALL be bounded and safe for logs and support bundles.

#### Scenario: Inspect a degraded control plane
- **WHEN** an operator requests the operational status endpoint during retrying or blocked reconciliation
- **THEN** the response identifies the degraded component, counts, timestamps, and correlation-safe failure class without returning payloads or credentials

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

### Requirement: Fleet metrics in JSON for console clients

The Hub's `GET /api/v1/metrics` endpoint SHALL serve Prometheus text exposition by default and SHALL serve the JSON aggregate `{items: [{node_id, metrics}], aggregate}` when the client explicitly requests JSON (via `Accept: application/json` or `format=json`). Both representations SHALL honor the `node_id` query filter. The default representation and its content MUST remain byte-identical for existing Prometheus scrapers.

#### Scenario: Prometheus scraper keeps working

- **WHEN** a scraper requests `GET /api/v1/metrics` with no Accept header or a Prometheus Accept value
- **THEN** the response has content type `text/plain; version=0.0.4` and the same exposition body as before this change

#### Scenario: Console requests JSON metrics

- **WHEN** the console requests `GET /api/v1/metrics` with `Accept: application/json`
- **THEN** the response is a JSON object with `items` keyed by node id and an `aggregate` object, and with `?node_id=` set, `items` contains only that node's entry

### Requirement: Job-detail diagnostics are job-scoped and measured-only

The Hub's Job detail diagnostics SHALL aggregate metrics from that Job's own observations reported by compute nodes, not from node-wide or fleet-wide sums, and SHALL NOT emit diagnostic keys for which no measurement source exists. Keys whose measurement is not implemented SHALL be absent from the response rather than serialized as zero-valued numbers.

#### Scenario: Job detail shows only that Job's gauges

- **WHEN** two Jobs run on one node and an operator opens the detail view of one Job
- **THEN** the Job's `watermark_lag_ms`, `checkpoint_duration_ms`, and `checkpoint_failures` reflect only the displayed Job's kernels, and a max-valued gauge is never summed across nodes

#### Scenario: Unmeasured keys are absent

- **WHEN** the Hub builds a Job detail response
- **THEN** keys without a measurement source (state size, recovery progress, task pressure, partition health) do not appear in the metrics object

### Requirement: Hub process logging and OTel trace export

The `arkflow-server` Hub binary SHALL initialize a tracing subscriber at startup, before any startup validation logging: log output goes to the console by default at level `info`, with level, file output, and format taken from the `logging` configuration (file or `ARKFLOW_LOG_LEVEL`/`RUST_LOG`). The effective log level SHALL follow the precedence chain `RUST_LOG` > `ARKFLOW_LOG_LEVEL` > `logging.level` (file) > default; an invalid `RUST_LOG` value SHALL warn on stderr and fall back to the static level. When `observability.tracing` is enabled (default off), the Hub SHALL export spans via OTLP/HTTP-JSON to the configured endpoint with the configured `service.name`; exporter build failure SHALL warn and disable export without preventing startup. On graceful shutdown (Ctrl-C or serve termination) the tracer provider SHALL be flushed before exit so buffered spans are not lost. When tracing is disabled the process MUST NOT build any OTel objects or issue OTLP network requests. Existing metrics, readiness, and liveness endpoint behavior SHALL be unchanged by this requirement.

#### Scenario: Default startup logs to the console
- **WHEN** the Hub starts with no configuration file and no logging-related environment variables
- **THEN** `tracing` events from Hub startup, election, and session handling appear on the console at `info` level, including the SQLite HA development warning path

#### Scenario: Tracing disabled stays inert
- **WHEN** `observability.tracing` is absent or `enabled: false`
- **THEN** no OTel exporter or provider is built, no OTLP requests are made, and logging behavior is identical to tracing being unconfigured

#### Scenario: Spans flushed on graceful shutdown
- **WHEN** tracing is enabled and the operator stops the Hub with Ctrl-C after it produced spans
- **THEN** the buffered spans are flushed to the OTLP endpoint before process exit

#### Scenario: Bad OTel endpoint never blocks serve
- **WHEN** `observability.tracing.endpoint` is invalid
- **THEN** startup logs a warning, tracing export is disabled, and the Hub serves normally

#### Scenario: Invalid RUST_LOG falls back
- **WHEN** `RUST_LOG` is set to a value EnvFilter cannot parse
- **THEN** the process warns on stderr and applies the static level from `ARKFLOW_LOG_LEVEL`/`logging.level` instead of failing startup

