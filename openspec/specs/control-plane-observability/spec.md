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
