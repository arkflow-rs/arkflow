# control-plane-observability Delta — console-hub-alignment

## ADDED Requirements

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
