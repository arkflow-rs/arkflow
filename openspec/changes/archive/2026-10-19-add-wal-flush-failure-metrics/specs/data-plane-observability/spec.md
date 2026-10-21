## MODIFIED Requirements

### Requirement: Kernel Job metrics export

The metrics endpoint SHALL export Prometheus series for each unified-kernel Job visible to the process: per-chain counters `batches_in`, `batches_out`, `rows`, and `errors`; per-chain gauges `in_flight` and mean processing latency; Job-level checkpoint duration and checkpoint failure counter; Job-level watermark lag and late-event counter; and a Job-level WAL background-flush failure counter. Series SHALL be labeled by Job identifier and, where the metric is per-chain, by chain (task) identifier. When no kernel Job is registered, the exposition SHALL remain valid with no Job series.

#### Scenario: Scrape a process running a kernel Job

- **WHEN** a kernel Job is running with an input chain and an SQL processor chain and a scraper requests the metrics endpoint
- **THEN** the exposition contains per-chain counters and gauges for each chain plus Job-level checkpoint, watermark-lag, late-event, and WAL flush-failure series, all labeled with the Job and chain identifiers

#### Scenario: Scrape with no kernel Jobs

- **WHEN** only legacy Streams are configured and no kernel Job is registered
- **THEN** the metrics endpoint still returns a valid exposition containing the legacy Stream series and no kernel Job series

#### Scenario: Job without a WAL exports a zero WAL flush-failure counter

- **WHEN** a kernel Job runs without input WAL durability and a scraper requests the metrics endpoint
- **THEN** the WAL flush-failure counter is exported with value 0 for that Job, without creating any additional series or labels

### Requirement: Legacy Stream series compatibility

The existing `arkflow_stream_*` series with the `stream_id` label SHALL continue to be exported with their current names, labels, and meanings after this change, and the Stream-level WAL background-flush failure counter SHALL be exported alongside them under the same `stream_id` labeling.

#### Scenario: Upgrade preserves stream series

- **WHEN** a process that previously exported `arkflow_stream_input_messages{stream_id=...}` runs the updated build with the same configuration
- **THEN** the same series name, label set, and meaning are still present in the exposition

#### Scenario: Stream WAL flush failures are exported

- **WHEN** a Stream with input WAL durability experiences background flush failures and a scraper requests the metrics endpoint
- **THEN** the Stream-level WAL flush-failure counter reflects the failures accumulated by that Stream's WAL, labeled only by `stream_id`

## ADDED Requirements

### Requirement: WAL flush-failure counters

The data-plane exposition SHALL expose the cumulative count of WAL background-flush failures as monotonically increasing counters named `arkflow_job_wal_flush_failures_total` (labels `job`, plus `node` on the Hub export) and `arkflow_stream_wal_flush_failures_total` (label `stream_id`). The values SHALL be sampled from the WAL when the snapshot is taken, so a failure is observable at the next scrape or Agent heartbeat without requiring further data to flow. The counters SHALL NOT expose consecutive-failure counts, timestamps, error text, or any other label beyond the existing closed vocabulary.

#### Scenario: Failure becomes visible at the next scrape

- **WHEN** a WAL background flush fails and the write path retries the staged batch
- **THEN** the next metrics scrape reports an incremented WAL flush-failure counter while the retry behavior and the staged entries' durability semantics are unchanged

#### Scenario: Hub exports Agent-reported WAL failures

- **WHEN** an Agent reporting kernel Job snapshots includes a non-zero WAL flush-failure count and an authorized scraper requests the Hub metrics endpoint
- **THEN** the Hub exports the same series name with the reporting node's label and the reported value

#### Scenario: Older reports and newer Hub interoperate

- **WHEN** a Hub receives a kernel Job snapshot from an Agent version that does not include the WAL flush-failure field, or a newer Agent reports to an older Hub
- **THEN** both sides decode the snapshot successfully, treating the absent field as zero, and the remaining series are unaffected
