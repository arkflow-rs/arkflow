# stream-runtime-control Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Stable Stream identity
Each configured Stream SHALL have a stable unique ID for runtime commands, API resources, metrics, and events. Legacy configurations without IDs SHALL receive deterministic `stream-<index>` IDs and a migration warning. The runtime registry SHALL assign stream indices from a monotonically increasing counter (never reusing an index after de-registration), so that two concurrently active Streams can never share a derived job ID, state namespace prefix, or checkpoint directory — even after `replace_config` removes and re-registers entries.

#### Scenario: Duplicate IDs are rejected
- **WHEN** a candidate configuration contains two Streams with the same ID
- **THEN** validation fails before any runtime Stream is stopped or replaced

#### Scenario: Legacy configuration is loaded
- **WHEN** a configuration contains Streams without IDs
- **THEN** the Engine assigns deterministic IDs and emits a migration warning

#### Scenario: De-registration and re-registration never reuse an index

- **WHEN** a Stream is de-registered (via `replace_config`) and a new or surviving Stream is subsequently registered
- **THEN** the new registration receives a strictly greater index than every previously assigned index, and its derived job ID / state namespace prefix cannot collide with any concurrently active Stream

### Requirement: Per-Stream lifecycle supervision
The runtime manager SHALL track each Stream independently with state, a per-Stream cancellation mechanism, and a supervised task handle. Startup failures SHALL be reported before readiness, and shutdown timeouts SHALL leave the Stream in a recoverable terminal or failed state rather than permanently stopping lifecycle commands in `Stopping` or `Restarting`.

#### Scenario: One Stream fails
- **WHEN** one Stream task exits with an error
- **THEN** that Stream becomes `failed`, its latest error is retained, and unrelated Streams continue running

#### Scenario: Engine shuts down
- **WHEN** the Engine receives its shutdown signal
- **THEN** the runtime manager requests shutdown for every Stream and waits for their tasks to finish or reports a bounded shutdown failure while updating each Stream state

#### Scenario: Startup fails
- **WHEN** graph construction or resource connection fails before a local Job starts
- **THEN** readiness is not reported and the temporary adapter/resources are closed

#### Scenario: Shutdown times out
- **WHEN** a Stream task does not exit within the shutdown timeout
- **THEN** the manager aborts it, records the timeout, and transitions the entry to a recoverable `failed` or `stopped` state

### Requirement: Stream start, stop, and restart
The runtime manager SHALL support starting, stopping, and restarting individual Streams with correct lifecycle semantics: start launches a fresh supervised task; stop requests shutdown and awaits the task; restart stops then starts with one cancellation cycle. A `stop` request issued while a `restart` is in progress SHALL NOT produce an inconsistent state where `stop` reports success while the Stream is left Running.

#### Scenario: Concurrent stop during restart does not leave a Running stream

- **WHEN** a `restart` is initiated and a concurrent `stop` arrives during the restart's stop phase
- **THEN** the `stop` completes without error (the restart's internal stop fulfils the stop intent), and the final state reflects the last-initiated intent — the `stop` never silently resurrects a stopped Stream into Running

#### Scenario: Stop and restart are individually correct

- **WHEN** `stop` is called on a Running Stream (no concurrent restart)
- **THEN** the Stream transitions to Stopped and its task has exited

- **WHEN** `restart` is called on a Running Stream (no concurrent stop)
- **THEN** the Stream's previous task exits, a fresh task launches, and restart metrics increment

### Requirement: Runtime metrics and recent errors
The runtime manager SHALL expose non-blocking counters and gauges for Stream state, input/output activity, processing errors, connector errors, restarts, and recent error events.

#### Scenario: Metrics are updated during processing
- **WHEN** a Stream receives, processes, or writes a batch
- **THEN** the corresponding counters change without awaiting the control API or metrics endpoint

#### Scenario: Prometheus scrape
- **WHEN** a Prometheus client requests `/metrics`
- **THEN** the response contains low-cardinality metrics labeled by stable Stream and stage identifiers

### Requirement: Runtime startup failure SHALL be terminally observable
If dry-run, graph construction, resource connection, or local Job startup fails after the runtime enters `Starting`, the runtime SHALL transition to `Failed` before returning the error. A later start MAY retry only through an explicit new lifecycle operation.

#### Scenario: Dry-run fails while starting
- **WHEN** the runtime's temporary adapter or WAL dry-run returns an error
- **THEN** the runtime state becomes `Failed`, the error is retained, and a subsequent start is not rejected merely because the old state remains `Starting`

### Requirement: Stream and Job resources SHALL close in dependency order
The runtime SHALL close processors/sinks, source connectors, temporary resources, state backends, and WAL flushers according to their ownership dependencies. Close operations SHALL be idempotent and close/flush errors SHALL remain observable to the owning lifecycle operation.

#### Scenario: WAL-backed Stream is replaced
- **WHEN** a running WAL-backed Stream is stopped before a replacement starts
- **THEN** the source closes, pending WAL entries flush, the flusher and WAL handle release, and the replacement can reopen the same path

### Requirement: Source reconnect observes cancellation

Source reconnect attempts and their backoff SHALL select on the owning cancellation token. A source that cannot reconnect SHALL stop promptly when its stream is stopped, restarted, or cancelled, and SHALL release its WAL and connector resources.

#### Scenario: Stop during repeated reconnect failures

- **WHEN** every reconnect attempt fails and the stream cancellation token is triggered during backoff
- **THEN** the source loop exits without waiting for the full retry delay and the normal close path runs

### Requirement: Processor pools join before chain shutdown

When processor parallelism is greater than one, cancellation and normal chain completion SHALL close submissions, propagate cancellation to workers and the collector, and await pool termination before processors, outputs, or state backends are closed.

#### Scenario: Cancel while a worker is processing

- **WHEN** a stream is cancelled while a processor worker and collector still have in-flight deliveries
- **THEN** the pool is joined before `finish_chain` closes the processors, with no worker publishing after resource shutdown

### Requirement: Worker failures survive drain and EOS

The processor pool SHALL propagate worker and collector failures through `drain()` and the chain's EOS path. A chain SHALL NOT report successful completion when a worker failed or an output/acknowledgement error was dropped during drain.

#### Scenario: Worker fails as input closes

- **WHEN** an input closes while a worker has just returned a fatal processing error
- **THEN** EOS drain returns that failure and the chain reports failure instead of silently succeeding

### Requirement: Startup failures close real resources

If real stream graph or resource startup fails before the graph takes ownership of the source and sink, the runtime SHALL explicitly close the real adapter and all resources it opened before returning the error.

#### Scenario: Restart after startup failure

- **WHEN** a durability-enabled stream fails during real graph construction or resource connection
- **THEN** the WAL flusher and adapter lock are closed before `start()` returns, and a subsequent start can reopen the same WAL path

