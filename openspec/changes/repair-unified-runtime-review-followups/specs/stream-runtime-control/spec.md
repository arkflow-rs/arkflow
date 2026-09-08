## ADDED Requirements

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
