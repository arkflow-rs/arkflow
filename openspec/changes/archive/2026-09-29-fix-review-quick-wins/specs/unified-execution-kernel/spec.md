## MODIFIED Requirements

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, and joining a retired pool — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress. A collector drain that exhausts its wait bound SHALL surface a failure for the chain (and therefore the checkpoint round) instead of reporting success, and an abandoned collector SHALL be joined or aborted before the chain's sink is closed, so no retired collector writes into a closed sink or publishes data after EOS.

The no-silent-park guarantee SHALL extend to processor-owned internal resource pools: a bounded internal resource acquired to process a batch (e.g. a plan-execution context) SHALL be released on every path, including error paths — a processing error MUST NOT leak the resource; and an acquisition wait that exhausts its bound SHALL surface an explicit failure for the batch instead of busy-waiting indefinitely.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

#### Scenario: A collector drain timeout fails the chain

- **WHEN** a chain's collector drain exceeds its wait bound and the chain proceeds to end-of-stream
- **THEN** the chain reports a drain failure instead of success, the abandoned collector is joined or aborted before the sink closes, and no downstream write or EOS-after-data ordering violation occurs

#### Scenario: Processor error paths release pooled resources

- **WHEN** a processor that owns a bounded internal resource pool hits processing errors on consecutive batches (e.g. plan failures on schema drift), with the pool fully occupied between attempts
- **THEN** every error path releases its acquired resource back to the pool, subsequent batches keep processing (failing with their own explicit errors if the data is still bad), and the chain never parks silently once the pool bound is reached
