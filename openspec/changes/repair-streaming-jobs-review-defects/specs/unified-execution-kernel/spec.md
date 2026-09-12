## MODIFIED Requirements

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, and joining a retired pool — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

## ADDED Requirements

### Requirement: Worker pool failure SHALL fail the chain

A processor worker pool SHALL report every abnormal exit as a failure the chain observes, and a disconnected failure channel or an unexpectedly retired pool SHALL NOT be interpreted as a clean shutdown. When a pool is retired the chain SHALL cancel and join the remaining workers and collectors, SHALL settle every queued or in-flight delivery by acknowledging or aborting it, and SHALL keep its edge-ordering and checkpoint fences in force or fail the chain, so no delivery is silently discarded and no barrier is sealed over unsettled work.

#### Scenario: A worker panics

- **WHEN** a processor worker panics while handling a pooled delivery and the remaining workers or the collector exit as a result
- **THEN** the chain fails with an explicit error, the in-flight delivery is settled, and the checkpoint round is not reported as valid

#### Scenario: Every worker exits without recording a failure

- **WHEN** every worker and the collector exit and the failure channel disconnects without an error
- **THEN** the chain settles its queued deliveries and either continues with its ordering fences intact or fails explicitly, instead of dropping the pool and continuing

#### Scenario: Flush observes a stalled pool

- **WHEN** the chain flushes the pool before a tick, barrier, or watermark and a delivery cannot complete
- **THEN** the flush fails or returns within its bound so the chain reports the condition instead of stopping silently
