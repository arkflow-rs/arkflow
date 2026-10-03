## MODIFIED Requirements

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, joining a retired pool, writing to its sink, and capturing a state snapshot — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress. A collector drain that exhausts its wait bound SHALL surface a failure for the chain (and therefore the checkpoint round) instead of reporting success, and an abandoned collector SHALL be joined or aborted before the chain's sink is closed, so no retired collector writes into a closed sink or publishes data after EOS. A sink write or state snapshot that exceeds its bound SHALL fail the chain (or the round) with an explicit timeout error naming the duration; a write cancelled by the timeout MAY have left partial effects on the external system, which at-least-once replay absorbs.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

#### Scenario: A collector drain timeout fails the chain

- **WHEN** a chain's collector drain exceeds its wait bound and the chain proceeds to end-of-stream
- **THEN** the chain reports a drain failure instead of success, the abandoned collector is joined or aborted before the sink closes, and no downstream write or EOS-after-data ordering violation occurs

#### Scenario: A hung sink write fails the chain within a bound

- **WHEN** the chain's sink write (external system hung mid-write) exceeds its bound
- **THEN** the chain fails with an explicit timeout error naming the duration (settling its acknowledgements per the failure path), unblocking both the data plane and shutdown; any partial external effect is absorbed by at-least-once replay

#### Scenario: A hung state snapshot fails the round within a bound

- **WHEN** a state backend snapshot exceeds its bound during a barrier round
- **THEN** the snapshot fails with an explicit timeout error naming the duration, the round fails per the existing snapshot-failure semantics, and a late-completing background snapshot result is discarded without side effects
