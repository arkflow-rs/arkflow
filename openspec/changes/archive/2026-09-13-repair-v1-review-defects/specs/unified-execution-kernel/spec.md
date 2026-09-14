## MODIFIED Requirements

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, and joining a retired pool — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress. A collector drain that exhausts its wait bound SHALL surface a failure for the chain (and therefore the checkpoint round) instead of reporting success, and an abandoned collector SHALL be joined or aborted before the chain's sink is closed, so no retired collector writes into a closed sink or publishes data after EOS.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

#### Scenario: A collector drain timeout fails the chain

- **WHEN** a chain's collector drain exceeds its wait bound and the chain proceeds to end-of-stream
- **THEN** the chain reports a drain failure instead of success, the abandoned collector is joined or aborted before the sink closes, and no downstream write or EOS-after-data ordering violation occurs

## ADDED Requirements

### Requirement: Checkpoint frontier capture SHALL fail closed

A checkpoint round SHALL fail, not seal, when it cannot capture a consistent frontier: a chain whose source-position snapshot errors SHALL fail the round instead of seeding stale or empty positions, queued snapshot errors SHALL be consumed and reported before the round can complete successfully, and a round whose execution graph is failing SHALL be invalidated before its manifest is persisted rather than sealed with a missing participant.

#### Scenario: Source positions fail during a round

- **WHEN** `current_positions()` for a source chain returns an error during checkpoint capture
- **THEN** the round fails with that error instead of sealing a checkpoint whose positions are empty or from a previous round

#### Scenario: A queued snapshot error cannot be bypassed

- **WHEN** a chain's snapshot report and its error are both pending and the report arrives last but the error was queued for the same round
- **THEN** the round does not return success until the error queue has been drained, and the checkpoint does not persist as successful

#### Scenario: A failing graph does not seal a partial manifest

- **WHEN** a chain exits with an error during a checkpoint round and has not delivered a snapshot for that round
- **THEN** the round is invalidated before persist instead of being sealed with that participant missing
