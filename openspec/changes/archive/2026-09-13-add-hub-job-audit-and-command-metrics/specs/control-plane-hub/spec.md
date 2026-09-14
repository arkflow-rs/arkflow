## ADDED Requirements

### Requirement: Job operation expiry and bounded retry

Job operations tracked in the durable operation record SHALL carry an expiry timestamp and a retry count. A non-terminal `job_start` or `job_stop` operation whose delivery window expires SHALL be transitioned by the periodic sweep to a retryable terminal state with its retry count incremented; reconciliation SHALL re-enqueue such operations with the accumulated retry count, and an operation whose retry count reaches the cap SHALL settle as terminal failed with failure classification `expired` and no further re-enqueue. Checkpoint and savepoint trigger operations carry the same expiry metadata, and their delivery-window expiry remains governed by the command-lease path, which replays the original trigger payload. Expired operations SHALL NOT be deleted while they remain the active deduplication record for their (Job, generation, operation).

#### Scenario: An expired start becomes retryable

- **WHEN** a queued or dispatched `job_start` or `job_stop` operation passes its expiry timestamp without reaching a satisfying terminal state
- **THEN** the periodic sweep marks it retryable with an incremented retry count, drops the undeliverable command, and reconciliation re-enqueues the desired-state operation

#### Scenario: Retry cap reached

- **WHEN** an expired lifecycle operation's retry count reaches the cap
- **THEN** the operation reaches a terminal failed state with failure classification `expired` and reconciliation stops re-enqueueing it

#### Scenario: A checkpoint trigger expiry replays its payload

- **WHEN** a dispatched checkpoint or savepoint trigger outlives its command lease
- **THEN** the command-lease path marks the operation retryable and re-enqueues the original trigger payload, preserving the recovery-artifact semantics

#### Scenario: Older records without expiry metadata keep working

- **WHEN** the Hub opens a database created before this change
- **THEN** existing operation rows gain default expiry metadata without behavioral change and remain queryable and deduplicated
