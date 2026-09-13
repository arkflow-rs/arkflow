## MODIFIED Requirements

### Requirement: Kafka restore SHALL preserve the full configured assignment
Kafka recovery SHALL merge checkpoint positions into the complete configured assignment or subscription. A checkpoint containing only a subset of topic partitions SHALL NOT unassign omitted configured partitions, and restored positions SHALL seed the in-memory acknowledged frontier used by later checkpoints. Waiting for a partition assignment SHALL NOT hold the consumer lock across the wait: a reconnect triggered while an assignment wait is in flight SHALL acquire the consumer lock without waiting out the assignment timeout, and a wait started against a consumer that is about to be replaced SHALL resolve without blocking the replacement.

#### Scenario: Restore a subset of partitions
- **WHEN** a checkpoint contains positions for only some configured topic partitions
- **THEN** all configured partitions remain assigned or subscribed, matching positions seek to the checkpoint offsets, and omitted partitions retain their configured starting behavior

#### Scenario: Checkpoint immediately after restore
- **WHEN** a recovered Kafka task reaches a checkpoint before acknowledging a new record
- **THEN** `current_positions()` still returns the restored positions rather than an empty cursor

#### Scenario: Reconnect is not blocked by an in-flight assignment wait
- **WHEN** a partition assignment is lost, an in-flight acknowledgement starts waiting for the old consumer's assignment, and the kernel immediately calls `connect()` to rebuild the consumer
- **THEN** the reconnect acquires the consumer lock without waiting for the in-flight wait's timeout, and the new consumer is not stuck behind a wait bound to the replaced consumer
