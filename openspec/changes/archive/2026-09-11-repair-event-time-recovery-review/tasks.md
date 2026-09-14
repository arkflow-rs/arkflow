## 1. OpenSpec and checkpoint model

- [x] 1.1 Add physical topic/partition watermark identity and additive per-partition checkpoint fields.
- [x] 1.2 Seed assigned watermark partitions, compute the minimum active frontier, and support restore compatibility with legacy task-level watermarks.
- [x] 1.3 Share downstream watermark trackers between compatible event-time source gates while keeping held deliveries on their owning chain.
- [x] 1.4 Exclude processing-time and static session timing from source event-time gating; preserve dynamic session ownership in the window operator.

## 2. Acknowledgement, WAL, and recovery

- [x] 2.1 Add acknowledgement compensation and make grouped source acknowledgements retryable and failure-safe.
- [x] 2.2 Make WAL acknowledgement progress one delivery at a time, keep failed source commits as a frontier fence, and avoid draining sibling outcomes into the caller.
- [x] 2.3 Flush group/periodic WAL appends before exposing reads, reconcile covered replay prefixes, and close the WAL with its input.
- [x] 2.4 Reconstruct connector source-position acknowledgements for WAL replay and reject Kafka acknowledgements after consumer shutdown.
- [x] 2.5 Couple window journal commits, source acknowledgements, and stale-key cleanup to one rollback-capable fired transaction.

## 3. Windows and execution graph

- [x] 3.1 Propagate emitted state while merging sessions and compact repeated staged window mutations.
- [x] 3.2 Preserve legacy window payload behavior and reject unsupported legacy joins explicitly.
- [x] 3.3 Remove ended inputs from barrier alignment and release buffered EOS/data without deadlocking checkpoints.
- [x] 3.4 Join/cancel processor pools correctly, propagate worker failures, route processor failures with all sibling acknowledgements, and keep metrics balanced.
- [x] 3.5 Connect partitioned edges to all downstream subtasks so key-group routing selects the real owner.

## 4. State backend and runtime lifecycle

- [x] 4.1 Exclude expired Redb entries from metrics and serialize budget checks/counter updates under one write-side lock.
- [x] 4.2 Close the real adapter on startup failure and make reconnect/backoff and Kafka receive failures cancellation-aware/retryable.
- [x] 4.3 Recover runtime state after shutdown timeout and surface local Job startup failures before readiness.

## 5. Hub and Agent recovery

- [x] 5.1 Include Job generation in operation deduplication and stop stale placements across generations.
- [x] 5.2 Restore persisted operations before reconciliation, requeue expired commands, and fence late terminal/mismatched-generation results.
- [x] 5.3 Aggregate multi-node operation status before deriving Job state.
- [x] 5.4 Preserve Agent boot identity across reports and restore every physical partition watermark/checkpoint task set.

## 6. Verification

- [x] 6.1 Add focused regression tests for event-time, ACK/WAL, window, lifecycle, and control-plane fixes.
- [x] 6.2 Run formatting, OpenSpec strict validation, targeted tests, workspace tests, and diff checks; document unavailable external Kafka/Docker checks.

Validation note: Docker-backed Kafka EOS tests remain unavailable in this environment because `/var/run/docker.sock` is not present; non-Docker workspace and targeted plugin tests passed.
