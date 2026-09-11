# Design: repair-kafka-reconnect-review

## Context

`KafkaInput::connect()` (explicit-partition mode) assigned the configured topics with `TopicPartitionList::add_partition`, which carries no offset. librdkafka then applies `auto.offset.reset` — `"latest"` when `start_from_latest` is set, `"earliest"` otherwise. The source chain's reconnect loop observes `Error::Disconnection` from `read()` and calls `connect()` again; `restore_positions` is never re-invoked (it runs once during startup recovery). The input's in-memory `CommitFrontier` tracks the contiguous acknowledged offsets — exactly the resume point the reconnect needs — but was only consulted for checkpoints (`current_positions`), never for reconnection.

## Goals / Non-Goals

**Goals:**

- A reconnecting explicit-partition reader resumes at the contiguous acknowledged frontier.
- First-connect behavior is unchanged in observable terms: an empty frontier degrades to the configured start (`End` for `start_from_latest`, `Beginning` otherwise), which is what `auto.offset.reset` produced before.
- No broker dependency in the regression tests.

**Non-Goals:**

- Subscription-mode reconnect seeks: group-managed consumers resume from broker-committed offsets (advanced by acked `store_offset` + auto-commit), which preserves at-least-once.
- Clearing failure fences on reconnect: resuming at the contiguous frontier re-delivers the failed record, and its retrying ack clears the matching fence (`clear_failure_if`), so the fence resolves through the normal retry path.

## Decisions

### Decision 1: Reuse `merged_restore_assignment` with the live frontier inside `connect()`

`connect()` now builds the explicit assignment via the existing pure helper `merged_restore_assignment(&topics, partition, &self.frontier.contiguous_positions(), start_from_latest)`. One code path now serves startup, restore-merge, and reconnect, so the recovery semantics cannot drift apart again.

Alternative rejected: re-invoking `restore_positions` from the reconnect loop — it performs broker watermark validation and seeks that belong to the recovery preparer, needs the coordinator's full checkpoint set, and would still miss the in-memory frontier for a job that never checkpointed.

Alternative rejected: tracking a separate `durable_positions` map updated on ack — redundant state; the frontier already IS the acknowledged cursor and is kept consistent by the ack path.

## Risks / Trade-offs

- [`assign()` with explicit offsets skips group offset management] → [explicit-partition mode already bypasses the group assignment; committed offsets are only advisory there.]
- [An empty frontier on first connect produces `Offset::End`/`Beginning` instead of a bare assign] → [same effective behavior as `auto.offset.reset`; covered by a regression test.]
- [In-flight acks that fail `store_offset` during the reassignment window] → [the failure fence blocks later acks until the failed offset retries; the resume point guarantees re-delivery, which retries the ack and clears the fence.]

## Open Questions

None — the resume behavior is covered by deterministic broker-free regression tests.
