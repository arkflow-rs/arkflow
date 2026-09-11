# Repair kafka reconnect review defect

## Why

An independent code review confirmed one high-severity gap in the Kafka input: the in-process reconnect path bypassed all of the durable recovery machinery that startup builds. In explicit-partition mode `connect()` built the assignment with `add_partition` (no offset, `crates/arkflow-plugin/src/input/kafka.rs:303` pre-fix), so librdkafka applied `auto.offset.reset`; the reconnect loop (`crates/arkflow-core/src/executor/task.rs:603-645`) only calls `source.connect()` and never re-invokes `restore_positions`. After a broker blip surfacing `Error::Disconnection`, `start_from_latest=true` permanently skipped every record produced during the outage (at-least-once violated) and `start_from_latest=false` reprocessed the entire retained log. The in-memory acknowledged frontier was never reconciled with the new consumer position either, and an in-flight ack failing `store_offset` on the new consumer recorded a failure fence that cascaded into later acks.

## What Changes

- **Explicit-partition assignments resume from the acknowledged frontier**: `connect()` builds the assignment with `merged_restore_assignment` over `frontier.contiguous_positions()`. On the first connect the frontier is empty and the configured start applies (`Offset::End`/`Offset::Beginning`, mirroring the previous `auto.offset.reset` policy); on a reconnect the acknowledged frontier becomes explicit offsets, so the consumer resumes exactly at the last acknowledged record — no outage window is skipped and no full-log replay happens. Because the resume point is the contiguous acknowledged frontier, a failed delivery is re-delivered and its retrying ack clears the recorded failure fence naturally.
- Regression tests cover both the reconnect resume and the unchanged first-connect semantics.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `checkpoint-recovery`: in-process source reconnects must restore the acknowledged cursor before consuming, not rely on `auto.offset.reset`.

## Impact

- Runtime: `crates/arkflow-plugin/src/input/kafka.rs` — the only production code change.
- Tests: `crates/arkflow-plugin/src/input/kafka.rs` tests module — two regression tests (broker-free; `assign()` is local metadata).
- Specs: 1 master spec updated via this change's delta at archive time.

## Non-goals

- Subscription-mode reconnect seeking (group-committed offsets already resume at-least-once).
- Blocking `fetch_watermarks`/`seek` calls moved off the async runtime (review P2).
- Escape hatch for checkpoints behind the retention low watermark (review P2).
- Resetting the close cancellation token on reconnect (dormant, review P2).
