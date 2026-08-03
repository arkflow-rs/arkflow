---
sidebar_position: 8
---

# Window semantics

Windows turn an unbounded stream into finite batches for aggregation, joins,
and periodic output. ArkFlow implements windows in the buffer layer and keeps
their state in the owning process.

## Tumbling windows

[`tumbling_window`](../components/1-buffers/tumbling_window.md) uses fixed,
non-overlapping intervals. Each record belongs to one interval, making it a
good default for periodic summaries and flush-oriented sinks.

## Sliding windows

[`sliding_window`](../components/1-buffers/sliding_window.md) advances by a
fixed step while retaining the configured range. A record can appear in more
than one emitted batch, so downstream aggregation must account for overlap.

## Session windows

[`session_window`](../components/1-buffers/session_window.md) groups activity
until an inactivity gap elapses. It suits user sessions and device bursts.

## Limits

Window state is local to one process. There is no cross-node watermark,
automatic checkpoint, or global event-time coordinator; use input WAL replay
when recovering a node.
