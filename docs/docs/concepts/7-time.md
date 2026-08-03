---
sidebar_position: 7
---

# Time and event-time semantics

ArkFlow receives records that may be delayed, retried, or replayed. Treat the
timestamp carried by a record as data, not as proof that it arrived in order.

## Processing time and event time

Processing time is when ArkFlow observes a record. Event time is the timestamp
provided by the source, commonly `__meta_timestamp` or a domain field. A
processing-time window is deterministic for one running node; event-time
ordering requires the application to handle late records explicitly.

## Choosing a timestamp

Preserve source timestamps in metadata or a named column before transforming
payloads. When a source has no trustworthy timestamp, use ingestion time and
document that choice. WAL replay preserves input sequence, not event time.

Use [window semantics](./8-windows.md) to choose a boundary. Window components
emit batches; they do not provide a distributed watermark or cross-node clock.
