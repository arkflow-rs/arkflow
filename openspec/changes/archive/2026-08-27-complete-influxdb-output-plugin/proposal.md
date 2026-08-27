## Why

The repository already registers an InfluxDB output, but its current implementation does not reliably expose the Arrow records it receives: `write` converts structured batches to binary payloads before applying field mappings (`crates/arkflow-plugin/src/output/influxdb.rs:437-457`), and the metadata example uses mapping keys that do not match `TagMapping`/`FieldMapping` (`crates/arkflow-plugin/src/output/influxdb.rs:544-551`). The plugin also builds the v2 write URL without encoding organization and bucket values (`crates/arkflow-plugin/src/output/influxdb.rs:364-368`), so the feature needs to be completed and made testable before it can be used as a dependable time-series sink.

## What Changes

- Complete the InfluxDB 2.x output contract for Arrow `MessageBatch` values.
- Support configurable measurement, tag, field, type coercion, and nanosecond timestamp mappings.
- Generate valid escaped InfluxDB Line Protocol and send buffered batches to `/api/v2/write`.
- Make buffering, retry, timeout, connect, and close behavior deterministic; failed writes retain data for retry.
- Align component metadata/schema/example with the deserializable configuration and add unit/in-process HTTP tests.

## Capabilities

### New Capabilities

- `influxdb-output`: Write mapped ArkFlow Arrow batches to InfluxDB 2.x using Line Protocol.

### Modified Capabilities

- None.

## Impact

- `crates/arkflow-plugin/src/output/influxdb.rs` and its tests.
- Existing `reqwest` dependency only; no new runtime dependency is required.
- Output configuration schema and examples become accurate; no change to the core `Output` trait.

## Non-goals

- Supporting InfluxDB 1.x authentication or query APIs.
- Adding a background timer task for flushes; partial batches flush on the configured interval check, subsequent writes, and `close`.
- Changing generic codec behavior for other output plugins.
