## Context

`InfluxDBOutput` is already registered as `influxdb` and uses the InfluxDB v2 HTTP write endpoint. The input to an output is an `Arc<MessageBatch>` containing Arrow columns, so field mappings must operate on that batch directly. The current implementation reconstructs a binary `MessageBatch`, which loses the configured Arrow columns, and serializes request parameters directly into a URL.

## Goals / Non-Goals

**Goals:**

- Preserve Arrow values until Line Protocol conversion.
- Keep the existing public configuration shape, correcting its schema/example and validation behavior.
- Implement correct escaping, null handling, timestamp fallback, request status handling, retries, and flush-on-close.
- Test conversion and HTTP requests without requiring a live InfluxDB service.

**Non-Goals:**

- InfluxDB query/read support, schemas, dashboards, or health polling.
- A new async background scheduler for flush intervals.
- Exactly-once guarantees beyond the existing output acknowledgement contract.

## Decisions

1. **Use the original Arrow batch for mapping.** The configured `tags`, `fields`, and `timestamp_field` refer to Arrow column names. A configured codec is rejected by the builder with a configuration error because codec output is opaque bytes and cannot be safely mapped to typed Influx fields; silently ignoring it would be worse.

2. **Use `reqwest::RequestBuilder::query` for `org`, `bucket`, and `precision`.** This lets reqwest percent-encode values and avoids malformed URLs for spaces or reserved characters. The endpoint remains `/api/v2/write`.

3. **Implement `write_batch` as the unit of buffering.** Each incoming `MessageBatch` contributes all mapped lines; the request is flushed when the line count reaches `batch_size` or the configured interval has elapsed. `close` flushes remaining lines. A failed request leaves the buffer untouched so the stream can retry the same acknowledgement range.

4. **Keep line protocol conversion pure.** Conversion skips null tags/fields and rows with no fields, uses `i` for integer values, quotes strings, escapes measurement/keys/tag values/strings, and uses the configured integer timestamp as nanoseconds. Missing or invalid timestamps use current nanoseconds.

5. **Test through a local HTTP server abstraction.** Conversion tests use Arrow `RecordBatch` values directly; request tests use a minimal in-process TCP HTTP responder to assert path, query, headers, body, retry behavior, and buffer retention without external services.

## Risks / Trade-offs

- [Unsupported Arrow types] → Skip unmappable values and document supported primitive types; rows without fields are not sent.
- [Flush interval is checked only during writes] → `close` always flushes, and the non-goal keeps the change free of a background task lifecycle.
- [Codec rejection may affect an existing configuration] → Return a clear config error at build time rather than emit empty or corrupt points.
- [Retries can duplicate a request after an ambiguous network failure] → Preserve the existing retry policy and rely on InfluxDB point overwrite semantics for identical measurement/tag/timestamp keys.

## Migration Plan

Existing configurations using `field`/`field_name` and `tag`/`tag_name` continue to deserialize. Configurations that attach a codec to `influxdb` must remove it. The corrected example can be copied directly into YAML. Rollback is a normal binary rollback; no storage migration is required.
