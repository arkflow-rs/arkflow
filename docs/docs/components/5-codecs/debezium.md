---
sidebar_label: Debezium JSON
---

# Debezium JSON

The `debezium_json` codec decodes Debezium CDC (Change Data Capture) Envelope JSON into a columnar Arrow `MessageBatch`. Attach it to a Kafka input that consumes a topic written by Debezium to turn database change events (`c`/`u`/`d`/`r`) into queryable rows. CDC offset is **not** managed here — it is the Kafka input's ack-gated offset.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: codec-debezium_json-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| — | object | no | — | no | No additional configuration fields. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - shop.users
  consumer_group: arkflow-cdc
  codec:
    type: debezium_json
```

Deployment topology:

```
Database → Debezium (Kafka Connect / Debezium Server) → Kafka topic → ArkFlow (kafka input + debezium_json codec)
```

See `examples/cdc_debezium.yaml`.

## Semantics

### Output schema

Each Debezium Envelope `{ before, after, op, source, ts_ms }` is flattened into one row:

| Column | Source | Notes |
| --- | --- | --- |
| `<business fields>` | `after` (falls back to `before` on deletes) | Promoted to top-level columns |
| `op` | `op` | `c` / `u` / `d` / `r` |
| `ts_ms` | `ts_ms` | Change timestamp |
| `source_db`, `source_table` | `source.db`, `source.table` | Top-level scalar columns |
| `before` | Full `before` object | JSON text column (parse with SQL JSON functions) |
| `source` | Full `source` object | JSON text column |

`before` / `source` are kept as JSON text (rather than nested structs) because mixing nulls and objects within the same batch (e.g. an insert's `before` is null while an update's is an object) would conflict with the Arrow JSON reader's single-pass schema inference.

### Delivery semantics

- The CDC offset is provided by the Kafka input's ack-gated offset (at-least-once). Ensure downstream sinks are idempotent.
- When `op="d"`, `after` is null and business fields are taken from `before`.

## Notes / Non-goals

- Does not connect directly to MySQL binlog / PostgreSQL logical replication (planned as a separate input in the future).
- Only Debezium JSON is supported; Avro / Protobuf formats are not supported yet (Avro requires a Schema Registry).

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
