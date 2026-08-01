# debezium_json

`debezium_json` is a **codec** that decodes Debezium CDC (Change Data Capture) Envelope JSON into a columnar Arrow batch. Attach it to a Kafka input that consumes a topic written by Debezium to turn database change events into queryable rows.

## When to use

- Real-time database change synchronization from MySQL / PostgreSQL / MongoDB / SQLServer / ... via Debezium.
- CDC pipelines that need `op`-aware routing (`c` / `u` / `d` / `r`) in downstream SQL.

## Deployment shape

Debezium writes change events to Kafka; ArkFlow consumes them with this codec:

```
Database → Debezium (Kafka Connect / Debezium Server) → Kafka topic → ArkFlow (kafka input + debezium_json codec)
```

## Configuration

The codec takes no options today:

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

## Output schema

Each Debezium Envelope `{ before, after, op, source, ts_ms }` is flattened into a row:

| Column | Source | Notes |
| --- | --- | --- |
| `<business fields>` | `after` (or `before` for `op="d"`) | promoted to top-level columns |
| `op` | `op` | `c` / `u` / `d` / `r` |
| `ts_ms` | `ts_ms` | change timestamp |
| `source_db`, `source_table` | `source.db`, `source.table` | scalar top-level columns |
| `before` | full `before` object | JSON text column (use SQL JSON functions to inspect) |
| `source` | full `source` object | JSON text column |

`before` / `source` are kept as JSON text (rather than nested structs) so that a `null`-vs-object mix within a batch (e.g. `before` is null on inserts but an object on updates) decodes uniformly.

## Delivery semantics

CDC offset is **not** managed by this codec — it is the Kafka input's ack-gated offset (see `input-durability`), providing at-least-once. Make downstream sinks idempotent (end-to-end exactly-once is tracked as a separate change).

## Example

See `examples/cdc_debezium.yaml`.

## Non-goals

- MySQL binlog / PostgreSQL logical replication **direct** connection (future independent input).
- Debezium Avro / Protobuf formats (JSON only for now; Avro depends on Schema Registry).
