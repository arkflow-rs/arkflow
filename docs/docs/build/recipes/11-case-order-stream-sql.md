---
sidebar_position: 11
description: End-to-end case — shop order events from Kafka filtered and appended into MySQL.
---

# Case: order stream into MySQL

An online shop emits order events (created, paid, shipped) as JSON on the
Kafka topic `shop.orders`. An analytics team wants paid orders appended to a
MySQL table for BI dashboards. Test events (`status != 'PAID'`) and malformed
messages must never reach the table — or stop the pipeline.

## Requirements

- Filter events before they reach the database
- Bad messages land in an error path instead of failing the stream
- Rows land in near real time; duplicates after a crash are tolerable
  (the sink table is keyed and upserted downstream)

## Architecture

```
┌──────────┐  JSON   ┌───────────┐   filter    ┌───────────┐  append  ┌───────┐
│  Shop    │────────▶│   Kafka   │────────────▶│  ArkFlow  │─────────▶│ MySQL │
│ services │         │  topic    │  + decode   │ sql + json│          │ table │
└──────────┘         └───────────┘             └───────────┘          └───────┘
                                                        │ malformed
                                                        ▼
                                                   ┌────────┐
                                                   │ stdout │  (error path)
                                                   └────────┘
```

## Configuration

Validated example: [`examples/case_order_stream_sql.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_order_stream_sql.yaml)

```yaml
streams:
  - id: orders-to-mysql
    input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - shop.orders
      consumer_group: arkflow-orders
      start_from_latest: true

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow WHERE status = 'PAID'"
        - type: arrow_to_json

    output:
      type: sql
      output_type:
        type: "mysql"
        uri: "mysql://root:1234@localhost:3306/arkflow"
      table_name: "orders"

    error_output:
      type: stdout
```

## Run and expected outcome

```bash
./target/release/arkflow --config examples/case_order_stream_sql.yaml --validate
./target/release/arkflow --config examples/case_order_stream_sql.yaml
```

Produce `{"order_id": 7, "status": "PAID", "amount": 42}` to `shop.orders`:
a row with `order_id = 7` appears in `arkflow.orders`. Produce
`{"order_id": 8, "status": "TEST"}`: no row appears (filtered). Produce
garbage: nothing stops; the record is logged via the error output.

## Trade-offs and variations

- Add a [window buffer](./3-windowed-aggregation.md) before the sink
  to batch inserts and cut database round-trips.
- The same pipeline targets PostgreSQL or SQLite — change only
  `output_type` (see [SQL output reference](../../components/3-outputs/sql.md)).
- For strict no-duplicate ingestion, use the transactional Kafka
  [exactly-once](../exactly-once.md) output plus an UPSERT sink.
