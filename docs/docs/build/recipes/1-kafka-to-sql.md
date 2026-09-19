---
sidebar_position: 1
description: Consume JSON messages from Kafka and append them into a SQL database.
---

# How to: consume Kafka and write to SQL

Move JSON events from a Kafka topic into a MySQL (or PostgreSQL/SQLite) table
with one ArkFlow stream.

**Prerequisites**

- Kafka reachable at `localhost:9092` with a topic `app.events` containing
  JSON messages
- MySQL reachable with database `arkflow` and a table that matches your event
  columns (ArkFlow appends rows; it does not create tables)
- ArkFlow [installed](/docs/get-started/install)

## Configuration

Save as `kafka-to-sql.yaml` (also validated in CI as
`examples/case_order_stream_sql.yaml`):

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - id: events-to-mysql
    input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - app.events
      consumer_group: arkflow-events
      start_from_latest: true

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: sql
      output_type:
        type: "mysql"
        uri: "mysql://user:password@localhost:3306/arkflow"
      table_name: "events"

    error_output:
      type: stdout
```

The processing chain is always the same three steps: **decode** the JSON
payload into Arrow (`json_to_arrow`), optionally **transform** with SQL, and
**re-encode** (`arrow_to_json`) for the sink. The `error_output` catches
malformed messages so one bad record does not stop the stream.

## Run and verify

```bash
./target/release/arkflow --config kafka-to-sql.yaml
```

Produce a test message:

```bash
echo '{"order_id": 1, "amount": 42}' | kafka-console-producer \
  --bootstrap-server localhost:9092 --topic app.events
```

Expected result: a row with `order_id = 1` appears in `arkflow.events`.
Malformed JSON (try producing `not-json`) lands in the engine log via the
error output instead of stopping the stream.

## Troubleshooting

- **Nothing arrives** — check the consumer group has no other active member
  holding the partitions, and that `start_from_latest` is not skipping
  existing messages.
- **Connection refused on MySQL** — the URI is
  `mysql://user:password@host:port/database`; confirm reachability with any
  SQL client first.
- For delivery guarantees when sinks fail, see
  [delivery semantics](../delivery-semantics.md).
