---
sidebar_position: 2
description: Ingest database change events with Debezium, JSON or Schema Registry encoded.
---

# How to: ingest CDC with Debezium

Stream row-level changes from your database into ArkFlow using
[Debezium](https://debezium.io/): Debezium writes change events to Kafka,
and ArkFlow decodes each envelope into a columnar batch you can query with
SQL.

**Prerequisites**

- A Debezium Kafka Connect connector publishing to a topic such as
  `shop.users` (see the [Debezium connector docs](https://debezium.io/documentation/))
- Kafka reachable at `localhost:9092`
- Optionally, a Confluent Schema Registry at `localhost:8081` (Protobuf
  messages only)

## JSON-encoded envelopes

The pipeline is a Kafka input with the `debezium_json` **input codec**, so
envelopes are decoded before the pipeline runs. A complete, CI-validated
configuration ships as
[`examples/cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml):

```yaml
streams:
  - input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - shop.users
      consumer_group: arkflow-cdc
      start_from_latest: true
      codec:
        type: debezium_json

    pipeline:
      thread_num: 4
      processors:
        - type: sql
          query: |
            SELECT op, id, name, source_db, source_table, ts_ms FROM flow

    output:
      type: stdout
```

The decoded batch contains the business columns (from `after`, or `before`
on deletes) plus `op`, `ts_ms`, `source_db`, `source_table`, `before`, and
`source`.

## Protobuf envelopes via Schema Registry

When Debezium is configured with the Confluent Protobuf converter, use the
`schema_registry` codec instead — validated as
`examples/howto_cdc_schema_registry.yaml`:

```yaml
      codec:
        type: schema_registry
        registry_url: "http://localhost:8081"
        message_type: "shop.Users"
```

## Run and verify

```bash
./target/release/arkflow --config cdc.yaml
```

Update a row in the source database. Expected result: an `op: u` (update)
record with the new column values printed on stdout; a delete produces
`op: d` with values from `before`.

## Delivery semantics

The CDC offset is the Kafka input's ack-gated offset — **at-least-once**.
After a crash an event may be replayed, so keep downstream idempotent (for
example, UPSERT keyed on the primary key). See
[delivery semantics](../concepts/4-delivery-semantics.md).
