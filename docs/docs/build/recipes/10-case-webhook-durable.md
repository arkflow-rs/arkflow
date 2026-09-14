---
sidebar_position: 10
description: End-to-end case — collect third-party webhooks durably and forward them to Kafka.
---

# Case: durable webhook collection

A SaaS platform receives webhooks from third-party providers (payments, CRM
events, CI notifications). Providers retry on 5xx but deliver exactly once
per attempt and keep no replay log — so **losing a message is permanent**.
The platform forwards normalized events onto a Kafka topic consumed by many
downstream services.

## Requirements

- Every accepted webhook must survive a process crash
- A malformed webhook must not block the stream
- At-least-once delivery is acceptable; downstream deduplicates by webhook id

## Architecture

```
┌──────────┐  POST   ┌──────────┐  persist  ┌─────┐   process   ┌───────┐
│ Webhooks │────────▶│  HTTP    │──────────▶│ WAL │────────────▶│ Kafka │
│ providers│         │  input   │  first    └─────┘             │ sink  │
└──────────┘         └──────────┘                               └───────┘
                                                  ack only after Kafka confirms
```

The load-bearing decision: HTTP is **not replayable**, so the WAL
(`durability:` block) sits between the input and the pipeline. The input is
acknowledged only after Kafka confirms the produce.

## Configuration

Validated example: [`examples/case_webhook_durable.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_webhook_durable.yaml)

```yaml
streams:
  - id: webhooks-to-kafka
    input:
      type: http
      address: "0.0.0.0:8080"
      path: "/webhooks"

    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: kafka
      brokers:
        - localhost:9092
      topic:
        type: value
        value: webhooks.raw
      client_id: arkflow-webhooks

    error_output:
      type: stdout
```

## Run and expected outcome

```bash
./target/release/arkflow --config examples/case_webhook_durable.yaml --validate
./target/release/arkflow --config examples/case_webhook_durable.yaml

# simulate a provider
curl -X POST http://localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"webhook_id": "wh-1", "event": "invoice.paid"}'
```

- The message appears on the Kafka topic `webhooks.raw`.
- `kill -9` the engine, restart it: unacknowledged webhooks replay from the
  WAL and reach Kafka — nothing accepted is lost.
- A non-JSON POST routes to the error output; the stream keeps serving.

## Trade-offs and variations

- `sync: per_entry` instead of `group_commit` trades throughput for a
  narrower loss window (see [WAL optimization](../wal.md)).
- Swap the SQL processor for [VRL](../../components/2-processors/vrl.md) to
  redact fields before forwarding.
- Exactly-once on the Kafka side is available with transactional output —
  see [exactly-once](../exactly-once.md).
