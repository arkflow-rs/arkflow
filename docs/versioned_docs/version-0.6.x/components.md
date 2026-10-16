---
slug: components
title: Components
description: Inputs, outputs, processors, buffers, codecs, and temporary storage components available in ArkFlow streams.
sidebar_position: 5
---

# Components

ArkFlow ships 41 registered components across six kinds. Every page in this
section is machine-checked against the live component registry: the type
names, configuration schemas, and examples you see here cannot silently
drift from the binary.

<div class="row af-cards">
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/inputs/kafka">
      <div class="card__header"><h3>Inputs</h3><span class="af-badge af-badge--feature">input</span></div>
      <div class="card__body">Consume from Kafka, HTTP, MQTT, NATS, Pulsar, Redis, SQL, files, WebSockets, Modbus, and more.</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/buffers/tumbling_window">
      <div class="card__header"><h3>Buffers</h3><span class="af-badge af-badge--feature">buffer</span></div>
      <div class="card__body">Tumbling, sliding, and session windows — event-time aware and columnar.</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/processors/sql">
      <div class="card__header"><h3>Processors</h3><span class="af-badge af-badge--feature">processor</span></div>
      <div class="card__body">Transform with SQL, Python UDFs, VRL, Protobuf conversion, and batching.</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/outputs/sql">
      <div class="card__header"><h3>Outputs</h3><span class="af-badge af-badge--feature">output</span></div>
      <div class="card__body">Deliver to SQL databases, Kafka (with exactly-once), HTTP, Redis, InfluxDB, MongoDB, and more.</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/codecs/json">
      <div class="card__header"><h3>Codecs</h3><span class="af-badge af-badge--feature">codec</span></div>
      <div class="card__body">Decode wire formats: JSON, Protobuf, Debezium CDC events, and Schema Registry payloads.</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/docs/components/temporary/redis">
      <div class="card__header"><h3>Temporary</h3><span class="af-badge af-badge--feature">temporary</span></div>
      <div class="card__body">Lookup tables for enrichment inside pipelines, backed by Redis.</div>
    </a>
  </div>
</div>

## Discovering components from the CLI

The registry is also queryable from the binary — see the
[CLI reference](/docs/reference/cli):

```bash
arkflow components list             # everything, grouped by kind
arkflow components list --kind codec --format json
arkflow components show input kafka # one component's schema + example
```

The generated [component inventory](/docs/reference/component-inventory) is
the same export, rendered as a table.
