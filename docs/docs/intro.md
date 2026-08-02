---
sidebar_position: 1
---

# Introduction

![ArkFlow logo](./logo.svg)

ArkFlow is a high-performance stream processing engine written in Rust on the
Tokio async runtime. It ingests data from many sources (Kafka, MQTT, HTTP,
files, SQL, Pulsar, NATS, Redis, Modbus, WebSocket…), transforms it with SQL,
VRL, Python UDFs, JSON/Protobuf codecs, and windowed joins, then writes to one
or more sinks — all driven by a single YAML file.

## Core features

- **High performance** — Rust + Tokio, columnar [Apache Arrow](https://arrow.apache.org/) data, multi-threaded pipelines.
- **Durable delivery** — at-least-once by default via per-stream WAL durability; opt-in exactly-once for transactional sinks. See [Delivery semantics](./concepts/4-delivery-semantics.md).
- **Many sources & sinks** — Kafka, MQTT, HTTP, files (with S3/GCS/Azure/HDFS), Pulsar, NATS, Redis, SQL, Modbus, WebSocket, InfluxDB, and more.
- **Powerful processing** — SQL (DataFusion), VRL, Python UDFs, JSON, Protobuf, batching, windowing, and multi-source joins.
- **Streaming codecs** — JSON, Protobuf, Debezium CDC envelopes, and Confluent Schema Registry wire-format.
- **Control plane** — an optional Hub and console to observe, configure, and operate many ArkFlow nodes as a fleet.
- **Extensible** — a uniform plugin model for inputs, outputs, processors, buffers, and codecs.

## Next steps

- [Getting started](./getting-started/2-quickstart.md) — install and run your first pipeline in minutes.
- [Concepts](./concepts/1-architecture.md) — how the engine, streams, pipelines, backpressure, and metadata fit together.
- [Configuration reference](./configuration/1-top-level.md) — the top-level YAML structure.
- [Components](./category/components) — every input, output, processor, buffer, and codec.
- [SQL reference](./sql/2-select.md) — query syntax and functions.
- [Control plane](./control-plane/1-overview.md) — operate ArkFlow as a fleet.
