---
sidebar_position: 1
description: Start with ArkFlow and build your first stream pipeline.
---

# Start here

ArkFlow is a Rust stream-processing engine for connecting inputs, transformations, buffers, and outputs.

## Choose your path

- **New to ArkFlow:** follow the [tutorials](#) — [install](getting-started/1-install.md), the [quickstart](getting-started/2-quickstart.md), then a [durable pipeline](tutorials/1-durable-pipeline.md).
- **Solving a specific task:** pick a [how-to guide](how-to/1-kafka-to-sql.md) — Kafka→SQL, CDC with Debezium, windowed aggregation, HTTP ingestion, control-plane rollout.
- **Browsing end-to-end scenarios:** read the [cases](cases/1-webhook-durable.md), each backed by a CI-validated example configuration.
- **Building a pipeline:** read [configuration basics](configuration/1-top-level.md), then choose an [input](components/0-inputs/kafka.md) and [output](components/3-outputs/stdout.md).
- **Operating in production:** start with [architecture](concepts/1-architecture.md), [delivery semantics](concepts/4-delivery-semantics.md), and [deployment](deploy/k8s-deployment.md).
- **Running a fleet:** use the [control plane overview](control-plane/1-overview.md) and [operations guide](control-plane/3-operations.md).
- **Contributing:** read the [documentation contributor guide](contribute).

## What to learn next

Understand the [data flow](concepts/1-architecture.md), [backpressure and ordering](concepts/2-backpressure-ordering.md), and [durability model](concepts/5-wal-optimization.md) before tuning a production deployment.
