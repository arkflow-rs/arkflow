---
slug: recipes
title: Recipes
description: Task-focused recipes — guided how-tos and end-to-end cases, each backed by a runnable example YAML from the repository.
sidebar_position: 8
---

# Recipes

Task-oriented guides for the things you actually build. Every recipe ships a
runnable YAML under `examples/` in the repository, and every one of those is
deep-validated by CI — the config you read is the config that runs.

## How-tos

1. [Consume Kafka and write to SQL](./recipes/1-kafka-to-sql.md)
2. [Ingest CDC with Debezium](./recipes/2-cdc-debezium.md)
3. [Aggregate in windows](./recipes/3-windowed-aggregation.md)
4. [Ingest over HTTP](./recipes/4-http-ingestion.md)

## End-to-end cases

- [Durable webhook collection](./recipes/10-case-webhook-durable.md)
- [Order stream into MySQL](./recipes/11-case-order-stream-sql.md)
- [IoT telemetry with windowed aggregation](./recipes/12-case-telemetry-windows.md)

Control-plane rollout is covered under
[Operate → Rollout](../operate/rollout.md).
