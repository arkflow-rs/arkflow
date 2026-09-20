---
slug: recipes
title: 实战配方
description: 面向任务的实战配方——分步操作指南与端到端案例,每一篇都配有仓库中可直接运行的示例 YAML。
sidebar_position: 8
---

# 实战配方

实战配方(Recipe)是面向实际构建任务的指南。每篇实战配方都在仓库的 `examples/` 目录下附带一份可直接运行的 YAML,且每份示例都经过 CI 深度校验——你读到的配置就是实际运行的配置。

## 操作指南

1. [消费 Kafka 并写入 SQL](./recipes/1-kafka-to-sql.md)
2. [通过 Debezium 采集变更数据捕获(CDC)数据](./recipes/2-cdc-debezium.md)
3. [在窗口中聚合](./recipes/3-windowed-aggregation.md)
4. [通过 HTTP 采集数据](./recipes/4-http-ingestion.md)

## 端到端案例

- [持久化的 Webhook 采集](./recipes/10-case-webhook-durable.md)
- [订单流写入 MySQL](./recipes/11-case-order-stream-sql.md)
- [IoT 遥测与窗口聚合](./recipes/12-case-telemetry-windows.md)

控制平面的灰度发布(rollout)参见[运维 → 灰度发布](/zh-Hans/docs/operate/rollout)。
