---
sidebar_position: 6
description: 部署、观测、排查并运维 ArkFlow。
---

# 运维 ArkFlow

本节面向负责可靠流式负载的运维人员。

## 运行

- [在 Kubernetes 上部署](./kubernetes.md)
- [灰度发布变更](./rollout.md)

## 控制平面

以机群(fleet)方式运维多个节点:

- [控制平面概览](./control-plane/overview.md) —— Hub、Agent 与期望状态(desired-state)模型
- [控制平面部署](./control-plane/deploy.md)
- [Web 控制台](./control-plane/console.md)
- [控制平面运维](./control-plane/operations.md)
- [调和与灰度发布](./control-plane/reconciliation.md)
- [HTTP API 参考](/zh-Hans/docs/reference/api)

## 保持健康

- [可观测性](./observability.md) —— 指标、事件、日志与告警
- [恢复手册](./recovery.md) —— WAL 重放、检查点与回滚

## 背景阅读

在调整吞吐或并发之前,请先阅读
[背压与有序投递](/zh-Hans/docs/build/backpressure)、
[WAL 优化](/zh-Hans/docs/build/wal)与
[投递语义](/zh-Hans/docs/build/delivery-semantics)。
