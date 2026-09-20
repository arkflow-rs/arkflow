---
title: 控制平面
description: 使用 Hub 运维 ArkFlow 节点机群——部署、Web 控制台、灰度发布编排与恢复。
sidebar_position: 5
---

# 控制平面

控制平面把一组 ArkFlow 节点变成一个可运维的机群:**Hub** 持有期望状态并中转命令,
**Agent** 运行作业分片并上报观测结果,**Web 控制台**为运维人员提供统一视图。

1. [概览](./control-plane/overview.md) —— 架构与期望状态(desired-state)模型。
2. [部署](./control-plane/deploy.md) —— 运行 Hub、Agent 与控制台。
3. [Web 控制台](./control-plane/console.md) —— 各个控制台视图导览。
4. [运维](./control-plane/operations.md) —— 上线后的运维工作流。
5. [调和与灰度发布](./control-plane/reconciliation.md) —— 变更如何安全传播。

控制台的每个操作都对应一条有文档的 HTTP 路由——见
[API 参考](/zh-Hans/docs/reference/api)。
