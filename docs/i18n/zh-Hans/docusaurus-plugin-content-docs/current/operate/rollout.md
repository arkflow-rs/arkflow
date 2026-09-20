---
sidebar_position: 5
description: 通过控制平面 Hub 将流处理作业灰度发布到机群。
---

# 操作指南:通过控制平面灰度发布作业

把你在本地调试好的同一份声明式作业,运行到由控制平面 Hub 管理的计算节点机群上。

**前提条件**

- 已[安装](/zh-Hans/docs/get-started/install) ArkFlow
- 了解[作业配置结构](/zh-Hans/docs/build/jobs)——从本地作业示例 `examples/jobs_local.yaml` 开始

## 组成部分

```
┌─────────┐   submit    ┌─────┐   assign    ┌───────┐   run   ┌──────┐
│  You    │────────────▶│ Hub │────────────▶│ Agent │────────▶│ Kernel│
└─────────┘             └─────┘             └───────┘         └──────┘
```

- **Hub** —— 控制平面服务端。存储期望的作业状态、为节点发放租约、分配作业并执行调和。
- **Agent** —— 运行在每个计算节点上。接收分配并驱动统一执行内核。
- 本地校验通过的作业在机群上原样运行,因为两条路径使用的是同一个 `JobSpec`。

## 1. 在本地调试作业

```bash
./target/release/arkflow --config examples/jobs_local.yaml --validate
./target/release/arkflow --config examples/jobs_local.yaml
```

预期结果:校验通过且作业在本地运行——这正是 Hub 将分发的那个 `JobSpec`。

## 2. 启动 Hub 并注册 Agent

`examples/control_plane_hub.yaml` 配置了一个 Hub 及其 API 与健康检查端点;`examples/control_plane_example.yaml`
展示了 Agent 侧的部署配置:

```bash
# 终端 1 —— Hub
./target/release/arkflow --config examples/control_plane_hub.yaml

# 终端 2+ —— 每个计算节点一个 Agent
./target/release/arkflow --config examples/control_plane_example.yaml
```

预期结果:每个 Agent 都会作为持有租约的节点出现在 Hub 的机群 API(`/api/v1`)中。
失联的节点仍会出现在机群中,但在租约刷新之前不再接收新的分配。

## 3. 提交作业并验证

通过控制平面 HTTP API 提交作业规格。期望状态由 Hub 存储;Agent 领取分配、通过内核运行作业并上报观测状态。
可以通过杀掉一个 Agent 来观察调和过程:Hub 会注意到过期的租约,并把作业重新分配到健康节点。

运维细节——部署、定向到特定节点以及完整的 HTTP API——见
[控制平面概览](./control-plane/overview.md)与[运维指南](./control-plane/operations.md)。

## 故障排查

- **作业一直处于 pending** —— 没有健康的 Agent 持有租约;检查 Agent 日志与机群列表。
- **更新时版本冲突** —— 作业版本是单调递增的;提交时请将 `version` 递增。
