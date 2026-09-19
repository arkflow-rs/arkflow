---
sidebar_position: 3
description: 编写并运行流式作业——从本地 YAML 作业到由 Hub 管理的分布式部署。
---

# 流式作业

每一个 ArkFlow 工作负载都运行在同一个统一执行内核上,该内核提供带背压的流水线并行、异步屏障检查点、事件时间 / 水位线处理、键控状态以及列式窗口算子。你通过三个配置层级使用这个内核:

| 模式 | 在哪里声明 | 在哪里运行 | 适用场景 |
| --- | --- | --- | --- |
| 流(Stream) | `streams:` | 单进程 | 线性流水线与既有配置;加 `durability` 获得抗崩溃能力。 |
| 本地作业 | `jobs:` | 单进程 | 需要显式时间、状态、检查点与恢复设置的有状态 DAG 作业。 |
| 分布式作业 | Hub API / 控制台 | 多个计算节点 | 水平扩展、保存点、集中式运维。 |

三者共享同一份作业规格,因此你在本地调试的作业可以原样提交到 Hub。流与作业编译进同一个内核——运行时契约见[分布式作业](/zh-Hans/docs/build/distributed-jobs)。

## 编写本地作业

作业是一个有向无环图:`operators` 通过 `edges` 连接,组件化的 `sources` 与 `sinks` 挂载在图中的 `source` 与 `sink` 算子上:

```yaml validate=full
streams: []
jobs:
  - id: sensor-window-job
    version: 1
    parallelism: 1
    max_parallelism: 128

    operators:
      - id: source
        kind: source
      - id: sink
        kind: sink
    edges:
      - id: e1
        from: source
        to: sink
        partitioned: true

    sources:
      - operator_id: source
        input_type: generate
        config:
          type: generate
          context: '{ "sensor": "temp_1", "value": 10, "ts": 1757000000000 }'
          interval: 100ms
          batch_size: 10
        time:
          mode: event_time
          timestamp_field: ts
          watermark:
            strategy: bounded_out_of_orderness
            out_of_orderness_ms: 2000
            idle_timeout_ms: 60000
          allowed_lateness_ms: 5000
          late_event_policy: update

    sinks:
      - operator_id: sink
        output_type: stdout

    state:
      backend: embedded_kv
      durability: durable
      root: ./data/arkflow-state
      ttl_ms: 3600000
      format_version: 1
    checkpoint:
      interval_ms: 30000
      retention: 3
      object_store_uri: "file://./data/checkpoints"
    recovery: latest_checkpoint
```

该示例以 [`examples/jobs_local.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/jobs_local.yaml) 维护。
逐字段参考:[顶层配置 → job](/zh-Hans/docs/reference/configuration#job)。

要点:

- `parallelism`(默认 `1`)设置任务并行度;`max_parallelism`(默认 `128`)限定 `partitioned` 边所使用的键组空间。
- `partitioned: true` 按键组归属路由,因此同一个 key 总会到达同一个下游任务,无论它来自哪个源分区或上游子任务。
- 算子 `kind` 是 `source`、`map`、`filter`、`aggregate`、`window`、`sink` 或 `udf` 之一。`join` 在公开结构中保留,但在专门的分布式多输入运行时出现之前会被拒绝。
- 事件时间源需声明 `mode`、`timestamp_field`、水位线参数与迟到事件策略(`drop`、`route` 或 `update`)。

### 校验并运行

校验执行与启动时相同的深度构建检查——未知组件、不支持的状态后端与非法的图边都会在任何东西运行之前报错:

```bash
./target/release/arkflow --config jobs.yaml --validate
./target/release/arkflow --config jobs.yaml
```

`./target/release/arkflow schema` 输出整个配置(包括所有 `jobs` 字段)的 JSON Schema,可用于编辑器补全。

## 将作业分布到多个节点

一个 Hub 加两个计算节点,就足以让作业跑在多台机器上。从仓库维护的示例开始:

```bash
./target/release/arkflow --config examples/control_plane_hub.yaml   # the Hub
./target/release/arkflow --config examples/control_plane_node.yaml  # node-a; repeat with node-b
```

每个节点配置设置 `health_check.hub_url`、`node_id`、`node_token` 与 `agent_lease_ttl_ms`;节点向 Hub 注册并上报心跳与观测状态。将要参与 `split` 放置的节点(见[哪些场景能扩展,以及如何扩展](#what-scales--and-how))还要设置 `health_check.data_port`(启用 shuffle 数据面监听)与 `health_check.data_host`(对等节点可路由的地址),节点会以此宣告其 `network_shuffle` 能力。

### 提交流程

所有路由都使用 Bearer 认证(Hub 上的 `health_check.api_token`):

```bash
H=http://127.0.0.1:8080/api/v1
A="Authorization: Bearer operator-secret"
```

1. **先校验。** 校验会针对目标节点构建真实的执行计划,在任何东西创建之前拒绝不支持的算子或状态后端:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/validate \
     -d '{"spec": <JobSpec>, "node_ids": ["node-a", "node-b"]}'
   ```

2. **以停止状态提交,检查,再运行。** 创建是一次期望状态写入,返回 `202 Accepted`:

   ```bash
   curl -s -H "$A" -X POST $H/jobs \
     -d '{"spec": <JobSpec>, "desired_state": "stopped"}'

   curl -s -H "$A" $H/jobs/sensor-window-job/plan     # explain the physical plan
   curl -s -H "$A" $H/jobs/sensor-window-job/detail   # assignments and convergence

   curl -s -H "$A" -X PUT $H/jobs/sensor-window-job/desired-state \
     -d '{"state":"running"}'
   ```

   `PUT desired-state` 支持与流相同的 `If-Match` 世代守卫和 `Idempotency-Key` 去重(见
   [HTTP API v1](/zh-Hans/docs/reference/api))。

3. **检查点与保存点。** 两者都会向作业的 `object_store_uri` 写入带校验和的恢复工件;检查点要求每个计划中的任务都参与同一个有效切面:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/checkpoints -d '{}'
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/savepoints  -d '{}'
   ```

4. **升级与回滚。** 升级要求作业已停止且已收敛,并选择一个状态格式兼容的已完成保存点;回滚则恢复上一个版本:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/upgrades -d '{"spec": <v2>}'

   curl -s -H "$A" -X POST \
     $H/jobs/sensor-window-job/upgrades/<upgrade_id>/rollback
   ```

控制台的 Job 工作台以可视化方式驱动同一流程:在编排器中构建 DAG、校验、对比版本、发布,然后在 Runtime 页面观察水位线延迟、检查点耗时与恢复进度。

### 失败与恢复

Hub 使用作业代数(job generation)对任务尝试做隔离(fencing):失联节点上的任务会被重新放置到其他节点,而失联节点恢复后会收到停止命令,而不是跑出一个重复任务。重启时,作业依据其 `recovery` 策略(`latest_checkpoint`、`latest_savepoint` 或 `fail`)选定的恢复工件恢复状态、源位置与各分区水位线。当恢复工件的状态格式与作业版本不兼容时,恢复会被提前拒绝,因此一次错误的还原不会破坏状态。杀进程再重启的恢复路径由 `crates/arkflow-server/tests/` 中的双节点冒烟测试端到端覆盖。

## 哪些场景能扩展,以及如何扩展 {#what-scales--and-how}

任务放置有两种模式,由作业的 `placement` 字段选择:

- **`colocated`(默认)** —— 一次分配绝不把一条边拆到两个节点上;相邻算子位于同一节点,算子的中间数据从不出境。它对源分区并行(例如 20 分区的 Kafka 主题由两个节点各消费一半)、相互独立的子任务以及机群中的大量作业扩展良好。需要跨整条流 shuffle 的计算应改经外部系统——例如,按 key 重新分区写入一个中间 Kafka 主题,再在其上运行第二个作业。
- **`split`(可选)** —— Hub 以确定性的计划顺序把物理任务轮询分配到目标节点,端点落在不同节点上的边成为 shuffle 数据面上的远程网络边:分区边按键组把记录路由到拥有该键组的子任务,使用与本地边相同的 FIFO/屏障/确认语义的有界 TCP 通道。旁路边(错误输出与迟到事件路由)必须保持同位共置,而且 Hub 只会把 `split` 放置派发给宣告了 `network_shuffle` 能力的节点(在每个参与节点上设置 `health_check.data_port` 与 `health_check.data_host`)——否则校验或派发会直接失败(fail closed)。从不设置这些字段的部署保持共置行为不变:没有数据面监听,没有能力宣告,放置结果完全一致。

无论哪种方式,运维模型都很轻:一个 Hub,N 个节点,数据面只存在于你选择启用的节点上。

## 检查清单

- [ ] 配置通过 `--validate`。
- [ ] `parallelism`/`max_parallelism` 与数据源的真实并行度匹配(对 Kafka 而言是分区数)。
- [ ] 有状态算子声明了 `state` 后端;`checkpoint` 指向持久化的 `object_store_uri`。
- [ ] 分布式作业在首次提交前已针对目标 `node_ids` 完成校验。
- [ ] 升级路径已演练:保存点 → 停止 → 升级 → 验证 →(回滚)。
