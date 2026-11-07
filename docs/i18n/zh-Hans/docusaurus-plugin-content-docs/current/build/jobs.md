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
- 多输入顶点跨输入聚合水位线:超过 5 分钟未发出水位线的输入被视为空闲并从聚合中排除——静默输入(例如混入事件时间扇入的处理时间源)不会再永久冻结窗口触发与状态清理。空闲输入恢复上报且水位落后时会被钳制到单调递增的下游前沿;其间迟到行按所配置的迟到策略处置。
- 事件时间门(event-time gate)等待窗口开启而持有的行数有上界(每个 gate 1,048,576 行)。水位线停顿时若超出上界,作为防无界增长的最后兜底,最旧的持有投递会被中止(abort),并以节流告警记录累计驱逐规模。语义:abort 回滚的是整个 fan-out 投递——仍被下游持有的兄弟确认可能在停顿解除后失败,导致任务失败并按 at-least-once 重启;重启后重放的行落后于已恢复水位,按既有 late 策略处置(Drop 策略即丢弃)。
- `resources`(可选)声明每 task 请求——`cpu_millicores` 与/或 `memory_bytes`。作业总请求 = 每 task 值 × 计划任务数。声明的作业参与放置记账:候选节点按**有效**余量排序(上报仪表减去已放置的声明占用),且放置仅在 (已分配 + 分摊) 装得下节点容量时成立(CPU 不超过 `node_cpu_cores × 1000` 毫核;内存不超过 `node_memory_total_bytes` 的 90%)——全无可行节点时给出显式的容量不足错误而不是盲目堆叠。声明 `cpu_millicores` 的作业还会在 `ceil(毫核/1000)`(至少 1)个工作线程的专属 runtime 上执行,单个作业无法占满 Agent 的共享 worker。这是 worker 级隔离,不是 cgroup/容器式硬限制:多作业仍共享 Agent 进程。未声明(默认)的作业行为逐位不变。
- `rescale: true`(默认 `false`)显式选择扩缩容恢复:在变更了 `parallelism`(或保存状态中的算子在新计划中全部保留的任务集)的情况下重启,按键组归属重分布 keyed 状态,而不是保守失败。参见[分布式 Job → 恢复期扩缩容](/zh-Hans/docs/build/distributed-jobs#恢复期扩缩容)。

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
ARKFLOW_NODE_TOKEN=replace-with-node-token ./target/release/arkflow-server  # the Hub
./target/release/arkflow --config examples/control_plane_node.yaml  # node-a; repeat with node-b
```

每个节点配置设置 `health_check.hub_urls`(列表——HA 部署中列出全部 Hub 实例,节点即可故障转移到当选 leader)、`node_id`、`node_token` 与 `agent_lease_ttl_ms`;节点向 Hub 注册并上报心跳与观测状态。将要参与 `split` 放置的节点(见[哪些场景能扩展,以及如何扩展](#what-scales--and-how))还要设置 `health_check.data_port`(启用 shuffle 数据面监听)与 `health_check.data_host`(对等节点可路由的地址),节点会以此宣告其 `network_shuffle` 能力。

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

   `PUT desired-state` 内部执行世代比较并交换(generation CAS):过旧的世代会被拒绝,
   而不是静默覆盖更新的意图。与流的 desired-state 路由不同,作业路由并未实现
   `If-Match` / `Idempotency-Key` 请求头(见
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

### 暂停与恢复 {#pause-and-resume}

停止作业再启动时会从最近一个已完成的恢复工件恢复,因此该工件之后处理的数据会按源位点重放。**暂停**是面向操作员的「冻结、稍后继续」操作:Hub 先在暂停世代上落一个恢复工件,然后停止作业;恢复(resume)时直接从该工件起跑,操作员无需指定检查点。

```bash
# 暂停:先落工件,再停机。返回 202 与作业记录;在所有目标节点观测到停机之前,
# 作业保持 `reconciling`。
curl -s -H "$A" -X POST $H/jobs/sensor-window-job/actions/pause

# 恢复:提升世代,并从暂停工件恢复。
curl -s -H "$A" -X POST $H/jobs/sensor-window-job/actions/resume

# desired-state 资源同样接受第三个取值。
curl -s -H "$A" -X PUT $H/jobs/sensor-window-job/desired-state \
  -d '{"state":"paused"}'
```

只有期望态与观测态均为 `running` 且已收敛、没有进行中的升级编排、也没有在途暂停的作业才会被接受暂停。创建作业时仍只接受 `running` 与 `stopped`;`paused` 是生命周期取值,不是初始状态。

暂停的执行序列与边界:

| 边界 | 行为 |
| --- | --- |
| 落工件 | 工件类型跟随 `spec.recovery`:`latest_checkpoint` 落检查点(暂停期间被保留期 pin 住,不会被回收),`latest_savepoint` 落保存点。`recovery: fail` 没有任何可选工件,暂停会被 `pause_requires_recovery_policy` 拒绝。 |
| 顺序 | 只有工件完成之后才派发停机。规格中没有 `checkpoint` 配置的作业跳过落工件,直接停机;其恢复等价于冷启动。 |
| 收敛 | `desired_state = paused` 只在 `pause_phase = paused` 且所有目标分配都被观测为 `stopped` 时才算收敛——收敛由相位定义,不做字符串相等。相位处于 `capturing` 或 `stopping` 时作业报告 `reconciling`。 |
| 离线节点 | 停机命令到不了离线节点,暂停会保持 `reconciling`(意图是持久的),节点回归后继续派发。操作员也可以 `stop` 作业,把它落回 stopped 并清除暂停记账。 |
| 失败 | 工件失败或五分钟的落工件超时(Hub 重启后允许有界的重派轮次)会中止暂停:作业回到 `desired_state = running`,恢复暂停前的恢复指针,并在 `last_error` 写入 `pause_savepoint_failed` 或 `pause_savepoint_timeout`。 |
| 恢复 | `resume` 只在 `paused` 相位被接受:提升世代,恢复指针保持指向暂停工件,并走普通恢复启动;只有作业收敛到观测 `running` 后相位才被清除。 |
| 重放窗口 | 暂停工件屏障之后、teardown 完成之前处理的事件会在 resume 时重放:事务型 sink 精确一次,非事务型 sink 至少一次。没有 checkpoint 配置的作业没有工件,resume 即冷启动。 |
| 升级 | 暂停相位存在时拒绝升级(`job_paused`);升级编排活跃时拒绝暂停(`orchestration_in_progress`)。参见[原子作业升级 → 暂停:编排暂停与作业暂停](/zh-Hans/docs/operate/control-plane/job-upgrades#pause-orchestration-vs-job)。 |

暂停与恢复要求 Hub 配置了持久化存储:volatile Hub 返回 `503 pause_requires_durable_storage`。作业记录暴露增量字段 `pause_phase`、`pause_artifact_id`、`pause_prev_checkpoint_id`、`pause_retry_count`、`paused_at_ms` 与 `paused_by`;相位转移也会以 `job.pause_requested`、`job.paused`、`job.resume_requested`、`job.resumed`、`job.pause_failed` 出现在 `GET /events/stream` 上。作业处于暂停态时,`start` 与 `restart` 等价于 `resume`,`stop` 则把它落回 stopped。路由与错误码参考:[HTTP API → 作业暂停与恢复](/zh-Hans/docs/reference/api#job-pause-and-resume)。

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
- [ ] 暂停/恢复已演练:暂停工件类型与 `spec.recovery` 一致,且重放窗口(工件屏障 → teardown)对非事务型 sink 可接受。
- [ ] 升级路径已演练:保存点 → 停止 → 升级 → 验证 →(回滚)。
