# Design: Unified Streaming Engine Kernel

## Context

两套运行时的执行层都将被统一内核替换。内核的形态对标 Flink 的
StreamElement/OperatorChain/CheckpointBarrier 模型，但数据面保持 ArkFlow 的
Arrow `MessageBatch` 列式批处理。

```
                 ┌─────────────────────────────────────────────┐
                 │            EngineConfig (YAML)              │
                 │  streams: [...]          jobs: [...]        │
                 └──────────┬──────────────────┬───────────────┘
                            │ compile          │ (直接 JobSpec)
                            ▼                  ▼
                 ┌─────────────────────────────────────────────┐
                 │              JobPlan::compile                │
                 │  (现有 job.rs，含任务/分区规划，不变)          │
                 └──────────────────┬──────────────────────────┘
                                    ▼
                 ┌─────────────────────────────────────────────┐
                 │      ExecutionGraphBuilder (新 executor/)    │
                 │  · 算子链融合（chain 内零通道）                │
                 │  · 通道边: bounded flume Envelope channel     │
                 │  · 每 vertex: TaskRunner 事件循环             │
                 └──────────────────┬──────────────────────────┘
              本地模式                │              Agent 模式
     (Engine 内嵌, 全图)              │        (本节点子图, Hub 派发)
                                    ▼
                 ┌─────────────────────────────────────────────┐
                 │           Unified Execution Kernel           │
                 │  source ─ch─▶ [map|filter|... ]─ch─▶ window  │
                 │        (chain)              (keyed state)   │
                 │                                    │        │
                 │            barrier/watermark 控制信封随数据流   │
                 └─────────────────────────────────────────────┘
```

## Goals / Non-Goals

Goals:
- source/算子/sink 流水线重叠执行（吞吐 = 各 vertex 并行度之和的能力）
- checkpoint 不停世界（异步 barrier 对齐）
- 列式窗口聚合算子成为一等公民（keyed state 落盘）
- 现有 YAML Stream 配置零改动跑在内核上
- 本地与 Agent 共用同一内核构建代码

Non-Goals（见 proposal）。

## Actual Design

### 1. Envelope 与通道

```rust
pub enum Envelope {
    Data(MessageBatchRef, Arc<dyn Ack>),   // ack 随批流动，链尾触发
    Barrier(CheckpointBarrier),            // 对齐后向上游 ack checkpoint 片段
    Watermark(i64),                        // 源驱动的事件时间推进
    EOS,                                   // 有界源结束
}
```

- 边是 `flume::bounded::<Envelope>(capacity)`（默认 1024，对齐现 BACKPRESSURE_THRESHOLD）。
- 反压靠 bounded channel 满则 await（天然传播，无需额外机制）。
- Ack 沿链传递：链内最后一个算子处理完 → 下游通道发送成功 → 该批的 ack
  在最终 sink 写成功后触发（沿用 Stream 运行时"output 成功才 ack"语义；
  kernel 在 sink vertex 写成功后调用 ack）。

### 2. 执行图与算子链融合

- `ExecutionGraph { vertices, edges }`，vertex = 链（1..n 个算子）。
- 融合规则（保守起步）：
  1. 单入单出、非 stateful、非 window、forward 分区的连续算子 → 同链；
  2. 链内共享一个事件循环，逐算子 `process` 直通，零通道开销；
  3. source 链只含 source；sink 链只含 sink（链尾写输出）。
- 保留 `partitioned` 边语义：key hash 路由到下游 subtask 通道（进程内多通道）。

### 3. TaskRunner 事件循环

每个 vertex 一个 task：

```
loop {
    select! {
        _ = cancellation.cancelled() => break,
        envelope = input_channel.recv() => match envelope {
            Data(batch, ack) => {
                for op in chain { batch = op.process(batch)? }   // 链内直通
                route_to_downstream(batch, ack).await?            // bounded, 反压
            }
            Barrier(b) => {
                对齐所有输入通道的 barrier;                          // 等待其它输入
                异步快照本链 keyed 状态 + 当前 source 位置;           // 不阻塞数据
                向下游转发 barrier; 向 coordinator 上报 TaskCheckpointAck
            }
            Watermark(w) => { 窗口算子尝试触发发射; 转发 min(w) }
            EOS => drain + 关闭下游
        }
    }
}
```

多输入 vertex（join/union）用 channel 聚合 select。

### 4. 异步 barrier checkpoint

- Coordinator（本地模式在 Engine 内，Agent 模式在 Agent 进程内）周期注入
  barrier 到所有 source。
- Barrier 对齐采用 Flink 经典对齐（v1 单输入链场景天然对齐；多输入链等待
  所有输入 barrier 到达，期间缓冲其它输入数据——有界内存）。
- 快照内容：该链的 keyed 状态（Arrow IPC bytes）+ source 位置 + watermark。
- 快照异步进行，数据继续流动；完成即上报 `TaskCheckpointAck`（复用现有
  `checkpoint.rs` 的 Coordinator/Manifest/Repository，零契约改动）。
- **废弃** `checkpoint_gate` 全局读写锁路径（`job_runner.rs:161,452,523`）。

### 5. 列式窗口算子

新 `WindowOperator`（executor 内实现，注册为 processor 类型 `window`）：

- **分配**：`window_start = event_time.div_euclid(size) * size`，Arrow compute
  对整批 Int64/Timestamp 列一次计算，按 window 分组 `partition_by`——不逐行复制。
- **状态**：keyed 聚合按 `(namespace=operator_id, key=window_start||key)` 存
  StateBackend，聚合缓冲用 Arrow IPC（RowAccumulator 或 group-by 增量状态）。
- **触发**：watermark ≥ window_end 时发射聚合结果批；processing-time 模式按
  trigger 间隔发射（兼容 Stream 攒批窗口语义）。
- **迟到**：复用 `event_time.rs` 的 `WindowAction` 决策（Drop/Route/Update）。

### 6. StreamConfig 编译器

`stream_compiler.rs`：`StreamConfig → JobSpec` 的确定性映射：

| StreamConfig | JobSpec |
| --- | --- |
| `input` | SourceSpec（time 默认 ProcessingTime） |
| `pipeline.processors[]`（线性） | Map/Filter 链 operator（同 config） |
| `buffer: memory` | 无算子（内存通道已是默认） |
| `buffer: tumbling/sliding/session` | WindowOperator（processing-time 模式） |
| `buffer: join` | **不支持 → 编译期报错**，提示迁移 Job DAG |
| `error_output` | 错误边（operator 失败批路由到 side sink） |
| `durability`（WAL） | source WAL（Input 内部机制不变） |
| `temporary` 表 | 随 processor 配置原样传递 |

- `EngineConfig` 加 `jobs: Vec<JobSpec>`（`#[serde(default)]`）。
- 编译后的 Stream 兼容行为目标：examples/ 下全部 YAML 通过且输出等价
  （window 攒批时序语义按 processing-time 模式对齐）。
- `multiple_inputs` input 已产 `__meta_source`，编译为多 SourceSpec。

### 7. 运行形态

- **本地**：`Engine` 遍历 `streams`（编译）+ `jobs`（直接），对每个
  JobSpec `JobPlan::compile` → `ExecutionGraphBuilder` → spawn TaskRunner；
  内嵌 CheckpointCoordinator（checkpoint 配置存在时）。
- **Agent**：`JobRuntime::start`（`agent.rs:305`）改为用
  `ExecutionGraphBuilder::build_subgraph(plan, assigned_task_ids)` 构建本节点
  子图，barrier coordinator 由 Agent 进程内嵌（Hub 命令驱动注入）。
- 顺序输出：forward 链天然有序；partitioned 边按 subtask 通道有序——
  Stream 的全局 seq 有序输出语义在单链等价，DAG 多 sink 时为 per-edge 有序
  （文档明确差异）。

### 8. 退役路径

1. 内核完成 + StreamConfig 编译器通过全量 examples 回归 →
2. `stream/mod.rs` 执行器删除，`Stream::new/run` 移除；`RuntimeEntry` 改持 JobSpec →
3. buffer 插件（tumbling/sliding/session）标记 deprecated（编译期 warning），
   join buffer 保留（仅 YAML Stream 旧形态使用期），memory buffer 无操作。

## Risks / Trade-offs

- **窗口语义变化**：Stream 攒批窗口（buffer 层、处理时间）与事件时间窗口
  不同——编译器默认 processing-time 模式对齐旧行为，事件时间为 Job 显式配置。
- **WAL 与 barrier 交互**：WAL 的 ack-gated cursor 需在 barrier 快照中纳入
  source 位置（Kafka offset 已有 `current_positions`），恢复时先 WAL 重放
  到 barrier 位置或直接从 checkpoint 位置开始——采用后者（checkpoint 优先，
  WAL 作为无 checkpoint Job 的兜底），spec 写明优先级。
- **递归 dispatch 删除的兼容**：现 `SingleComputeJobRunner` 被内核替换，
  其 25 个评审 fix 覆盖的行为（代际隔离、恢复校验等）由测试迁移保证。
- **并发正确性**：链内多输入对齐缓冲可能内存膨胀——bounded 通道 + 对齐缓冲
  上限（超限 fail checkpoint 而非 OOM）。

## Migration Plan

1. 落地内核 + 窗口算子 + barrier（Job 路径先行，`SingleComputeJobRunner` 仍并存）。
2. StreamConfig 编译器 + 本地 `jobs` 字段。
3. examples 回归（Stream 全走内核）→ 删除旧执行器。
4. Agent 切换到内核子图。
5. 文档 + spec 同步（`stream-backpressure` 等验收措辞更新）。

## Open Questions

- sliding/session 窗口的列式分配实现顺序（tumbling 先行）。
- `temporary` 表在内核多 task 下的共享语义（现单进程共享，DAG 内 forward 链保持单 task）。
