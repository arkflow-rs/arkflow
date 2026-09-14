## Why

统一执行内核已经替换了旧的 Stream/Job 执行路径，但审查发现若干正常配置下仍会破坏数据一致性和运行生命周期：source position 与状态快照可能不在同一个已确认切点，Kafka 恢复可能遗漏分区或跳过未连续确认的 offset，事件时间窗口可能提前触发、错误处理迟到更新或丢失滑动窗口贡献；同时临时资源、WAL、本地 Job 启动和 Hub–Agent 状态聚合仍存在失败不透明或无法恢复的路径。证据包括 `executor/task.rs:286-307,701-724`、`input/kafka.rs:325-353,390-405`、`executor/window.rs:234-253,670-718`、`executor/event_time_gate.rs:195-201`、`runtime.rs:523-530` 和 `server/hub.rs:2258-2278`。

这些问题直接违反现有 checkpoint、事件时间、输入持久化和控制面规格，必须在继续扩展统一内核前修复，否则重启、故障注入和多分区生产环境会出现数据丢失、重复聚合或错误健康状态。

## What Changes

- 统一 Job/Stream 生命周期管理：在运行图前连接所有 temporary 资源，在关闭时按依赖顺序关闭 input、WAL flusher、processor 和 sink；dry-run/deep-validation 使用的临时 WAL 必须释放；本地 Job 的构建和立即启动错误必须反馈给 Engine，并将失败 runtime 置为 `Failed`。
- 建立 checkpoint 的单一一致切点：source position、acknowledged cursor、watermark、算子状态和 barrier 必须来自同一个已确认边界；多输入 barrier 快照必须在释放 barrier 后数据前取得不可变快照；checkpoint 必须覆盖完整 assignment 集合，并正确处理无状态链的状态格式。
- 修正 Kafka 分区与 offset 语义：单 source task 订阅全部分区，多 task 才执行物理分区分配；恢复时合并完整 assignment 并保留未出现在 checkpoint 中的分区；初始化恢复 cursor；按每个 topic-partition 跟踪最高连续已确认 offset。
- 完善事件时间和窗口语义：支持 Arrow 秒/毫秒/微秒/纳秒 timestamp；按实际 source partition 恢复 watermark，并对多上游使用最小活跃 watermark；watermark 推进后重新评估当前批次；显式处理空时间戳；在允许迟到期间更新并重新发出已关闭窗口；修正非整除 sliding window 的包含关系。
- 保持窗口聚合的数值类型：Float32/Float64 必须求和并以兼容的浮点 schema 输出，min/max/count 不得使用错误的整数哨兵或强制转换；该行为修正可能改变依赖当前错误 Int64 输出的消费者（**BREAKING**）。
- 强化状态与 acknowledgement 原子性：下游处理失败时不得留下已提交的 keyed-state 变更；状态提交、输出成功、source ack 和 WAL cursor 必须具有可重试或事务性恢复语义。
- 修正 savepoint 和控制面收敛：允许状态格式兼容的 Job 版本升级；Hub 只有在所有 assignment operation 聚合后才更新 Job observed state；离线 assignment 不得生成可恢复的部分 checkpoint；Agent 新 session 重置 report cursor，避免重连后的报告被误判为过期。
- 补齐配置与执行能力：所有 Job 校验入口执行组件、backend 和图的深度构建；`pipeline.thread_num` 映射为实际 processor 并发度；将 `jobs` 纳入生成的 Engine schema。
- 为上述语义增加单元、恢复、Kafka 分区/offset、窗口类型、WAL 生命周期、本地启动失败以及多节点 checkpoint/restart 回归测试，并同步主规格中的验收场景。

## Non-goals

- 不改变 Hub–Agent 的认证协议、命令协议或 Console 的整体 API 形状。
- 不新增 Kafka、Redis、数据库或对象存储连接器；只修正现有适配器在统一内核生命周期中的连接、恢复和关闭行为。
- 不实现跨节点数据 shuffle、分布式事务 sink 或新的 exactly-once 外部系统协议。
- 不将当前审查中发现的问题扩展为性能重构；并发度只恢复已有配置语义，不引入新的调度模型。
- 不自动修改历史 checkpoint 的内容；不兼容或无法证明完整性的历史 artifact 必须被拒绝并保留最后一个有效恢复点。

## Capabilities

### New Capabilities

无。审查问题均属于现有统一执行、状态、恢复、配置和控制面能力的正确性补强。

### Modified Capabilities

- `checkpoint-recovery`: 要求状态、source position、watermark 和 task assignment 在同一已确认切点生成；拒绝缺少 task 的 checkpoint；允许兼容 Job 版本的 savepoint 恢复；统一无状态链与 Job 状态格式的校验。
- `event-time-processing`: 支持全部已声明的 Arrow timestamp 单位，按分区取最小 watermark，恢复实际分区进度，并严格执行迟到、空时间戳和窗口更新语义。
- `keyed-state-backend`: 要求 keyed-state 只在处理单元成功或可恢复事务提交后可见，并保留窗口聚合的数值类型。
- `input-durability`: 要求 WAL、source acknowledgement、连续 Kafka offset 和 checkpoint position 协同推进，并保证临时及运行中 WAL 在关闭和重启时正确释放、刷新和恢复。
- `message-acknowledgment`: 要求输出成功、状态提交、WAL cursor 和 source ack 的失败传播与重试边界一致。
- `distributed-job-runtime`: 修正 source task 分区分配、统一资源生命周期、本地 Job 启动失败传播、配置并发度和恢复前初始化顺序。
- `streaming-job-api`: 将 Job 的深度可构建性、schema 生成和兼容 savepoint 迁移校验纳入部署前验证契约。
- `configuration-management`: 对本地 Job、组件、状态 backend 和运行时构建执行与 Stream 一致的深度验证，并在 dry-run 失败时记录失败状态。
- `control-plane-reconciliation`: 依据所有 assignment operation 的聚合状态更新 Job observed state，不以单节点结果提前宣告收敛或失败。
- `control-plane-fleet`: 要求完整 assignment 才能封存 checkpoint，并为重新注册的 Agent 建立独立 report session cursor。
- `stream-runtime-control`: 本地 Job 和 Stream 的启动、失败、停止及 WAL/temporary 资源关闭必须可观察且可恢复。

## Impact

- **核心执行层**：`crates/arkflow-core/src/executor/{task,graph,window,event_time,event_time_gate,stateful,kernel_handle}.rs`，以及 `runtime.rs`、`stream_adapter.rs`、`engine/mod.rs`、`configuration.rs`、`config.rs` 和 `stream_compiler.rs`。
- **插件层**：`crates/arkflow-plugin/src/input/kafka.rs` 的 assignment、恢复 cursor 和连续 offset 跟踪；现有 temporary/WAL adapter 的生命周期接口。
- **控制面**：`crates/arkflow-server/src/agent.rs`、`hub.rs` 及 checkpoint/Job operation 聚合和恢复校验逻辑。
- **规格与测试**：新增本 change 下各能力的 delta spec，补充 executor、Kafka、窗口、WAL、配置、Agent/Hub 的回归和故障注入测试。
- **兼容性**：普通整数窗口和已有无 checkpoint Stream 的行为保持；浮点窗口输出 schema、兼容 savepoint 版本校验、单任务 Kafka 的分区订阅行为会按正确语义调整。
