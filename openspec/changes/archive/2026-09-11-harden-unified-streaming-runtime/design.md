## Context

本变更紧接 `rebuild-unified-streaming-engine`，目标不是再次重写执行内核，而是修正统一内核在真实故障、重启、多分区和多节点场景下的正确性边界。当前数据面由 `ExecutionGraph`、bounded `Envelope` channel、source/processor/sink chain 和异步 barrier 组成；控制面由 Hub–Agent Job operation、checkpoint manifest 和 Agent 本地子图组成。

审查暴露出四类相互关联的问题：

1. barrier 位置、下游 acknowledgement 和状态 backend 没有共享一个明确的提交切点；detached snapshot 可能读取到 barrier 后的状态。
2. Kafka 的 assignment、恢复 offset 和 acknowledgement cursor 没有区分“已观察”与“连续已确认”，恢复时还可能用部分 assignment 覆盖完整订阅。
3. 事件时间 gate、watermark、窗口 buffer 和输出 schema 在单位、分区、迟到更新、滑动窗口边界及浮点值上不一致。
4. temporary/WAL、local Job、深度校验和 Hub–Agent session/operation 聚合没有统一的失败及清理边界。

设计必须继续使用现有 Arrow `RecordBatch`、`StateBackend`、WAL、checkpoint repository 和 Hub–Agent 协议，不以新增连接器或跨节点 shuffle 解决这些问题。

## Goals / Non-Goals

**Goals:**

- 让 source position、acknowledged cursor、watermark、operator state 和 checkpoint manifest 来自同一个可证明的已确认切点。
- 在不引入全局 stop-the-world 的前提下，保证 barrier 后数据不会污染当前 checkpoint；失败重放不会重复提交 keyed state。
- 使单任务 Kafka 消费全部配置分区，多任务 Kafka 保持显式物理分区，并让恢复后的 assignment/cursor 可持续用于后续 checkpoint。
- 统一所有 Arrow timestamp 单位、活跃分区 watermark、迟到策略、窗口关闭/更新和数值聚合类型。
- 让所有资源、WAL、local Job 和 Agent operation 在成功、失败、取消、重连和重启路径上可清理、可观察、可恢复。
- 让 Hub 与 Agent 使用同一套 checkpoint 完整性和 savepoint 兼容性判断，避免控制面已授权但数据面拒绝，或反之。

**Non-Goals:**

- 不重新设计 `Envelope` 或 Hub–Agent 的认证、命令和 Console API；必要的字段补充必须保持向后兼容。
- 不实现跨节点数据传输、分布式事务 sink 或外部系统的全新 exactly-once 协议。
- 不把 `thread_num` 扩展成新的调度器；只恢复现有 Stream 配置表达的处理器并发语义。
- 不自动修复或改写历史不完整 checkpoint；历史 artifact 必须通过兼容性和完整性检查后才能使用。

## Decisions

### 1. 使用提交前沿和不可变状态切点统一 checkpoint

引入 execution-local 的 `CommitFrontier`/state epoch 概念，而不是在 detached task 中直接读取可变 backend：

- 每个 source partition 维护“下一条待确认位置”和 out-of-order acknowledgement 集合。只有连续确认的记录才能推进 frontier；Kafka 和本地 WAL 都只暴露这个 frontier。
- source 注入 barrier 时先封存一个 `CheckpointCut`，其中包含该时刻的 acknowledged source positions、source partition identity 和 watermark 进度。尚未完成的 pre-cut 输出/状态事务不得被算入该 cut。
- stateful processor 先把本批状态写入 execution-local mutation journal，输出 ack 成功后才提交到 backend；输出失败或 task 失败时丢弃/回滚 journal。后续同一 chain 的记录可以读取 committed state 加 pending overlay，但 checkpoint 只读取已提交 epoch。
- 多输入 chain 在所有输入 barrier 对齐后，必须先从 committed epoch 捕获不可变 `StateSnapshot`，再调用 `aligner.release()` 处理 barrier 后缓存数据。序列化、校验和对象存储写入仍可异步进行，因此不会持有 Job-wide lock。
- checkpoint report 只有在该 chain 的 pre-cut 状态事务完成、snapshot 校验通过且 source cut 可追溯时才发送。任何一个参与 task 缺失或失败都不能生成 Completed artifact。

选择提交前沿加 execution-local journal，是因为现有 `StateBackend` 的同步 `get/put/update` 接口没有跨组件事务；直接给所有 backend 增加事务协议会扩大范围。替代方案“barrier 到达后直接异步读取 backend”会产生审查中指出的状态超前；“每次 checkpoint 全局暂停并排空所有输出”虽然简单，但会恢复已明确退役的 stop-the-world 语义。

### 2. 用统一资源守卫管理 temporary、input、WAL、processor、sink 和 state backend

图构建完成后创建一个带逆序关闭能力的 Job resource guard：

- 在 spawn chain 前，按 dependency order 连接所有 temporary、source、sink；任何一步失败都关闭已经连接的资源并返回错误。
- `Resource` 继续承载 temporary 实例，但增加统一的 connect/close 辅助路径；temporary 的连接失败必须发生在 graph 正式运行前。
- `StreamJobAdapter` 的 dry-run/deep-validation 实例显式关闭 WAL 和 temporary，确认锁已释放后才创建真实运行实例。
- `WalInput::close` 先关闭 wrapped input，再停止 flusher、刷新 pending append 并关闭 WAL；重复 close 必须幂等。state backend、processor 和 sink 也加入同一清理链，关闭错误按既有错误传播规则汇总。
- local Job 的启动流程先等待 graph/resource startup result，再发布 readiness；运行中错误进入 Job/runtime 的 `Failed` 状态，而不是只在 shutdown 时记录日志。

选择显式 guard 而不是依赖 `Drop`，是因为 WAL flusher 和异步 connector 的生命周期不能靠同步析构保证 flush 顺序。资源连接与关闭仍由运行时拥有，不改变插件 trait 的业务职责。

### 3. 保持 Kafka 订阅拓扑与恢复拓扑一致

`ExecutionGraphBuilder` 根据同一 source operator 的实际 task 数决定 Kafka 模式：

- 只有一个 source task 时不调用 `assign_partition`，Kafka 保留 subscribe-all-partitions 路径。
- 多个 source task 时才设置物理 partition，并为该 task 的全部配置 topic 构造完整 assignment。
- 恢复时不使用 checkpoint 中出现的 subset 替换 assignment。订阅模式保留完整 subscription，只 seek/安装匹配的恢复 offset；显式分区模式合并恢复 offset，未记录的配置分区保留默认起点。
- 恢复 position 同时初始化内存中的 `CommitFrontier`，避免下一轮 checkpoint 因没有新 ack 而丢失恢复 cursor。
- `KafkaAck` 只将连续 frontier 交给 `store_offset` 和 `current_positions`；超前完成的 offset 进入 pending set，直到间隙填补后才推进。

选择“单 task 订阅、multi-task 分配”的判断来源于 Job 实际 task 数，而不是 `parallelism` 的默认值或第一个 partition。选择连续 frontier 而不是最大观察值，是为了在 fan-out 完成乱序时不跳过未确认记录。

### 4. 让事件时间 gate 和窗口 operator 共享分区化、类型化语义

- `FieldTimestampExtractor` 对 Arrow `TimestampSecond/Millisecond/Microsecond/Nanosecond` 以及现有 Int64 统一做 checked conversion 到毫秒；溢出返回可定位错误，负时间戳使用与 `div_euclid` 一致的窗口边界规则。
- gate 和 window operator 的 watermark 均以稳定的 source task/physical partition 作为 key。多个活跃上游的 operator watermark 使用最小值；idle partition 按既有 idle policy 排除。恢复时使用 report 中的实际 partition，不写死为 0。
- gate 在推进本批最大可用事件时间后，以推进后的 watermark 重新计算 current rows；只有未来窗口保留为 Hold。空 timestamp 没有可计算的窗口结束时间，必须按显式 invalid-timestamp 处理：有配置的 route 时送 side output，否则 drop 并 ack，不能无限 Hold。
- window operator 将已触发窗口保留到 allowed-lateness deadline，并记录 emitted/closed 状态。Update 会修改同一个 `(operator, key, window)` 聚合并发出带更新标识的完整修正结果；deadline 后才清理。Route 不得再次进入正常聚合。
- sliding window 从最后一个候选 start 向前枚举到 `start + size > event_time` 为止，不依赖 `size / slide` 整除。
- 聚合 buffer 使用带数值类型的表示（至少 Int64、Float32、Float64），sum/min/max/count 按类型输出；state payload 携带类型信息，旧的可安全迁移格式继续可读，不可推断的旧浮点状态拒绝恢复并标记兼容性失败。

选择最小 watermark 和保留 closed buffer 是为了满足“每个活跃输入都已推进”以及允许迟到更新的定义。替代方案“使用最快输入的 max”会提前关闭窗口；“迟到 Update 创建新 buffer”会破坏同一窗口的修正语义。

### 5. 集中 checkpoint/savepoint 兼容性和完整性判断

增加共享的 recovery compatibility 校验结果，Hub 授权、Agent 恢复和 repository 读取都使用相同规则：

- 必须匹配 Job identity、barrier generation、task/attempt identity 和 manifest checksum。
- target Job version 不要求与 artifact 精确相等；只要 state namespace、operator identity 和 state format 存在明确的 migration/compatibility path，就允许受控升级；降级或无迁移路径仍拒绝。
- checkpoint aggregation 的格式以 Job/backend contract 为准；无状态 chain 的 format-1 空 snapshot 不得否决配置了其他状态格式的 Job。
- Agent 聚合和 repository `write_manifest` 都必须将“计划参与的完整 task set”与 manifest task set 做精确比较。缺失、重复或额外 task 都生成失败结果而不是 Completed artifact。

选择共享规则而不是分别在 Hub/Agent 复制版本比较逻辑，是为了避免一方授权、另一方拒绝。选择完整集合校验是因为只验证已列出的 task 无法发现离线分区导致的可恢复数据缺口。

### 6. 用 generation/action/assignment 维度聚合控制面状态

Hub 为每次 Job operation 保留预期 assignment 集合及每个 assignment 的结果，只有聚合后才更新 Job observed state：

- 所有 assignment 达到 running/succeeded 才能得到 running/in-sync。
- 任一 assignment 仍 pending/running 时保持 converging/applying；离线或可重试错误标记 degraded/retrying。
- 只有在完整集合确认不可恢复的永久失败后才将整个 Job 标记 failed/blocked；单节点暂态非成功不能覆盖健康 peer。
- checkpoint commit 使用相同 assignment 集合，不能因为 Hub 过滤离线节点后仍执行部分 commit。
- Agent 注册返回的 session token 作为该 session 的 report identity；每次新 session 从 report sequence 0 开始，Hub 重置对应 cursor，旧 session 的报告按旧 identity 拒绝。

这保留现有 desired/observed/convergence 分层，同时避免用单个 command result 伪造整个分布式 Job 的状态。使用 registration session identity 也避免继续依赖可能复用的 PID boot id。

### 7. 统一深度校验并保留 Stream processor 并发语义

- 抽出一个无副作用的 `build_job_for_validation` 路径，统一用于 CLI、配置 API、local Job 和 Stream 编译结果；它构造组件、temporary、state backend 和 graph，但不启动消费、不执行外部写入。运行前再走同一 resource lifecycle 的真实 connect。
- 编译 Stream 时保留 `pipeline.thread_num` 为 chain-level processor worker parallelism，并使用有界、可取消、保持顺序的 worker pool；source 仍保持单 task，避免把旧的 processor worker 数误变成 Kafka 分区数。stateful/window chain 的状态提交仍由单一 epoch 顺序化。
- 生成的 engine schema 增加 `jobs` 属性及 JobSpec 的结构/schema；当 streams 和 jobs 都为空时按现有配置约束给出明确诊断，jobs-only 配置可以被 schema 驱动客户端表达。

选择 chain-level worker pool 而不是直接把 `thread_num` 映射为 Job parallelism，是为了不改变 source partition assignment 和已有 YAML 语义；选择统一 deep-build 是为了让 `--validate` 与真正启动使用同一组件解析路径。

### 8. 用分层测试证明切点和失败边界

测试按以下层次落地：

- executor：提交前沿、状态 journal、barrier-before-release、多输入最小 watermark、gate current-row/null timestamp、closed-window Update、sliding 非整除和浮点 schema。
- plugin：Kafka 单 task 全分区、多 task assignment、乱序 ack 连续 frontier、部分 position 恢复和恢复后空 checkpoint；WAL close/flush/reopen。
- runtime/config：temporary connect/close、dry-run WAL lock release、deep validation、local Job startup failure、runtime Failed 和 `thread_num`。
- server：兼容 savepoint 版本、完整/不完整 checkpoint task set、Hub operation 聚合、Agent session cursor、非 0 分区 watermark restore。
- end-to-end：双节点 checkpoint/restart、输出失败后重放不重复提交状态、旧有效 checkpoint 保留及错误 artifact 拒绝。

## Risks / Trade-offs

- **[Risk]** execution-local journal 会增加 pending state 的内存占用。→ **Mitigation:** 按 chain 设置有界 mutation/ack 数量和 bytes 上限，超限使当前 checkpoint 或 Job 失败并保留上一有效 checkpoint；指标暴露 pending state。
- **[Risk]** barrier 需要等待 pre-cut acknowledgement，可能增加 checkpoint latency。→ **Mitigation:** 只在对应 chain/epoch 设置有界提交栅栏，不恢复全局写锁；记录等待原因、超时和失败计数。
- **[Risk]** 旧浮点 window state 无法从整数哨兵可靠迁移。→ **Mitigation:** 为类型化 payload 增加格式版本；可证明的整数状态迁移，无法证明的状态在恢复前拒绝并保留旧 artifact。
- **[Risk]** Kafka subscription 与 seek/assign 的组合依赖 rdkafka 的 rebalance 时序。→ **Mitigation:** 将 assignment 合并逻辑封装在 Kafka input，并用 broker/mock integration 覆盖 connect、restore、reconnect 和多分区场景。
- **[Risk]** 完整 task 集合校验会使离线节点期间 checkpoint 更容易失败。→ **Mitigation:** 明确报告 degraded/last-valid-checkpoint，禁止生成不完整恢复点；节点恢复后由既有 reconciliation 重试。
- **[Risk]** 浮点窗口输出类型修正会影响依赖错误 Int64 schema 的消费者。→ **Mitigation:** 在 schema/文档中标记行为变更，提供迁移测试；整数输入保持原 schema。

## Migration Plan

1. 先在 core 中加入 `CommitFrontier`、state epoch/journal、资源守卫、类型化 timestamp/window 和共享 recovery compatibility 接口，保持现有 API 默认值。
2. 切换 Kafka input、WAL close、Stream compiler、validation/schema 和 local runtime；先运行 core/plugin focused tests，再运行 workspace 回归。
3. 切换 Agent/Hub 的 manifest 完整性、savepoint 兼容性、watermark restore、operation 聚合和 session cursor；运行双节点 checkpoint/restart smoke。
4. 对已存在的 checkpoint/savepoint 执行只读兼容性扫描。可迁移 artifact 继续使用，不完整、校验失败或无迁移路径的 artifact 被排除，自动恢复仍选择上一个有效点。
5. 发布前运行 strict OpenSpec、格式检查、workspace tests 和故障注入测试；若新版本运行异常，可回滚二进制并继续使用未被新兼容性规则接受前的最后有效 artifact，不删除任何历史状态。

## Open Questions

- 初始实现的 mutation journal 是否仅作为 executor 内部抽象，还是同时扩展 `StateBackend` trait 以供未来自定义事务 backend 使用；本 change 默认先保持 trait 最小化。
- 对 Float32 聚合是否严格输出 Float32，还是允许配置为 Float64 promotion；默认方案是保持输入类型，除非 Job schema 明确要求 promotion。
- Kafka 订阅模式在恢复时是等待首次 assignment 后逐分区 seek，还是由 input 保存完整 assignment 后一次性安装；两者必须保持同一对外语义，具体 API 由 rdkafka 测试结果决定。
