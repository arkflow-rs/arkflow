## Why

v1.0 就绪度审查 P1-9（`openspec/CODE_REVIEW_2026-09-29.md`）：batch processor 与 memory buffer 的失败/关闭路径存在静默数据丢失，且 memory buffer 无界累积违反全仓背压纪律。证据：

- batch processor `close()` 直接 `batch.clear()` 丢弃已缓冲未下发的消息（`crates/arkflow-plugin/src/processor/batch.rs:120-125`）；引擎在**每条**链路退出路径都会调用它（`crates/arkflow-core/src/executor/task.rs:384-389`）。Processor trait 已有可向下游发数据的 EOS 钩子 `finish()`（`crates/arkflow-core/src/processor/mod.rs:90-96`）与空闲钩子 `on_tick()`（`:98-103`），引擎也已分发 `Finish`/`Tick` 控制事件（`task.rs:2380-2420`），但 batch processor 两者均未实现——部分批次既不在 EOS 排空，也不能在无新消息到达时按超时刷新（超时判定只在 `process()` 内做，`batch.rs:56-70,105`）。
- memory buffer `process_messages()` 先 `pop_back()` 清空队列再做 `concat_batches`，合并失败时已弹出的整批消息与其 ack 一起丢失（`crates/arkflow-plugin/src/buffer/memory.rs:108-140`）。
- memory buffer `capacity` 不设限：`write()` 无条件 `push_front`，capacity 仅用于提前唤醒读者（`memory.rs:153-171`），违反架构规则"producers must propagate backpressure, never buffer unboundedly"与 `stream-backpressure` spec 的 bounded 要求。
- `flush()` 与 `close()` 共用同一个 `CancellationToken`（`memory.rs:206-217` vs `:223-226`）：任何一次 flush 都会永久终止超时释放的后台任务（`:85-89` select 该 token 后 break）——flush 语义上是非终止操作，却造成"半关闭"。
- `ArrayAck` 在第一个子 ack 失败即中止且无补偿，`undo`/`abort`/`mark_held` 均为默认空实现（`memory.rs:230-241`），违反 `message-acknowledgment` spec "Composite acknowledgement propagates errors" 的补偿要求；core 已有带完整补偿语义的 `VecAck`（`crates/arkflow-core/src/input/mod.rs:333-388`）可直接复用。

## What Changes

- batch processor 实现 `finish()`：EOS 时排空部分批次并正常下发（ack 按常规输出路径结算）；实现 `on_tick()`：空闲期到达 `timeout_ms` 即刷新，不再依赖下一条消息触发；`close()` 回归纯资源释放，若仍有残留（仅异常终止路径可达）记录带条数的 warn 而非静默丢弃。
- memory buffer `process_messages()` 改为先在锁内克隆合并、成功后才清除队列：合并失败时队列原样保留，读侧可重试，不丢消息不丢 ack。
- memory buffer `capacity` 真实生效：队列占用达到 capacity 时 `write()` 等待排空（背压向上游传播），等待中对 `close` 敏感以保障关停活性。
- memory buffer 分离 flush 与 close 语义：`flush()` 仅唤醒读者排空、不终止任何后台任务；`close()` 保持终止语义，且关闭后 `read()` 仍先排空余量再返回 `None`（保持现状）。
- memory buffer 以 core `VecAck` 替换本地 `ArrayAck`，获得失败补偿（反向 undo）、`abort`、`mark_held`/`release_held` 语义。
- 同步更新 memory buffer 与 batch processor 的组件文档（en + zh-Hans）中关于 capacity 语义与 flush/close 行为的描述。

## Capabilities

### New Capabilities

- `buffering-stage-drain`: 缓冲类组件（batch processor、memory buffer）的排空与失败路径契约——EOS/超时排空、合并失败不丢已持有消息、flush 与 close 语义分离。

### Modified Capabilities

- `stream-backpressure`: 新增"缓冲阶段的持有量受配置约束"要求——memory buffer 的队列长度受 `capacity` 约束，满时 `write()` 等待而非无界累积。
- `message-acknowledgment`: 在"Composite acknowledgement propagates errors"要求下新增场景——memory buffer 合并批次的组合 ack 失败时补偿已成功的子 ack（`VecAck` 语义）。

## Impact

- `crates/arkflow-plugin/src/processor/batch.rs`：新增 `finish()`/`on_tick()` 实现，`close()` 改为非破坏性。
- `crates/arkflow-plugin/src/buffer/memory.rs`：`write()` 背压、`process_messages()` 失败保留、flush/close token 分离、删除 `ArrayAck` 改用 `arkflow_core::input::VecAck`。
- 行为变化：memory buffer 写满后阻塞（既有测试 `test_memory_buffer_capacity_limit` 需改为并发读者形态）；batch 部分批次在流停顿 `timeout_ms` 后即下发（此前需等下一条消息）。
- 文档：`docs/docs/components/buffer/memory`（若存在对应页）与 batch processor 页 en/zh 双语同步。

## Non-goals

- 不改 batch processor 的 `count` 语义（当前按输入 batch 引用数计数，改为按行计数是独立的行为变更）。
- 不修 batch processor schema 元数据与文档的字段漂移（`size`/`interval` vs `count`/`timeout_ms`）——已列 P2 文档脱节批次。
- 不为 processor 缓冲引入 `process_with_ack` 的 ack 托管重设计（EOS 排空经正常输出路径结算已覆盖丢失面）。
- 不处理窗口族 buffer（`buffer/window.rs`）的 Notify 丢唤醒竞态与异构 schema 合并问题（P2）。
- 不做磁盘 buffer 或其他 buffer 实现的同类排查。
