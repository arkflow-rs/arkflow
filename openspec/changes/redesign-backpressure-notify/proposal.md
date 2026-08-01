## Why

Stream 的 backpressure 感知目前由 atomic + sleep 轮询实现：`do_processor` 在每次循环里 `load` 两个 `AtomicU64`（`sequence_counter` 与 `next_seq`），当 `pending > BACKPRESSURE_THRESHOLD (1024)` 时 `tokio::time::sleep(100–500ms)` 再 `continue`（`crates/arkflow-core/src/stream/mod.rs:342-353`）。`next_seq` 由 `do_output` 在按序写出后 `fetch_add(1, Release)` 推进（`crates/arkflow-core/src/stream/mod.rs:433`）。换言之，processor 在「等待 output 把 in-flight 管道排空」时，靠**周期性睡眠**去感知 output 的推进。

这带来三个问题：

1. **恢复延迟与吞吐抖动**：output 推进 `next_seq` 后，被阻塞的 processor 最多还要再睡一个完整的 `wait_time`（100–500ms，且 pending 越大睡得越久）才会重新检查。backpressure 期间的有效吞吐被人为压低，解除时呈阶梯式恢复而非即时恢复——这与「尽快排空积压」的目标相反。
2. **轮询开销随并发放大**：每个 processor worker（`thread_num` 个）都独立 sleep-loop；未来提高 `thread_num` 会线性放大这种无效唤醒。
3. **活跃性靠隐式假设、无契约/无测试**：当前的「最终能退出 backpressure」依赖「时间总会流逝、loop 总会回到 `recv`」这一副作用，仓库里没有任何针对 backpressure 的回归测试（`grep backpressure crates/arkflow-core/src/stream/mod.rs` 仅命中常量定义 `:35` 与使用点 `:346`），换信号机制时极易引入关闭/取消路径的死锁而无测试兜底。

**Why now**：reliability roadmap（CDC / Schema Registry / 端到端 EOS / WAL 优化）刚落地，backpressure 是 at-least-once 与 EOS 下保证 in-flight 有界、`tree_map` 不无限增长的关键约束。在 EOS 之后处理它，恰好把「in-flight 有界 + 恢复即时 + 关闭无死锁」这一组一直隐式存在的质量属性形式化下来。

## What Changes

- 新增 `Arc<tokio::sync::Notify>` 信号，共享给所有 processor worker 与 output worker。
- `do_output` 每次推进 `next_seq`（`fetch_add`）后调用 `notify_one()`，显式通知「in-flight 已排空一格」（每释放一格、精确唤醒一个 processor，且存 permit 不丢信号）。
- `do_processor` 的 backpressure 由 `sleep` 轮询改为**检查后等待**：先获取 `notified()` future 注册 interest、再 `load` pending，超阈值则 `await` 该 future（规避 `Notify` 的丢信号语义）。
- 移除 `wait_time` 计算与 `tokio::time::sleep` 调用（`crates/arkflow-core/src/stream/mod.rs:347-351`）。
- 新增 backpressure 回归测试（当前为 0 条）。

非 backpressure 的语义保持不变：阈值仍为 `BACKPRESSURE_THRESHOLD = 1024`、in-flight 上界不变、`do_output` 的顺序重排逻辑不变、s3 WAL flusher 不受影响（其本身已是 `Notify` 驱动，见 `crates/arkflow-plugin/src/wal/s3.rs:1080-1103`）。

## Capabilities

### New Capabilities

- `stream-backpressure`: Stream 通过 sequence-number 驱动的 in-flight 上界对 processor 施加 backpressure 的可观测契约——in-flight 有界、解除即时（信号驱动而非轮询）、在输入结束与取消时保持无死锁的活跃性。

### Modified Capabilities

<!-- 无。现有 specs（message-acknowledgment、input-durability）对 backpressure 仅作泛指引用，其 requirement 不随本次实现机制变更而改变。 -->

## Impact

- **代码**：`crates/arkflow-core/src/stream/mod.rs`——`Stream` 结构体（新增 `Notify` 字段）、`Stream::new`、`run_inner`（创建并下发信号）、`do_processor`（轮询 → 信号）、`do_output`（推进 `next_seq` 后 `notify_one`）。
- **API / 配置 / 依赖**：无变化。`tokio` 已是工作区依赖；不新增任何配置项；`BACKPRESSURE_THRESHOLD` 不变。
- **可观测行为**：backpressure 恢复延迟由 O(100–500ms 轮询粒度) 降为即时；in-flight 上界与按序输出语义不变。
- **测试**：`crates/arkflow-core/src/stream/mod.rs` 的 `tests` 模块新增 backpressure 回归用例。

## Non-goals

- 不改 backpressure 的**语义**（仍是限制「已分配 seq 未 output」的 in-flight ≤ ~1024，保证 `do_output` 的 `tree_map` 有界）。
- 不改 `do_output` 的顺序重排（`BTreeMap` + `next_seq`）逻辑。
- 不改 s3 WAL 的 background flusher（已是 `Notify` 驱动）。
- 不调整 `BACKPRESSURE_THRESHOLD` 的值，也不把它做成可配置项。
- 不重写 input 侧的重连退避（`do_input` 的 `sleep(5s)` 重连，`crates/arkflow-core/src/stream/mod.rs:271`）——那是断连重试退避，不属于 drain。
