## Why

v1.0 就绪度审查 P1-8（`openspec/CODE_REVIEW_2026-09-29.md`）：multiple_inputs 的重连与错误路径存在三连缺陷——读任务代际翻倍、子错误热循环、生产路径唯一无界通道。证据：

- **重连翻倍读任务**：`connect()` 无在运行守卫，每次调用都为全部子 input 再 spawn 一套读任务（`crates/arkflow-plugin/src/input/multiple_inputs.rs:51-97`）；唯一停止手段是 `close()` 里才触发的 cancellation token（`:116`）。而引擎在 `read()` 返回 `Disconnection` 时会对**同一 Input 实例**循环调用 `connect()` 重连（`crates/arkflow-core/src/executor/task.rs:718-759`）——每次重连叠加一代任务：兄弟 input 被并发读两份（重复投递），TaskTracker 跨代累积（`:94` 的 `close()` 在重连路径上语义失真）。
- **子错误热循环**：子 input 读错误若非 `Disconnection`/`EOF`，读任务发送 `Msg::Err` 后**继续循环立刻再读**（`multiple_inputs.rs:73-84`）——子 input 处于立即出错的损坏状态时，该任务全速空转发错误，无退避无退出。
- **无界通道**：`(sender, receiver) = flume::unbounded()`（`multiple_inputs.rs:132`）是生产路径唯一的 unbounded 通道，违反架构规则"inter-chain edges are bounded flume channels (capacity 1024); producers must propagate backpressure, never buffer unboundedly"与 `stream-backpressure` spec 的有界要求——下游停顿时子读任务持续入队，内存无界增长。

## What Changes

- `connect()` 改为代际重启语义：先取消并等待上一代读任务（若在运行），再重建 token/tracker 并 spawn 新一代——任意时刻每个子 input 至多一个活跃读任务，TaskTracker 不跨代累积。
- 子 input 读错误（任意类别）单次上浮后该读任务退出，交由引擎既有 Disconnection/错误路径决定重连——消除热循环。
- 内部消息通道改 `flume::bounded(1024)`，与链间边同一容量常数：满载时子读任务在 `send_async` 上等待，背压传导至源。
- 重连时清理上一代残留的 `Msg::Err`（陈旧错误不得触发多余的引擎重连），保留 `Msg::Message`（真实数据不得丢弃）。
- 同步更新 multiple_inputs 组件文档（en + zh-Hans）中的重连与背压行为描述。

## Capabilities

### New Capabilities

- `multiple-inputs-lifecycle`: 组合输入（multiple_inputs）的代际生命周期契约——重连恰好重启一代读任务、子错误单次上浮即退出、内部通道有界。

### Modified Capabilities

（无——`stream-backpressure` 的有界要求已覆盖目标语义，本变更只是让组件遵守它，不改变该 spec 的需求文本。）

## Impact

- `crates/arkflow-plugin/src/input/multiple_inputs.rs`：`connect()`/`close()` 改造（token/tracker 移入代际结构）、读任务错误路径、通道改 bounded、重连时清 Err。
- 行为变化：子 input 非断连类错误从"无限热重试"变为"上浮一次由引擎处置"；无界缓冲变为满载背压。
- 文档：`docs/docs/components/0-inputs/multiple_inputs.md` 与 zh-Hans 对应页。

## Non-goals

- 不改子 input 各自的 `connect()`/重连内部语义（子组件自己的重连策略归各组件）。
- 不引入子 input 级别的健康检查或自动剔除。
- 不改 `read()` 的取消安全契约（P1-5 已覆盖，当前实现合规）。
- 不做跨子 input 的公平性调度（有界通道先到先得）。
- 不动 versioned_docs（0.5.x 历史快照）。
