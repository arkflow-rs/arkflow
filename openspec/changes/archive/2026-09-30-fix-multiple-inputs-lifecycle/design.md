## Context

multiple_inputs 用 TaskTracker + CancellationToken 管理子 input 读任务，但两者都是构造期创建的单例字段，`connect()` 无代际概念；引擎的 Disconnection 重连循环（`task.rs:718-759`）反复调用同一实例的 `connect()` 使任务翻倍。三个缺陷共享同一根因：**组件把"首次启动"与"重连"当成同一件事，且没有为读任务一代的生命周期建模**。

## Goals / Non-Goals

**Goals:**

- 代际化的 `connect()`/`close()`：每代 = 一个 token + 一个 tracker，重连即换 代。
- 子错误单次上浮即退出；内部通道 bounded(1024)；重连清 Err 留 Message。

**Non-Goals:**

- 子组件自身的重连语义、健康检查、公平调度（见 proposal Non-goals）。

## Decisions

1. **代际状态放 `tokio::sync::Mutex<Option<Generation>>`，而非 AtomicBool 守卫。** 备选"运行中则直接返回 Ok 的幂等守卫"更简单，但引擎重连的意图是**换一代**（子 input 可能已换连接句柄），直接返回会让旧代任务对着已重连的子 input 继续读、语义含混。`Generation { token, tracker }` 整体替换：`connect()` 先 `take()` 旧代 → cancel → `wait()` → 再建新代，天然串行化且 close() 复用同一路径。Mutex 用 tokio 版因 `wait()` 是异步的。**实现修正**：锁需横跨整个 `connect()`（含子 input `connect()`，即临界区内有 IO）——否则 `close()` 可在两段加锁之间穿行，漏掉刚建好的新代；串行化后 close() 排在前面（无代可漏）或后面（正常取消新代）。实例级 `closed` token 的 `child_token()` 作为各代 token，connect-after-close 拿到的是已取消的 token，任务立即退出。
2. **读任务错误路径统一为"发一次、退出"。** 对齐单输入语义：错误交给引擎分类处置（Disconnection→重连循环会再进 `connect()`，其余按既有错误策略）。备选"任务内带退避重试"会把重连策略复制进组件，与引擎的重连循环（含 5s 退避、cancellation 感知，`task.rs:724-758`）双轨。
3. **通道容量 1024，与链间边同常数。** 备选更大/可配容量：无证据需要差异化，遵守"与边同容量"最简单；如需配置属后续增强。
4. **重连时 `try_recv` 循环只清 `Msg::Err`。** `Msg::Message` 是已消费未投递的真实数据，丢弃即丢数据；`Msg::Err` 属于已被引擎处置过一次的旧代事件，留着会伪造一次重连。用 `try_recv`（非阻塞）避免在没有残留时挂起。
5. **`close()` 走与重连相同的代际收尾 + 子 input `close()`。** 保持"取消→等待→关子输入"顺序不变（与现状一致），只是改为作用于当前代。

## Risks / Trade-offs

- [`connect()` 临界区内 `wait()` 等旧代退出；若某子 input 的 `read()` 长期不返回，重连被卡] → 读任务本就 select 了 cancellation（现状已如此）；P1-5 已把全部 input 的 read 改为取消安全，等待时长有界。
- [bounded 通道满时子任务停在 `send_async`，若消费者正好在关闭流程中，发送者需被释放] → **发送本身包进对 token 敏感的 `select!`**（实现确认：旧代码的发送在 select 之外，unbounded 下侥幸不阻塞；bounded 后必须改，否则任务停在满通道的发送上时 `close()` 的 `wait()` 挂死）。关闭路径先取消代际再等待；接收端 drop 后发送返回 Err 亦退出。
- [重连清 Err 与并发 read() 的竞争：read() 可能恰好在清理前取走该 Err] → 无害：该 Err 最多被上浮一次（清理与消费取其一）。
- [非 Disconnection 类子错误现在会终止该子读任务，上游若不再重连则该源静默停摆] → 引擎对非 Disconnection 错误走失败路径（可见、有声），优于热循环静默空转；行为变化已在 proposal 标注。

## Migration Plan

纯插件内改动，无配置面。验证：`cargo test -p arkflow-plugin` + workspace + clippy；回滚即 revert。

## Open Questions

无。
