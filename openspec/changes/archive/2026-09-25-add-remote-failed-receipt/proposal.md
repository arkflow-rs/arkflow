# Proposal: add-remote-failed-receipt

## Why

远程边 v1 的失败发现语义是「靠 barrier drain 超时」：下游 chain 处理某批次失败时，上游对应分支保持未完成直到超时（`network-shuffle-data-plane` spec 明示「v1 无主动 Failed 回执，失败发现延迟一个 drain 超时属预期行为」）。这是 PLANNING.md 第八节 P2（harden-shuffle-recovery）中可独立交付的第一增量：把失败发现从超时缩短到即时，fail-closed 语义不变。

## What Changes

1. `ReceiptKind` 新增 `Failed` 变体（线协议向后兼容：未知回执变体本就按连接错误处理，混布升级先升内核两端）。
2. `RemoteAck::abort()` 覆写：向对端发送 `Failed { seq }` 回执帧。
3. 上游 `PendingReceipts::apply` 收到 `Failed`：移除该 seq 的 pending 条目并异步 `branch.abort()`（分支补偿），不再等 drain 超时。
4. 重复 `Failed` 回执为幂等 no-op（条目已移除）。
5. 透明重连与远程边去重（P2 其余两项）仍为 Non-goal，留待后续 change。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `network-shuffle-data-plane`: Ack 三态镜像回执扩展为四态（Acked/Held/Released/Failed）；「下游处理失败靠超时发现」场景更新为「Failed 回执即时中止，drain 超时降级为帧丢失兜底」。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：`ReceiptKind`、`RemoteAck::abort`、`PendingReceipts::apply`（Failed 分支）。
- 测试：Failed 回执中止分支 + 幂等重复 + 线协议往返（新增变体）。

## Non-goals

透明重连；远程边去重（effectively-once 收敛）；连接状态机改造。
