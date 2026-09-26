# Proposal: fix-remote-replay-ack-semantics

## Why

CR 发现 `add-remote-transparent-reconnect` 的投递级去重用**镜像 Acked** 完成重放帧的上游分支：帧已投递到本地通道、尚未处理完成时，镜像回执使上游分支提前完成 → 源位点持久推进越过未处理数据 → 该窗口内节点崩溃即数据丢失。这违反引擎"ack 以处理完成为准"的核心不变量（基线是 at-least-once：只重复、不丢失）。同一缺陷簇还有两处：投递记录在通道发送**前**推进（发送失败时重放会静默丢帧）；`RemoteAck` 绑定连接级回执通道，重连后原 ack 无处送达。

## What Changes

1. **会话级回执路由**：`NetworkManager` 按会话键持有有界回执队列 + 连接可换写的转发槽；`RemoteAck` 发往会话队列；转发任务把回执写往当前服务的连接，写失败（连接已换/已死）原条目重试直到新连接接槽——回执不随连接丢失，原 ack 在重连后仍能完成。
2. **删除镜像回执**：重复帧（seq ≤ 已投递记录）静默丢弃，不补发任何回执；上游分支只由**处理完成后的真实 ack** 经会话路由结算。回执真正丢失（罕见：写往已死 socket 的窗口）回落既有的 barrier drain 超时 fail-closed（at-least-once 重放），不再有任何"越过未处理数据"的路径。
3. **投递记录时序修正**：`delivered_seq` 仅在本地通道**成功接收**后推进；发送失败（链已亡）不记录，重放照常投递。
4. **重放保留门控**（P2 顺修）：`PendingReceipts` 的批次驻留仅在重连启用（`reconnect_attempts > 0`）时发生，关闭重连时零额外内存。
5. 会话清理（确认丢失/`remove_job_session`）撤销回执路由并取消转发任务。

## Capabilities

### New Capabilities
（无）

### Modified Capabilities

- `network-shuffle-data-plane`: 「重放帧按投递级去重」需求改写——去重丢弃不再镜像回执；新增会话级回执路由需求。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：SessionReceiptRoute/转发任务、RemoteAck 换会话队列、去重分支、delivered_seq 时序、清理路径、重放门控。
- 测试：重连测试改按正确语义断言（重放不重投递、原 ack 经新连接完成、分支只在处理后结算）。
