# Proposal: fix-session-receipt-lifecycle

## Why

四轮 CR 在 fix-remote-replay-ack-semantics 引入的会话回执路由里发现两个缺陷：
1. **槽位清空竞态（P1）**：旧连接退出清理无条件清回执写槽。透明重连重叠窗口（旧连接退出路径的多个 await 尚未完成、新连接首帧已装自己的 sender）里，旧清理会把新连接的槽清掉，转发器无限轮询、新连接回执停摆、drain 超时 fail-closed——间歇性打断重连特性本身。
2. **转发器不监视 manager 关停（P2）**：外层 select 只看会话 cancel；无回执到达时任务永久停在 recv 上，manager.shutdown 后任务泄漏且强持 Arc<NetworkManager>。

## What Changes

1. 清槽改为通道身份守卫（flume same_channel）：连接退出只清自己安装的槽；serve 循环记录本连接的 (会话键, sender) 对。
2. 转发器外层 select 增加 manager.shutdown.cancelled() 分支，关停即退出。

## Capabilities

### New Capabilities
（无）

### Modified Capabilities

- `network-shuffle-data-plane`: 「回执经会话级路由跨越连接更替」补充槽所有权与关停语义。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`。
