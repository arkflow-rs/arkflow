# Proposal: add-remote-transparent-reconnect

## Why

远程边 fail-closed 语义使任何网络抖动 = 作业失败 + 代数围栏重放置（PLANNING.md 第八节 P2 harden-shuffle-recovery 的第①项，前序变更 `add-remote-failed-receipt` 交付了②）。本变更交付①+③：bounded window 内的有限次透明重连 + 接收端投递级 seq 去重，把常见网络瞬断从"作业失败"收敛为"无感恢复"。

## What Changes

1. **上游监督循环**：边会话由监督任务持有；流级失败（任一半程出错）在预算内（`reconnect_attempts`，默认 5）以指数退避重拨，重放全部**未回执**数据帧（复用原 seq）。
2. **接收端投递级去重**：按会话键记忆已投递的最大 seq；重放帧 seq ≤ 已投递即丢弃并**镜像补发 Acked 回执**（原回执可能随断连丢失；上游回执幂等）——端到端在重连路径上收敛到 effectively-once。
3. **接收端丢连宽限**：EOF-无-Eos 的失败报告延迟 `reconnect_grace`（默认 10s）；宽限内同会话键重新注册即抑制，超时确认丢失才报告并清理注册表。
4. **注册表保留**：宽限期内的会话注册不被连接退出清除，重拨连接即刻可路由；确认丢失的延迟任务负责清理。
5. **泵语义细化**：可重连边的泵在失败/取消退出时**不**中止分支（留给监督循环在预算耗尽时统一中止）；排空后的读循环继续收晚到回执（保持既有不变量）。
6. `reconnect_attempts: 0` 完整保留旧的立即 fail-closed 行为（既有测试以此钉住）。
7. 会话清理（remove_job_session）同步清除去重与注册计数状态。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `network-shuffle-data-plane`: 「远程边失败 fail-closed」修订为「预算内透明重连 + 预算耗尽 fail-closed」；新增投递级去重与宽限语义。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：`PendingReceipts` 重放日志、监督循环（`attach_stream_with_redial`/`run_edge_connection`/`replay_pending`）、接收端 `delivered_seq` 去重、`session_registrations` 宽限、配置两个字段。
- 新测试：透明重连重放恢复、预算耗尽 fail-closed；既有 28 个远程测试回归 + 全 core 506 绿。

## Non-goals

跨节点死节点的恢复（对端进程死亡仍走 fail-closed → 围栏重放置）；处理级 exactly-once（投递级去重已覆盖重连路径，处理中批次的 at-least-once 残余维持既有语义）；重连期间的 barrier 保全（断连窗口内的 checkpoint 轮仍按超时失败）。
