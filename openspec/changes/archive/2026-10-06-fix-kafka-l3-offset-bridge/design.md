# Design: fix-kafka-l3-offset-bridge

## Context

L3 桥的四个缺陷（见 proposal Why 节 file:line 证据）共享一个根因主题：**位点提交决策与"已结算"事实脱钩**——批内最大值不知道别处的在途分支，异步错误不上浮到决策点，配对缺失在运行期不可见。修复全部围绕"把已结算事实带到位点提交点"。

## Goals / Non-Goals

**Goals:**

- 事务提交的组位点永远 ≤ 配对输入的连续 ack 前沿（无跳过、无丢失）。
- `send_offsets_to_transaction` 的任何失败模式都使本批 fail-closed（abort + 重放）。
- 配对缺失在启动期被拒绝。
- L3 组的 broker 位点只有事务一个写者。

**Non-Goals:** 见 proposal Non-goals（跨事务逐 offset 账本、L2、跨进程注册表、auto.offset.reset 文档）。

## Decisions

### D1 — 前沿快照经组注册表暴露，输出侧钳制

`kafka_txn.rs` 的组注册表现存 `(group → 消费者组元数据)`；扩展为 `(group → { metadata, frontier: Arc<CommitFrontier> })`——输入侧 `KafkaAck::ack` 既有 frontier（`input/kafka.rs:87` 起）即注册对象，零新状态。输出侧 `transactional_offsets_for_batches` 推导后逐分区：

```
commit_next[p] = min(batch_max_next[p], frontier_next[p])
commit_next[p] = max(commit_next[p], last_sent[p])   // 进程内单调，防回退重放循环
```

`last_sent` 挂在输出实例上（每 producer 生命周期）。**钳制即可，无需 gap fail-closed**：前沿定义保证低于它的 offset 全部结算（其 ack 在输出确认后完成），高于它的不提交——路由/过滤/扇出拓扑下不再有"批内缺失但未结算"的歧义。尾部滞后由后续事务收敛；停机前最后一批的未结算部分不提交 → 重放（at-least-once 下界，docs 说明）。

### D2 — 异步错误：delivery 回调记录 + commit 前检查

rdkafka 的事务性 offset 发送错误经 delivery 回调异步上浮（当前 producer 的回调只服务发送确认）。方案：回调中除既有路径外记录 `txn_offset_error: Mutex<Option<KafkaError>>`（挂 producer 共享句柄）；`write_batch` 在 `commit_transaction` **前**检查——有错即走既有 abort 路径（`output/kafka.rs:420-434`）并返回错误。备选「改用返回 Result 的 API」被否：当前 rdkafka 版本的该调用不返回同步结果，升级 rdkafka 才有，牵连面大（Non-goal）。

### D3 — 配对校验放 connect 阶段（构建完成后）

输入 build 时向注册表声明 `transactional_offsets`（组名）；输出 build 时声明 `offset_commit_group`（组名）。校验挂点：`arkflow-core` 的 resource_guard connect 段（所有组件已构建、连接前）——注册表中每个声明了 `transactional_offsets` 的组必须至少被一个输出的 `offset_commit_group` 指名。缺失 → `Error::Config`，启动失败。跨 crate 依赖方向：core 提供校验回调注册，plugin 在组件 build 时挂声明（与现有 `component-config-honesty` 校验同层）。

### D4 — undo 分支

`KafkaAck::undo`（`input/kafka.rs:1042-1049`）在 `transactional_offsets` 下跳过 `store_offset` 调用，仅做内存 frontier 回退（既有 `restore_partition_if_current` 路径）。非事务路径行为不变。

## Risks / Trade-offs

- **前沿滞后造成位点提交延后一批**：稳态下每次事务提交的是上一批结算的前沿——EOS 语义不变（提交当刻数据与位点一致），仅位点推进节奏变慢；testcontainers EOS 测试的时间窗断言需复核。
- **delivery 回调是全局热路径**：错误记录为一把短临界区的 Option 写，无分配；不影响吞吐。
- **last_sent 单调防回退**：进程重启后 last_sent 归零，但 broker 已提交位点 ≥ 前沿初值（重放对齐），无回退风险。

## Migration Plan

单 PR。无配置迁移；错配部署（输入声明 transactional_offsets 无配对输出）升级后启动失败——这正是要暴露的静默丢失配置，release notes 说明。

## Open Questions

（无——rdkafka API 形态已在 D2 定案按回调捕获；若实现时发现当前版本回调不含事务性 offset 错误码，回退方案是 commit 前显式 `position()` 与前沿比对校验。）
