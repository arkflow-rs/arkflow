## Why

`verify-hub-production-readiness` 的 soak_ci(2026-09-13)抓获一个稳定复现的生产缺陷:Hub 重启会楔死 job 生命周期。证据链:(1)`Hub::with_storage`(`hub.rs:506-510`)只挂载存储,**不恢复任何内存态**——终端态 skip 记忆随重启丢失,reconcile 对 agent 已在运行的 job 重新派发 `job_start`(连 `/operations` 读 API 也返回空);(2)agent 侧 `JobRuntime::start`(`agent.rs:419-427`)对重复 start 先取消旧内核再 **无界 await** 其退出,teardown 在负载下挂起时 `starts` 互斥量被永久持有,该 agent 后续一切 start 阻塞 → 租约过期 → 重试 ×3 → 预算耗尽 → 操作永久停在 Dispatched/Queued(日志中 reconcile 每 50ms tick 撞一次 "exhausted its retry budget")。30s 内可复现,违背 `control-plane-reconciliation`「Recovery SHALL restore unfinished Operations」的语义闭环,并阻塞验证 change 的全部重启场景。

## What Changes

- **启动恢复持久操作**:`serve_hub` 在绑定监听前,从 `cp_operations` 恢复最近操作(既有 `list_operations`,≤1024 行)反序列化回内存映射——终端态 skip 记忆跨重启存活,`/operations` 读 API 重启即完整;恢复不可解析的行 fail-open 跳过并告警。
- **Agent 侧 teardown 有界化**:`JobRuntime::start` 等待前内核退出加 10s 上限;超时记 WARN 后继续新 start(旧任务已取消、detached 收尾)。冗余 start 从此要么完成、要么终态失败,**不再可能无限阻塞**。
- 未恢复前内核状态目录锁导致的偶发 start 失败由既有重试机制吸收(终态失败 → reconcile 重试),不再产生无限 pending。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `control-plane-reconciliation`:扩展「Reconciliation triggers and recovery」——恢复 SHALL 同时重建终端态生命周期结果的内存映射,使 dispatch-skip 决策跨重启存活;重启后 SHALL NOT 对当前代已满足的 (node, job, generation) 重新派发生命周期命令。
- `compute-node-agent`:新增「Redundant lifecycle starts settle within a bounded wait」——冗余 `job_start` 遇到超时未完成的旧内核 teardown 时 SHALL 在有界时间内以终态结果收敛,不得无限阻塞。

## Impact

- `crates/arkflow-server/src/hub.rs`:`restore_persisted_operations()` 方法 + `serve_hub` 启动调用(有存储时)。
- `crates/arkflow-server/src/agent.rs`:`JobRuntime::start` teardown 等待加界(WARN 遥测顺带为 teardown 挂起的根因分析留下信号)。
- `two_node_job_smoke` 等手动路由(`hub_router`)不经 `serve_hub`、且不重启 Hub,不受影响;`arkflow-server` bin 与 fleet harness(经 `serve_hub`)自动获得恢复。
- 无 schema 迁移、无新依赖。
- 明确不在本 change:内核 teardown 挂起的根因分析与根治(本 change 只做有界化 + 遥测)、重试预算耗尽后 job 级楔死的运营语义(spec 既定)、Hub 高可用。
