## Why

对 `repair-hub-restart-lifecycle-wedge` 落地代码（2007483）的 review 发现:Agent 侧 kernel 崩溃的可见性存在两扇门,根源同为一处——**`tasks` 表的读者与 `take_finished` 的轮询排空之间存在"死而未排空"窗口**:

1. **同代幂等幻报成功**:`JobRuntime::start` 的同代幂等分支(`agent.rs:417-427`)只比较 `task.generation == generation`,不检查 `task.handle.is_finished()`。若 kernel 在"poll tick 取回命令之后、命令任务执行 `start()` 之前"崩溃(Hub 重启重派同代 `job_start` 正是该命令的来源),重投的 start 会对一个已死 kernel 幻报 `Ok`,Hub 短暂记录假 `Succeeded` 操作。
2. **替换路径吞掉崩溃观测**:generation bump 替换路径(`agent.rs:436-438`)对被移除条目 `await_previous_teardown` 后**丢弃 join 结果**,且不调用 `state.close()`(`take_finished`(`agent.rs:921-929`)/`stop`/`stop_all` 三个正常路径都会关闭并上抛,唯独这条路径既漏关闭又把崩溃观测永久吞掉——`take_finished` 再也见不到该条目)。gen bump 恰好落在崩溃之后的窗口内时,Hub 永远不知道这次崩溃。

两扇门都自愈(Hub 经观测/重试最终收敛),但都产生可观测的错误状态:假 `Succeeded` 记录、或静默消失的崩溃。修复成本低(局部几行 + 确定性单测),且同一份改动同时关闭两扇门。

## What Changes

- **同代幂等加存活判断**:`start()` 幂等早退条件收窄为「同代 **且** kernel 未退出」;已死 kernel 落入现有拆除/重启路径重新拉起。
- **替换路径的崩溃可见化**:`await_previous_teardown` 改为返回 join 结果(超时为 `None`);`start()` 拿到 `Err` 时把 `(job_id, 旧generation, Err)` park 进 `pending_observations`,经既有 job-observations 通道投递给 Hub——门 A(同代重投)与门 B(gen bump 替换)共用此机制。优雅取消(`Ok(())`)不 park,避免正常替换制造假警报。
- **补句柄关闭**:替换路径对被移除条目补 `state.close()`,与 `take_finished`/`stop`/`stop_all` 对齐。
- **fleet harness 本地跳过开关**:`staircase_ci`/`soak_ci` 支持 `ARKFLOW_SKIP_FLEET=1` 早退,默认行为不变(CI 照常执行)。
- **Agent HTTP 客户端 loopback 判定用 IP 语义**:host 解析为 `IpAddr` 后按 `is_loopback()` 判定(覆盖 127/8 全段、`::1`、`0.0.0.0`),`localhost` 保留字面量匹配;不再只认 `127.0.0.1` 字面量。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `compute-node-agent`:新增「同代生命周期 start 的幂等与崩溃可见」要求——对健康 kernel 的同代重投 SHALL 幂等返回成功(不churn数据面);对已退出 kernel 的同代重投与 gen bump 替换 SHALL 先排空旧 kernel 并将其崩溃经 job-observation 通道上报 Hub,SHALL NOT 产生幻报成功或静默吞没崩溃。

## Impact

- `crates/arkflow-server/src/agent.rs`:`JobRuntime::start`(幂等判断、park 观测、`state.close()`)、`await_previous_teardown`(返回值)、`build_agent_client`(loopback 判定)。
- `crates/arkflow-server/tests/fleet_readiness.rs`:env 跳过开关。
- `openspec/specs/compute-node-agent/spec.md`(归档时经 delta 同步)。
- 无 schema 迁移、无新依赖、无 Hub 侧改动;重试预算语义不变(park 的崩溃观测是 job 级观测,不占用命令操作预算)。
- 收敛性:Hub 先后收到 failed 观测与 succeeded 启动结果,乱序无害——failed 触发的 re-drive 命中健康同代幂等返回 `Ok`,无数据面 churn;park 机制保证观测投递失败时不丢(`pending_observations` 跨会话重试)。

## Non-goals

- **exit-watcher 架构**(kernel 退出即清理 map,消灭整类竞态):本 change 只做外科修补收窄窗口;若此类"读者未赶上排空节奏"的缺陷第三次出现,再立项换架构。
- 内核 teardown 挂起的根因分析与根治(沿用前序 change 的 non-goal,只保有界化)。
- Hub 侧行为与 `distributed-job-runtime`/`control-plane-reconciliation` 语义:本轮缺陷全部在 Agent 侧。
- fleet harness 的延迟/RSS 门控、CI 预算调整:skip 开关只服务本地迭代,CI 默认路径不动。
