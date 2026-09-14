## Context

soak_ci 复现链(2026-09-13,完整证据见 `verify-hub-production-readiness` 的 tasks 注记与日志):

1. `serve_hub` 运行中 Hub 重启(取消 → 重建 `Hub::with_storage(同 store)`)。`with_storage` 只设置 `hub.storage = Some(storage)`(`hub.rs:506-510`),**内存态全空**:`operations` 映射(终端态 skip 的唯一依据,`hub.rs:975-989`)、节点队列、租约全部丢失。
2. 新 Hub 的 reconcile tick 发现 desired running + 内存无该 (node, job, generation) 的 Succeeded 记录 → 重新派发 `job_start`——但 agent 上旧内核还在跑(重启不影响数据面)。
3. agent `JobRuntime::start` 处理重复 start:`existing.cancellation.cancel()` 后 **无界 `existing.handle.await`**(`agent.rs:419-427`),等待旧内核 WAL-safe teardown。teardown 挂起 → `starts` 互斥量(`agent.rs:404`)被持有不释放 → 该 agent 后续一切 start 阻塞。
4. 命令租约(5s)过期 → `expire_stale_job_operations` retry++ → reconcile 重入队 → 再次阻塞 → ×3 → 预算耗尽:`enqueue_with_metadata`(`hub.rs:2951-2981`)按 (node, resource, operation, generation) 继承最大 retry_count,≥3 即拒绝 —— reconcile 每 tick 撞 rejection(soak 日志 3942 条),操作永久停在 Dispatched/Queued。
5. 附带发现:重启后 `/operations` 读 API 返回空(`hub.operations()` 只读内存映射,`hub.rs:3671-3679`),直到产生新操作。

关键既有设施:`ControlPlaneStore::list_operations(node_id)`(`storage.rs:3015-3020`,ORDER BY created DESC LIMIT 1024)返回含全量 `operation_json` 的行;`expire_stale_job_operations` 的过期语义完整(重试计数随 json 持久化)。

## Goals / Non-Goals

**Goals:**

- Hub 重启后:终端态 skip 记忆存活(不对已满足的生命周期重复派发)、`/operations` 读 API 即时完整、未完成操作经既有过期/重试机制收敛。
- 冗余 start 的最坏后果从「无限阻塞」变为「有界失败 → 终态 → 既有重试吸收」。
- soak_ci 的重启场景可关闭。

**Non-Goals:**

- 内核 teardown 挂起的根因分析与根治(本 change 加 10s 有界 + WARN 遥测,根因留待专门排查)。
- 重试预算耗尽后 job 级「楔死直至 generation 递增」的运营语义(spec 既定行为,非缺陷)。
- `with_storage` 同步签名改造、手动 `hub_router` 路径的恢复(smoke 不重启 Hub,无需)、Hub 高可用。

## Decisions

### D1:恢复点放在 `serve_hub`,不在 `with_storage`

`with_storage` 是同步构造器,StorageActor 是异步边界,无法在构造器内 await;`serve_hub` 是生产唯一入口(`arkflow-server` bin 与 fleet harness),在解析地址、**绑定监听之前**执行恢复,满足 spec「before reporting readiness」。手动 `hub_router` 路径(仅测试使用)不重启 Hub,无需覆盖——边界写入代码注释。

被否决:首次 reconcile tick 时懒恢复——把恢复延迟与 50ms tick 语义耦合,且恢复完成前 skip 仍会漏,窗口内照样重复派发。

### D2:恢复实现 = 反序列化 `operation_json` 回内存映射,fail-open

`list_operations(None)`(≤1024 行,恰好等于 `MAX_OPERATIONS`)→ 逐行 `serde_json::from_str::<HubOperation>` → 插入内存映射(已存在则跳过;达到 `MAX_OPERATIONS` 即止)。解析失败的行 WARN 跳过——恢复是可用性优化而非正确性依赖:漏掉一行最坏导致该 job 重复派发一次(回到现状),不能让单行坏数据挡住启动。

恢复后的未完成操作交给**既有机制**收敛:json 携带 `expires_at_ms` → `expire_stale_job_operations` 过期 → TimedOut + 重试 → reconcile 重入队 → agent 侧(配合 D3)有界完成。不需要为「无命令的恢复操作」发明新路径。

### D3:teardown 等待 10s 上限,超时 WARN 后继续

```rust
if let Some(existing) = existing {
    if tokio::time::timeout(KERNEL_TEARDOWN_JOIN_TIMEOUT, existing.handle)
        .await
        .is_err()
    {
        warn!(job_id = %job_id, "previous kernel teardown did not finish within the bounded wait; continuing with the new start");
    }
}
```

- 超时**不 abort** 旧任务:它已被 cancellation 取消,detached 收尾即可;abort 打断 redb/文件句柄的 drop 语义风险更大。
- 超时后继续新 start 的真实风险:新旧内核短暂并存,`state_root`(version-generation 键控)上的 redb 文件锁可能让新内核的 state 打开失败 → start 终态失败 → 既有重试吸收 → 旧内核最终退出后重试成功。**有界的失败循环优于无界的挂起**,且失败可见(job last_error / 操作终态)。
- `KERNEL_TEARDOWN_JOIN_TIMEOUT = 10s`:远大于正常 teardown(smoke 与 staircase 中为毫秒级),远小于命令租约(5s)…… 需注意 10s > 租约 5s——等待期间命令会过期一次并重试,新命令会在 guard 上排队;10s 后 guard 释放,排队的重试成功。两常数独立生效,不引入耦合。

被否决:给旧 teardown 注入强制取消(重新设计内核 stop 语义,超出修复范围);把等待改成轮询 tasks 映射(复杂度无收益)。

### D4:遥测为根因分析铺路

teardown 超时 WARN 带 `job_id` 与等待时长;恢复完成打一条 info(恢复条数)。teardown 挂起的根因(内核 stop 路径的哪个 await)留待专门排查——有日志后,下次复现即可定位。

## Risks / Trade-offs

- [恢复 1024 行操作增加启动延迟] → 单次 SQLite 顺序读 + 反序列化,毫秒级;远小于既有 storage recovery。
- [恢复的 pending 操作其命令已随旧 Hub 消失] → 既有过期/重试路径收敛(D2);`expires_at_ms` 随 json 持久化,恢复后第一次 sweep 即处理。
- [teardown 超时后新旧内核并发的 state 锁竞争] → 失败终态 + 重试吸收,有界;WARN 可观测。
- [手动 `hub_router` 路径无恢复] → 仅测试使用且不重启 Hub;边界注释写明。

## Migration Plan

纯增量部署即生效;回滚即回到楔死现状(无数据损坏)。恢复不写库、不改 schema。

## Implementation Additions(2026-09-14,fleet harness 排障中发现并落地)

实施与 soak 验证循环中确认了四个同威胁模型的相邻缺口,均已随本 change 修复(tasks 2.3/2.4):

- **loopback 代理绕过**:本机拦截代理(Clash 类,:7890)会劫持 agent 对 127.0.0.1 Hub 的请求(register 间歇 400)——loopback Hub 一律 `no_proxy`。
- **请求硬超时**:agent client 无超时,Hub 进程被杀后 accept-backlog 中"已建立永不响应"的连接把 agent 永久挂起(11/16 agent 静默失踪的直接原因)——connect 5s / total 10s,呼应「Hub 暂时不可用 → 有界退避」。
- **同代幂等 start**:重复 `job_start`(generation 相同)直接幂等成功,不再取消/重启健康内核;异代才真正重启。这使 D3 的有界等待降级为最后防线(Hub 重发场景不再触碰 teardown)。
- **投递失败保结果**:`deliver_result` 失败时仍返回 Ok 让结果进入 CompletedCommandCache——此前投递 401 会使结果既不达 Hub 也不入缓存,重发后无物可重放,操作在重试预算耗尽后楔死。修复后:会话终结 → 重注册 → Hub 重发过期命令 → 缓存重放恰好一次,与 spec「Expired session does not lose a terminal command result」精确一致。

## Open Questions

- 无。
