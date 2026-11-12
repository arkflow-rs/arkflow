---
sidebar_position: 6
title: 原子作业升级
---

# 原子作业升级

经典的作业升级是一条六步手工管线：停止作业、等待收敛、打保存点、等保存点完成、提交升级、再启动。每一步都是操作员延迟，作业在步骤之间处于停止状态。

**原子模式**把同样的序列放进一次 API 调用：

```text
POST /api/v1/jobs/{id}/upgrades
{
  "mode": "atomic",
  "spec": { ...版本号严格更大的新 JobSpec... },
  "expected_generation": 7
}
→ 202 { "upgrade_id": "job-upgrade-12", "state": "saving_savepoint", ... }
```

Hub 编排以下阶段并立即返回 `202`。作业在保存点期间继续处理；停机窗口压缩为“保存点 + 恢复”的时长。停止模式（stopped）原样保留，供计划内维护使用。

## 暂停：升级编排暂停与作业暂停 {#pause-orchestration-vs-job}

「暂停」指两套不同的机制，归属与生命周期都不同——不要把其中一个当作另一个的证据：

| | 升级编排暂停 | 作业暂停 |
| --- | --- | --- |
| 路由 | `POST /jobs/{id}/upgrades/{upgrade_id}/actions`，体为 `{"action":"pause"}` | `POST /jobs/{id}/actions/pause`（或 `PUT /jobs/{id}/desired-state {"state":"paused"}`） |
| 被暂停的对象 | 升级阶段机：停止推进并冻结阶段超时 | 作业本身：Hub 先落恢复工件，再停止作业 |
| 数据处理 | 保存点阶段暂停时旧代仍在继续处理 | 作业停止消费并释放计算资源 |
| 对 `desired_state` 的影响 | 无 | `running` → `paused`（resume 后回到 `running`） |
| resume 的含义 | 重新武装阶段超时并继续升级 | 提升世代并从暂停工件重新起跑 |
| 持续时间 | 应尽量短；长时间暂停会拉长重放窗口 | 可以任意长（工件被保留期 pin 住） |

两者双向互斥：

- 升级编排未进入终态期间，暂停与恢复会以 `409 orchestration_in_progress` 被拒绝，与下文列出的 desired-state 和生命周期动作拒绝并列。
- 暂停相位存在期间（`capturing`、`stopping`、`paused` 或 `resuming`），原子模式与停止模式的升级都会以 `409 job_paused` 被拒绝。先 resume，或 stop 以清除暂停记账；之后既有升级前置条件照常适用。

不要把编排暂停理解为作业已静默：旧代可能仍在处理数据。需要作业停止并在之后从某个已捕获点继续时，请使用作业暂停；捕获后停机序列与重放边界参见[流式作业 → 暂停与恢复](/zh-Hans/docs/build/jobs#pause-and-resume)。

## 阶段状态机

```text
saving_savepoint ──▶ committing_version ──▶ verifying ──▶ succeeded
   │ (重试/超时)                            │ (超时 / 无法收敛)
   ▼                                       ▼
 aborted                                 rolling_back ──▶ rolled_back
                                            │ (恢复失败 / 无法收敛)
                                            ▼
                                          failed
```

| 阶段 | 发生什么 | 默认超时 |
| --- | --- | --- |
| `saving_savepoint` | 以当前代数走普通检查点路径派发保存点。失败轮次最多重试 2 次；耗尽或超时则放弃（aborted），作业不受影响继续运行。 | 5 分钟 |
| `committing_version` | 一次带代数围栏的写入提交新 spec 并设 `desired_state = running`。恢复指针不由该写入携带——它已指向刚完成的保存点，围栏写入会保留它。 | 1 分钟 |
| `verifying` | 普通调和启动新代（同时停掉旧代）。编排观察直到作业在目标版本上运行。 | 10 分钟（请求可用 `verify_timeout_ms` 覆盖） |
| `rolling_back` | 用同一个保存点、同一种围栏写回恢复上一版本；调和重启之。终态 `failed` 会把作业留在停止态且恢复指针原样保留，等待人工处置。 | 10 分钟 |

阶段转移会以 `job.upgrade` 事件广播到 `GET /api/v1/events/stream`；操作员动作全部审计（`job.upgrade.atomic.initiate/pause/resume/cancel/rollback`）。

## 重放语义

以检查点为媒介的切换并非对所有 Sink 都无重复：旧代在保存点屏障*之后*处理的事件会被新代从保存点的源位点重放。

- 事务性 Sink：精确一次（重放事件被 Sink 事务去重）。
- 非事务性 Sink：至少一次。重复窗口以“保存点→切换”的间隔为界，编排器通过在保存点完成的同一个调和 tick 内提交来压缩该窗口。

## 编排所有权与冲突

编排处于非终态期间，作业被围栏保护：保存点、提交、暂停阶段中通用调和器推迟重放置（重放置会 bump 保存点派发所键控的代数），且作业级变更被
`409 orchestration_in_progress` 拒绝：

- `PUT /jobs/{id}/desired-state`
- `POST /jobs/{id}/actions/{start|stop|restart}`
- `POST /jobs/{id}/actions/{pause|resume}`
- 再次升级（原子或停止模式）与手动版本回滚

观察阶段（`verifying`、`rolling_back`）刻意放行普通调和——它正是那里的启动机制。

检查点保留永不删除活跃编排引用的工件；编排进入终态后解除钉住。

## 状态查询与动作

```text
GET  /api/v1/jobs/{id}/upgrades/{upgrade_id}      → 阶段、保存点、超时、最近错误
POST /api/v1/jobs/{id}/upgrades/{upgrade_id}/actions  {"action": "pause"|"resume"|"cancel"|"rollback"}
```

- `pause` 冻结编排；`resume` 重新武装该阶段超时。在保存点阶段暂停时旧版本继续运行，但在验证中途暂停可能把作业冻结在切换间隙（旧版本已停、新版本未起），直到 `resume`。长时间暂停也会拉长重放窗口——建议改为取消后发起新的升级。
- `cancel` 释放作业（verifying 阶段取消后，新版本由普通调和自行收敛继续）。
- `rollback` 在编排进入 verifying 后可用。

## Hub 重启与故障切换

编排阶段、超时、保存点引用都是持久化的（`cp_job_upgrades`）。带非终态编排启动或获得领导权的 Hub 会幂等地恢复它：保存点阶段在丢失命令结算后重新派发一轮；提交阶段应用冲突解释规则（已应用过的提交推进到下一阶段，其余放弃）；观察阶段继续观察。任何操作都不会盲目重放——幂等性以作业的可观察状态为准，而非阶段行。

## 控制台

Jobs 页对运行中的作业提供**原子升级**入口（DAG 编辑器，无需选择保存点），并在详情面板展示活跃编排的阶段、保存点与动作。确认对话框中会展示上述重放语义说明。
