## Context

Agent 的 `JobRuntime` 用一个 `tasks` map 跟踪运行中的 kernel,死亡清理靠 `take_finished` 的**轮询排空**(每 poll tick 扫 `is_finished()`,`agent.rs:913-933`)。任何在两次排空之间读 map 的代码都会看到"死而未排空"的条目。review 发现两个读者踩中此窗口:

- 同代幂等分支(`agent.rs:417-427`)只比对 generation,不查存活 → 对已死 kernel 幻报 `Ok`(门 A);
- generation bump 替换路径(`agent.rs:436-438`)丢弃 join 结果、不 `state.close()` → 崩溃观测被静默吞掉(门 B)。

一个关键既有约束限制了修复的形状:`run_session` 中**命令任务返回 `Err` 是会话致命的**(`agent.rs:1500-1505`,Err → abort 全部命令 → 拆会话重注册)。因此"发现旧 kernel 崩溃"不能作为命令错误上抛。

`run_session` 的 poll tick 内先 `take_finished` 再取命令(`agent.rs:1516-1545`),同 tick 内有保护;窗口只在"跨 tick"方向(取命令之后、下次排空之前崩溃)。

## Goals / Non-Goals

**Goals:**

- 已死 kernel 不再获得幻报成功:同代重投落入重启路径,新 kernel 真正拉起。
- 被替换 kernel 的崩溃经既有 job-observation 通道到达 Hub,两扇门共用一份机制。
- 替换路径的 `state.close()` 与 `take_finished`/`stop`/`stop_all` 对齐。
- 决策语义可确定性单测(不依赖时序)。

**Non-Goals:**

- exit-watcher 架构(kernel 退出即清理,消灭竞态类):仅记录为后续方向,本 change 收窄窗口。
- teardown 挂起的根因分析(前序 change non-goal,维持)。
- Hub 侧、重试预算、spec 语义(除新增 ADDED requirement 外)的任何改动。

## Decisions

1. **外科修补而非 exit-watcher 重构**。watcher 形状(kernel 退出即锁 map 清理 + push 观测)能消灭整类竞态,但需与 placeholder-handle 注册(`agent.rs:544-569`)、`starts` 锁序、stop 路径全部重新对齐,触碰面大。本 change 沿用仓库"review 缺陷外科关闭"的先例,只在幂等分支加 `!handle.is_finished()` 收窄窗口。watcher 记录于此:若本类缺陷第三次出现,立项换架构。

2. **崩溃经 park 观测上报，而非命令错误**。三选一:①`start()` 返回 `Err` → 会话致命,为一个已发生的崩溃付出会话重建代价,排除;②维持现状(幻报 `Ok`)→ 掩盖崩溃,正是要修的缺陷;③把 `await_previous_teardown` 改为返回 `Option<Result<(), String>>`(超时为 `None`;实现时定为 `String` 而非 `arkflow_core::Error`,因为 join 层 panic 错误也要归一,且 `FinishedJob` 的 payload 本就是 `String`),`start()` 拿到 `Some(Err(e))` 时 park `(job_id, 旧gen, Err(e))` 进 `pending_observations`,下个 tick 走正常 job-observations 通道。选 ③:命令本身报 succeeded(新 kernel 确实起来了,是事实),崩溃作为 job 级观测独立到达,不占命令重试预算。

3. **只 park `Err`,不 park `Ok(())`**。gen bump 对健康 kernel 的正常替换走 `Ok(())`,若也 park 会为每次正常升级制造假 failed 警报。`None`(teardown 超时、任务 detached)不 park——该路径已由「Redundant lifecycle starts settle within a bounded wait」要求覆盖,新 start 的 state-dir open 失败会以显式失败结果收敛。

4. **`state.close()` 放在 teardown join 之后、判定结果之前**。无论 `Ok`/`Err`/`None`,被移除条目的 state backend 都应关闭(与 `stop` 的 `agent.rs:970-974` 对齐);`Ok` 早退分支(健康同代幂等)不经过此路径,不动。

5. **锁定顺序**:park 发生在 `starts` guard 持有期间,取 `pending_observations` 锁。既有路径中无人以「pending_observations → starts」顺序取锁(`take_finished`/`park_observations` 均不碰 `starts`),无死锁风险。

6. **确定性测试**。竞态时序不做注入;决策语义在 `mod tests` 内直接构造:用已完成且返回 `Err` 的 `JoinHandle`(`tokio::spawn(async { Err(...) })`,yield 至 `is_finished()` 后再入 map)调用同代与 gen bump 两种 `start()`,断言:不早退、park 含崩溃观测、`state.close()` 被调用、新 kernel 注册。健康同代幻报防护由独立回归测试覆盖。落地细节:每个测试用独立 job_id——真实 kernel 的 state 目录按 `(job_id, generation)` 派生且 redb 持独占文件锁,并行测试共用 job_id 会互撞;合成 backend 的路径同样加进程内序号。`await_previous_teardown` 的返回值语义扩展现有 wedged 测试(`None`)+ 新增 `Some(Err)` 用例。

7. **loopback 判定**:`url::Url::host_str()` 去方括号后 `parse::<IpAddr>()`,成功则 `is_loopback()`(覆盖 127/8、`::1`、`0.0.0.0`),失败则保留 `localhost` 字面量匹配。不引入新依赖。

8. **fleet skip 开关**:`staircase_ci`/`soak_ci` 入口检查 `ARKFLOW_SKIP_FLEET=1`,命中打印说明并返回;文件头注释写明。CI 不设该变量,默认路径零变化。

## Risks / Trade-offs

- [Hub 侧乱序:failed 观测(旧 gen)先于/后于 succeeded 启动结果到达] → 两者都带 generation,fenced 处理;failed 触发的 re-drive 命中健康同代幂等返回 `Ok`,无 churn。乱序最坏情形是短暂的观测-结果交错,由下一轮 reconcile 收敛。
- [park 的崩溃观测重复投递(会话重建后重发)] → Hub 对 job 观测按 (job, generation) 幂等处理,重复 failed 不改变终态。
- [`ARKFLOW_SKIP_FLEET` 被误设导致 CI 静默跳过] → 命中时打印醒目 WARN;CI 环境不定义该变量;文档写明仅限本地。
- [窗口收窄后仍非零(轮询架构的固有属性)] → 接受;消灭而非收窄属 exit-watcher 的 non-goal 范畴。

## Migration Plan

纯 Agent 侧行为收紧,无 schema/协议/配置变更:随二进制发布即生效,回滚即回退二进制。Hub 对新增的 crash 观测按既有观测通道处理,新旧混布(Hub 旧 + Agent 新)安全——旧 Hub 本就接受 failed 观测。

## Open Questions

(无)
