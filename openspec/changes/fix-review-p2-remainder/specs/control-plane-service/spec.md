# control-plane-service Delta

## ADDED Requirements

### Requirement: Storage actor SHALL isolate blocking database work from async workers
存储 actor 的命令执行 SHALL 将同步数据库工作（SQLite/rusqlite 含 busy_timeout 等待）隔离到阻塞线程池，MUST NOT 在 tokio worker 线程上同步阻塞；命令 FIFO 顺序 SHALL 保持不变。actor SHALL 暴露队列深度 gauge 指标，并对每条命令的派发记录操作名与耗时的 tracing。

#### Scenario: busy_timeout 不再占用 worker
- **WHEN** SQLite 后端某命令因锁竞争在 `busy_timeout`（5s）内重试
- **THEN** 等待发生在阻塞线程池的线程上，tokio worker 线程不被同步阻塞，且后续命令仍按 FIFO 顺序执行

#### Scenario: 队列深度可观测
- **WHEN** 存储命令到达速率暂时超过执行速率
- **THEN** actor 队列深度 gauge 反映积压，操作级 tracing 记录各命令操作名与耗时

### Requirement: Placement plan compilation SHALL be cached per job spec
reconcile 循环对 Job 的 plan 编译结果 SHALL 以 (job id, spec 内容哈希) 为键缓存；同一 tick 与跨 tick 的重复编译 MUST 命中缓存，spec 未变更的 Job MUST NOT 每 tick 重复编译。缓存 SHALL 有容量上限并在超限时整体清空（防无界增长）。

#### Scenario: spec 未变更时不重复编译
- **WHEN** reconcile tick 处理一个 spec 与上一 tick 相同的 Job
- **THEN** plan 编译命中缓存，不重新执行 JobPlan::compile

#### Scenario: spec 变更后缓存失效
- **WHEN** 某 Job 的 spec 字符串发生变更
- **THEN** 缓存键不再命中，新 spec 被重新编译并替换缓存条目

### Requirement: Maintenance loops SHALL log swallowed errors
控制面周期维护任务（retention 清扫、事件裁剪等）对存储操作的错误 MUST 显式记录日志（含任务名与错误），MUST NOT 以 `let _ =` 静默吞掉。

#### Scenario: 清扫失败可感知
- **WHEN** 某周期清扫任务的存储调用返回错误
- **THEN** 该错误以带任务名的 warn/error 日志记录，而非被静默丢弃
