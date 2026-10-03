## Context

三处无界等待（round collect、sink 写、快照 join）共享同一性质：把"慢"放大成"永久"。已核实超时放弃的安全性前提：straggler 报告被既有 stale-report 吸收路径消化，链侧按 FIFO 处理先后到达的旧/新 barrier，不级联。

## Goals / Non-Goals

**Goals:** 三个等待点全部有界；超时错误显式且可诊断；round 失败语义复用（数据面不停）；shutdown 路径随 sink 超时解锁。

**Non-Goals:** barrier 优先级、配置面、后台写中止、WAL 路径（见 proposal）。

## Decisions

**D1 — Round deadline 挂在等待循环的 select 上，而非外层整体包裹。**
在 `checkpoint_barrier_inner` 的 collect select 增加 `sleep_until(deadline)` 臂：deadline 从进入 collect 时起算（barrier 注入不计时——注入失败已有显式错误）。超时返回 `Error::Process("checkpoint round exceeded its deadline (...)")`。*外层 `tokio::time::timeout(checkpoint_barrier_inner(...))` 备选被否决*：外层取消会中断报告通道的锁获取与丢弃语义，且错误信息无法区分阶段；select 臂与既有"取消/错误/链退出"臂同层，语义对齐。默认 `CHECKPOINT_ROUND_TIMEOUT = 10 分钟`（远大于正常轮次——秒级 barrier 传播 + 快照；只为抓挂死不抓慢）；句柄存 `AtomicU64` 毫秒 + `#[cfg(test)]` 注入口（对齐 wal 的 `override_ack_drain_window_for_tests` 模式）。

**D2 — 超时放弃的 straggler 安全性由既有机制承担，不新增清理。**
超时后：仍在通道里排队的旧 barrier 会被链正常处理并报告 → 下一轮等待循环按 `report.barrier != barrier` 吸收（`kernel_handle.rs` 既有 "ignoring stale checkpoint report"）；链侧 Aligner 对同轮 barrier 的期望不受协调器放弃影响。**风险**：若旧 barrier 永不到达（通道断/链死），该链随后走链退出路径，下轮按 ended 豁免——既有语义。

**D3 — Sink 写超时 = Fatal chain 失败，不做重试。**
`tokio::time::timeout(SINK_WRITE_TIMEOUT, sink.write_batch(...))`，超时映射为 `FatalFailure`（error 文本点名超时时长）。取消语义：timeout 丢弃内部 future = 写被取消，外部系统可能已有部分副作用（at-least-once 重放兜底——与 sink 写返回 Err 的既有路径同待遇，不引入新的不确定面）。`SINK_WRITE_TIMEOUT = 5 分钟`（大批次上传合法地慢；只抓挂死连接）。副作用收益：chain loop body 不再存在无界 await，**shutdown 的 FuturesUnordered 随 sink 超时而必然返回**。

**D4 — 快照超时作用于 join，不终止后台 blocking 任务。**
`snapshot_state` 的 `spawn_blocking(...).await` 包 timeout：超时返回显式错误（走既有快照失败 → round 失败）；后台 blocking 任务若最终完成，其**只读**快照结果被丢弃，无副作用、无泄漏（任务自然结束）。`SNAPSHOT_TIMEOUT = 5 分钟`。

**D5 — 错误文案统一模式**：`"<what> timed out after <duration>"`，三处一致，可 grep 可诊断。

## Risks / Trade-offs

- [合法慢轮次（>10min）被误杀] → 阈值取远超正常形态的 10 分钟；round 失败不停数据面、下轮重试——误杀代价是丢一个 checkpoint 点而非停机；配置化列后续。
- [大批次 sink 写合法地超过 5 分钟] → 同上：chain 失败重启，at-least-once 重放；文档明示阈值语义；配置化列后续。
- [sink 超时取消留下外部部分写] → 与 sink 写返回 Err 的既有语义一致（部分写 + Fatal + 重放），不新增不确定面。
- [straggler 报告被吸收时的日志噪音] → 既有 warn 已节流于报告粒度；超时本身另有显式错误。

## Migration Plan

无配置/格式变更。回滚 = revert。已运行管线不受影响（超时只在挂死场景触发）。
