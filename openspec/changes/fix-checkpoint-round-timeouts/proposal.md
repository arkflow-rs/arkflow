## Why

P2 内核批（`openspec/CODE_REVIEW_2026-09-29.md`「checkpoint 全链路无 round 超时」+「sink 写与状态快照无超时」）。当前三处无界等待，任何一处挂起都会把"慢"放大成"永久停摆"：

1. **Round 无 deadline**：`kernel_handle.rs` 的 round 等待循环（`checkpoint_barrier_inner` 的 collect 阶段）只 select 取消/错误/报告/链退出——**没有任何时间界**。慢而未死的管线（持续背压 + barrier 在满通道排队）让 round 无限挂起；`checkpoint_lock` 串行化后续轮次，一次挂起 = checkpoint 永久停摆（`kernel_handle.rs:154` 起，等待循环约 `:203-330`）。
2. **Sink 写无超时**：`task.rs:2732` `sink.write_batch(...).await` 无超时也无取消 select——外部 sink（挂死的 HTTP/DB 连接）同时卡死数据面与**关闭路径**（chain 在 loop body 内不再轮询取消令牌，关闭时 `FuturesUnordered` 永不返回）。
3. **状态快照无界**：`barrier.rs:361-364` `snapshot_state` 的 `spawn_blocking` join 无超时——一个卡死的 state backend（如 redb 锁争用）同时冻结 chain 与 round。

对照 spec：`unified-execution-kernel` 的 "Cancellation and drain" 需求已要求"Every wait a chain performs on its own infrastructure SHALL have a bounded wait"——sink 写与快照是该条款的既有豁口；`async-checkpoint-barriers` 无轮次时限条款。

**安全性前提（已核实）**：round 超时放弃后，迟到 barrier 的 straggler 报告由等待循环既有的 "ignoring stale checkpoint report" 路径吸收（`kernel_handle.rs:305-317`），链侧 Aligner 按序处理同通道内先后到达的旧/新 barrier——**超时不会级联成链失败**，下一轮正常推进。

## What Changes

- **Round deadline**（默认 10 分钟，句柄原子量测试可注入）：collect 阶段整体包 deadline，超时显式返回错误（走既有 round 失败语义：保留上一有效 checkpoint、数据面继续、下轮重试）。
- **Sink 写超时**（`SINK_WRITE_TIMEOUT = 5 分钟`常量）：`write_batch` 包 `tokio::time::timeout`，超时以 Fatal 失败该 chain（显式错误；被取消的写可能留下外部部分副作用，由 at-least-once 重放兜底）——数据面与 shutdown 路径同时解锁。
- **快照超时**（`SNAPSHOT_TIMEOUT = 5 分钟`常量）：`snapshot_state` 的 join 包超时，超时报错走既有快照失败路径（round 失败；后台 blocking 任务若最终完成，其只读结果被丢弃，无副作用）。
- 三个超时的错误信息均显式点名超时与时长（可诊断，不与真实错误混淆）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `async-checkpoint-barriers`：新增"checkpoint 轮次 SHALL 有时间界"需求（超时显式失败、迟到报告吸收、下轮推进）。
- `unified-execution-kernel`："Cancellation and drain" 需求补 sink 写与状态快照的有界等待场景（补齐既有条款的豁口）。

## Impact

- `crates/arkflow-core/src/executor/kernel_handle.rs`（round deadline + 测试注入）
- `crates/arkflow-core/src/executor/task.rs`（sink 写超时）
- `crates/arkflow-core/src/executor/barrier.rs`（快照超时）
- 测试：round deadline（挂起报告不达 → 超时错误）；sink 超时（挂死 sink → chain Fatal 含超时字样）；快照超时（挂死 backend → 错误）；straggler 吸收（既有行为已有注释与测试，超时路径复用之）
- 无配置面变更（常量 + 测试注入，配置化列为后续）；无 wire/存储格式变更。

## Non-goals

- 不做 barrier 通道优先级（独立结构性改造）。
- 不做超时值的配置面（job/stream 配置管线另行立项；本轮常量 + 测试注入）。
- 不取消/中止超时后的 sink 后台写（tokio timeout 丢弃 future 即取消；外部系统的部分副作用由 at-least-once 语义吸收——文档如实声明）。
- 不为 WAL append/ack 路径加超时（其已有 drain 窗口与 fail-closed 语义）。
