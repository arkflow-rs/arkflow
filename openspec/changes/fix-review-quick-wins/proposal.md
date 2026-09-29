## Why

2026-09-29 的 v1.0 就绪度代码审查（详见 `openspec/CODE_REVIEW_2026-09-29.md`，全部论断带 file:line）发现五个修复量小、风险消除大的缺陷，全部属于"静默丢数据 / 静默挂死 / 静默失联"级别：

1. **remote 投递去重竞态（P1-3）**：`crates/arkflow-core/src/executor/remote.rs:2651-2687` 的去重是"检查 → `send_async().await` → `delivered.insert()`"，且 insert 为**无条件覆盖而非 max**。重连重叠期同一 seq 可双投递（聚合翻倍）；旧连接 send 完成后把去重水位回写为低位，后续重放全部穿透。违反 `network-shuffle-data-plane` spec 第 156 行既有的"记忆已投递的最大 seq、仅在成功接收后推进"要求——实现与规格背离。
2. **Kafka L3 位点行级归因错误（P1-4）**：`crates/arkflow-plugin/src/output/kafka.rs:654-687` 对批次**每一行**统一套用 group topic，不校验行级 `__meta_*` 来源。扇入/多输入图混入第二个 Kafka 源的行会把 offset 以 max 语义折进 group topic 分区 → 位点被事务性跳前，中间记录静默跳过（丢数据）。
3. **SQL processor 错误路径泄漏池 context（P1-6）**：`crates/arkflow-plugin/src/processor/sql.rs:217-228,322-372` 只有成功路径 `release_context`（`:243`），任一 `?` 早退泄漏一个；池大小 4、空池 `acquire()` 为 1ms 忙等死循环（`context_pool.rs:91-103`）——第 5 次 schema drift 类错误起整条管线**无任何报错地永久挂死**，违反 `unified-execution-kernel` spec 第 82 行"a chain SHALL NOT park silently"的要求。
4. **HTTP input bind 失败被吞（P1-10）**：`crates/arkflow-plugin/src/input/http.rs:167` 的 `TcpListener::bind().expect()` 在 spawn 任务里 panic，`:178` 的 `connect()` 无条件置 connected 返回 Ok——端口被占用时 input"已连接"但永久读不到数据，零错误暴露。
5. **join inner 容量驱逐零观测（P2）**：`crates/arkflow-core/src/executor/join.rs:716-718` 超限驱逐最老行时，outer 侧会作为未匹配行发射，inner 侧直接丢弃且**连日志都没有**——高倾斜 key 下静默产出错误的 join 结果，`stream-join-operator` spec 第 49 行仅规定"维持只丢弃"、无观测要求。

## What Changes

- remote 接收端去重记录改为单调 max 更新，并在本地通道 send 成功后复查（消除重连重叠期的双投递与水位回退）。
- Kafka L3 位点推导对每行校验来源 topic 与 group topic 一致，不一致显式 fail-closed（整批报错），不再静默归因。
- SQL processor 的 context 归还改为失败安全（错误路径全部释放），`acquire()` 空池等待改为有超时上限并有诊断日志的等待。
- HTTP input 的 listener bind 结果同步回传 `connect()`（bind 失败即 connect 失败），listener 任务异常退出反映为 read 错误。
- join 容量驱逐（inner 与 outer 侧）增加带结构化字段的 warn 日志（侧别/key/缓冲深度/max_per_key，节流防刷屏）；驱逐计数指标列入可观测性 backlog。

## Capabilities

### New Capabilities

- `http-input`: HTTP server input 的监听器生命周期与失败可见性契约（bind fail-fast、listener 异常退出可见）。

### Modified Capabilities

- `network-shuffle-data-plane`: 重放去重要求补强——去重记录单调推进（不得回退）+ 并发连接重叠场景显式化。
- `exactly-once-output`: L3 位点推导要求修改——行级来源 topic 校验、不一致 fail-closed。
- `stream-join-operator`: 容量驱逐要求修改——驱逐 SHALL 可观测（warn + 计数，inner 与 outer 一致）。
- `unified-execution-kernel`: "chain 不得静默停摆"要求扩展——processor 内部资源池（context 池）在所有路径（含错误路径）释放，有界获取等待超时 SHALL 上抛失败。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`（去重 insert/max + 复查）
- `crates/arkflow-plugin/src/output/kafka.rs`（`transactional_offsets_for_batches` 行级校验）
- `crates/arkflow-plugin/src/processor/sql.rs`、`sql/context_pool.rs`（RAII 释放 + acquire 上界）
- `crates/arkflow-plugin/src/input/http.rs`（bind 回传 connect + 任务异常可见）
- `crates/arkflow-core/src/executor/join.rs`（驱逐 warn + 计数）
- 配套单测：每项修复至少一个针对性回归测试（remote 双连接重叠去重、L3 异 topic 行、SQL 连续错误不挂死、HTTP 端口占用、join 驱逐计数）。
- 无配置面变更、无 wire 格式变更、无破坏性变更。

## Non-goals

- 不修复 CODE_REVIEW 文档中的其余 P1/P2（S3 WAL 异步化、租约 epoch 写围栏、input 取消安全、Pulsar input/output、multiple_inputs、buffer 丢数据路径等）——各自独立立项。
- 不改变 L3 的同进程配对限制、不实现跨进程组注册表。
- 不为 SQL processor 建立完整 capability spec（本 change 仅在 unified-execution-kernel 补错误路径资源纪律条款）。
- 不改动 remote 重连状态机、grace 宽限、回执路由等相邻机制。
