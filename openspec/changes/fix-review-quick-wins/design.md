## Context

五项缺陷来自 2026-09-29 代码审查（`openspec/CODE_REVIEW_2026-09-29.md` P1-3/P1-4/P1-6/P1-10 与 join 驱逐 P2）。共同特征：修复面小（每项 ≤ 百行）、彼此独立、全属"静默丢数据/静默挂死/静默失联"。本 change 只做这五项，不触碰相邻机制（重连状态机、事务 producer 生命周期、SQL 计划缓存本身）。

## Goals / Non-Goals

**Goals:**

- remote 去重：消除重连重叠期双投递与水位回退两个竞态窗口。
- L3：位点推导对行级来源 topic fail-closed，堵住多源图静默跳位点。
- SQL 池：错误路径不再泄漏 context，空池等待有界且可诊断。
- HTTP input：bind 失败同步可见于 connect。
- join：容量驱逐可观测。

**Non-Goals:**

- 不改 remote 重连预算/grace/回执路由语义；不做投递级以外的端到端去重。
- 不实现 L3 跨进程配对；不改 Kafka 事务 producer 生命周期。
- 不重构 SQL processor（临时表 deregister、concat 多 schema 等另行立项）。
- 不动其他 input 的取消安全（独立 change `fix-input-cancellation-safety`）。
- 驱逐计数指标（需内核算子级指标面，列入观测性 backlog）。

## Decisions

**D1 — remote 去重：per-route-key 投递锁 + 单调 max 更新。**
竞态根因是"检查→send→标记"对同一 route_key 非原子，且两条连接可并发服务同一 key。选择：为每个 route_key 引入一把 `Arc<tokio::sync::Mutex<()>>` 投递锁，横跨"查 delivered → send_async → 以 max 更新 delivered"全过程；`delivered.insert` 改为 `*entry = max(*entry, seq)`。锁粒度是单 key，互斥代价只落在重连重叠这一罕见窗口；持锁等待 send 的时长由本地有界通道（1024）的消费速率界定，本地链退出时 send 失败即释放。*备选否决*：仅改 max 更新（不能防双投递，只能防水位回退）；注册表级互斥（拒绝同 key 第二条连接，改动大且违背"宽限重注册"语义）。

**D2 — L3：逐行校验 `__meta_topic`，存在即必须等于 group topic。**
`transactional_offsets_for_batches` 增读可选的 `__meta_topic` 列：列存在时逐行比较，任何不等于 group topic 的行使整批以显式数据错误失败（事务不提交、位点不推进）；列整体缺失时维持现状（与"无元数据批次贡献空位点"同族的兼容路径——partition/offset 列也在时缺失 topic 属旧元数据形态，不新增破坏）。实现前先核实 kafka input 确实产出 `__meta_topic` 列（stream_adapter.rs:199-209 已按行匹配 topic，可佐证）。*备选否决*：按行路由到各自 topic 的位点（语义扩张，超出 fail-closed 修复目标）。

**D3 — SQL 池：RAII 归还 + 有界 acquire。**
归还改为 guard 对象 Drop 兜底（成功路径显式 release 即正常归还，`?` 早退由 Drop 覆盖，双保险且不动调用点结构）；`acquire()` 的 1ms 忙等循环加截止时间（10s），超时返回带诊断的错误（池大小、已占用说明），从"无限静默忙等"变为"有界 + 显式失败 + 可诊断日志"。*备选否决*：condvar/Notify 重构等待（改动大，忙等加界后已无正确性问题，性能问题留给后续）。

**D4 — HTTP input：bind 移入 connect()，accept 失败走消息通道。**
bind 从 spawn 任务移到 `connect()` 内直接 await（天然 fail-fast，去掉 `expect` panic 路径）；accept 循环留在 spawn 任务，accept/连接处理错误通过既有消息通道送 `Err`，read 自然以错误返回进入引擎失败路径。connected 标志只在 bind 成功后置位。*备选否决*：oneshot 回传 bind 结果（等价但多一层间接）。

**D5 — join 驱逐：节流 warn，结构化字段。**
容量驱逐（inner 与 outer）打 `tracing::warn!`，字段：侧别、key（截断防日志膨胀/泄露）、当前深度、`max_per_key`、节流窗口内累计次数。每算子实例 1s 节流（`Instant` + 计数器），持续倾斜下不刷屏但每秒可见。watermark 驱逐不日志（正常语义）。*备选否决*：逐次 warn（病态倾斜下刷屏，反而淹没有效信号）；指标计数（需要算子级指标面，backlog）。

## Risks / Trade-offs

- [D1 投递锁在本地链消费极慢时拉长重叠窗口] → 窗口上限=本地通道 1024 帧排空时间；本地链死亡时 send 立即失败释放锁，无死锁路径（锁内无其他锁、无嵌套获取）。
- [D2 旧行为兼容：topic 列缺失时仍按 group topic 归因] → 该路径等价现状，不引入新丢数据面；spec 明确该兼容边界。
- [D3 acquire 超时把"慢"误报为"错"] → 10s 远大于任何合法 plan 执行窗口（context 持有期间是执行期，不是等待期——等待期只发生在池耗尽，而池耗尽本身就是泄漏或并发的病态信号）。
- [D5 key 截断可能丢诊断信息] → 截 64 字节足够定位热点 key；深度与累计次数保留。
- [五项合一个 change 的回归面交叉] → 每项独立单测 + 全量 workspace 测试门禁；五处文件互不相交，无交叉修改。

## Migration Plan

无配置面/存储面/wire 变更，直接合入。回滚 = revert 单个 commit。
