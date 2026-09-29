# add-outer-join — Design

## Context

统一内核 join 算子(`crates/arkflow-core/src/executor/join.rs`)现状:双输入按 `__meta_input_index` 路由,`JoinOperatorConfig`(join.rs:34-66)声明 `left_from/right_from`、键列、时间列(回退 `__meta_timestamp`)、`window_ms`/`ttl_ms`/`max_per_key`;匹配对即时发射;两侧 `SideBuffer`(HashMap<key, VecDeque<BufferedRow>>)在 `on_watermark` 中按 `watermark - window_ms - ttl_ms` 淘汰(join.rs:499-511),只丢不射;算子无独立快照,恢复靠 checkpoint 重放重建(spec:60-67)。链级 watermark 是所有活跃入边的逐边最小值(`executor/task.rs:1735-1752`);window 算子已有从 watermark 路径发射输出的先例(`task.rs` `dispatch_watermark`)。

侧别解析把 `left_from/right_from` 映射为**单个**通道索引(join.rs:206-223 `with_input_producers`,取 producer 在通道表中的首个 position);上游算子并行度 > 1 时该侧存在多个通道,未解析到的通道批次在运行期撞 Config 错误(join.rs:490-495)。

## Goals / Non-Goals

**Goals:**

- `join_type: inner(缺省)| left_outer | right_outer | full_outer`,未声明时行为与现状逐位一致。
- outer 侧未匹配行在淘汰时发射,对侧列全 null;已匹配行不再作为未匹配发射。
- join 侧别「上游多子任务」从运行期失败前移为图构建期显式拒绝。
- 内核机制零新增:发射挂在既有 `on_watermark` 通路,不引入新定时器/新状态后端依赖。

**Non-Goals:**

- temporal/lookup join、非 equi-join、跨节点 shuffle join。
- 支持多子任务侧(仅构建期拒绝)。
- join 状态进 StateBackend(维持重放重建契约)。
- retract/changelog 语义(未匹配行发射后不可收回)。

## Decisions

### D1 未匹配发射时机 = 复用既有淘汰边界

outer 侧未匹配行在行被淘汰的时刻发射,不新造定时器。理由:行被淘汰当且仅当 `watermark > ts + window_ms + ttl_ms`,即"永不可能再匹配"——这正是 outer join 标准的最终性边界,`ttl_ms` 天然成为"结果最终性宽限"。备选:① watermark 越过 `ts + window` 即发射(不等 ttl)——引入第二套边界,与既有 ttl 契约冲突;② 独立事件时间定时器机制——内核新增件,违背最小改动原则。均否决。

### D2 「已匹配」标记进 BufferedRow

`BufferedRow` 增加 `matched: bool`(或 packed flag),任一侧匹配成功即置位两侧参与行;淘汰时仅 `!matched` 的行按 join_type 决定是否发射。备选:淘汰时反向探测对侧缓冲——对侧行可能已被先行淘汰,判定不可靠,否决。成本:每行 1 字节,可忽略。

### D3 null 装配与 schema nullable

未匹配行本侧列照常重建(复用 `gather`/`interleave`),对侧列用 `arrow::array::new_null_array(field.data_type(), n)` 装配;输出 schema 中 outer 侧字段强制 `nullable = true`(现状直接克隆源字段 nullable,join.rs:458-473)。inner 模式输出 schema 保持不变——不无条件 nullable,避免 inner 用户可观测的 schema 漂移。full_outer 两侧 nullable;left/right_outer 仅外侧 nullable。

### D4 恢复正确性 = 复用链级 min-watermark 论证,不加机制

outer join 的理论风险是"假未匹配"(未匹配行已发射,对侧迟到行本应匹配)。现有机制天然挡住:链级 watermark 为逐边最小值且通道内 FIFO,左行被淘汰 ⇒ `min(left_wm, right_wm)` 已过界 ⇒ 对侧所有可能匹配的行已被实际处理。重放时 watermark 由源 gate 从事件时间重新生成,同一最小值聚合重新成立,matched 标记确定性重建——与 inner 同构,无需新协议。残余风险写入 spec 契约而非代码:① 处理时间模式(无 watermark)下未匹配行永不发射(活性问题,文档前置声明);② 迟到超过 `window + ttl` 的行在未匹配发射后到达会产生"未匹配 + 匹配"双发(既有 ttl 契约的放大,at-least-once 内);③ `max_per_key` 容量淘汰在 outer 下同样发射,但无 watermark 保证,可能双发(文档化)。

### D5 构建期侧别单通道校验

在图构建调用 `with_input_producers` 处(或其内部)校验:每个声明的生产者在该链入边通道表中恰好贡献一个通道(即上游子任务数 = 1),违者在图构建期以指引用户「将上游算子 parallelism 设为 1」的错误失败。运行期 tag 校验(join.rs:490-495)保留为纵深防御。备选:直接支持多子任务侧(侧别解析改索引集合)——属 temporal join 的前置改造,超出本变更范围。

### D6 join_type 的兼容性

`#[serde(default)]` + 缺省 `inner`,存量配置零迁移;输出 schema 仅在显式 outer 模式变化。

### D7 对侧 schema 未知时的有界待发队列(实现期补充决策)

未匹配行的 null 列需要对侧 schema;对侧可能在其被逐出时尚未产生任何批次(如空维表、慢启动)。选项:① 无 schema 即丢弃——outer 语义静默丢数据;② 无界暂存——违反有界状态原则;③ **每侧以 `max_per_key` 为界的待发队列,超限丢最旧并告警,schema 已知后的下一个发射点(process 或 watermark)补发,至关闭仍未发射则丢弃**。选③:有界、无静默丢失的常见路径、与既有容量参数复用。缓冲键序从 HashMap 换为 BTreeMap,保证多 key 淘汰的发射顺序确定,重放重建逐字节一致。

## Risks / Trade-offs

- [watermark 饥饿 → outer 行永不发射] → 文档前置声明 outer 依赖事件时间驱动;源侧 idle 刷新既有机制可缓解;属活性而非正确性。
- [大 watermark 跳变 → 单轮淘汰发射大批次] → 单批组装、量级与 window 算子 watermark 发射同阶,受 `max_per_key` × key 基数约束;接受,不在首版分片。
- [容量淘汰/超界迟到导致双发] → at-least-once 契约内,spec 显式声明,下游幂等吸收。
- [构建期校验误伤既有合法图] → 仅当上游并行度 > 1 时拒绝;该形态今天必然运行期失败,不存在被破坏的可用场景。

## Migration Plan

无迁移:缺省 `join_type = inner` 行为逐位不变。回滚 = 配置层去掉 join_type 声明即可回到 inner。
