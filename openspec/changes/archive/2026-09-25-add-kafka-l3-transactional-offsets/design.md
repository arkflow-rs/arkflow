# Design: add-kafka-l3-transactional-offsets

## 决策

1. **位点从输出侧批次元数据推导，而非输入侧累计**：write_batch 看到的 `__meta_partition/__meta_offset` 恰是"本事务写出的消息"的来源位点——提交它们使事务边界 = 重放边界（与 `exactly-once-output` 的 one-write_batch-per-ack-range 不变量对齐）。输入侧累计会提交超前于输出的位点。
2. **组元数据走进程内注册表（weak 生命周期）**：`ConsumerGroupMetadata` 非 Clone 非 Send 跨界约束由 Arc 包裹 + RwLock 解决；输入 drop 后注册表项自动失效（weak upgrade 失败 → 显式报错）。
3. **输入跳过 store_offset 但保留 frontier**：内存 frontier 继续支撑 checkpoint/重连语义；仅 broker 提交位改为事务内。事务回滚时 broker 位点不动，重放从上一次已提交事务之后开始 ✓。
4. **单主题限定**：批次元数据只有分区维度；多主题输入的分区归属无法从元数据恢复，显式报错优于猜测。
5. **连接时机**：connect 后立即取 group_metadata——librdkafka 允许在加入组前构造元数据对象，事务携带它在 broker 侧按 group 解析。

## 风险

- 输入/输出不同进程（分布式部署）：注册表查不到 → 显式报错（fail-closed），文档明示 L3 限单进程管道。
- 事务超时窗口内输入继续消费：位点提交只在事务内，超前消费不影响正确性（重放语义覆盖）。
