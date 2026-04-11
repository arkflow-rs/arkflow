# ArkFlow 后续工作计划

## P0 - 必须完成（本周）

### 1. 完善 Input Checkpoint 接口 ✅ 部分完成
**状态**: Input trait 已有 `get_position()` 和 `seek()` 方法

**剩余工作**:
- [x] 创建 checkpoint 扩展模块
- [ ] Kafka Input 实现 checkpoint 支持
- [ ] Redis Input 实现 checkpoint 支持
- [ ] 测试 checkpoint 恢复

### 2. 完善 Stream Barrier 处理
**目标**: 在 Stream::run() 中集成 barrier 处理

**需要实现**:
```rust
// 在 processor workers 中：
async fn do_processor(..., barrier_receiver: Receiver<Barrier>) {
    loop {
        tokio::select! {
            // 处理 barrier
            Some(barrier) = barrier_receiver.recv() => {
                // 1. 停止处理新消息
                // 2. 完成当前批处理
                // 3. 保存状态快照
                // 4. 确认 barrier
                barrier_manager.acknowledge_barrier(barrier.id).await?;
            }
            // 处理数据消息
            Some(msg) = input_receiver.recv() => { ... }
        }
    }
}
```

- [ ] 实现 barrier 接收和处理
- [ ] 实现状态快照
- [ ] 测试 barrier 对齐

### 3. 完善 Engine 集成
**目标**: Engine 协调 checkpoint

**需要实现**:
```rust
pub struct Engine {
    checkpoint_coordinator: Option<Arc<CheckpointCoordinator>>,
    // ...
}

impl Engine {
    pub async fn run_with_checkpoint(&mut self) -> Result<(), Error> {
        // 1. 初始化 checkpoint coordinator
        // 2. 为每个 stream 注入 barrier
        // 3. 定期触发 checkpoint
        // 4. 处理 checkpoint 完成/失败
    }
}
```

- [ ] Engine 添加 checkpoint 支持
- [ ] Stream 注册到 coordinator
- [ ] 健康检查集成

### 4. 状态恢复逻辑
**目标**: 从 checkpoint 恢复状态

**需要实现**:
```rust
impl Stream {
    async fn restore_from_checkpoint(
        &mut self,
        checkpoint: &CheckpointMetadata,
    ) -> Result<(), Error> {
        // 1. 恢复 input 位置
        self.input.seek(&checkpoint.input_state).await?;

        // 2. 恢复 processor 状态
        self.pipeline.restore_state(&checkpoint.processor_state).await?;

        // 3. 恢复 output 事务状态
        if let Some(ref tx_coord) = self.transaction_coordinator {
            tx_coord.recover_transactions().await?;
        }

        Ok(())
    }
}
```

- [ ] 实现 Stream 恢复
- [ ] Pipeline 状态恢复
- [ ] 事务状态恢复
- [ ] 端到端恢复测试

## P1 - 重要功能（本月）

### 5. Kafka Checkpoint 实现
**目标**: Kafka input 完整的 checkpoint 支持

**需要实现**:
- [ ] Offset 存储到 checkpoint
- [ ] 从 checkpoint 恢复 offset
- [ ] 分区状态管理
- [ ] 事务性消息消费

### 6. Metrics Export
**目标**: Prometheus 指标导出

**需要实现**:
- [ ] HTTP metrics endpoint
- [ ] Checkpoint 指标
- [ ] Transaction 指标
- [ ] 自定义 labels

### 7. 增量 Checkpoint
**目标**: 避免全量快照，只保存变更

**需要实现**:
- [ ] 变更跟踪
- [ ] 增量序列化
- [ ] Checkpoint 合并
- [ ] 清理策略

### 8. 分布式协调
**目标**: 多节点 checkpoint 协调

**需要实现**:
- [ ] 全局 checkpoint ID
- [ ] 跨节点 barrier 传播
- [ ] 分布式状态同步
- [ ] 故障检测和恢复

## P2 - 增强功能（下月）

### 9. Savepoint
- [ ] 手动触发 savepoint
- [ ] Savepoint 版本化
- [ ] 跨版本迁移

### 10. 自适应 Checkpoint
- [ ] 基于负载调整间隔
- [ ] 动态超时调整
- [ ] 背压感知 checkpoint

## 当前优先级

### 立即开始
1. ✅ Input checkpoint 接口（基础架构）
2. ⏳ Stream barrier 处理（正在进行）
3. ⏳ Engine checkpoint 集成
4. ⏳ 状态恢复逻辑

### 验收标准
- [ ] 端到端 checkpoint 流程工作
- [ ] 故障恢复验证
- [ ] 性能基准测试
- [ ] 完整的 E2E 测试

## 进度跟踪

| 任务 | 负责人 | 状态 | 预计完成 |
|------|--------|------|----------|
| Input Checkpoint | TBD | 🚧 进行中 | 2 天 |
| Barrier 处理 | TBD | 📋 待开始 | 3 天 |
| Engine 集成 | TBD | 📋 待开始 | 2 天 |
| 状态恢复 | TBD | 📋 待开始 | 2 天 |
| E2E 测试 | TBD | 📋 待开始 | 2 天 |

**总预计时间**: 11 个工作日

---

*最后更新: 2026-03-29*
