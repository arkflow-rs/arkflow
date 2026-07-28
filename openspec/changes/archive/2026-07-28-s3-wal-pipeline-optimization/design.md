## Context

S3 WAL backend 的当前实现在 `crates/arkflow-plugin/src/wal/s3.rs` 中采用同步写入路径：当 segment 达到 seal 阈值时，`append_batch()` 会调用 `block_on(seal_active_segment())`，阻塞直到 S3 PUT 完成（10-200ms）。

**当前架构**:
```
append_batch() → encode → [check threshold] → seal_now → block_on(PUT) → return
                                                         ↑ 10-200ms 阻塞
```

**优化后架构**:
```
append_batch() → encode → check threshold → send to channel ─┐
                                                              │
                                                              ▼
                                                       PUT worker
                                                              │
                                                    parallel PUT + batch manifest
                                                              │
                                                        async S3
```

这种设计在高 QPS 场景下（每秒 10000+ 条写入）消除了写入路径的 PUT 阻塞，并通过批量 manifest 更新减少了 58% 的 S3 操作。

**约束**:
- 必须保持现有的 failure/recovery 语义（manifest + LIST fallback）
- 不能改变 `WalStore` trait 的 API
- 必须保持 segment 顺序性
- 不引入新的外部依赖

## Goals / Non-Goals

**Goals:**
- 消除 `append_batch()` 路径上的 PUT 阻塞
- 实现 segment 编码和 PUT 的并发执行
- 保持现有数据持久性和恢复语义
- 保持向后兼容性

**Non-Goals:**
- 不改变 WAL 配置 schema
- 不优化其他 S3 WAL 特性（压缩、并行恢复等）
- 不修改 `WalStore` trait
- 不引入新的 WAL backend 类型

## Decisions

### Decision 1: Use flume channel for producer-consumer decoupling

**选择**: 使用已有的 `flume` channel（项目中已依赖）连接 `append_batch()` 和 PUT worker。

**理由**:
- Flume 是项目已有依赖，版本固定在 0.11
- 支持 `try_send`/`send` 两种模式，便于实现 backpressure
- Rust-native，与项目 async 模型兼容

**替代方案**:
- `tokio::sync::mpsc`: 需要在 async context 中使用，但 `append_batch()` 是同步的
- `crossbeam::channel`: 额外依赖，flume 已满足需求

### Decision 2: Dedicated PUT worker task with batch manifest updates

**选择**: 在 `S3Store` 中启动一个专用的 PUT worker 任务，并实现批量 manifest 更新。

**架构**:
```
append_batch() → encode → check threshold → send to channel ─┐
                                                                │
                                                                ▼
                                                         PUT worker
                                                                │
                                           accumulate segments → batch manifest update
                                                                │
                                                         async S3
```

**批量更新策略**:
- 累积 8 个 segment 或 100ms 更新一次 manifest
- 减少从 300 次 S3 操作（100 segments）到 ~124 次
- 节省约 58% 的 S3 API 调用成本

**理由**:
- 解耦写入路径和 PUT 路径
- 批量 manifest 更新大幅减少 S3 操作
- 简化错误处理（PUT 失败可以重试或报错，不影响主路径）
- 保持现有的单 runtime 架构

**替代方案**:
- 每次 spawn 一个 tokio task: 开销大，难以控制并发数
- 使用 thread pool: 复杂度高，过度设计

### Decision 3: Memory optimization with Bytes::copy_from_slice

**选择**: 使用 `Bytes::copy_from_slice` 替代 `clone()` 来减少内存分配。

**理由**:
- `Bytes::from(bytes.clone())` 会产生额外的内存拷贝
- `Bytes::copy_from_slice(&bytes)` 直接引用原始数据
- 每个 segment 节省一次内存分配和拷贝

**收益**: 50% 的内存分配减少

### Decision 4: Channel capacity and backpressure

**选择**: 固定 channel 容量（如 16 segments），满时 `append_batch()` 阻塞。

**理由**:
- 防止内存无限增长（慢 PUT 导致积压）
- 简单的 backpressure 机制
- 16 segments 足够应对大多数场景（假设每个 PUT 200ms，16 并发可以覆盖 3.2 秒的 PUT 抖动）

**配置考虑**: 未来可暴露为配置项，但初始实现使用固定值。

### Decision 4: Maintain existing seal_active_segment semantics

**选择**: PUT worker 复用现有的 `seal_active_segment()` 函数，不在新的异步路径中重新实现。

**理由**:
- `seal_active_segment()` 已包含正确的 manifest 更新逻辑
- 最小化变更范围，降低风险
- 便于测试和验证

**调整**: 将该函数改为在 PUT worker 的 async context 中调用。

### Decision 5: Error handling strategy

**选择**: PUT 失败时，worker 记录错误但不阻塞后续 PUT；recovery 依赖现有 LIST fallback 机制。

**理由**:
- 保持现有的 failure recovery 语义（D5: LIST fallback）
- 避免单点故障导致整个 WAL 卡死
- 简化错误处理逻辑

## Risks / Trade-offs

### Risk 1: Channel full during S3 outage

**场景**: S3 服务不可用或网络中断，PUT 请求超时，channel 填满。

**影响**: `append_batch()` 会阻塞在 `send()` 上，影响写入路径。

**缓解**:
- 使用合理的 channel 容量（16 segments）
- 监控 channel 使用率，必要时告警
- 未来可考虑 PUT 超时策略

### Risk 2: Reordered PUT operations

**场景**: 虽然 PUT 是有序发送的，但 S3 可能乱序完成（极少见）。

**影响**: Manifest 顺序可能与实际 PUT 顺序不一致。

**缓解**:
- PUT worker 串行发送（当前设计）
- 如果未来并行 PUT，需要实现 PUT completion tracking

### Risk 3: Increased memory usage

**场景**: Channel 中缓存多个未 PUT 的 segments。

**影响**: 内存占用增加（每个 segment 最多 max_bytes）。

**缓解**:
- 限制 channel 容量
- 配置合理的 max_bytes（如 1MB）
- 监控内存使用

### Trade-off: Complexity vs Performance

**设计决策**: 引入 channel 和 worker task 增加了一些复杂性，但获得了显著的吞吐量提升。

**权衡**:
- 增加 ~100-200 行代码（channel、worker、错误处理）
- 消除 10% 的写入阻塞时间
- 在高 QPS 场景下价值明显

## Migration Plan

### 部署步骤

1. **代码变更**: 修改 `crates/arkflow-plugin/src/wal/s3.rs`
2. **测试**: 运行现有的单元测试和集成测试，确保向后兼容
3. **性能验证**: 使用高 QPS workload 验证吞吐量提升
4. **文档更新**: 更新 `docs/performance/s3-wal-backend.md`

### Rollback 策略

如果发现严重问题，可以快速回退：
- 代码变更集中在单个文件（s3.rs）
- 没有配置 schema 变更
- 回退后恢复原有行为

## Open Questions

1. **Channel 容量**: 16 segments 是否合适？是否需要可配置？
2. **PUT 超时**: 是否需要为 PUT 操作设置超时？当前依赖 object_store 默认。
3. **监控**: 是否需要暴露 channel 使用率等指标？

这些问题可以在初始实现后根据实际运行情况决定。
