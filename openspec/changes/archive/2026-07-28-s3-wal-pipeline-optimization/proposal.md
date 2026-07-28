## Why

当前 S3 WAL backend 在每次 segment seal 时会阻塞写入路径，导致高 QPS 场景下的性能瓶颈。

**证据**: `crates/arkflow-plugin/src/wal/s3.rs:424`
```rust
if seal_now {
    self.runtime.block_on(seal_active_segment(self))?;  // 阻塞 10-200ms
}
```

当达到 `max_entries` 或 `max_bytes` 阈值时，`append_batch()` 会同步等待 segment PUT 完成（10-200ms），期间所有后续写入被阻塞。

**问题规模**: 假设每 1000 条触发一次 seal，每秒 10000 条写入 → 每秒 10 次阻塞 → 约 10% 的时间在等待 PUT。

## What Changes

- **并发写入路径**: 将 segment 编码和 PUT 操作从 `append_batch()` 的同步路径中移除
- **Channel 解耦**: 使用 flume channel 连接编码线程和 PUT 线程，实现 pipeline 执行
- **批量 manifest 更新**: 累积多个 segment 的 manifest 更新，减少 S3 操作约 58%
- **内存优化**: 消除 Bytes.clone()，减少 50% 的内存分配
- **保持语义**: 确保在 failure/recovery 场景下的数据持久性和一致性，与现有实现一致
- **向后兼容**: 不改变 WAL 配置 schema 或公共 API

## Capabilities

### New Capabilities
- `s3-wal-pipeline`: S3 WAL 并发写入能力，支持 segment 编码和 PUT 的 pipeline 执行

### Modified Capabilities
- (无 - 此变更不改变现有的 spec-level 需求)

## Impact

- **主要修改**: `crates/arkflow-plugin/src/wal/s3.rs`
  - `S3Store` 结构体：增加 channel、PUT worker 和批量 manifest 更新字段
  - `append_batch()`: 改为发送到 channel 而非直接 seal
  - 新增 `ManifestUpdate` 结构体：管理批量 manifest 更新
  - 新增 `spawn_put_worker()` 函数：启动 PUT worker 任务
  - 新增 `put_segment_from_data()` 函数：处理 segment PUT
  - 新增 `flush_manifest_updates()` 函数：批量刷新 manifest 更新
  - 内存优化：使用 `Bytes::copy_from_slice` 替代 `clone()`
- **测试修改**: `crates/arkflow-plugin/tests/minio_integration.rs` 和 s3.rs 的单元测试需要验证并发场景
  - 新增 7 个 S3 WAL 专用测试
  - 新增高 QPS workload 集成测试
- **依赖变更**: 无新增依赖（flume 已存在）
- **文档更新**: 
  - `docs/performance/s3-wal-backend.md` 需要更新性能特征
  - 新增 `comparison-analysis.md` 对比分析报告
  - 新增 `latency-optimization-guide.md` 延迟优化指南

## Non-goals

- **不在本次变更中**:
  - S3 WAL 的其他优化（压缩、并行恢复、HTTP/2 调优等）
  - WAL 配置 schema 的变更
  - 新的 WAL backend 类型
  - 改变 WAL trait 的语义或 API
