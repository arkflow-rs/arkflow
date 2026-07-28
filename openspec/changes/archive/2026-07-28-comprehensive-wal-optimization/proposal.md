## Why

当前 ArkFlow 的 S3 WAL 后端存在性能瓶颈，限制了高吞吐场景的应用：

1. **S3 PUT 延迟高** - 单个 segment PUT 需要 10-200ms（网络 RTT + S3 处理时间）
2. **写入吞吐量受限** - 实际吞吐量限制在 100-150 MB/s（`crates/arkflow-plugin/src/wal/s3.rs:21-28`）
3. **S3 API 成本较高** - 高频 segment PUT 请求产生显著的 API 成本（`docs/performance/s3-wal-backend.md:127-145`）

虽然最近优化 (#1186) 通过异步 pipeline 解耦了写入和 PUT 操作，但核心的 PUT 延迟瓶颈仍未解决。在高吞吐场景（10,000+ msg/s）下，系统需要更激进的优化策略。

## What Changes

本 change 实施三项互补的 WAL 性能优化：

1. **Segment 大小调优** - 暴露并优化 segment batching 配置参数
   - 当前 `max_entries: 1000, max_bytes: 1MB, flush_interval: 1s` 固定为默认值
   - 新增可配置的 segment 策略：`aggressive`（高吞吐）、`balanced`（默认）、`low-latency`（小崩溃窗口）
   - 允许用户根据场景选择预设或自定义参数

2. **并行 PUT** - 实现多 worker 并发上传 segments
   - 当前单 PUT worker 串行上传（`crates/arkflow-plugin/src/wal/s3.rs:70-74`）
   - 新增 4-8 个并行 PUT workers，提升吞吐量 2-3x
   - 保持 segment 顺序性，使用优先级队列确保有序完成

3. **压缩支持** - 添加 segment 级压缩
   - 新增 `compression: "zstd" | "lz4" | "none"` 配置
   - 在 PUT 前压缩 segment 数据，降低 S3 存储和网络传输成本 50-70%
   - 恢复时自动解压

## Capabilities

### New Capabilities

- `wal-segment-tuning`: Segment batching 配置优化，支持预设策略和自定义参数
- `wal-parallel-put`: 多 worker 并发上传 segments，提升写入吞吐量
- `wal-compression`: Segment 级压缩支持，降低存储和传输成本

### Modified Capabilities

- `input-durability`: 扩展现有的 `input-durability` spec，添加 segment 调优、并行 PUT 和压缩配置项

## Impact

**Affected Code:**
- `crates/arkflow-core/src/wal/config.rs` - 添加新配置字段
- `crates/arkflow-plugin/src/wal/s3.rs` - 实现并行 PUT 和压缩
- `crates/arkflow-plugin/src/wal/segment.rs` - 添加压缩/解压逻辑
- `docs/performance/s3-wal-backend.md` - 更新性能文档
- `examples/durability_example_s3.yaml` - 更新示例配置

**Dependencies:**
- 新增依赖：`zstd` / `lz4` 压缩库（按需启用）

**API Changes:**
- WalConfig 新增字段：`segment_tuning`, `parallel_put_workers`, `compression`
- S3StoreBuilder 支持新配置参数

**Performance Impact:**
- 写入吞吐量：150 MB/s → 300-450 MB/s（并行 PUT）
- S3 PUT 成本：减少 90%（aggressive segment tuning）
- 存储成本：减少 50-70%（压缩）

## Non-goals

- **不改变 WAL 语义** - 仍保证 at-least-once 交付和崩溃恢复
- **不优化读取路径** - 读取性能优化（缓存层）不在本次 scope
- **不实现本地缓存** - 热数据缓存留待后续 change
- **不修改 Local (redb) 后端** - 优化仅针对 S3 backend
