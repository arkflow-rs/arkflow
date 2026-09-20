---
title: S3 WAL 后端性能
description: S3 后端 WAL 管道的基准测试与设计笔记。
---
# S3 WAL 后端性能

本文描述基于 S3 的 WAL 后端的性能特征,并针对不同工作负载提供配置调优指引。

## 架构总览

```
Input → append_batch (μs, memory) → segment buffer → channel → PUT worker (async) → S3
                                     ↓ flush triggers                         ↓
                               max_entries / max_bytes / flush_interval      backpressure
                                                                        ↓
                                                                   channel full
```

**管道优化**:段(segment)编码与 PUT 现在通过一条有界容量(16 段)的 flume 通道与写入路径解耦。写入路径(`append_batch`)在发送到通道后立即返回,而 PUT 操作在后台异步执行。这消除了写入路径上 10-200ms 的阻塞性 PUT 延迟。

**批量 Manifest 更新**:Manifest 更新现在被批量处理——累积 8 段或 100ms 后再刷写到 S3。这将 S3 操作减少了约 58%(从每段 3 次操作降至平均每段约 1.24 次)。

## 性能特征

### 延迟分解

| 操作 | 延迟 | 说明 |
|-----------|---------|-------|
| `append_batch` | ~1-50μs | 内存写入 + 通道发送(非阻塞) |
| 段 PUT | 10-200ms | 在 PUT 工作线程中异步执行 |
| 通道发送 | `<1μs` | 仅在通道满时阻塞(背压) |
| Manifest PUT(批量) | 10-100ms | 小 JSON 负载,频率降低 8 倍 |
| 恢复(LIST + GET) | 100-500ms | 取决于段数量 |

**关键变更**:
1. `append_batch` 不再阻塞于 PUT 操作。10-200ms 的 PUT 延迟被移到后台。
2. Manifest 更新被批量处理(每 8 段或 100ms 一次),S3 GET/PUT 操作减少约 58%。

### 吞吐上限

| 因素 | 典型范围 | 瓶颈 |
|--------|---------------|------------|
| S3 PUT(单工作线程) | 50-150 MB/s | 网络带宽 |
| S3 PUT(并行,4-8 个工作线程) | 200-450 MB/s | 网络带宽 |
| S3 LIST 恢复 | 20-100 MB/s | API 速率限制 |
| 本地编码 | 500+ MB/s | CPU(CRC32 + 可选压缩) |
| 通道背压 | 16 段/工作线程 | 可配置的有界通道 |

**实际吞吐**:默认 1 个工作线程时为 100-200 MB/s;`parallel_put.workers: 8` 时最高 450 MB/s。

**并行 PUT 的收益**:多个工作线程在相互独立的有界通道上运行并轮流分配段,在不改变 at-least-once 契约的前提下,为高 QPS 工作负载带来最高 2-3 倍的吞吐提升。使用 8 个工作线程时请注意 S3 的每前缀速率限制。

### 崩溃窗口

"丢失窗口"(loss window)指节点/pod 消失时面临风险的条目数:

```
loss_window = min(
    segment.max_entries,           # entry count trigger
    segment.max_bytes / avg_msg_size,  # byte size trigger
    segment.flush_interval * msg_rate  # time trigger
)
```

| flush_interval | @1000 msg/s | @10000 msg/s | @100000 msg/s |
|----------------|-------------|--------------|---------------|
| 100ms | 100 条 | 1,000 条 | 10,000 条 |
| 1s | 1,000 条 | 10,000 条 | 100,000 条 |
| 5s | 5,000 条 | 50,000 条 | 500,000 条 |

## 配置调优

### 段批处理(D4)

控制内存中的段何时封存(seal)并 PUT 到 S3。

```yaml validate=foreign reason="WAL tuning option fragment"
segment:
  max_entries: 1000      # default
  max_bytes: 1048576     # 1 MiB default
  flush_interval: 1s    # default
```

**权衡:**

| 配置 | 延迟 | 崩溃窗口 | PUT 频率 |
|---------------|---------|--------------|--------------|
| 小段(100, 100KB) | 更低的 PUT 延迟 | 更小的窗口 | 更高的 PUT 频率 |
| 大段(10000, 10MB) | 更高的 PUT 延迟 | 更大的窗口 | 更低的 PUT 频率 |
| 短间隔(100ms) | 快速刷写 | 小窗口 | 高 PUT 频率 |
| 长间隔(10s) | 延迟刷写 | 大窗口 | 低 PUT 频率 |

**建议:**

- **高吞吐、放宽的持久性**:`max_entries: 10000`、`max_bytes: 10MB`、`flush_interval: 10s`
- **均衡**:默认值(`1000`、`1MB`、`1s`)
- **小崩溃窗口**:`max_entries: 100`、`max_bytes: 100KB`、`flush_interval: 100ms`

### 游标刷写(D6)

控制已提交游标(水印)持久化到 `manifest.json` 的频率。

```yaml validate=foreign reason="WAL tuning option fragment"
cursor:
  max_entries: 1000
  interval: 1s
```

**权衡:**

| interval | 崩溃恢复时的重复量 | PUT 频率 |
|----------|---------------------------|---------------|
| 100ms | 极少(约 100 条消息) | 高 |
| 1s | 低(约 1,000 条消息) | 中 |
| 10s | 高(约 10,000 条消息) | 低 |

**注意**:游标批量刷写会引入 at-least-once 的重复,但不影响正确性。无论该设置如何,输出端都必须容忍重复。

## 与本地后端的对比

| 指标 | 本地(redb) | S3 后端 | 对比 |
|--------|--------------|-------------|-------|
| 追加延迟 | ~1μs | ~50μs | 慢 50 倍 |
| 刷写延迟 | ~2ms | ~50ms | 慢 25 倍 |
| 吞吐 | 500 MB/s | 150 MB/s | 慢 3 倍 |
| 崩溃窗口 | ~100 条 | ~1000 条 | 大 10 倍 |
| 节点恢复 | ❌ 不支持 | ✅ 支持 | 不适用 |

## 成本考量

### S3 API 成本

- **PUT 请求**:每 1,000 次请求 $0.005(US-East-1)
- **GET 请求**:每 1,000 次请求 $0.0004
- **LIST 请求**:每 1,000 次请求 $0.005
- **存储**:每 GB/月 $0.023(前 50TB)

**成本示例** @ 10,000 msg/s,每条消息 1KB:

```
- Segment PUTs: (10,000 / 1,000) × 3600 = 36 PUTs/hour × 24 × 30 = 25,920 PUTs/month
- PUT cost: 25,920 / 1,000 × $0.005 = $0.13/month
- Manifest PUTs (batched): ~0.125/second = 324,000 / 1,000 × $0.005 = $1.62/month
  (Previously: ~1/sec = $12.96/month, now ~87% reduction due to batching)
- Storage: 10,000 × 1KB × 86400 × 30 / 1e9 = 259 GB × $0.023 = $5.96/month
- Total: ~$7.71/month per stream (down from ~$19/month)
```

**优化**:调大 `segment.max_entries` 与 `cursor.interval` 可进一步降低 PUT 成本。批量处理已自动应用(8 段或 100ms)。

## 性能调优策略(自 2026-07-28 起)

S3 WAL 后端通过 `segment_tuning.strategy` 支持三种预设策略:

### 激进(Aggressive)策略

面向高吞吐、低成本——容忍更大的崩溃窗口。

```yaml validate=foreign reason="WAL tuning option fragment"
segment_tuning:
  strategy: aggressive
```

默认值:
- `max_entries: 10000`
- `max_bytes: 10MB`
- `flush_interval: 10s`

**崩溃窗口 @ 10K msg/s**:节点丢失时约 100,000 条消息面临风险
**PUT 成本**:较均衡策略降低约 10 倍
**吞吐**:最高 200 MB/s

### 均衡(Balanced)策略(默认)

吞吐与崩溃窗口之间的默认权衡。

```yaml validate=foreign reason="WAL tuning option fragment"
segment_tuning:
  strategy: balanced
```

默认值:
- `max_entries: 1000`
- `max_bytes: 1MB`
- `flush_interval: 1s`

**崩溃窗口 @ 10K msg/s**:约 10,000 条消息面临风险
**吞吐**:100-150 MB/s

### 低延迟(Low-Latency)策略

面向最小崩溃窗口——PUT 频率最高。

```yaml validate=foreign reason="WAL tuning option fragment"
segment_tuning:
  strategy: low_latency
```

默认值:
- `max_entries: 100`
- `max_bytes: 100KB`
- `flush_interval: 100ms`

**崩溃窗口 @ 10K msg/s**:约 1,000 条消息面临风险
**吞吐**:因频繁刷写而较低

### 自定义覆盖

覆盖任意预设的单个参数:

```yaml validate=foreign reason="WAL tuning option fragment"
segment_tuning:
  strategy: aggressive
  max_entries: 20000      # override default 10000
  flush_interval: "30s"   # override default 10s
```

## 并行 PUT 工作线程

配置多个 PUT 工作线程可获得 2-3 倍的吞吐提升。

```yaml validate=foreign reason="WAL tuning option fragment"
parallel_put:
  workers: 4              # 1-8, default 1
  shutdown_timeout: "30s" # how long to wait for in-flight uploads
```

**收益:**
- 各工作线程通过独立通道并行运行
- 轮流分配(最旧的段优先)
- 每个工作线程独立的背压(各 16 段)

**注意 S3 速率限制**:8 个工作线程可能触及每前缀限制。

**Manifest 写入安全**:当 `workers > 1` 时,多个工作线程可能同时完成一段的上传并封存它,各自重写 `manifest.json`。这些并发重写由基于 ETag 的乐观并发控制协调(读取 ETag → 条件 PUT → 不匹配则重试,最多 8 次尝试,覆盖工作线程数上限),因此任何工作线程的游标推进或已封存段条目都不会被静默覆盖——前提是后端支持条件 PUT。S3 与 S3 兼容存储都支持;不支持的后端(例如 `LocalFileSystem`)会退化为单写者无条件写入,因此那里请保持 `workers: 1`。在 `workers: 1`(默认)时这一切不可见:无竞争、无重试,行为不变。投递契约细节参见 `docs/docs/components/0-inputs/delivery-semantics.md`。

## 压缩

通过段压缩将 S3 存储与网络成本降低 50-70%。

```yaml validate=foreign reason="WAL tuning option fragment"
compression:
  type: zstd  # or "lz4", "none"
  level: 3    # algorithm-specific
```

**算法对比:**

| 算法 | 压缩比 | CPU 开销 | 最适用于 |
|-----------|-------------------|----------|----------|
| `none` | 1.0x | 0% | 默认,低 CPU |
| `lz4` | 2-3x(Arrow IPC 实测约 108×) | 2-5% | 快速压缩 |
| `zstd-3` | 3-5x(Arrow IPC 实测约 181×) | 5-10% | 均衡(启用压缩时的默认值) |
| `zstd-9` | 5-8x | 20-30% | 最大压缩 |

在实际 WAL 段负载(带重复 schema 元数据的 Arrow IPC 帧)上**实测的压缩比**显著高于上面 50-70% 的粗略估计,因为 Arrow IPC 存在大量重复。以下数字来自单元测试套件(`compression_ratio_across_*_levels`):

```
lz4-1:  50000 -> 462 bytes (ratio: 108.23x)
lz4-4:  50000 -> 462 bytes (ratio: 108.23x)
lz4-9:  50000 -> 462 bytes (ratio: 108.23x)
zstd-1: 50000 -> 276 bytes (ratio: 181.16x)
zstd-3: 50000 -> 276 bytes (ratio: 181.16x)
zstd-6: 50000 -> 276 bytes (ratio: 181.16x)
zstd-9: 50000 -> 276 bytes (ratio: 181.16x)
```

在生产负载(混合数据、重复较少)上的实际比率会低于这些合成基准,但仍然可观。

**最小大小阈值**:小于 10KB 的段不压缩直接上传。

## 故障场景

### S3 不可用

- **append**:通道填满(16 段)时阻塞 → 输入停顿(与之前相同,只是缓冲更大)
- **recovery**:流启动失败 → 引擎错误
- **缓解措施**:使用带跨区域复制的 S3,或增加本地缓存

### 网络分区

- **当前行为**:与 S3 不可用相同,但通道在阻塞前提供 16 段缓冲
- **未来改进**:带重试队列的本地直写(write-through)缓存

### 段 PUT 部分失败

- **现状**:丢失的段在恢复时被跳过(D7)。PUT 工作线程记录错误但继续处理
- **影响**:仅该段的数据丢失
- **缓解措施**:启用 S3 服务端加密与版本控制

## 监控

### 关键指标

| 指标 | 描述 | 告警阈值 |
|--------|-------------|-----------------|
| `segment_put_latency` | PUT 一个段所需时间 | >500ms p99 |
| `segment_put_frequency` | 每秒 PUT 次数 | >10 次/秒(可能需要调优) |
| `segment_size` | 段平均字节数 | `<100KB 或 >10MB` |
| `cursor_lag` | 游标与最大已写序列的差距 | >10,000 条 |
| `recovery_latency` | 启动时重放 WAL 所需时间 | >5s |

### 日志

需要关注的关键日志行:

```
[DEBUG] Sealing segment: entries={n}, bytes={size}
[INFO]  Segment PUT complete: {segment_key}, {size} bytes in {duration_ms}ms
[INFO]  Recovery: replayed {n} entries from {m} segments in {duration_ms}ms
[WARN]  Segment PUT failed: {error}, retrying...
```

## 参考资料

- 用户侧调优指南:`docs/docs/components/0-inputs/wal-optimization.md`
- 设计:`openspec/changes/archive/2026-07-27-add-wal-s3-backend/design.md`
- 设计:`openspec/changes/archive/2026-07-28-comprehensive-wal-optimization/design.md`
- 规格:`openspec/specs/input-durability/spec.md`
- 实现:`crates/arkflow-plugin/src/wal/s3.rs`
- 压缩:`crates/arkflow-plugin/src/wal/compression.rs`
