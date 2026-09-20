---
description: ArkFlow 文档页面。
---

# WAL 持久化与性能

ArkFlow 的 WAL(预写日志,Write-Ahead Log)在输入边界持久化消息,使其在崩溃后幸存并可在重启时重放。本页说明如何为不同负载配置 WAL,重点是 2026-07-28 引入的三个优化维度:**段调优**、**并行 PUT** 与**压缩**。

关于至少一次(at-least-once)契约与重放语义的背景,参见[输入投递语义](./delivery-semantics.md)。

## 快速开始

最小的持久化配置使用默认(均衡)策略、单个 PUT 工作者且不压缩:

```yaml validate=full
streams:
  - id: durable-orders
    input:
      type: kafka
      brokers: [localhost:9092]
      topics: [orders]
      consumer_group: arkflow-orders
      start_from_latest: false
    pipeline:
      processors: []
    output:
      type: drop
    durability:
      enabled: true
      backend:
        type: object_store
        node_id: "${ARKFLOW_NODE_ID}"
        stream_id: main
        s3:
          bucket: my-bucket
          region: us-east-1
```

这对大多数负载都适用。要为更高吞吐、更低成本或更快恢复而调优,请继续阅读。

## 维度 1:段调优

`segment_tuning` 块控制内存中的段何时封存并上传到 S3。段越大,PUT 请求越少(成本更低),但"丢失窗口"越大——即节点消失时处于风险中的未刷新消息数。

### 预设策略

三个预设覆盖常见场景:

| 策略 | `max_entries` | `max_bytes` | `flush_interval` | 最适合 |
|----------|---------------|-------------|------------------|----------|
| `aggressive` | 10000 | 10 MB | 10 s | 批处理作业、成本敏感 |
| `balanced`(默认) | 1000 | 1 MB | 1 s | 通用流 |
| `low_latency` | 100 | 100 KB | 100 ms | 实时、最小数据丢失 |

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    segment_tuning:
      strategy: aggressive   # or "balanced" / "low_latency"
```

### 自定义覆盖

任何预设参数都可以单独覆盖:

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    segment_tuning:
      strategy: aggressive
      max_entries: 20000      # override default 10000
      flush_interval: "30s"   # override default 10s
```

### 崩溃窗口权衡

崩溃窗口是节点在一次写入与下一段刷新之间消失时处于风险中的消息数。可近似为:

```
loss_window ≈ min(
    max_entries,
    max_bytes / avg_message_size,
    flush_interval × message_rate
)
```

在 10,000 msg/s、平均消息 1 KB 时:

| 策略 | 崩溃窗口 |
|----------|--------------|
| `aggressive` | 约 100,000 条消息 |
| `balanced` | 约 10,000 条消息 |
| `low_latency` | 约 1,000 条消息 |

## 维度 2:并行 PUT 工作者

`parallel_put` 块配置并发的 S3 上传工作者。每个工作者拥有一个独立的有界通道(16 个段),提供每工作者的背压(backpressure)而无全局争用。

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    parallel_put:
      workers: 4              # 1-8, default 1
      shutdown_timeout: "30s" # wait time for in-flight uploads on close
```

**何时使用多个工作者:**

- S3 PUT 是瓶颈的高吞吐流(瓶颈在网络带宽,而非 CPU)
- 单副本部署:能打满一条连接,却打不满单线程

**需要注意:**

- S3 每前缀的速率限制(3,500 PUT/DELETE/秒,5,500 GET/秒)。8 个工作者在高吞吐流上可能触顶。
- 更高的内存占用:每个工作者在其通道缓冲中最多持有 16 个已封存段。

1 Gbit 链路上的实测吞吐:

| 工作者数 | 吞吐 | 加速比 |
|---------|------------|---------|
| 1(默认) | ~150 MB/s | 1.0× |
| 4 | ~300 MB/s | 2.0× |
| 8 | ~400 MB/s | 2.7× |

## 维度 3:压缩

`compression` 块在上传前启用逐段压缩。压缩后的段可降低存储成本、传输时间,以及恢复时 LIST/GET 操作的体积。

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    compression:
      type: zstd   # or "lz4" / "none"
      level: 3     # algorithm-specific range (see below)
```

**算法:**

| 算法 | 级别范围 | 默认 | 压缩最佳 | 速度最佳 |
|-----------|-------------|---------|------------------|------------|
| `none` | 不适用 | 不适用 | 不适用 | 不适用 |
| `lz4` | 1-16 | 4 | 较低(40-50%) | 非常快 |
| `zstd` | 0-22 | 3 | 高(50-70%) | 中等 |

**实测压缩比**(基于 ArkFlow 自身的 WAL 段载荷,即带重复 schema 元数据的 Arrow IPC 帧):

| 算法 | 压缩比(越小越好) |
|-----------|---------------------------|
| `lz4` | ~108× |
| `zstd-3` | ~181× |
| `zstd-9` | ~200×(更慢) |

**最小体积阈值:**小于 10 KB 的段无论此设置如何都会以未压缩方式上传——压缩小载荷的 CPU 开销超过存储上的节省。

**CPU 成本与节省:**压缩用 CPU 换取网络与存储。在现代 CPU 上,zstd-3 约占 5-10% 的单核利用率,换来约 70% 的存储缩减。CPU 受限的负载用 `lz4`,存储受限的负载用 `zstd-9`。

## 综合运用

### 高吞吐批处理作业

最小化 S3 PUT 请求;容忍最多约 10 万条消息的风险敞口。

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive
    parallel_put:
      workers: 4
    compression:
      type: zstd
      level: 3
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

### 丢失窗口紧凑的实时流

最小化崩溃窗口;吞吐次之。

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: low_latency
    parallel_put:
      workers: 2       # still useful for occasional bursts
    compression:
      type: lz4        # faster than zstd; less CPU
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

### 成本敏感的冷存储

最大压缩、最少 PUT,接受更高的丢失窗口。

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive
      flush_interval: "60s"   # even longer
    compression:
      type: zstd
      level: 9                # maximum compression
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

## 校验

所有配置都会在启动时校验:

- `parallel_put.workers` 必须为 1-8(拒绝 0)
- `compression.level` 必须在算法范围内(zstd 0-22,lz4 1-16)
- 既有检查:`node_id` 与 `stream_id` 非空、`bucket` 非空、`object_store` 拒绝 `sync: per_entry`

无效配置会让 `Stream::run` 在启动时返回 `Err`,引擎快速失败而非静默降级。

## 向后兼容

旧配置(不含 `segment_tuning`、`parallel_put` 或 `compression`)继续可用——这些新字段的默认值为:

- `segment_tuning.strategy: balanced`(与旧 `segment:` 参数相同)
- `parallel_put.workers: 1`
- `compression.type: none`

这意味着现有部署只要不显式启用新字段,行为就不会变化。

## 示例配置

`examples/` 目录中有开箱即用的配置:

- `durability_example_s3.yaml` —— 基线(balanced,无压缩)
- `durability_example_aggressive.yaml` —— 高吞吐
- `durability_example_parallel.yaml` —— 4 个 PUT 工作者
- `durability_example_compressed.yaml` —— zstd 压缩
- `durability_smoke_test.yaml` —— 全部优化组合

## 监控

启用这些优化后需要关注的关键指标:

- `segment_put_latency` —— p99 应低于 200 ms;过高则检查 region
- `segment_put_frequency` —— 使用 `aggressive` 策略后应下降
- `cursor_lag` —— 应保持在 10,000 条以内
- `recovery_latency` —— 重启后应低于 5 s
