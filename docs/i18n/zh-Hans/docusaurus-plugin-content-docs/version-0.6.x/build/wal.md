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

## 源确认以段密封为门控

在对象存储后端上,源确认(例如 Kafka 的 offset 提交)只有在被确认的条目**已被密封进段对象**——即 PUT 到对象存储并完成 manifest 更新——之后才会完成。因此"确认已完成"永远意味着该条目已持久化在存储上:无论节点何时消失,已确认的条目都不会丢失。

仍暂存在内存、尚未密封的条目同样不会丢失——它们构成**重放窗口**:由于其源确认尚未完成,重启后由源重新投递(至少一次,输出仍需容忍重复,与既有契约一致)。

限制重放窗口的段刷新触发条件,同时也限制了该门控带来的**确认延迟**:一次确认最多等待一个 `flush_interval`(外加段 PUT 时间)等其所在段密封。默认 `balanced` 预设约为 1s;`aggressive` 预设最多约 10s。同一段内多条记录的确认共享同一次密封,等待按批次摊销。如果确认延迟比 PUT 成本更重要,调低 `flush_interval`(以及 `max_entries`)——不要试图绕过门控:正是它保证了"已确认即已密封"。后台刷洗器停止密封时,确认会在有界等待后以可重试错误失败,而不是静默提交未密封数据;持续失败会升级为 error 级日志(参见 [S3 WAL 后端性能](../develop/s3-wal-performance.md))。

本地(`redb`)后端不受影响:其 flush 本身就是持久提交,确认行为与之前完全一致,没有额外延迟。

## 维度 1:段调优

`segment_tuning` 块控制内存中的段何时封存并上传到 S3。段越大,PUT 请求越少(成本更低),但"重放窗口"越大——节点消失时需要源重新投递的未密封消息越多——确认延迟也越长(见上文)。

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

### 重放窗口权衡

重放窗口是节点在一次写入与下一段刷新之间消失时,需要源重新投递的消息数(它们会被重放,而不是丢失——确认门控使源游标始终落后于密封边界)。可近似为:

```
replay_window ≈ min(
    max_entries,
    max_bytes / avg_message_size,
    flush_interval × message_rate
)
```

在 10,000 msg/s、平均消息 1 KB 时:

| 策略 | 重放窗口 |
|----------|--------------|
| `aggressive` | 约 10,000 条消息 |
| `balanced` | 约 1,000 条消息 |
| `low_latency` | 约 100 条消息 |

在该速率下 `max_entries` 与 `max_bytes` 会先于 `flush_interval` 触发,由它们主导重放条数;密封门控带来的确认延迟仍以 `flush_interval` 为上界(上述预设分别为 10s / 1s / 100ms)。速率更低时,`flush_interval × 消息速率` 一项成为约束项。

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

最小化 S3 PUT 请求;在 10K msg/s 摄入下容忍节点丢失时最多约 1 万条消息的重新投递(`max_entries`/`max_bytes` 先触发),以及最多约 10s 的确认延迟。

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

### 重放窗口紧凑的实时流

最小化重放窗口与确认延迟;吞吐次之。

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

最大压缩、最少 PUT,接受更大的重放窗口与更长的确认延迟。

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
