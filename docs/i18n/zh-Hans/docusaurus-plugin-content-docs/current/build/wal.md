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

`parallel_put` 块为并发 S3 上传工作者预留配置空间。每个工作者将拥有一个独立的有界通道(16 个段),提供每工作者的背压(backpressure)而无全局争用。

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

:::warning

**该旋钮当前无效。** 段密封(seal)在锁下串行化,以确保已确认的边界永远不会越过更早的未密封段;因此工作者池没有生产调用点。该字段会通过校验(1-8)并为前向兼容而保留,但设置 `workers > 1` 只会在启动时记录一条警告("parallel_put.workers has no effect")。请保持默认值 `1`,改用[段调优](#维度-1段调优)与[压缩](#维度-3压缩)来为 WAL 定容——真正影响吞吐的因素见[投递语义](../build/delivery-semantics.md)与 [S3 WAL 性能](../develop/s3-wal-performance.md)。

:::

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

:::note

`lz4` 的级别会通过校验(1-16)但当前**未被使用**:LZ4 以单一速度压缩,与配置的级别无关,任何取值都产生逐字节相同的输出——下方实测中 lz4-1/4/9 完全一致正是这个原因。请调 `zstd` 的级别;`lz4` 的默认值 4 只是 schema 记录值,不是真实的调优输入。

:::

**实测压缩比**(基于 ArkFlow 自身的 WAL 段载荷,即带重复 schema 元数据的 Arrow IPC 帧):

| 算法 | 压缩比(越小越好) |
|-----------|---------------------------|
| `lz4` | ~108× |
| `zstd-3` | ~181× |
| `zstd-9` | ~200×(更慢) |

**最小体积阈值:**小于 10 KB 的段无论此设置如何都会以未压缩方式上传——压缩小载荷的 CPU 开销超过存储上的节省。

**CPU 成本与节省:**压缩用 CPU 换取网络与存储。在现代 CPU 上,zstd-3 约占 5-10% 的单核利用率,换来约 70% 的存储缩减。CPU 受限的负载用 `lz4`,存储受限的负载用 `zstd-9`。

## 维度 4:段保留策略与自动回收

如果没有回收机制,S3 WAL 的已密封段和 `manifest.json` 会无界增长,重启时需要下载并过滤全部历史段。`retention` 块用于控制已确认密封段的自动回收。

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    retention:
      enabled: true        # default: true
      max_segments: 16     # optional: max acknowledged sealed segments to keep
      max_age: "24h"       # optional: max age (supports ms, s, m, h, d, e.g. "1h30m")
      max_bytes: 67108864  # optional: max total bytes across kept acknowledged segments
```

### 默认策略与多上限语义

- **默认策略(省略 `retention`,或 `enabled: true` 且三项上限均未设置):**后台回收循环会立即回收**所有**满足安全下界的已确认密封段(与本地 `redb` WAL 回收语义一致)。
- **显式上限(`max_segments`、`max_age`、`max_bytes`):**只要显式配置了其中一项或多项,仅按已配置的上限生效。系统从新到旧扫描可回收段,遇到首个超限段即停止保留并回收该段及所有更老的段,始终保留连续的最新后缀,不产生历史空洞。
- **关闭回收(`enabled: false`):**ArkFlow 不执行任何对象删除(适合交由 S3 Bucket Lifecycle 规则统一归档或过期的场景)。

### 安全性与崩溃恢复保障

- **Rewind 安全下界:**仅 `sealed_segments` 中满足 `last_seq <= min(manifest.cursor, reclaimable_hwm)` 的段可被回收。`reclaimable_hwm` 仅在源确认提交成功后(`mark_committed(seq)`)推进到 `seq - 1`,与本地 `redb` 保持一致,确保在途 `rewind_cursor` 或 `Wal::undo(seq)` 绝不会读到已删除段。未确认段与最新的 `active_segment` 永远不会被删除。
- **Manifest 优先的崩溃安全顺序:**回收运行在独立、可取消的后台循环中(与段密封循环解耦,慢速或失败的 S3 删除不会阻塞摄入、不会拉长确认门控延迟,也不会卡住 `close()`):
  1. 通过 ETag 条件写入更新 `manifest.json`,将待回收段从 `sealed_segments` 移入 `pending_deletes`。
  2. 按批(每轮上限 64 个对象)尽力执行 S3 `DELETE`(`NotFound` 视为幂等成功)。
  3. 通过 ETag 条件写入将已确认删除的段名从 `pending_deletes` 中清理。
- **只读必要段的快速恢复:**`manifest.json` 持久化每段的序列范围、字节大小与密封时间(`segment_meta`,保持向后兼容的 schema v2)。重启时,`recover()`、`read_after_cursor()` 与 `read_after()` 会跳过 `pending_deletes` 以及所有 `last_seq <= manifest.cursor` 的段,不发 `GET` 请求,仅下载未确认段(或尚无元数据的历史/LIST-fallback 段)。

### 对象存储成本模型

| 成本项 | 启用保留策略后的行为 |
| --- | --- |
| **对象存储占用** | 默认策略下收敛为"未确认段 + `active_segment`(1 个已密封段)"。显式设限时收敛为 `active + 未确认段 + min(max_segments, max_age × 封段速率, max_bytes)`。 |
| **DELETE 请求** | AWS S3 及主流兼容对象存储对标准 `DELETE` 免费;每轮上限 64 个对象,存量平滑消化。 |
| **Manifest PUT** | 实际触发回收的轮次增加 2 次小对象 manifest PUT(摘除注册 + 清理队列);空闲轮次为 **0** 次 PUT。 |
| **重启 GET 开销** | 从 `O(全量历史段数)` 降至 `O(仅未确认段数)`,消除长稳运行后的重启线性劣化。 |

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
- `retention.max_age` 设置时必须大于 0(需要立即回收请设 `retention.max_segments: 0` 或省略三项上限)
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

WAL 导出后台刷洗失败计数——关注其任何增长:

- `arkflow_stream_wal_flush_failures_total` —— 流输入 WAL 的后台刷洗失败(未启用持久化时为 0)
- `arkflow_job_wal_flush_failures_total` —— 作业输入 WAL 的后台刷洗失败(无 WAL 时为 0)

完整指标列表参见[可观测性](../operate/observability.md)。
