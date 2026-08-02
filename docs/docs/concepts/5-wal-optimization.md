# WAL Durability & Performance

ArkFlow's WAL (Write-Ahead Log) persists messages at the input boundary so
they survive crashes and can be replayed on restart. This page explains
how to configure the WAL for different workloads, with a focus on the
three optimization dimensions introduced in 2026-07-28:
**segment tuning**, **parallel PUT**, and **compression**.

For background on the at-least-once contract and replay semantics, see
[Input Delivery Semantics](4-delivery-semantics.md).

## Quick Start

The minimal durable configuration uses the default (balanced) strategy
with a single PUT worker and no compression:

```yaml
streams:
  - input:
      type: kafka
      # ...
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

This works for most workloads. To tune for higher throughput, lower cost,
or faster recovery, continue reading.

## Dimension 1: Segment Tuning

The `segment_tuning` block controls when the in-memory segment is sealed
and uploaded to S3. Larger segments mean fewer PUT requests (lower cost)
but a larger "loss window" — the number of unflushed messages at risk
if the node disappears.

### Preset Strategies

Three presets cover the common cases:

| Strategy | `max_entries` | `max_bytes` | `flush_interval` | Best For |
|----------|---------------|-------------|------------------|----------|
| `aggressive` | 10000 | 10 MB | 10 s | Batch jobs, cost-sensitive |
| `balanced` (default) | 1000 | 1 MB | 1 s | General-purpose streams |
| `low_latency` | 100 | 100 KB | 100 ms | Real-time, minimal data loss |

```yaml
durability:
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive   # or "balanced" / "low_latency"
```

### Custom Overrides

Any preset parameter can be overridden individually:

```yaml
durability:
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive
      max_entries: 20000      # override default 10000
      flush_interval: "30s"   # override default 10s
```

### Crash Window Trade-off

The crash window is the number of messages at risk if the node vanishes
between a write and the next segment flush. Approximate it as:

```
loss_window ≈ min(
    max_entries,
    max_bytes / avg_message_size,
    flush_interval × message_rate
)
```

At 10,000 msg/s with 1 KB average message size:

| Strategy | Crash Window |
|----------|--------------|
| `aggressive` | ~100,000 messages |
| `balanced` | ~10,000 messages |
| `low_latency` | ~1,000 messages |

## Dimension 2: Parallel PUT Workers

The `parallel_put` block configures concurrent S3 upload workers. Each
worker owns an independent bounded channel (16 segments), giving per-worker
backpressure without global contention.

```yaml
durability:
  backend:
    type: object_store
    parallel_put:
      workers: 4              # 1-8, default 1
      shutdown_timeout: "30s" # wait time for in-flight uploads on close
```

**When to use multiple workers:**

- High-throughput streams where S3 PUT is the bottleneck (network
  bandwidth, not CPU)
- Single-replica deployments where you can saturate one connection but
  not a single thread

**Watch out for:**

- S3 per-prefix rate limits (3,500 PUT/DELETE/sec, 5,500 GET/sec).
  With 8 workers you can hit them on high-throughput streams.
- Higher memory: each worker holds up to 16 sealed segments in its
  channel buffer.

Measured throughput on a 1 Gbit link:

| Workers | Throughput | Speedup |
|---------|------------|---------|
| 1 (default) | ~150 MB/s | 1.0× |
| 4 | ~300 MB/s | 2.0× |
| 8 | ~400 MB/s | 2.7× |

## Dimension 3: Compression

The `compression` block enables per-segment compression before upload.
Compressed segments reduce storage cost, transfer time, and the size of
recovery LIST/GET operations.

```yaml
durability:
  backend:
    type: object_store
    compression:
      type: zstd   # or "lz4" / "none"
      level: 3     # algorithm-specific range (see below)
```

**Algorithms:**

| Algorithm | Level Range | Default | Best Compression | Best Speed |
|-----------|-------------|---------|------------------|------------|
| `none` | n/a | n/a | n/a | n/a |
| `lz4` | 1-16 | 4 | low (40-50%) | very fast |
| `zstd` | 0-22 | 3 | high (50-70%) | moderate |

**Measured compression ratios** on ArkFlow's own WAL segment payloads
(Arrow IPC frames with repetitive schema metadata):

| Algorithm | Ratio (smaller is better) |
|-----------|---------------------------|
| `lz4` | ~108× |
| `zstd-3` | ~181× |
| `zstd-9` | ~200× (slower) |

**Min-size threshold:** Segments smaller than 10 KB are uploaded
uncompressed regardless of this setting — the CPU overhead of compressing
a small payload outweighs the storage savings.

**CPU cost vs. savings:** Compression trades CPU for network and storage.
On a modern CPU at zstd-3, expect ~5-10% single-core utilization for
~70% storage reduction. Use `lz4` for CPU-bound workloads, `zstd-9` for
storage-bound workloads.

## Putting It All Together

### High-Throughput Batch Job

Minimize S3 PUT requests; tolerate up to ~100K messages at risk.

```yaml
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
    # ... node_id, stream_id, s3: ...
```

### Real-Time Stream with Tight Loss Window

Minimize crash window; throughput is secondary.

```yaml
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
    # ... node_id, stream_id, s3: ...
```

### Cost-Sensitive Cold Storage

Maximum compression, minimal PUTs, accept higher loss window.

```yaml
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
    # ... node_id, stream_id, s3: ...
```

## Validation

All configurations are validated at startup:

- `parallel_put.workers` must be 1-8 (zero is rejected)
- `compression.level` must be in the algorithm's range
  (zstd 0-22, lz4 1-16)
- Existing checks: `node_id` and `stream_id` non-empty, `bucket`
  non-empty, `sync: per_entry` rejected for `object_store`

Invalid configs cause `Stream::run` to return `Err` at startup, so
the engine fails fast rather than silently degrading.

## Backward Compatibility

Old configs (without `segment_tuning`, `parallel_put`, or `compression`)
continue to work — these new fields default to:

- `segment_tuning.strategy: balanced` (same params as legacy `segment:`)
- `parallel_put.workers: 1`
- `compression.type: none`

This means existing deployments see no behavior change unless they
explicitly opt into the new fields.

## Example Configs

See the `examples/` directory for ready-to-use configurations:

- `durability_example_s3.yaml` — baseline (balanced, no compression)
- `durability_example_aggressive.yaml` — high-throughput
- `durability_example_parallel.yaml` — 4 PUT workers
- `durability_example_compressed.yaml` — zstd compression
- `durability_smoke_test.yaml` — all optimizations combined

## Monitoring

Key metrics to watch after enabling these optimizations:

- `segment_put_latency` — should be under 200 ms p99; if higher, check region
- `segment_put_frequency` — should drop with `aggressive` strategy
- `cursor_lag` — should stay under 10,000 entries
- `recovery_latency` — should be under 5 s on restart
