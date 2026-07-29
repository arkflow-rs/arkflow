# S3 WAL Backend Performance

This document describes the performance characteristics of the S3-backed WAL backend and provides guidance for tuning configuration based on workload requirements.

## Architecture Overview

```
Input → append_batch (μs, memory) → segment buffer → channel → PUT worker (async) → S3
                                     ↓ flush triggers                         ↓
                               max_entries / max_bytes / flush_interval      backpressure
                                                                        ↓
                                                                   channel full
```

**Pipeline Optimization**: Segment encoding and PUT are now decoupled from the write path via a flume channel with bounded capacity (16 segments). The write path (`append_batch`) returns immediately after sending to the channel, while PUT operations execute asynchronously in the background. This eliminates the blocking 10-200ms PUT latency from the write path.

**Batch Manifest Updates**: Manifest updates are now batched—accumulated for 8 segments or 100ms before flushing to S3. This reduces S3 operations by ~58% (from 3 operations per segment to ~1.24 operations per segment on average).

## Performance Characteristics

### Latency Breakdown

| Operation | Latency | Notes |
|-----------|---------|-------|
| `append_batch` | ~1-50μs | In-memory write + channel send (non-blocking) |
| Segment PUT | 10-200ms | Executes asynchronously in PUT worker |
| Channel send | <1μs | Blocking only when channel full (backpressure) |
| Manifest PUT (batched) | 10-100ms | Small JSON payload, 8x less frequent |
| Recovery (LIST + GET) | 100-500ms | Depends on segment count |

**Key changes**:
1. `append_batch` no longer blocks on PUT operations. The 10-200ms PUT latency is moved to the background.
2. Manifest updates are batched (every 8 segments or 100ms), reducing S3 GET/PUT operations by ~58%.

### Throughput Limits

| Factor | Typical Range | Bottleneck |
|--------|---------------|------------|
| S3 PUT (single worker) | 50-150 MB/s | Network bandwidth |
| S3 PUT (parallel, 4-8 workers) | 200-450 MB/s | Network bandwidth |
| S3 LIST recovery | 20-100 MB/s | API rate limits |
| Local encoding | 500+ MB/s | CPU (CRC32 + optional compression) |
| Channel backpressure | 16 segments/worker | Configurable bounded channel |

**Practical throughput**: 100-200 MB/s with the default 1-worker setup;
up to 450 MB/s with `parallel_put.workers: 8`.

**Parallel PUT benefit**: Multiple workers operate on independent bounded
channels and round-robin assign segments, increasing throughput up to
2-3× for high-QPS workloads without changing the at-least-once
contract. Watch for S3 per-prefix rate limits with 8 workers.

### Crash Window

The "loss window" is the number of entries at risk on node/pod disappearance:

```
loss_window = min(
    segment.max_entries,           # entry count trigger
    segment.max_bytes / avg_msg_size,  # byte size trigger
    segment.flush_interval * msg_rate  # time trigger
)
```

| flush_interval | @1000 msg/s | @10000 msg/s | @100000 msg/s |
|----------------|-------------|--------------|---------------|
| 100ms | 100 msgs | 1,000 msgs | 10,000 msgs |
| 1s | 1,000 msgs | 10,000 msgs | 100,000 msgs |
| 5s | 5,000 msgs | 50,000 msgs | 500,000 msgs |

## Configuration Tuning

### Segment Batching (D4)

Controls when the in-memory segment is sealed and PUT to S3.

```yaml
segment:
  max_entries: 1000      # default
  max_bytes: 1048576     # 1 MiB default
  flush_interval: 1s    # default
```

**Trade-offs:**

| Configuration | Latency | Crash Window | PUT Frequency |
|---------------|---------|--------------|--------------|
| Small segments (100, 100KB) | Lower PUT latency | Smaller window | Higher PUT rate |
| Large segments (10000, 10MB) | Higher PUT latency | Larger window | Lower PUT rate |
| Short interval (100ms) | Quick flush | Small window | High PUT rate |
| Long interval (10s) | Delayed flush | Large window | Low PUT rate |

**Recommendations:**

- **High throughput, relaxed durability**: `max_entries: 10000`, `max_bytes: 10MB`, `flush_interval: 10s`
- **Balanced**: defaults (`1000`, `1MB`, `1s`)
- **Small crash window**: `max_entries: 100`, `max_bytes: 100KB`, `flush_interval: 100ms`

### Cursor Flushing (D6)

Controls how often the committed cursor (watermark) is persisted to `manifest.json`.

```yaml
cursor:
  max_entries: 1000
  interval: 1s
```

**Trade-offs:**

| interval | Crash Recovery Duplication | PUT Frequency |
|----------|---------------------------|---------------|
| 100ms | Minimal (~100 msgs) | High |
| 1s | Low (~1000 msgs) | Moderate |
| 10s | High (~10000 msgs) | Low |

**Note**: Cursor batching adds at-least-once duplication but does not affect correctness. Outputs MUST tolerate duplicates regardless of this setting.

## Comparison with Local Backend

| Metric | Local (redb) | S3 Backend | Ratio |
|--------|--------------|-------------|-------|
| append latency | ~1μs | ~50μs | 50x slower |
| flush latency | ~2ms | ~50ms | 25x slower |
| throughput | 500 MB/s | 150 MB/s | 3x slower |
| crash window | ~100 msgs | ~1000 msgs | 10x larger |
| node recovery | ❌ No | ✅ Yes | N/A |

## Cost Considerations

### S3 API Costs

- **PUT requests**: $0.005 per 1,000 requests (US-East-1)
- **GET requests**: $0.0004 per 1,000 requests
- **LIST requests**: $0.005 per 1,000 requests
- **Storage**: $0.023 per GB/month (first 50TB)

**Cost example** @ 10,000 msg/s, 1KB per message:

```
- Segment PUTs: (10,000 / 1,000) × 3600 = 36 PUTs/hour × 24 × 30 = 25,920 PUTs/month
- PUT cost: 25,920 / 1,000 × $0.005 = $0.13/month
- Manifest PUTs (batched): ~0.125/second = 324,000 / 1,000 × $0.005 = $1.62/month
  (Previously: ~1/sec = $12.96/month, now ~87% reduction due to batching)
- Storage: 10,000 × 1KB × 86400 × 30 / 1e9 = 259 GB × $0.023 = $5.96/month
- Total: ~$7.71/month per stream (down from ~$19/month)
```

**Optimization**: Increase `segment.max_entries` and `cursor.interval` to reduce PUT costs further. Batching is already applied automatically (8 segments or 100ms).

## Performance Tuning Strategies (since 2026-07-28)

The S3 WAL backend supports three preset strategies via `segment_tuning.strategy`:

### Aggressive Strategy

For high throughput, low cost — tolerates a larger crash window.

```yaml
segment_tuning:
  strategy: aggressive
```

Defaults:
- `max_entries: 10000`
- `max_bytes: 10MB`
- `flush_interval: 10s`

**Crash window @ 10K msg/s**: ~100,000 messages at risk on node loss
**PUT cost reduction**: ~10x vs balanced
**Throughput**: Up to 200 MB/s

### Balanced Strategy (default)

Default trade-off between throughput and crash window.

```yaml
segment_tuning:
  strategy: balanced
```

Defaults:
- `max_entries: 1000`
- `max_bytes: 1MB`
- `flush_interval: 1s`

**Crash window @ 10K msg/s**: ~10,000 messages at risk
**Throughput**: 100-150 MB/s

### Low-Latency Strategy

For minimal crash window — highest PUT frequency.

```yaml
segment_tuning:
  strategy: low_latency
```

Defaults:
- `max_entries: 100`
- `max_bytes: 100KB`
- `flush_interval: 100ms`

**Crash window @ 10K msg/s**: ~1,000 messages at risk
**Throughput**: Lower due to frequent flushes

### Custom Overrides

Override individual parameters of any preset:

```yaml
segment_tuning:
  strategy: aggressive
  max_entries: 20000      # override default 10000
  flush_interval: "30s"   # override default 10s
```

## Parallel PUT Workers

Configure multiple PUT workers for 2-3x throughput improvement.

```yaml
parallel_put:
  workers: 4              # 1-8, default 1
  shutdown_timeout: "30s" # how long to wait for in-flight uploads
```

**Benefits:**
- Workers operate in parallel via independent channels
- Round-robin assignment (oldest segments first)
- Per-worker backpressure (16 segments each)

**Watch for S3 rate limits**: 8 workers can hit per-prefix limits.

**Manifest write safety**: With `workers > 1`, multiple workers can finish a
segment upload and seal it concurrently, each rewriting `manifest.json`. These
concurrent rewrites are coordinated with ETag-based optimistic concurrency
(read ETag → conditional PUT → retry on mismatch, up to 8 attempts covering the
worker ceiling), so no worker's cursor advancement or sealed-segment entry is
silently overwritten. This is invisible at `workers: 1` (the default): no
contention, no retries, behavior unchanged. See
`docs/docs/components/0-inputs/delivery-semantics.md` for the delivery-contract
details.

## Compression

Reduce S3 storage and network costs by 50-70% via segment compression.

```yaml
compression:
  type: zstd  # or "lz4", "none"
  level: 3    # algorithm-specific
```

**Algorithm comparison:**

| Algorithm | Compression Ratio | CPU Cost | Best For |
|-----------|-------------------|----------|----------|
| `none` | 1.0x | 0% | Default, low CPU |
| `lz4` | 2-3x (measured ~108× on Arrow IPC) | 2-5% | Fast compression |
| `zstd-3` | 3-5x (measured ~181× on Arrow IPC) | 5-10% | Balanced (default for compression) |
| `zstd-9` | 5-8x | 20-30% | Maximum compression |

**Measured compression ratios** on actual WAL segment payloads
(Arrow IPC frames with repetitive schema metadata) significantly exceed
the generic 50-70% estimate above because Arrow IPC has substantial
repetition. The numbers below are from the unit-test suite
(`compression_ratio_across_*_levels`):

```
lz4-1:  50000 -> 462 bytes (ratio: 108.23x)
lz4-4:  50000 -> 462 bytes (ratio: 108.23x)
lz4-9:  50000 -> 462 bytes (ratio: 108.23x)
zstd-1: 50000 -> 276 bytes (ratio: 181.16x)
zstd-3: 50000 -> 276 bytes (ratio: 181.16x)
zstd-6: 50000 -> 276 bytes (ratio: 181.16x)
zstd-9: 50000 -> 276 bytes (ratio: 181.16x)
```

Real-world ratios on production payloads (mixed data, less repetition)
will be lower than these synthetic benchmarks but still substantial.

**Min size threshold**: Segments smaller than 10KB are uploaded uncompressed.

## Failure Scenarios

### S3 Unavailable

- **append**: Blocks when channel fills up (16 segments) → input stalls (same as before, but with larger buffer)
- **recovery**: Fails to start stream → engine error
- **Mitigation**: Use S3 with cross-region replication, or add local cache

### Network Partition

- **Current behavior**: Same as S3 unavailable, but channel provides 16-segment buffer before blocking
- **Future improvement**: Local write-through cache with retry queue

### Segment PUT Partial Failure

- **Current**: Lost segment is skipped in recovery (D7). PUT worker logs error but continues processing
- **Impact**: Data loss for that segment only
- **Mitigation**: Enable S3 server-side encryption and versioning

## Monitoring

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `segment_put_latency` | Time to PUT a segment | >500ms p99 |
| `segment_put_frequency` | PUTs per second | >10/sec (may need tuning) |
| `segment_size` | Average segment bytes | <100KB or >10MB |
| `cursor_lag` | Cursor vs max written seq | >10,000 entries |
| `recovery_latency` | Time to replay WAL on startup | >5s |

### Logging

Key log lines to monitor:

```
[DEBUG] Sealing segment: entries={n}, bytes={size}
[INFO]  Segment PUT complete: {segment_key}, {size} bytes in {duration_ms}ms
[INFO]  Recovery: replayed {n} entries from {m} segments in {duration_ms}ms
[WARN]  Segment PUT failed: {error}, retrying...
```

## References

- User-facing tuning guide: `docs/docs/components/0-inputs/wal-optimization.md`
- Design: `openspec/changes/archive/2026-07-27-add-wal-s3-backend/design.md`
- Design: `openspec/changes/archive/2026-07-28-comprehensive-wal-optimization/design.md`
- Spec: `openspec/specs/input-durability/spec.md`
- Implementation: `crates/arkflow-plugin/src/wal/s3.rs`
- Compression: `crates/arkflow-plugin/src/wal/compression.rs`
