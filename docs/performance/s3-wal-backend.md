# S3 WAL Backend Performance

This document describes the performance characteristics of the S3-backed WAL backend and provides guidance for tuning configuration based on workload requirements.

## Architecture Overview

```
Input → append_batch (μs, memory) → segment buffer → PUT (ms) → S3
                                     ↓ flush triggers
                               max_entries / max_bytes / flush_interval
```

## Performance Characteristics

### Latency Breakdown

| Operation | Latency | Notes |
|-----------|---------|-------|
| `append_batch` | ~1-50μs | In-memory write, returns immediately |
| Segment PUT | 10-200ms | Depends on size, network, S3 region |
| Manifest PUT | 10-100ms | Small JSON payload, less frequent |
| Recovery (LIST + GET) | 100-500ms | Depends on segment count |

### Throughput Limits

| Factor | Typical Range | Bottleneck |
|--------|---------------|------------|
| S3 PUT (single stream) | 50-150 MB/s | Network bandwidth |
| S3 LIST recovery | 20-100 MB/s | API rate limits |
| Local encoding | 500+ MB/s | CPU (CRC32) |

**Practical throughput**: 50-150 MB/s for most workloads

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
- Manifest PUTs: ~1/second = 2,592,000 / 1,000 × $0.005 = $12.96/month
- Storage: 10,000 × 1KB × 86400 × 30 / 1e9 = 259 GB × $0.023 = $5.96/month
- Total: ~$19/month per stream
```

**Optimization**: Increase `segment.max_entries` and `cursor.interval` to reduce PUT costs.

## Failure Scenarios

### S3 Unavailable

- **append**: Blocks in `seal_active_segment` → input stalls
- **recovery**: Fails to start stream → engine error
- **Mitigation**: Use S3 with cross-region replication, or add local cache

### Network Partition

- **Current behavior**: Same as S3 unavailable
- **Future improvement**: Local write-through cache with retry queue

### Segment PUT Partial Failure

- **Current**: Lost segment is skipped in recovery (D7)
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

- Design: `openspec/changes/archive/2026-07-27-add-wal-s3-backend/design.md`
- Spec: `openspec/specs/input-durability/spec.md`
- Implementation: `crates/arkflow-plugin/src/wal/s3.rs`
