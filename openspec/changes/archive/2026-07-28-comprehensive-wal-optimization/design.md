## Context

### Current State

ArkFlow's S3 WAL backend (`crates/arkflow-plugin/src/wal/s3.rs`) implements an async pipeline architecture that decouples writes from PUT operations:

```
Input → append_batch (<1μs) → channel → PUT worker (async) → S3
                                    ↓
                            16-segment buffer
```

The current implementation:
- Single PUT worker uploads segments sequentially
- Fixed segment batching parameters (`max_entries: 1000`, `max_bytes: 1MB`, `flush_interval: 1s`)
- No compression support
- Throughput limited to 100-150 MB/s

### Constraints

- Must maintain at-least-once delivery semantics
- Must preserve crash recovery behavior
- Must not break existing local (redb) backend
- Rust 1.88+ with Tokio async runtime
- Compression libraries must be async-compatible (or wrapped in blocking tasks)

### Stakeholders

- Users running high-throughput streams (10,000+ msg/s)
- Cost-conscious users (S3 API and storage costs)
- K8s deployments with frequent pod restarts

---

## Goals / Non-Goals

**Goals:**

1. Enable 2-3x higher throughput via parallel PUT workers
2. Reduce S3 PUT costs by 90% through aggressive segment tuning
3. Reduce S3 storage and network costs by 50-70% via compression
4. Provide user-friendly presets for common scenarios
5. Maintain backward compatibility (default behavior unchanged)

**Non-Goals:**

- Optimizing read/recovery path (cache layer is separate work)
- Modifying local (redb) backend behavior
- Changing WAL semantics (at-least-once, crash recovery)
- Implementing tiered storage (hot/warm/cold)
- Adding metrics beyond basic compression ratio

---

## Decisions

### Decision 1: Segment Preset Strategies

**Choice:** Implement three preset strategies (`aggressive`, `balanced`, `low-latency`) with override capability.

**Rationale:**
- Presets reduce configuration complexity for common scenarios
- Overrides provide flexibility for edge cases
- Aligns with user mental model (similar to database connection pool presets)

**Alternatives considered:**
- Only custom parameters: Rejected as too complex for average users
- More than three presets: Rejected to avoid choice paralysis

**Implementation:**
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SegmentStrategy {
    Aggressive,
    Balanced,
    LowLatency,
}

impl SegmentStrategy {
    fn defaults(&self) -> SegmentConfig {
        match self {
            Self::Aggressive => SegmentConfig {
                max_entries: 10000,
                max_bytes: 10_485_760,  // 10 MB
                flush_interval: Duration::from_secs(10),
            },
            Self::Balanced => SegmentConfig { /* current defaults */ },
            Self::LowLatency => SegmentConfig {
                max_entries: 100,
                max_bytes: 102_400,  // 100 KB
                flush_interval: Duration::from_millis(100),
            },
        }
    }
}
```

### Decision 2: Parallel PUT Workers

**Choice:** Implement multiple PUT workers with priority queue and ordered completion tracking.

**Rationale:**
- Maximizes throughput (2-3x improvement target)
- Priority queue ensures older segments (blocking manifest) upload first
- Ordered completion tracking maintains consistency

**Alternatives considered:**
- True parallel with relaxed ordering: Rejected as it complicates recovery
- Hash-based worker assignment: Rejected as it can cause head-of-line blocking

**Implementation:**
```rust
struct ParallelPutWorkers {
    workers: Vec<PutWorker>,
    queue: PriorityQueue<Segment, SeqNum>,
    completions: HashMap<SeqNum, JoinHandle<Result<()>>>,
    next_expected: AtomicU64,
}

impl ParallelPutWorkers {
    async fn schedule_put(&self, segment: Segment) {
        self.queue.push(segment.seq, segment);
        while let Some(worker) = self.next_idle_worker() {
            if let Some(segment) = self.queue.pop() {
                worker.put(segment).await;
            }
        }
    }
}
```

### Decision 3: Compression Library Selection

**Choice:** Support both `zstd` (via `zstd` crate) and `lz4` (via `lz4_flex` crate).

**Rationale:**
- `zstd`: Best compression ratio (3-4x), widely used in industry
- `lz4`: Fastest compression/decompression, good for CPU-sensitive workloads
- Both have mature Rust implementations with permissive licenses

**Alternatives considered:**
- `gzip`: Rejected due to slower compression
- Single algorithm only: Rejected as different workloads have different needs

**Implementation:**
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompressionType {
    None,
    Zstd { level: i32 },
    Lz4 { level: i32 },
}

impl CompressionType {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Zstd { level } => zstd::compress(data, *level),
            Self::Lz4 { level } => lz4_flex::compress_level(data, *level),
        }
    }
}
```

### Decision 4: Configuration Schema

**Choice:** Extend existing `ObjectStoreWalConfig` with new optional fields.

**Rationale:**
- Maintains backward compatibility (existing configs work unchanged)
- Allows fine-grained control without breaking changes
- Aligns with existing YAML hierarchy

**Implementation:**
```yaml
# New configuration fields
durability:
  backend:
    type: object_store
    s3:
      # ... existing S3 config
    segment_tuning:
      strategy: aggressive  # or balanced, low-latency
      max_entries: 5000     # optional override
      max_bytes: 5MB        # optional override
      flush_interval: 5s    # optional override
    parallel_put_workers: 4
    compression: zstd
    compression_level: 3
    compression_min_size: 10KB
```

### Decision 5: Per-Worker vs Global Backpressure

**Choice:** Per-worker bounded channels (16 segments each) with global fallback.

**Rationale:**
- Per-worker channels allow independent backpressure
- Global fallback prevents unbounded memory when all workers are busy
- Simpler than priority-based global queue

**Implementation:**
```rust
struct PutWorker {
    sender: flume::Sender<Segment>,
    _handle: JoinHandle<()>,
}

impl PutWorker {
    fn new() -> Self {
        let (sender, receiver) = flume::bounded(16);  // per-worker limit
        let handle = tokio::spawn(async move {
            while let Ok(segment) = receiver.recv_async().await {
                put_segment(segment).await;
            }
        });
        Self { sender, _handle: handle }
    }
}
```

---

## Risks / Trade-offs

### Risk 1: Increased CPU usage with compression

**Risk:** Compression may increase CPU usage, especially at higher levels.

**Mitigation:**
- Default to moderate compression levels (zstd-3, LZ4-4)
- Document CPU trade-offs in performance guide
- Provide `compression_min_size` to skip small segments

### Risk 2: S3 rate limiting with parallel PUT

**Risk:** Multiple parallel PUT workers may trigger S3 rate limits.

**Mitigation:**
- Cap workers at 8 (configurable)
- Implement exponential backoff on 503 errors
- Document rate limits in performance guide

### Risk 3: Memory increase with larger segments

**Risk:** Aggressive segment tuning increases per-segment memory footprint.

**Mitigation:**
- Document memory requirements (10 MB per active segment)
- Implement segment size validation at config load time
- Default to `balanced` strategy for moderate memory usage

### Risk 4: Decompression failures during recovery

**Risk:** Corrupted compressed segments could block recovery.

**Mitigation:**
- Store compression type in manifest (don't guess)
- Validate decompression before parsing entries
- Skip malformed segments with error logging

### Trade-off: Crash Window vs Throughput

**Trade-off:** Aggressive segment tuning increases crash window (unflushed entries at risk).

**Acceptance:**
- Document crash window for each strategy
- Users choose based on their loss tolerance
- Default to `balanced` for moderate trade-off

---

## Migration Plan

### Phase 1: Configuration Schema (Week 1)

1. Add new config fields to `arkflow-core/src/wal/config.rs`
2. Update `--validate` to handle new fields
3. Add integration tests for config validation

### Phase 2: Segment Tuning (Week 2)

1. Implement `SegmentStrategy` enum and defaults
2. Update `S3Store` to use configurable parameters
3. Add tests for each preset strategy

### Phase 3: Parallel PUT Workers (Week 3-4)

1. Implement `ParallelPutWorkers` struct
2. Add priority queue and completion tracking
3. Update `S3Store::build` to spawn multiple workers
4. Add stress tests for concurrent uploads

### Phase 4: Compression (Week 5)

1. Add `zstd` and `lz4_flex` dependencies
2. Implement compression before PUT
3. Implement decompression during recovery
4. Update manifest schema to include compression field
5. Add compression metrics

### Phase 5: Documentation and Examples (Week 6)

1. Update `docs/performance/s3-wal-backend.md`
2. Create `examples/durability_example_optimized.yaml`
3. Add migration guide for existing users

### Rollback Strategy

- New fields are optional; old configs work unchanged
- Feature flags behind `backend: s3 + segment_tuning` (no impact on local backend)
- Can disable individual optimizations (e.g., use segment tuning without compression)

---

## Open Questions

1. **Should compression be enabled by default for new S3 WAL configs?**
   - Recommendation: No, opt-in to avoid surprise CPU usage

2. **Should we auto-tune segment parameters based on observed throughput?**
   - Recommendation: Out of scope, manual tuning for now

3. **Should we support custom compression algorithms via plugins?**
   - Recommendation: Out of scope, zstd/lz4 cover 99% of use cases

4. **What should the `shutdown_timeout` default be for parallel workers?**
   - Recommendation: 30 seconds (configurable)

---

## Performance Targets

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Throughput | 150 MB/s | 300-450 MB/s | Benchmark with 10K msg/s |
| PUT Cost (10K msg/s) | $7.71/month | $0.77/month | 90% reduction via aggressive tuning |
| Storage Cost | $5.96/month | $2.38/month | 60% reduction via compression |
| Crash Window (balanced) | 1,000 msgs | 1,000 msgs | Unchanged for default |

---

## Testing Strategy

### Unit Tests

- Config validation for each new field
- Segment strategy defaults
- Compression ratio verification
- Worker queue ordering

### Integration Tests

- End-to-end with MinIO (S3-compatible)
- Recovery with compressed segments
- Parallel PUT ordering guarantees
- Backpressure activation

### Benchmarks

- Throughput with 1, 4, 8 workers
- Compression ratio vs level (zstd 1-9, LZ4 1-9)
- Crash window measurement per strategy
