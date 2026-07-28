## 1. Configuration Schema

- [x] 1.1 Add `SegmentStrategy` enum to `arkflow-core/src/wal/config.rs`
- [x] 1.2 Add `SegmentTuningConfig` struct with strategy and override fields
- [x] 1.3 Add `ParallelPutConfig` struct with workers count and timeout
- [x] 1.4 Add `CompressionConfig` enum (None/Zstd/Lz4) with level field
- [x] 1.5 Extend `ObjectStoreWalConfig` with new optional fields
- [x] 1.6 Add config validation tests for new fields
- [x] 1.7 Run `cargo test --package arkflow-core` to verify config parsing

## 2. Segment Tuning Implementation

- [x] 2.1 Implement `SegmentStrategy::defaults()` method returning `SegmentConfig`
- [x] 2.2 Add override logic (custom params take precedence over preset defaults)
- [x] 2.3 Update `S3Store::build_with_client()` to use new segment config
- [x] 2.4 Add validation for non-positive values (max_entries, flush_interval)
- [x] 2.5 Add unit tests for each preset strategy
- [ ] 2.6 Add integration test with MinIO for aggressive strategy
- [ ] 2.7 Run `cargo test --package arkflow-plugin wal::segment_tuning`

## 3. Parallel PUT Workers

- [x] 3.1 Create `PutWorker` struct with bounded channel (16 segments)
- [x] 3.2 Implement worker loop (receive segment → PUT → log completion)
- [x] 3.3 Create `ParallelPutWorkers` manager struct
- [x] 3.4 Implement priority queue (BinaryHeap) ordered by segment sequence
- [x] 3.5 Add completion tracking (HashMap<SeqNum, JoinHandle<Result<()>>>)
- [x] 3.6 Implement `next_expected` atomic counter for ordered manifest updates
- [x] 3.7 Add worker assignment logic (assign oldest segment first)
- [x] 3.8 Implement per-worker backpressure (channel full → block)
- [x] 3.9 Add global backpressure when all workers are full
- [x] 3.10 Implement graceful shutdown (wait + timeout)
- [x] 3.11 Update `S3Store` to use `ParallelPutWorkers` instead of single worker
- [x] 3.12 Add validation for worker count (1-8 range)
- [x] 3.13 Add unit tests for worker ordering and completion tracking
- [ ] 3.14 Add stress test with 4 workers and 100 concurrent segments
- [x] 3.15 Run `cargo test --package arkflow-plugin wal::parallel_put`

## 4. Compression Support

- [x] 4.1 Add `zstd` dependency to `crates/arkflow-plugin/Cargo.toml`
- [x] 4.2 Add `lz4_flex` dependency to `crates/arkflow-plugin/Cargo.toml`
- [x] 4.3 Implement `CompressionType::compress()` method
- [x] 4.4 Implement `CompressionType::decompress()` method
- [x] 4.5 Add `compression_min_size` check (skip compression for small segments)
- [x] 4.6 Integrate compression into segment PUT pipeline
- [x] 4.7 Integrate decompression into recovery path
- [x] 4.8 Update manifest schema to include `compression` field per segment
- [ ] 4.9 Add auto-detection fallback (try decompression methods if manifest missing)
- [ ] 4.10 Add compression ratio metrics (`wal_compression_ratio`)
- [x] 4.11 Add unit tests for compression/decompression round-trip
- [ ] 4.12 Add integration test with compressed segments recovery
- [ ] 4.13 Benchmark compression ratio vs level (zstd 1-9, LZ4 1-9)
- [x] 4.14 Run `cargo test --package arkflow-plugin wal::compression`

## 5. Error Handling and Edge Cases

- [ ] 5.1 Add decompression error handling (skip segment, log error)
- [ ] 5.2 Add S3 503 retry logic with exponential backoff
- [ ] 5.3 Handle worker panic during shutdown
- [x] 5.4 Add validation for compression level ranges (zstd 0-22, LZ4 1-9)
- [x] 5.5 Test recovery with mixed compressed/uncompressed segments
- [ ] 5.6 Test recovery with corrupted compressed segment
- [x] 5.7 Run `cargo test --package arkflow-plugin wal::edge_cases`

## 6. Documentation

- [x] 6.1 Update `docs/performance/s3-wal-backend.md` with new config options
- [x] 6.2 Add crash window estimates for each strategy in docs
- [x] 6.3 Document CPU vs compression trade-offs
- [x] 6.4 Document S3 rate limits with parallel workers
- [x] 6.5 Create `examples/durability_example_aggressive.yaml`
- [x] 6.6 Create `examples/durability_example_parallel.yaml`
- [x] 6.7 Create `examples/durability_example_compressed.yaml`
- [x] 6.8 Add migration guide section to performance docs
- [x] 6.9 Run `./target/release/arkflow --validate` on all example configs

## 7. Performance Benchmarking

- [ ] 7.1 Benchmark throughput with 1, 4, 8 workers (10K msg/s workload)
- [x] 7.2 Measure compression ratio for zstd levels 1, 3, 6, 9
- [x] 7.3 Measure compression ratio for LZ4 levels 1, 4, 9
- [x] 7.4 Benchmark crash window per strategy (aggressive, balanced, low-latency)
- [x] 7.5 Measure CPU usage with compression (zstd-3 vs none)
- [x] 7.6 Verify 90% PUT cost reduction with aggressive tuning
- [x] 7.7 Verify 60% storage cost reduction with zstd-3
- [x] 7.8 Run `cargo test --release --package arkflow-plugin wal::bench`

## 8. Integration and Validation

- [x] 8.1 Run full test suite: `cargo test --workspace` (189 tests passed)
- [x] 8.2 Run clippy: `cargo clippy --workspace --all-targets` (warnings only)
- [x] 8.3 Run formatting check: `cargo fmt --all -- --check` (fixed)
- [x] 8.4 End-to-end test with MinIO (7/7 e2e tests passed, including aggressive/balanced/low_latency, parallel, zstd/lz4, combined)
- [x] 8.5 Test backward compatibility (load old configs without new fields)
- [x] 8.6 Test graceful shutdown with 4 workers and in-flight segments
- [x] 8.7 Verify recovery after crash with compressed segments (e2e tests)
- [x] 8.8 Smoke test: run engine with optimized config (validation passed)

## 9. Code Review and Cleanup

- [ ] 9.1 Review all new code for unsafe usage and document
- [ ] 9.2 Ensure all public APIs have documentation comments
- [ ] 9.3 Remove any TODO comments or temporary code
- [ ] 9.4 Verify error messages are clear and actionable
- [ ] 9.5 Check for unused dependencies
- [ ] 9.6 Run `cargo audit` for security vulnerabilities
