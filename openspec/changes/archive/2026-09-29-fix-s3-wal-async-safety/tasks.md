## 1. 异步驱动安全（wal/mod.rs + s3 build）

- [x] 1.1 `wal/mod.rs` 新增私有 `call_store`（spawn_blocking + JoinError 映射），全部 async 方法内的同步 store 调用（append_batch/advance_cursor/cursor/read_after_cursor/mark_committed/rewind_cursor/close，含 acknowledge/undo_ack/reconcile_covered/flush_pending 内的调用点）改经它执行
- [x] 1.2 `S3Store::build` 的初始 block_on（client 构建 + recovery）移入短命 std 线程，任何调用上下文安全；既有同步单测仍全绿
- [x] 1.3 异步路径集成测试（LocalFileSystem 离线）：经真实 `Wal` async API 驱动 append→advance→acknowledge→read_after_cursor（修复前的 panic 场景即回归断言）

## 2. S3 rewind 补偿（s3.rs）

- [x] 2.1 `floor: AtomicU64` 毒化机制：`rewind_cursor` 覆写（fetch_min(r+1)、镜像钳制、必要时纠正性 flush），`advance_cursor` 在 `s+1 >= floor` 时解除毒化
- [x] 2.2 内存 cursor 镜像：advance/rewind/flush/recovery 维护，`cursor()` 读镜像（消除每-ack S3 GET）
- [x] 2.3 `flush_manifest` target 取 `min(acked_hwm, max_sealed_seq, floor-1)`
- [x] 2.4 行为测试：advance(N)→rewind(N-1)→强制 flush→read_after_cursor 含 N 且 manifest cursor ≤ N-1；re-ack advance(N) 后 flush 恢复推进越过 N

## 3. 验证

- [x] 3.1 `cargo test -p arkflow-core --lib wal` 与 `cargo test -p arkflow-plugin --lib wal` 全绿
- [x] 3.2 `cargo test --workspace --all-targets` 全绿
- [x] 3.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 3.4 `openspec validate fix-s3-wal-async-safety` 通过
