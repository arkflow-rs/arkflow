## Why

P1-1（`openspec/CODE_REVIEW_2026-09-29.md`，**已运行时实证**）：S3/object_store WAL 后端在引擎的异步路径上必然 panic。`S3Store` 全部 trait 方法内部用私有 runtime 做 `self.runtime.block_on(...)`（`crates/arkflow-plugin/src/wal/s3.rs:661,675,685,708,729,751,787`），而引擎唯一调用方式是异步路径（`crates/arkflow-core/src/wal/mod.rs` 的 `append/advance/acknowledge/undo_ack/read_after_cursor/reconcile_covered/cursor/flush_pending/close` 全部 async，`:411,437-443,502-555,625-688,704,725-735,748,760,799` 直呼同步 store 方法）。2026-09-29 审查中以临时 `#[tokio::test]` 实测：`store.cursor()` 第一个调用即 panic——`Cannot start a runtime from within a runtime`（tokio 1.53.1）。全部既有 e2e 测试为同步 `#[test]`，异步路径零覆盖——`backend: object_store` 配置在生产引擎里开箱即崩，但它有完整 spec、通过配置校验、文档照常宣传。

同根因的第二问题：即便不 panic（同步测试路径），store 的阻塞 I/O（redb fsync、S3 PUT/GET）直接跑在 tokio worker 线程上——并发的 P1 发现（CODE_REVIEW 并发纪律一节）。

第三问题（同为 P1-1 记录）：**S3 的 rewind 补偿被破坏**。`S3Store` 未覆写 `rewind_cursor`（走默认实现：manifest 未 flush 时静默 no-op、已 flush 时报错，`wal/store.rs:147-154`），且 `advance_cursor` 的 `acked_hwm.fetch_max` 无任何回退路径（`s3.rs:672`）、周期性 `flush_manifest` 会把 manifest cursor 推进到失败序列（仅受 max_sealed_seq clamp，`s3.rs:912-919`）——违反 `input-durability` spec 既有要求 "Both WAL backends keep the replay guarantee"（spec.md:228-231）：源提交失败后的重放保证在 S3 后端上不成立。另外 `S3Store::build` 的首个 `block_on`（client 构建与 recovery，`s3.rs:289-292`）在 async 上下文调用时同样 panic（`build_wal_store` 被 sync builder 链从 async connect 调用）。

## What Changes

- `wal/mod.rs` 新增私有 `call_store` 辅助（`spawn_blocking` 驱动），**全部 async 方法里的同步 store 调用改经它执行**——一箭双雕：消灭嵌套 runtime panic（blocking 线程上 `block_on` 合法）+ 把 store 的阻塞 I/O 移出 async worker。
- `S3Store::build` 的初始 block_on（client 构建 + recovery + flusher spawn）移到短命 std 线程上执行——构造路径在任何上下文（sync builder 自 async connect）下安全。
- **S3 rewind 补偿**：实现 `rewind_cursor` 覆写——"毒化 floor"（被回退的序列在源重新 ack 通过它之前不得被 manifest 封存）+ 内存 cursor 镜像（`advance/rewind/flush/recovery` 维护，`cursor()` 读镜像——顺带消灭每次 ack 一次 S3 GET 的热路径问题）；`flush_manifest` 的 target 取 `min(acked_hwm, max_sealed_seq, floor-1)`；源重新 ack 越过 floor 后解除。
- **异步路径集成测试**（离线，LocalFileSystem）：把审查时的临时复现转为永久测试——经真实 `Wal` async API（append→acknowledge→read_after_cursor）驱动 S3Store，加上 rewind 补偿的行为测试（advance→rewind→manifest flush 不能越过 floor；re-ack 后 floor 解除）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `input-durability`: ①"Pluggable WAL storage backend" 增补异步驱动安全条款（后端构建与 store 调用在异步上下文 SHALL 不 panic、不阻塞 worker——引擎经 blocking 池驱动）；②"WAL cursor advancement precedes wrapped source commit" 的双后端重放保证场景补强——显式覆盖"回退与再推进之间发生 manifest flush"的窗口（S3 此前违反）。

## Impact

- `crates/arkflow-core/src/wal/mod.rs`（call_store + 调用点改造）
- `crates/arkflow-plugin/src/wal/s3.rs`（build 线程化、rewind/floor、cursor 镜像）
- 配套测试：`s3.rs` 内嵌测试新增异步路径集成 + rewind 补偿行为
- 无配置面变更、无存储格式变更、无 wire 变更；本地 redb 后端行为不变（其 store 调用同样改经 spawn_blocking，语义等价、阻塞移出 worker）。

## Non-goals

- 不实现段回收（D7，spec 既有缺口，独立立项）；不清理压缩/并行 PUT 死代码（`s3-wal-pipeline` spec 与实现的差距另行收口）。
- 不把 `WalStore` trait 异步化（spawn_blocking 已同时解决 panic 与阻塞，trait 改造牵连面大）。
- 不做 S3 WAL 的性能优化（批量 PUT、manifest 缓存策略）——仅 cursor 镜像顺带消除每-ack GET。
- 不处理弱一致存储的 read-after-write 边缘（minio 集成为 `#[ignore]` 真库测试，维持现状）。
