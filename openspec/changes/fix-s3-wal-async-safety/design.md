## Context

S3Store 的同步 `WalStore` 方法内部 `self.runtime.block_on(...)`，引擎从 async 上下文直呼（已实证 panic）；rewind 无补偿（floor 缺失 + `acked_hwm` 单调无回退）；`cursor()` 每次调用做一次完整 S3 GET；`build` 的首个 block_on 在 async 上下文同样 panic。本地 redb 后端正确但同样把阻塞 I/O 跑在 async worker 上。

## Goals / Non-Goals

**Goals:**
- `backend: object_store` 在引擎异步路径上可用（不 panic、不阻塞 worker）。
- S3 后端满足 "Both WAL backends keep the replay guarantee"：源提交失败的序列不会被 manifest 封存越过，重放可见。
- 永久回归防线：离线异步路径集成测试 + rewind 补偿行为测试。

**Non-Goals:**
- 段回收、死代码清理、trait 异步化、性能专项（见 proposal Non-goals）。

## Decisions

**D1 — `Wal::call_store`：全部 async 方法内的 store 调用经 `spawn_blocking`。**
`WalStore: Send + Sync + 'static`，闭包 `FnOnce(&Arc<dyn WalStore>) -> Result<T, Error> + Send + 'static`，`JoinError` 映射为 Process 错误（store panic 可见而非悬挂）。blocking 线程不在 runtime 上下文中，S3 的私有 runtime `block_on` 在其上合法——panic 与 worker 阻塞一并消除。*备选否决*：trait 异步化（牵连 store.rs/两个后端/全部调用点，收益相同代价大）；`block_in_place`（依赖多线程 runtime 且污染嵌套调用方）。`Wal::new`（sync，`:305-330`）中的 `store.cursor()` 保留直呼——构造期由 D2 保证安全。

**D2 — `S3Store::build` 的初始 block_on 移到短命 std 线程。**
`std::thread::scope` 内跑 client 构建 + recovery，join 后返回——任何调用上下文（含 sync builder 自 async connect）安全。flusher spawn 仍在私有 runtime 上（其内部本来就不在 engine runtime 上下文）。

**D3 — rewind 补偿：毒化 floor + 内存 cursor 镜像。**
- `floor: AtomicU64`（`u64::MAX` = 无毒化）。`rewind_cursor(r)`：`floor.fetch_min(r + 1)`（毒化 r+1 与其后），并把镜像 cursor 钳到 ≤ r；若 manifest 已持久化越过 r，做一次纠正性 flush（钳回，尽力而为——失败显式报错，符合 spec "or the compensation reports an explicit failure"）。
- `advance_cursor(s)`：镜像 `fetch_max`；若 `s + 1 >= floor` 则解除毒化（`floor = u64::MAX`）——源重新 ack 通过毒化序列后恢复正常推进。
- `flush_manifest` 的 target 取 `min(acked_hwm, max_sealed_seq, floor - 1)`；毒化期间 mutator **允许下调**已持久化的 cursor（纠正与 flusher 竞速的先发 flush），水位在 mutator 内读取（每次 ETag 重试用当前值，防陈旧 ceiling 回封）——CR 轮修正：初版只增不减的 mutator 使纠正性 flush 成为 no-op。
- `cursor()` 读内存镜像（构造期从 recovery 加载）——同时消灭每-ack 一次 S3 GET。
- 镜像仅本进程写（node_id 命名空间隔离，spec 既有保证），缓存语义可靠。

**D4 — 测试形态。**
① 异步路径集成：LocalFileSystem 离线构造 S3Store，经**真实 `Wal` async API**（`Wal::append`/`advance`/`acknowledge` 链或等价的 store 经 call_store 路径）驱动——修复前的 panic 场景即回归断言（不 panic + 数据往返）；② rewind 行为：advance(N) → rewind(N-1) → 强制 flush → `read_after_cursor` 必含 N / manifest cursor ≤ N-1 → 模拟 re-ack advance(N) → flush 恢复推进。

## Risks / Trade-offs

- [spawn_blocking 每次 store 调用的任务开销] — WAL 调用频率为批次级（非行级）；对 redb 后端是可忽略的池调度成本，换来 fsync 不再卡 worker。
- [cursor 镜像与 manifest 短暂不一致] — 镜像先于持久化（既有 flush 语义不变），崩溃时以 manifest 为准恢复（镜像重建），一致性由 recovery 路径保证。
- [纠正性 flush 失败] — 显式报错走 fail-closed（spec 允许"compensation reports an explicit failure"），不静默丢。
- [floor 解除条件的边界] — `s + 1 >= floor` 即源已重新提交通过毒化序列；提前/延后一格都会破坏重放语义，D4 的行为测试钉死两个方向。

## Migration Plan

无配置/格式变更。回滚 = revert。已存在的 S3 WAL 数据不受影响（floor 是纯运行态）。
