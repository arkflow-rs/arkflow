## Context

`Stream::do_input` 在 `loop` 之前跑 WAL recovery，目的是在吃新 input 之前把上次崩溃未 ack 的 entries replay 回 pipeline（commit `b31220b`）。但 `do_input` 的函数签名是 `async fn do_input(...) -> ()`，无法把 recovery 错误传给上层 `Stream::run`。两条 recovery 失败路径都被静默吞掉：

1. `wal.read_after_cursor()` 整体返回 `Err` 时只 `error!` 一行后继续进 `loop`（`crates/arkflow-core/src/stream/mod.rs:199`）。
2. Replay 循环里 `Self::forward(...)` 失败时只 `break` 出 `if let Some(wal)` 块，然后继续进 `loop`（`crates/arkflow-core/src/stream/mod.rs:192-197`）。

redb 2.6.3 在 `Database::open` 时通过 `verify_primary_checksums()` 已经走完整棵 B-tree 校验 xxh3 128-bit checksum（`redb/src/db.rs:783 do_repair`），运行时再次损坏的概率极低。但 redb 的保护层管不到下游 channel 关闭或 buffer 持续报错等场景，`forward` 失败在正常运行中是有可能触发的（buffer 满、buffer.write 返回 Err）。无论哪一种，一旦发生都应该让 stream 启动失败而不是降级运行——降级运行意味着部分 entries 不会被 replay、cursor 不前进、新 input 仍然进入 WAL，下次重启时仍卡在同一位置，状态持续不一致且没有任何报警。

上一波 hardening（`harden-protobuf-codec`、`harden-vrl-processor`、`close-wal-on-stream-shutdown`）已经建立了"不让错误静默"的风格，本 change 延续这条线。

## Goals / Non-Goals

**Goals:**

- WAL recovery 失败（`read_after_cursor` 返回 `Err` 或 replay `forward` 失败）时，`Stream::run` SHALL 返回 `Err`，stream 不进入运行态。
- 保持现有所有 happy-path 行为：clean restart 仍然 replay nothing，正常的 crash recovery replay 仍然工作。
- 用回归测试覆盖两条失败路径。

**Non-Goals:**

- 不引入 `recovery_policy: FailFast | Continue` 等可配置项。如果将来有用户需要 degraded 模式，再开单独 change。
- 不改 `read_after_cursor` 内部对 ENTRIES 表不存在的处理（`Ok(空)`，正确行为）。
- 不改 redb 的 sync 策略或 group-commit/periodic 的丢失窗口（文档化的设计权衡）。
- 不动 Engine 层（`cli.rs`）的错误处理——已经在 stream run 失败时让进程退出，本 change 只依赖现有行为。

## Decisions

### Decision 1: 把 recovery 抽到 `Stream::run` 开头（`tracker.spawn` 之前）

**选择**：把 recovery 块从 `do_input` 顶部移到 `Stream::run` 的 `self.input.connect()` 之后、`tracker.spawn(Self::do_input(...))` 之前。错误能直接 `?` 上抛。

**备选 A**（被否决）：在 `do_input` 里通过 `cancellation_token.cancel()` + oneshot channel 把错误传给 `run`。
- 否决理由：复杂。cancel 时下游 processor/output worker 已经 spawn，需要等 `tracker.wait()` 才能干净退出，且 oneshot channel 的生命周期管理多一层。语义也不清晰——"recovery 失败导致 stream 想死"和"recovery 失败是 stream 启动失败"是两件事。

**备选 B**（被否决）：保持 recovery 在 `do_input` 里，但失败时直接 `return`。
- 否决理由：`do_input` 是 spawned task，`return` 只是结束 task，`run` 里的 `tracker.wait()` 仍然返回 `Ok(())`，错误照样丢了。除非再加 channel 传错——退化成备选 A。

**为何 connect 之后而非之前**：recovery 顺序读取 redb 文件，不依赖 input 状态；但放在 `connect()` 之后能让 input 连接问题（DNS、认证等）和 WAL 损坏问题分开诊断——WAL 损坏时 input 还没 connect，下游不会困惑于"为什么 stream 起不来但 input 已经连上"。

**为何在 `tracker.spawn` 之前**：recovery 失败时，没有任何 worker 被 spawn，cleanup 路径最简单（只需 `self.close()` 关闭 WAL 自己）。

### Decision 2: recovery forward 失败也 fail-fast

**选择**：replay 循环里 `Self::forward(...)` 返回 `Err` 时，`?` 上抛，`Stream::run` 返回 `Err`。

**备选**（被否决）：跳过当前 entry 继续下一个。
- 否决理由：跳过等于丢数据，且 cursor 不会因为跳过而推进——下次重启还会卡在同一 entry，等于把问题推到未来。

**备选**（被否决）：背压重试。
- 否决理由：recovery 阶段下游 worker 还没 spawn（见 Decision 1），buffer 满 / channel 关闭这种"持久性故障"靠重试无解；如果是瞬时故障，应该让 stream 启动失败、人工或 supervisor 重启。

### Decision 3: 不引入 `recovery_policy` 配置项

**选择**：固定 fail-fast，不开 `recovery_policy: FailFast | Continue`。

**理由**：
- 现在没有用户明确表达需要 degraded 模式。YAGNI。
- degraded 模式违反配置语义（用户配了 `durability.enabled: true` 期待 durable，结果降级到 at-most-once 是撒谎）。
- 真要支持，将来开独立 change 走完整讨论（影响面、监控、用户教育），比现在拍脑袋加一个 flag 安全。

### Decision 4: 不改 `read_after_cursor` 内部语义

**选择**：不动 `Wal::read_after_cursor` 的实现。

**理由**：
- "ENTRIES 表不存在 → `Ok(空)`" 是正确行为（fresh database 第一次起，表确实不存在）。
- redb 在 `Database::open` 时已经走完整棵树 checksum 校验，能 open 成功就说明所有表完整。
- "iter 中途失败导致已读条整批丢弃" 是边缘场景（open 校验通过 + 运行时新损坏），概率极低，且 Decision 1/2 让外层 fail-fast 后即使发生也不会静默继续。
- 改 `read_after_cursor` 内部会扩大 scope，且收益不明朗。

### Decision 5: Engine 层不动

**选择**：不动 `crates/arkflow-core/src/cli.rs`（或 `engine.rs`）对 `Stream::run` 错误的处理。

**理由**：现有的 close-wal-on-stream-shutdown change 已经验证过 Engine 在 `Stream::run` 返回 `Err` 时会让整个进程退出、依赖 k8s/systemd 重启。本 change 只依赖现有行为，不引入新的 Engine 层逻辑。

### Decision 6: 下游 worker 在 recovery 之前 spawn，`do_input` 在 recovery 之后 spawn

**选择**：在 `run_inner` 中按 `do_buffer → do_processor → do_output → recovery → do_input` 的顺序 spawn。

**原因**（review 发现）：input channel 是 `flume::bounded(thread_num * 4)`，无 buffer 时 recovery 直接 `send_async` 到 input_sender。如果下游 worker（do_processor 等）未 spawn，backlog > 容量时 `send_async` 会 await 永远——deadlock。有 buffer 时 memory buffer 是 unbounded 不会卡，但其它 future buffer 实现可能不是。所以统一前置 spawn 更安全。

**为何不是其它方案**：
- 把 recovery 也后移到所有 spawn 之后、`tracker.close()` 之前 → 仍能避免 deadlock，但 do_input 会和 recovery 并发读 input，破坏 spec scenario 4「replay 先于 new input」契约。
- 取消 input channel 的 capacity → 改变正常路径的背压语义，超出本 change scope。

**WAL ack 行为不变**：`WalAck::new(wal, seq, NoopAck)` 包装的 ack 仍由 do_output 调；不论 do_output spawn 在 recovery 之前或之后，entry 通过管道最终都会被 ack 推进 cursor。新增测试 `stream_run_replays_more_entries_than_channel_capacity_without_deadlock` 验证 50 条 entry（远超容量 4）能完整通过且 cursor 推进到 50。

## Risks / Trade-offs

- **[行为变更：WAL 损坏时从"继续运行"变成"启动失败"]** → 在 changelog 中明确强调；现有用户遇到 WAL 损坏（罕见）需要人工介入或回滚到 checkpoint，这正是 fail-fast 的目的——让问题被看见而不是被吞掉。
- **[recovery 抽出 do_input 是结构性改动]** → 改动集中在 `Stream::run` 和 `do_input` 两个函数；保留 `forward` 辅助函数；现有 `stream_close_flushes_group_commit_pending` 测试覆盖 happy path，新增 2 个测试覆盖失败 path；任何 cursor/WAL 引用生命周期的回归都会在测试中暴露。
- **[recovery 在 connect 之后但 worker spawn 之前，失败时需要 close 哪些资源？]** → recovery 失败时，已经 connect 的有：input、output、error_output、temporaries。WAL 已经 open。`self.close()` 走完整个关闭链即可（包括 WAL，参考 close-wal-on-stream-shutdown 的成果）。`Stream::run` 末尾的 `self.close()` 应该在错误路径上也跑——可以用 `match` 或 `let result = ...; self.close().await; result` 模式。
- **[依赖 redb 的 open-time checksum 校验作为第一道防线]** → redb 2.6.3 的行为已经在源码中确认（`db.rs:783 do_repair` + `verify_primary_checksums`）。如果将来 redb 升级改了这个语义，需要重新评估。在 design.md 中记录这个依赖。
- **[recovery 阶段 forward 失败的具体场景需要 mock 验证]** → 测试用 `StubBuffer` 故意返回 `Err`，模拟 buffer.write 失败；不需要构造真实的 channel-close 场景。

## Migration Plan

无配置 / 数据迁移。

部署新版本后：
- 启用 durability 的 stream 在 WAL 损坏（罕见）或 recovery 阶段下游不可用时，进程会因 stream 启动失败而退出。
- 依赖 k8s/systemd 的重启策略。
- 如果 WAL 损坏且重启无效，需要人工介入：删除 WAL 文件（接受这些数据丢失）或从备份恢复。

回滚：旧版本仍能 open 现有 WAL 文件；回滚后 recovery 失败会回到"继续运行+log"的行为，但不影响数据正确性（只是再次静默）。

## Open Questions

无。
