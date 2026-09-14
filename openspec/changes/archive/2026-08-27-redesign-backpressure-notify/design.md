## Context

Stream 的数据流为 `Input → [Buffer] → do_processor (× thread_num) → do_output`。为支持**按序输出**，多个 processor worker 并发处理、各自用 `sequence_counter.fetch_add` 领号，`do_output` 用 `BTreeMap<seq, _>` + `next_seq` 重排出序的消息（`crates/arkflow-core/src/stream/mod.rs:407-434`）。

为防止「已分配 seq 但尚未按序写出」的 in-flight 无界增长（会让 `tree_map` 与内存/WAL 失控），`do_processor` 在每次读取下一条输入前检查 `pending = sequence_counter - next_seq`，超过 `BACKPRESSURE_THRESHOLD = 1024`（`crates/arkflow-core/src/stream/mod.rs:35`）就让该 worker 暂停。**问题不在这个阈值语义，而在感知 `next_seq` 推进的方式**：当前是 `load` 两个原子量后 `tokio::time::sleep(100–500ms)` 再 `continue`（`crates/arkflow-core/src/stream/mod.rs:342-353`），而 `next_seq` 的推进发生在 `do_output`（`crates/arkflow-core/src/stream/mod.rs:433`）。processor 在「等 output 把 in-flight 排空」（即 drain），却靠周期性睡眠去发现 output 的进展。

`do_output` 与 s3 WAL flusher 都已是事件/信号驱动（`crates/arkflow-plugin/src/wal/s3.rs:1080-1103` 用 `Notify`）；`do_processor` 的 backpressure 是仓库里**唯一**的 atomic 轮询点。

## Goals / Non-Goals

**Goals:**

- 把 backpressure 的「感知 output 推进」从 `sleep` 轮询改为显式信号，使解除即时（无 100–500ms 等待）。
- 形式化 backpressure 的可观测契约（in-flight 有界、解除即时、关闭/取消无死锁），并补回归测试（当前为 0）。
- 外科手术式改动：只动 `Stream` 的 backpressure 感知路径，不改阈值、不改顺序重排、不引新依赖。

**Non-Goals:**

- 不改 `BACKPRESSURE_THRESHOLD` 的值，也不把它可配置化。
- 不改 `do_output` 的顺序重排逻辑、不改 in-flight 上界的语义。
- 不重写 input 侧重连退避（`do_input` 的 `sleep(5s)`，`:271`，属于断连重试而非 drain）。
- 不把 backpressure 改为「纯 channel 容量驱动」（移除 pending-count）——这是更彻底的后续重构，不在本次范围。

## Decisions

### Decision 1：信号原语用 `tokio::sync::Notify`

新增 `next_seq_notify: Arc<Notify>`，由 `do_output` 在推进 `next_seq` 后 `notify_one()`，由 `do_processor` 在 backpressure 时 `notified().await`。

**为什么不 `watch::channel`：** `watch` 基于 version 比较、天然不丢更新，语义上更贴合「等一个共享值变化」。但 `next_seq` 已是 `AtomicU64`（`do_output` 用 `fetch_add`、`do_processor` 用 `load`），引入 `watch` 等于给同一个值维护**第二份表示**并需时刻保持同步，改动更大。`Notify` 只承担「铃铛」职责，不触碰 `next_seq` 的存储，满足 surgical-change 原则。`watch` 列为备选（见 Risks）。

**为什么不 `Semaphore`/`mpsc`：** 信号语义是「值变化了，去 re-check」，不是「获取许可」或「传消息」，`Notify` 最直接。

### Decision 2：触发点 = `do_output` 每次推进 `next_seq` 后 `notify_one()`

在 `:433` 的 `next_seq.fetch_add(1, Release)` 之后立即 `next_seq_notify.notify_one()`。逐条推进逐条通知，不做「跨阈值才通知」之类的聚合——聚合会重新引入延迟。

用 `notify_one` 而非 `notify_waiters`：backpressure 的释放语义是「output 每按序写出一条 → `next_seq` +1 → 释放**一格** in-flight 额度」，正好对应唤醒**一个**等待中的 processor 去消费这一格；`notify_waiters` 会一次唤醒所有等待者（thundering herd），多数因额度仍不足 re-check 后回去再等。更关键的是 `notify_one()` 在无 waiter 时会**存一个 permit**（`notify_waiters` 不存），因此即便 processor 的 `notified()` future 尚未 poll 注册，permit 也会保留到它的首次 `await`——**不丢信号**（见 Decision 3）。

### Decision 3：等待模式 = check-then-await

`notify_one` 存 permit、本身不丢信号；但等待顺序仍采用「先取 future、再判条件、最后 await」，以收紧「判定 → 注册」窗口、避免无谓阻塞：

```rust
loop {
    let notified = next_seq_notify.notified();        // 1. 先取 future
    let pending = sequence_counter.load(Acquire)
        - next_seq.load(Acquire);                     // 2. 再判条件
    if pending > BACKPRESSURE_THRESHOLD {
        notified.await;                               // 3. 超阈值才等
        continue;                                     //    醒来后 re-check
    }
    drop(notified);
    let Ok((msg, ack)) = input_receiver.recv_async().await else { break; };
    // ... 处理（fetch_add sequence_counter）
}
```

两点理由：(1) 先 `notified()` 再 `load`，确保「判完到 `await`」之间若有 `notify_one`，其 permit 能被随后的 `await` 消费，不漏掉推进；(2) 只在 `pending > THRESHOLD` 时 `await`，否则 `drop(notified)` 释放可能持有的 permit 并回到 `recv_async`，避免在 in-flight 已降下时无谓阻塞。

### Decision 4：信号生命周期 = `Stream` 字段

`next_seq_notify` 作为 `Stream` 字段、在 `Stream::new` 里初始化、在 `run_inner` 里 `clone` 下发给各 worker，与既有的 `sequence_counter` / `next_seq` 字段保持同一风格（`crates/arkflow-core/src/stream/mod.rs:47-48,77-78`）。不做成 `run_inner` 局部变量，以免与这两个兄弟字段分列两处。

### Decision 5：无死锁活跃性论证

backpressure 等待的唯一新风险是「processor 卡在 `notified().await` 无人唤醒」。证明其不发生：

- `pending > THRESHOLD` ⟹ `sequence_counter - next_seq > 1024` ⟹ 尚有 >1024 条已领号消息未按序写出 ⟹ 它们要么在 `output_sender` 通道里、要么在 `do_output` 的 `tree_map` 里 ⟹ `do_output` **必然还有消息要处理**，会继续按序写出并 `fetch_add(next_seq)` + `notify_one()`。
- 故只要 `pending > THRESHOLD`，`do_output` 必会再次发信号，processor 必被唤醒。反之 processor 醒来时若 `pending ≤ THRESHOLD`，本就不该继续等。

输入 EOF / 取消场景同理：`do_input` 退出 → `input_sender` drop；`do_output` 持续排空剩余 in-flight，每排出一条即推进 `next_seq` 并通知，processor 逐次被唤醒；待 `pending` 降到阈值以下，processor 回到 `recv_async` 拿到 `None` 退出，`output_sender` 全部 clone 释放后 `do_output` 排出 `tree_map` 残留并退出。**比现状更快**：现状每个 processor 要等下一个 `sleep` 周期（最多 500ms）才感知推进，信号版是即时唤醒。

## Risks / Trade-offs

- **[关闭时 processor 卡在 `notified().await`]** → 由 Decision 5 的 EOF 论证覆盖：`do_output` 排空剩余 in-flight 的过程会持续 `notify_one()`。无新增超时/兜底，因为论证已闭合（且现状的 `sleep` 版同样依赖 output 排空、只是更慢）。
- **[多余 permit / spurious wakeup]** `notify_one()` 在无 waiter 时存的 permit，可能让 processor 在 `pending` 已降到阈值以下时仍被立即唤醒一次。→ check-then-await 的 `loop { continue }` 会 re-check 条件、`drop(notified)` 释放未消费的 permit，伪唤醒只会多走一圈循环。
- **[行为可观测性变化]** 恢复延迟从 O(100–500ms) 降为即时，是有意的改进；in-flight 上界与按序输出不变。回归测试需覆盖「慢 output + 快 input」下 in-flight 被压住、恢复后无丢消息。

## Migration Plan

纯内部实现重构：无配置项、无协议、无公共 API 变化；`tokio` 已是工作区依赖。部署即随新版本二进制生效；回滚 = `git revert` 单个 commit。无需灰度，无数据兼容性问题。

## Open Questions

- 未来是否进一步把 backpressure 改为「纯 `output_sender` 通道容量驱动」、移除 pending-count？那会让 in-flight 上界与 `tree_map` 乱序容量的耦合更松，但需重新论证 `tree_map` 有界性，留作独立 change。
