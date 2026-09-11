# Design: repair-kernel-review-defects

## Context

`KernelJobHandle::checkpoint_barrier_inner` (`crates/arkflow-core/src/executor/kernel_handle.rs:138-259`) injects a barrier into every source chain's bounded barrier channel and then waits until `snapshots.len() == participants.len()`. `participants` is populated once at spawn (`kernel_handle.rs:561-563`) and never changes, but chain tasks do end: a source chain returns after EOF (`crates/arkflow-core/src/executor/task.rs:593-594`) and an interior chain returns once all inputs ended (`task.rs:1193-1201`). Their checkpoint reporters drop with the task, so a later round can never collect their reports; the wait `select!` watches only cancellation, the error channel, and the report channel, with no timeout — the round hangs while the error channel stays open (any other live chain holds a sender clone).

`ProcessorWorkerPool::flush` (`task.rs:1767-1786`) waits for `flushed >= submitted` on a `tokio::sync::Notify` signalled with `notify_waiters()` from the reorder collector (`task.rs:1680-1681`). `notify_waiters()` stores no permit, so the wakeup only reaches waiters registered before the call. `flush()` checks `flushed` first and creates `Notified` afterwards; a store+notify landing between the two steps is lost, and if it was the final result the waiter parks forever.

The interior chain loop fences the pool before every control event except the idle tick: barrier (`task.rs:1353-1355`), watermark (`task.rs:1480-1484`), EOS (`task.rs:1196-1198`). The tick branch (`task.rs:1176-1179`) calls `tick_chain` directly, so tick-generated batches are published while earlier deliveries are still inside the pool.

## Goals / Non-Goals

**Goals:**

- Checkpoint rounds complete when any participant chain has already ended, without weakening the "every live participant reports" rule.
- Eliminate the lost-wakeup window in `ProcessorWorkerPool::flush`.
- Make the idle tick obey the same pool-fence discipline as every other control event.
- Keep the changes surgical: no protocol changes, no new configuration.

**Non-Goals:**

- Untagged checkpoint-error draining / identity-tagged errors (review P2).
- Distinct channels for late-event route edges (review P2).
- Narrowing the pool's cancellation token scope (review P2).
- JoinError propagation in `watcher()` (review P2).

## Decisions

### Decision 1: Chains report their own exit through the existing hook plumbing

Add `finished_reporter: Option<tokio::sync::mpsc::UnboundedSender<String>>` to `CheckpointHook`. `run_chain` sends the chain's entry task id on it immediately after `run_chain_inner` returns (before component close), on every exit path. `KernelJobHandle` stores the receiver plus one keep-alive sender clone (so the channel never closes while the handle lives). `checkpoint_barrier_inner` drains finished ids at round start into an `ended` set: barrier injection skips ended source chains (their barrier receiver dropped with the hook), and the required set is `participants - ended`. While waiting, finished notifications keep shrinking the required set, and a report from an already-exempted chain (it processed the barrier just before exiting) is still accepted and included.

Alternative rejected: watching chain `JoinHandle`s — the handles live inside `run_graph_inner`'s `FuturesUnordered`, are not accessible to the handle, and the hook plumbing already carries task identity.

Alternative rejected: a timeout on the wait loop — it converts a hang into flaky checkpoint failures without fixing the root cause, and a safe timeout would be large enough to mask real stalls.

### Decision 2: Register the `Notified` future before checking `flushed`

`flush()` creates `Notified`, pins it, calls `enable()`, and only then checks `flushed >= target`; the subsequent `select!` awaits the pinned future. This is the documented Tokio pattern for `notify_waiters` without stored permits: any notify landing after `enable()` wakes the waiter, and a notify that landed before `enable()` is covered by the post-enable condition check.

### Decision 3: Fence the tick like the barrier

The tick branch calls `pool.flush().await?` before `tick_chain` when a pool exists — identical to the barrier path (`task.rs:1353-1355`). After the flush no worker holds a delivery, so `on_tick` runs exclusively and its generated batches are published in order.

## Risks / Trade-offs

- [A chain that ends mid-round without processing an injected barrier is exempted even though the barrier was buffered in its channel] → [its pre-barrier data was already delivered; a bounded source at EOF has no meaningful position to snapshot, so nothing is lost.]
- [`finished_reporter` grows `CheckpointHook`] → [one `Option<UnboundedSender<String>>`; hooks already carry two senders.]
- [A round with zero remaining participants returns an error] → [preserves the loud "kernel ended before checkpoint completed" failure the all-ended case produced before; the interval loop already logs and continues.]

## Open Questions

None — behavior is verified by the regression tests drafted in this change.
