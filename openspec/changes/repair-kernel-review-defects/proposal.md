# Repair kernel review defects

## Why

An independent code review of the unified execution kernel confirmed three defects that surface as silent stalls or ordering violations rather than loud errors:

1. **Checkpoint rounds hang once any participant chain has ended** (`crates/arkflow-core/src/executor/kernel_handle.rs:169`): the wait loop requires one report from every statically registered participant, but a chain whose event loop exited (EOF on a bounded source, or all inputs ended) never reports again, and the wait `select!` has no timeout — a Job mixing a finite source with a continuous source permanently stops completing checkpoints after the finite subtree drains.
2. **Lost wakeup in `ProcessorWorkerPool::flush`** (`crates/arkflow-core/src/executor/task.rs:1775-1778`): the loop checks `flushed >= target` before registering the `Notified` future while the collector signals with `notify_waiters()` (no stored permit), so a concurrent final store+notify landing between the check and the registration parks `flush()` forever; the vertex stops consuming and the whole upstream stalls at zero throughput.
3. **`on_tick` bypasses the worker-pool fence** (`crates/arkflow-core/src/executor/task.rs:1176-1179`): barrier, watermark, and EOS paths flush the pool before control flow, but the idle-tick path publishes tick-generated batches while earlier-submitted deliveries are still in flight, letting tick output overtake data and breaking the per-edge ordered-delivery contract with `pipeline.thread_num > 1`.

## What Changes

- **Exempt ended chains from checkpoint rounds**: chains notify the handle when their event loop exits; barrier injection skips them and the wait loop's required set shrinks accordingly. Checkpoints complete for Jobs where a bounded source drained while other sources keep flowing.
- **Register the flush waiter before checking the condition**: `flush()` creates and enables its `Notified` future before reading `flushed`, eliminating the missed `notify_waiters` window.
- **Fence the idle tick with the worker pool**: the tick branch waits for in-flight pool deliveries exactly like the barrier/watermark/EOS paths.
- Regression tests cover all three behaviors at the kernel level.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `async-checkpoint-barriers`: checkpoint completion no longer requires reports from vertices whose event loops have already ended; barrier injection skips ended source vertices.
- `unified-execution-kernel`: ordered delivery per edge explicitly covers worker-pool chains — control events that generate data (idle ticks) must not overtake in-flight pooled data, and control fences must not stall while workers are healthy.

## Impact

- Runtime: `crates/arkflow-core/src/executor/kernel_handle.rs`, `crates/arkflow-core/src/executor/task.rs` — the only production code change.
- Tests: `crates/arkflow-core/src/executor/tests.rs` — three regression tests.
- Specs: 2 master specs updated via this change's deltas at archive time.

## Non-goals

- Tagging checkpoint errors with barrier identity to stop stale-error poisoning of the next round (review P2).
- Routing late-event edges through distinct channels (review P2).
- Scoping the worker pool's cancellation token narrower than the graph token (review P2).
- Propagating `JoinError` through `watcher()` (review P2).
