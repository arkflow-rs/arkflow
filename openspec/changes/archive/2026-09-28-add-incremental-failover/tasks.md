## 1. Hub: incremental placement

- [x] 1.1 Add `start_dispatch_fingerprints` memory (job, node, generation) → task-set hash to `Hub`; record at start dispatch
- [x] 1.2 Implement incremental slot replacement in `reconcile_job`: previous dispatch order (placement_order, fallback sorted previous_nodes) with failed/evicted slots replaced in place by ranked candidates (shuffle-capable for split; duplicate a survivor when no candidate exists); full ranked placement when nothing survives
- [x] 1.3 Tighten the dispatch skip: Succeeded start + matching fingerprint; mismatch supersedes the stale start through the existing abandoned-fencing path
- [x] 1.4 Failed Job observation at the current generation invalidates that node's Succeeded start (TimedOut + `runtime_failed`), persisted, mirroring the boot-change invalidation pattern

## 2. Agent: assignment-aware start

- [x] 2.1 `JobTask` records its assignment task-id set
- [x] 2.2 Same-generation live-kernel start: matching task set stays a no-op; differing set falls through to the teardown-and-recover replace path

## 3. Tests

- [x] 3.1 Hub: three-node split placement loses one node with one candidate → survivors' task sets byte-identical, dead node's tasks on the replacement; superseded/stop for the dead node still applies
- [x] 3.2 Hub: no candidate → survivor duplicates into the slot; no survivors → full ranked placement (existing behavior)
- [x] 3.3 Hub: fingerprint mismatch (simulate hub restart / stale memory) supersedes and re-dispatches; matching fingerprint skips
- [x] 3.4 Hub: failed observation invalidates the Succeeded start and reconcile re-dispatches with recovery
- [x] 3.5 Agent: matching set no-op; drifted set replaces the kernel (reuse the existing agent test harness for starts)
- [x] 3.6 `cargo test --workspace --all-targets` green; clippy adds no new warnings

## 4. Docs

- [x] 4.1 Update distributed-jobs docs (en + zh-Hans): partial-failure behavior per placement strategy, recovery window, automatic re-dispatch after kernel failure
- [x] 4.2 `pnpm docs:check` passes
