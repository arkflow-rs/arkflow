# Repair control-plane review defects

## Why

An independent code review of the Hub–Agent control plane confirmed four defects, one of which was reproduced with a throwaway probe test:

1. **Retried intents can never be re-dispatched** (`crates/arkflow-server/src/hub.rs` `enqueue_with_metadata` override branch): the operation-id dedupe short-circuited on `generation == generation` regardless of state, so after a transient attempt failure (`temporary_execution`/`transport`/`node_unavailable`), the intent sat in `retrying` while `enqueue_attempt` kept returning the terminal record without queueing any command — every Stream lifecycle mutation became unrecoverable after one transient failure. Verified empirically: after a `TimedOut` result, `reconcile_once` dispatched 0 commands.
2. **Session tokens were sequentially guessable** (`crates/arkflow-server/src/hub.rs` registration): `node-session-{global counter}` made per-node request authentication enumerable to anyone with network reach, defeating the constant-time comparisons used downstream.
3. **`observe_job` read-check-write TOCTOU** (`crates/arkflow-server/src/hub.rs` + `storage.rs` `update_job`): the generation fence was checked before the await, then the write unconditionally set the (possibly stale) generation back — one concurrent desired-state bump pinned the Job in a reconciling loop with all newer observations rejected.
4. **Auto-placement duplicated a running Job after a node blip with no fencing path back**: when the placed node's lease expired, re-placement started the whole plan on another node at the SAME generation; the abandoned node's `Succeeded` start survived, sticky placement deduped it back into the target set on return, and `nodes_to_stop` computed an empty set — two Agents consumed the same sources forever.

## What Changes

- **Terminal-failure operations no longer wedge their intent**: an operation-id hit still returns in-flight and terminally-successful records, but a terminal failure falls through and enqueues a replacement operation carrying the retry attempt's fresh command id.
- **Session tokens come from a CSPRNG**: 32 bytes of OS entropy per registration (`rand` workspace dependency added), replacing the sequential counter.
- **Job observations are compare-and-set on generation**: a new storage-level `update_job_observation` CAS applies the observation only when the generation still matches what the caller read; a conflict returns the current record instead of regressing it. The in-memory branch re-checks the generation under the write lock.
- **Re-placement fences the abandoned node**: when the reconciler targets a Job start at a node set that excludes a node holding a current-generation `Succeeded` start, that operation is marked `Superseded` (in memory and durable) so sticky placement stops claiming it and the node receives a stop command when it reappears.
- Regression tests cover all four behaviors.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `control-plane-reconciliation`: retryable intents produce a fresh command even when the prior operation record is terminal; the Job observation write is a generation compare-and-set.
- `compute-node-agent`: session credentials issued at registration SHALL be CSPRNG-generated and non-enumerable.
- `distributed-job-runtime`: re-placing a Job to a new node set supersedes the abandoned current-generation start and stops the abandoned node when it reappears.

## Impact

- Runtime: `crates/arkflow-server/src/hub.rs`, `crates/arkflow-server/src/storage.rs` — the only production code change; `Cargo.toml`/`crates/arkflow-server/Cargo.toml` gain the `rand` workspace dependency.
- Tests: `crates/arkflow-server/src/hub.rs` tests module — three regression tests.
- Specs: 3 master specs updated via this change's deltas at archive time.

## Non-goals

- Moving session tokens out of URL query strings (API-shape change, separate change).
- Persisting per-task assignment rows into the unused `cp_job_tasks` table (review P2).
- Streaming command-result cache eviction policy (review P2).
- Per-Job runtime locks on the Agent (review P2).
