# Design: repair-control-plane-review-defects

## Context

`enqueue_with_metadata`'s `operation_id_override` branch returned any existing operation with a matching generation, including terminal failures. The reconciler's retry flow (`complete_attempt` marks the intent `retrying` with a due outbox row → `claim_attempt` creates a fresh attempt with a new `command_id` → `enqueue_attempt` passes `Some(intent_id)` as the operation id override) therefore re-found the terminal record and queued nothing. The only removal from the operations map is bounded eviction, and `recover_persisted_state` reloads the record after a Hub restart — the intent is wedged for the life of the process.

Registration minted session tokens from a process-global `AtomicU64` counter. Session tokens authenticate every subsequent agent request (commands poll, results, reports), so the entropy of a small sequential integer — compared with `subtle` constant-time equality — provided no protection against an attacker who can reach the Hub.

`observe_job` read the Job, compared generations, then wrote the observation through `update_job`, whose SQL `COALESCE`s unconditionally overwrite the generation column. A concurrent `update_job_desired_state` CAS that bumped the generation between the read and the write was silently rolled back by the stale report, after which every newer observation was rejected and reconciliation dispatched commands that Agents reject as stale.

`reconcile_job`'s auto-placement falls back to "all online nodes" when the previous placement is not fully online. The node that lost its lease keeps a `Succeeded` `job_start` at the current generation; the next placement excludes it, but nothing invalidates the record — sticky placement then re-includes the node on return and the per-generation dedupe skips dispatching any stop. Two Agents run the same Job with no converging path.

## Goals / Non-Goals

**Goals:**

- A retryable intent always converges to a fresh queued command after its backoff expires.
- Session tokens carry 128+ bits of OS entropy.
- A stale Job observation can never regress a newer generation, in both the storage and in-memory paths.
- A re-placed Job has exactly one live runner: the abandoned node's claim is fenced and stopped.

**Non-Goals:**

- Moving tokens out of URL query strings.
- Persisting task-assignment rows (`cp_job_tasks`).
- Changing the Agent's polling or session-reconnect protocol.

## Decisions

### Decision 1: Terminal-failure operations fall through the override dedupe

The override branch short-circuits only for `Queued/Dispatched/Acknowledged/Running/Succeeded`. Terminal failures (`Failed`, `TimedOut`, `NodeUnavailable`, `Cancelled`, `Superseded`) fall through to normal enqueue, which inserts a fresh queued record under the same intent id with the attempt's new command id — replacing the terminal record in the map and in storage. `Succeeded` keeps short-circuiting so idempotent re-delivery stays free.

Alternative rejected: keying operations by attempt id — the operation id is the intent-level idempotency contract used by result reporting; changing the key shape touches more call sites than the state check.

### Decision 2: CSPRNG session tokens from OS entropy

`rand` 0.9 joins `[workspace.dependencies]`; registration fills 32 bytes from `rand::rngs::OsRng` and hex-encodes them. `SESSION_SEQUENCE` is deleted. Constant-time comparison sites are unchanged.

### Decision 3: Observations become a storage-level CAS

`ControlPlaneStore::update_job_observation` runs `UPDATE ... WHERE job_id=? AND generation=expected` inside an `immediate_transaction` and maps zero rows to `GenerationConflict` — the same shape as `update_job_desired_state`. The Hub passes the generation it read as both the write value and the expected value; on conflict it re-reads and returns the current record (a stale report is ignored, never applied). The in-memory branch re-checks `job.generation` under the already-held write lock.

### Decision 4: Re-placement supersedes the abandoned current-generation start

Before dispatching starts, `reconcile_job` marks every `Succeeded` `job_start` at the job's current generation whose node is outside the target set as `Superseded` (with `superseded_generation`) in the map and via `persist_operation`. Effects: sticky `previous_nodes` (non-terminal records only) no longer claims the node; the per-generation dedupe no longer suppresses the node; `nodes_to_stop` (built from history including superseded records) dispatches a stop when the node is reachable again. Nodes that never lose reachability keep their records — the marking only fires on a real target-set divergence.

Alternative rejected: bumping the Job generation on re-placement — it would force recovery-policy churn and re-start healthy nodes; superseding the abandoned record fences exactly the node that left.

## Risks / Trade-offs

- [A re-placement marks the abandoned start Superseded before the new node confirms] → [if the new start then fails, the Job re-places again from the current online set per its recovery policy; no state is lost beyond what the blip already lost.]
- [`rand` becomes a workspace dependency] → [already in the lock graph transitively; no new supply-chain surface beyond the existing `rand 0.9.5` pin.]
- [Superseded records keep nodes in `historical_nodes`] → [that is the fencing path: history minus targets is exactly what produces the stop command.]

## Open Questions

None — all four behaviors are covered by deterministic regression tests in this change.
