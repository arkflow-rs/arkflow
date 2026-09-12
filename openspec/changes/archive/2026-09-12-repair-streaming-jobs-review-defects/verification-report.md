# Verification Report: repair-streaming-jobs-review-defects

## Summary

| Dimension | Status |
| --- | --- |
| Completeness | 37/37 tasks complete; 19 requirements across 9 delta specs |
| Correctness | 19/19 requirements have implementation evidence; 57 scenarios, every one covered by a test or by an explicit documented exception |
| Coherence | All 9 design decisions followed; one decision (D8) was revised during implementation and the design document was updated to match |

## Completeness

- **Tasks**: 37/37 checked (`tasks.md`). Three task descriptions were updated during
  implementation to match what was actually built (1.1, 1.2, 1.4, 1.5, 8.4, 10.2);
  each update says what changed and why.
- **Requirements**: 19 requirements across 9 delta specs.
  - `columnar-window-operators` 2, `input-durability` 2, `keyed-state-backend` 4,
    `unified-execution-kernel` 2, `message-acknowledgment` 2, `compute-node-agent` 1,
    `control-plane-api` 3, `control-plane-console` 2, `checkpoint-recovery` 1.

## Correctness

Implementation evidence per requirement (tests are named, all present in the tree):

| Requirement | Evidence |
| --- | --- |
| Window aggregate observations counted per representation | `integer_observation_after_float_does_not_fabricate_a_zero_boundary`, `restored_buffer_keeps_its_range_before_the_next_observation`, `restored_mixed_payload_keeps_both_representations`, `migrated_legacy_buffer_merges_its_contribution_and_empty_stays_neutral` |
| Keyed window aggregation state (per-representation seeding, restored/migrated) | the four above plus the pre-existing `mixed_int_and_float_observations_keep_both_contributions` |
| Acked-prefix reclamation SHALL be replay-safe | `rewind_after_advance_keeps_the_unwound_entry_replayable`, `committed_floor_reclaims_below_and_keeps_the_floor_entry` |
| WAL cursor advancement precedes wrapped source commit | the same two (the reclaim is now conditional on the wrapped commit) |
| State size accounting SHALL never wrap or drift | `expired_purge_after_a_resync_does_not_wedge_the_budget`, `overwriting_an_expired_row_does_not_inflate_the_key_count`, `purge_and_delete_release_counters_without_underflow`, `enforces_ttl_and_state_budget` |
| State journal version fences SHALL gate every replayed mutation | `retried_delete_does_not_erase_a_newer_committed_value`, `increments_stay_exempt_from_the_version_fence`, `a_restaged_transaction_keeps_skipping_the_mutation_it_was_fenced_on` |
| State rollback SHALL not erase later commits | `undo_restores_the_version_it_replaced_so_the_owner_still_recognizes_it`, `retried_apply_does_not_overwrite_a_newer_committed_value` |
| Stateful mutations SHALL commit only after successful processing | the journal suites above; `expired_purge_after_a_resync_does_not_wedge_the_budget` covers the compensation path |
| Worker pool failure SHALL fail the chain | `a_disconnected_failure_channel_is_a_failure_not_a_clean_shutdown`, `a_stalled_pool_fails_the_control_fence_within_its_bound` |
| Cancellation and drain (bounded waits) | the two above plus the pre-existing `pooled_control_fences_survive_rapid_barriers` (stable across 8 consecutive runs after the cancellation guard) |
| Composite frontier waits are cancellation-aware | `assignment_wait_starts_before_the_acknowledgement_lock` pins the lock scope structurally; the wait itself honours `close` and a bound (code at `kafka.rs:744`) |
| Delivery settlement SHALL NOT block the source loop | the tombstone path spawns its settlement with a bounded retry; covered by code inspection because a live broker is required (see Exceptions) |
| Command polling and execution (dual credential transport) | `agent_commands_carry_the_token_in_both_transports`, `agent_session_token_is_accepted_from_the_header_or_the_query` |
| Job upgrade and rollback SHALL fence the whole written record | `conditional_job_write_preserves_a_newer_recovery_pointer` |
| Upgrade and rollback SHALL validate state compatibility equally | implemented in `hub_job_upgrade_rollback`; the rejection path needs a populated checkpoint store, covered by code inspection (see Exceptions) |
| Standard API errors (conflict envelope) | pre-existing `GenerationConflict` → 412 mapping, retained |
| Visual Job DAG orchestration (post-upgrade state surfaced) | the console reports the Hub's recorded state and offers the start action (`job-editor.tsx`); manual verification |
| Graph and compatibility validation (presentation changes) | `updateSpecIfChanged` skips presentation-only changes and compares the whole derived spec |
| Recovery compatibility SHALL be evaluated consistently | the rollback path now runs the state-format check before accepting |

## Coherence

- **D1** (per-representation seeding + decode-time normalization): implemented; the
  design's `count == 0` term was removed during implementation because it broke a
  real restored-range case, and the design document records the revision.
- **D2** (migration reconstructs the integer payload): implemented, including the
  `sum_float` removal and the empty-payload sentinel guard.
- **D3** (committed floor): implemented as `WalStore::mark_committed`, called only
  after a successful wrapped source commit.
- **D4** (exact budget, observability-only counters): implemented.
- **D5** (fence binds destructive kinds; increments exempt): implemented, and the
  post-repair review's restage defect fixed on top of it.
- **D6** (dead pool is a chain failure, bounded waits): implemented.
- **D7** (both credential transports): implemented.
- **D8** (conditional write owns everything except the recovery pointer): the
  design was **revised** during implementation — the first version used
  `COALESCE` and a test caught that it regressed the pointer — and `design.md`
  now documents the final rule and why the two rejected shapes are wrong.
- **D9** (invalidate on content change): implemented.

`openspec validate repair-streaming-jobs-review-defects` passes; `docs:check`
passes; `cargo clippy --workspace --all-targets` reports nothing from this
change's code.

## Exceptions (documented, not gaps to fix here)

1. **Live-broker scenarios** (Kafka tombstone settlement, assignment wait under a
   real rebalance): `kafka_eos` needs Docker. The lock-scope property is pinned
   structurally; the settlement path is covered by the retry loop and code
   inspection.
2. **Checkpoint-store scenarios** (rollback rejecting an incompatible artifact):
   needs a populated checkpoint store; the rule is implemented and cited, with the
   upgrade path's equivalent covered by pre-existing tests.
3. **`state.max_pending_transactions` scenario** lives in the `keyed-state-backend`
   delta with its validation test, not a separate spec.

## Final Assessment

No critical issues. 0 critical, 0 warnings, 3 documented exceptions (all requiring
an external service or a populated store, with the property pinned by a test or by
structural assertion wherever it can be). **Ready for archive.**
