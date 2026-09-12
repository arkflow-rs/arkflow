# Verification notes

Every regression test below was checked against the pre-fix behavior, not just
against the fixed tree: the probe was reintroduced, the test observed failing,
and the fix restored.

## Per-test before/after

| Test | Pre-fix evidence | Post-fix |
| --- | --- | --- |
| `window::sliding_enumeration_tests::integer_observation_after_float_does_not_fabricate_a_zero_boundary` | `observe_i64` guarded on `count == 0`; probe printed `min_float=0 min_i64=100 wmin=0` for `[99.5, 100]` | passes; emits `min = 99.5` |
| `window::…::restored_buffer_keeps_its_range_before_the_next_observation` | state with `count > 0` and zeroed counters re-seeded on the next observation | passes; counters are back-filled at decode and the range survives |
| `window::…::migrated_legacy_buffer_merges_its_contribution_and_empty_stays_neutral` | an empty legacy payload kept `i64::MIN`/`i64::MAX` placeholders | passes; migration returns a fresh buffer, so a merge cannot adopt a sentinel |
| `wal::store::tests::rewind_after_advance_keeps_the_unwound_entry_replayable` | with the reclaim put back into `advance_cursor`: `left: [3] right: [2, 3]` — the unwound entry was gone | passes; the entry survives the cursor compensation |
| `wal::store::tests::committed_floor_reclaims_below_and_keeps_the_floor_entry` | n/a (new behavior) | passes; entries below the floor are reclaimed, the floor entry stays replayable |
| `state::tests::enforces_ttl_and_state_budget` | n/a (unchanged contract) | passes; the budget still rejects an over-budget write |
| `state::tests::expired_purge_after_a_resync_does_not_wedge_the_budget` | the counter-based budget wrapped to ~`u64::MAX` and rejected every write | passes; the budget measures the table exactly |
| `state::tests::overwriting_an_expired_row_does_not_inflate_the_key_count` | the `had_previous == false` branch incremented `keys` for a row that was present | passes; purging the expired row keeps the count exact |
| `state_journal::tests::retried_delete_does_not_erase_a_newer_committed_value` | with the fence restricted to `Put`: the stale delete erased the newer value | passes; the apply reports `stale delete` and the newer value survives |
| `state_journal::tests::increments_stay_exempt_from_the_version_fence` | n/a (pins the contract the fence must not break) | passes; the delta composes with the newer value |
| `state_journal::tests::undo_restores_the_version_it_replaced_so_the_owner_still_recognizes_it` | with `previous_versions: staged_versions`: the owner's rollback skipped its own key and the increment survived | passes; the rollback recognizes its version and removes the key |
| `executor::task::worker_pool_tests::a_disconnected_failure_channel_is_a_failure_not_a_clean_shutdown` | with `fail() -> Option<Error>`: `fail()` never resolved for a retired pool | passes; the disconnect is reported as a failure |
| `executor::task::worker_pool_tests::a_stalled_pool_fails_the_control_fence_within_its_bound` | with an unbounded `flush()`: the fence parked forever | passes under paused time; the bound reports the stalled delivery |
| `input::kafka::tests::assignment_wait_starts_before_the_acknowledgement_lock` | the wait was inside the `ack_lock` scope | passes; the wait starts before the lock |
| `agent::tests::agent_commands_carry_the_token_in_both_transports` | the Agent sent only the header | passes; both transports carry the credential |
| `lib::tests::agent_session_token_is_accepted_from_the_header_or_the_query` | n/a (new precedence rule) | passes; the header wins, the query parameter still authenticates |
| `storage::tests::conditional_job_write_preserves_a_newer_recovery_pointer` | the write copied the handler's older pointer back | passes; the pointer is preserved and a NULL pointer stays NULL |
| `job::tests::rejects_a_zero_pending_transaction_bound` | `Some(0)` validated and then failed every journal begin | passes; the spec is rejected where it can still be fixed |

## Full-suite runs

- `cargo test --workspace`: all targets pass (`arkflow-core` 376, `arkflow-plugin` 240, `arkflow-server` 87, plus integration targets).
- `cargo clippy --workspace --all-targets`: no warning from this change's code. Two pre-existing dead-code warnings were removed (`split_by_physical_partition`, `split_by_partition`) along with the now-unused `LegacyAggregateBuffer::sum_float`; `KernelJobHandle::states` is left in place (untouched by this change, and removing it is a standalone signature refactor).
- `console`: `npm run typecheck` and `npm run build` pass.
- `docs`: `npm run docs:check` passes (75 pages, 39 inventory entries).

## One race this change introduced and fixed

Making the pool's disconnected failure channel a failure also fired during the
shutdown path: `cancel_and_join` cancels the pool, its workers and collector drop
the channel, and the chain's failure arm reported a fault before the cancellation
arm could win. `pooled_control_fences_survive_rapid_barriers` failed
intermittently (2 of 5 runs) until the failure arm was guarded with
`cancellation.is_cancelled()`, matching the judgement the tick arm already makes.
Eight consecutive runs and five consecutive full `executor::` runs pass after the
guard.
