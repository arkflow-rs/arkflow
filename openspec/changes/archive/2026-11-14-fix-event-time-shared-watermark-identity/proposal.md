# Proposal: fix-event-time-shared-watermark-identity

## Why

`job_runner_adapter.rs` (`event_time_gates`) shares ONE `WatermarkTracker` per
watermark group so a multi-source window fires on the minimum progress of all
its inputs. The tracker keys partitions by `(topic, partition)`. Two
independent source chains feeding the same window may legitimately read the
same topic/partition pair (different consumer groups, replay side-inputs, two
clusters with a same-named topic): their observations then collapse onto one
tracker entry. The faster chain advances that entry, the shared minimum rises,
and the window closes while the slower chain's events for that partition are
still in flight — those events are then classified late and dropped/routed.
`EventTimePartition::with_source_identity` deliberately preserves a
connector-provided topic, so the per-chain identity is currently only assigned
when the connector has NO topic (numeric partitions).

Distinct from the fixed checkpoint-side identity bug
(`source_positions_by_task`): this is the event-time watermark path.

## What Changes (sketch)

- Namespace tracker keys per source chain whenever a tracker is SHARED by
  more than one chain: internal key = `{entry_task_id}\u{1f}{connector_topic_or_empty}`,
  partition unchanged. Single-chain groups keep the raw key (no format churn
  for the common case).
- Persist RAW keys: `known_partitions()` strips the namespace and the
  checkpoint `watermark_partitions` keep `(topic, partition)`; restore
  re-applies the current chain's namespace. Old checkpoints keep working
  unchanged.
- Reject/upgrade `restore_partition_key` for namespaced gates when a saved
  topic already carries the namespace (idempotent bypass).
- Tests: two chains, same topic/partition, disjoint event times — fast chain
  must NOT close a window that the slow chain still has data for; restore
  round-trip preserves per-chain watermarks; single-chain behavior
  byte-identical.

## Impact

- `crates/arkflow-core/src/event_time.rs` (key namespacing helper),
  `executor/job_runner_adapter.rs` (shared/unique tracker classification,
  seed/restore mapping), `executor/event_time_gate.rs` (observe/restore
  pass-through), `executor/kernel_handle.rs` and
  `arkflow-server/src/agent/kernel.rs` (restore call sites).
- No config changes; checkpoint format unchanged (raw keys persisted).
