---
sidebar_position: 7
title: Recovery
description: Runbook for crash recovery — WAL replay, checkpoint restore, and rollback procedures.
---

# Recovery

ArkFlow has two independent recovery mechanisms, and knowing which one covers
your failure is half the runbook:

| Mechanism | Protects | Trigger |
|-----------|----------|---------|
| **WAL replay** | Every message that entered a durability-enabled stream | Automatic on startup |
| **Checkpoint / savepoint restore** | Stateful job state: source positions, watermarks, keyed state, window state | Automatic on restart (checkpoints) or explicit (savepoints) |

## What happens automatically

### Stream crash (WAL replay)

When durability is enabled for a stream, every message is persisted and
`fsync`'d to the write-ahead log **before** it enters the pipeline. The WAL
cursor advances only through the highest **contiguous** acknowledged sequence,
and the source commit happens only after the output confirms the write.

On restart the engine:

1. Reconciles WAL entries already covered by the restored source position and
   advances the cursor past that covered prefix.
2. Replays every WAL entry past the committed cursor before streams resume.

The result is at-least-once delivery: an outage window is neither skipped nor
silently dropped — it is replayed. Duplicate handling, when you need
end-to-end exactly-once, is covered in [exactly-once delivery](/docs/build/exactly-once).

Out-of-order and fan-out acknowledgements never skip a gap: if sequence N+1
acks before N, the durable cursor stays before N until the gap closes.

### Stateful job crash (checkpoint restore)

A completed checkpoint identifies the job version, task assignments, source
positions, watermark state, state snapshots, format versions, and integrity
checksums — all from **one acknowledged cut**. In-flight records whose
downstream acknowledgement had not completed at barrier injection are
excluded from the cut and replayed from the recorded source position on
restore.

On compute-node restart the job restores from the last **valid** checkpoint,
replays the required source range, and reports recovery progress before
becoming healthy. Incomplete, corrupt, checksum-invalid, or task-set-
incomplete checkpoints are never selected; the previous valid checkpoint is
retained until its replacement is durably committed (manifests are written
through a temporary file and an atomic replace).

Source reconnects resume from the source's acknowledged cursor — explicitly
carried offsets, not `auto.offset.reset` — so recovery is deterministic.

## Operator procedures

### Trigger a checkpoint or savepoint

```bash
# Barrier checkpoint (automatic recovery point)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/checkpoints

# Savepoint (explicit, kept for upgrade/rollback)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/savepoints

# List recovery artifacts
curl -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/checkpoints
```

### Roll a job back to a savepoint

Upgrades require the job to be stopped and converged. If a new version
misbehaves, roll back to a compatible savepoint:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/upgrades/{upgrade_id}/rollback
```

Recovery artifacts are bound to the job version and state format version; an
incompatible artifact is rejected before restore instead of corrupting state.

### Verify recovery completed

1. Watch convergence on the job detail: the job reports recovery progress
   before becoming healthy.
2. Check `GET /api/v1/jobs/{id}/detail` for restored source positions.
3. Confirm checkpoint health — after a failed snapshot round the previous
   valid checkpoint remains selected and the job reports degraded checkpoint
   health; investigate rather than waiting for it to self-heal.

## Failure scenarios

| Symptom | Cause | Action |
|---------|-------|--------|
| Data repeats after restart | At-least-once replay from WAL/source cursor | Expected; deduplicate downstream or enable exactly-once output. |
| Data lost after restart | Durability was not enabled for the stream | Enable durability; without a WAL the source commit is the only guarantee. |
| Job stuck "recovering" | Checkpoint restore or source replay in progress | Inspect `detail`; large state or deep replay windows take time. |
| Checkpoint rounds keep failing | A task cannot snapshot or checksums mismatch | Check node disk/object-store health; the last valid checkpoint is still in force. |
| Rollback rejected as incompatible | Savepoint belongs to another version or state format | Use an artifact from the compatible version history. |

## WAL backends

The local filesystem store is the default. For shared or remote durability,
the `object_store` WAL backend writes to S3-compatible storage — see
[S3 WAL backend performance](../develop/s3-wal-performance.md) for the
design and measured trade-offs.

## Related pages

- [WAL durability & performance](/docs/build/wal) — how the WAL is structured and tuned.
- [Delivery semantics](/docs/build/delivery-semantics) — the guarantee ladder.
- [HTTP API reference](../reference/api.md) — every recovery route shown above.
