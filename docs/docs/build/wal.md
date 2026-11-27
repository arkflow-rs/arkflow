---
description: ArkFlow documentation page.
---

# WAL Durability & Performance

ArkFlow's WAL (Write-Ahead Log) persists messages at the input boundary so
they survive crashes and can be replayed on restart. This page explains
how to configure the WAL for different workloads, with a focus on the
three optimization dimensions introduced in 2026-07-28:
**segment tuning**, **parallel PUT**, and **compression**.

For background on the at-least-once contract and replay semantics, see
[Input Delivery Semantics](./delivery-semantics.md).

## Quick Start

The minimal durable configuration uses the default (balanced) strategy
with a single PUT worker and no compression:

```yaml validate=full
streams:
  - id: durable-orders
    input:
      type: kafka
      brokers: [localhost:9092]
      topics: [orders]
      consumer_group: arkflow-orders
      start_from_latest: false
    pipeline:
      processors: []
    output:
      type: drop
    durability:
      enabled: true
      backend:
        type: object_store
        node_id: "${ARKFLOW_NODE_ID}"
        stream_id: main
        s3:
          bucket: my-bucket
          region: us-east-1
```

This works for most workloads. To tune for higher throughput, lower cost,
or faster recovery, continue reading.

## Source acknowledgement is gated on sealing

On the object-store backend, a source acknowledgement (for example the
Kafka offset commit) completes **only after the acknowledged entry has
been sealed into a segment object** — a PUT to object storage plus the
manifest update. A completed acknowledgement therefore always means the
entry is durable on the store: if the node disappears at any moment, no
acknowledged entry can be lost.

Entries that are staged in memory but not yet sealed are not lost either —
they are the **replay window**: because their source acknowledgement has
not completed, the source re-delivers them after a restart
(at-least-once, so outputs must tolerate duplicates as usual).

The same segment flush triggers that bound the replay window also bound
the **acknowledgement latency** added by this gating: an acknowledgement
waits at most one `flush_interval` (plus the segment PUT time) for its
entry's segment to seal. With the default `balanced` preset that is about
1s; the `aggressive` preset adds up to ~10s. Acknowledgements for many
entries in the same segment share a single seal, so the wait amortizes
across the batch. If acknowledgement latency matters more than PUT cost,
lower `flush_interval` (and `max_entries`) — do not try to bypass the
gate: it is what makes "acknowledged" imply "sealed". A flusher that
stops sealing fails the acknowledgement retryably after a bounded wait
instead of silently committing unsealed data, and sustained flusher
failures are logged at error level (see
[S3 WAL backend performance](../develop/s3-wal-performance.md)).

The local (`redb`) backend is unaffected: its flush is already a durable
commit, so acknowledgements complete exactly as before with no added
latency.

## Dimension 1: Segment Tuning

The `segment_tuning` block controls when the in-memory segment is sealed
and uploaded to S3. Larger segments mean fewer PUT requests (lower cost)
but a larger replay window — more unsealed messages that the source must
re-deliver if the node disappears — and longer acknowledgement latency
(see above).

### Preset Strategies

Three presets cover the common cases:

| Strategy | `max_entries` | `max_bytes` | `flush_interval` | Best For |
|----------|---------------|-------------|------------------|----------|
| `aggressive` | 10000 | 10 MB | 10 s | Batch jobs, cost-sensitive |
| `balanced` (default) | 1000 | 1 MB | 1 s | General-purpose streams |
| `low_latency` | 100 | 100 KB | 100 ms | Real-time, minimal data loss |

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    segment_tuning:
      strategy: aggressive   # or "balanced" / "low_latency"
```

### Custom Overrides

Any preset parameter can be overridden individually:

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    segment_tuning:
      strategy: aggressive
      max_entries: 20000      # override default 10000
      flush_interval: "30s"   # override default 10s
```

### Replay Window Trade-off

The replay window is the number of messages the source must re-deliver if
the node vanishes between a write and the next segment flush (they are
replayed, not lost — acknowledgement gating keeps the source offset
behind the seal frontier). Approximate it as:

```
replay_window ≈ min(
    max_entries,
    max_bytes / avg_message_size,
    flush_interval × message_rate
)
```

At 10,000 msg/s with 1 KB average message size:

| Strategy | Replay Window |
|----------|---------------|
| `aggressive` | ~10,000 messages |
| `balanced` | ~1,000 messages |
| `low_latency` | ~100 messages |

At that rate the `max_entries` and `max_bytes` triggers fire before
`flush_interval` does, so they dominate the replay count; the
acknowledgement latency added by seal gating is still bounded by
`flush_interval` (10s / 1s / 100ms for the presets above). At lower rates
the `flush_interval × message_rate` term becomes the binding one.

## Dimension 2: Parallel PUT Workers

The `parallel_put` block reserves configuration space for concurrent S3
upload workers. Each worker would own an independent bounded channel
(16 segments), giving per-worker backpressure without global contention.

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    parallel_put:
      workers: 4              # 1-8, default 1
      shutdown_timeout: "30s" # wait time for in-flight uploads on close
```

:::warning

**This knob currently has no effect.** Segment seals are serialized under a
lock so the acknowledged frontier can never advance past an earlier
unsealed segment; the worker pool therefore has no production call site.
The field validates (1-8) and is accepted for forward compatibility, but
setting `workers > 1` only logs a startup warning
("parallel_put.workers has no effect"). Keep the default `1` and size the
WAL with [segment tuning](#dimension-1-segment-tuning) and
[compression](#dimension-3-compression) instead — see
[Delivery semantics](./delivery-semantics.md) and
[S3 WAL performance](../develop/s3-wal-performance.md) for what actually
moves throughput.

:::

## Dimension 3: Compression

The `compression` block enables per-segment compression before upload.
Compressed segments reduce storage cost, transfer time, and the size of
recovery LIST/GET operations.

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    compression:
      type: zstd   # or "lz4" / "none"
      level: 3     # algorithm-specific range (see below)
```

**Algorithms:**

| Algorithm | Level Range | Default | Best Compression | Best Speed |
|-----------|-------------|---------|------------------|------------|
| `none` | n/a | n/a | n/a | n/a |
| `lz4` | 1-16 | 4 | low (40-50%) | very fast |
| `zstd` | 0-22 | 3 | high (50-70%) | moderate |

:::note

The `lz4` level is validated (1-16) but currently **unused**: LZ4 compression
runs at a single speed regardless of the configured level, so any value
produces byte-identical output — the measured ratios below were identical
for lz4-1, lz4-4, and lz4-9. Tune `zstd` levels instead; the `lz4` default
of 4 is what the schema records, not a real tuning input.

:::

**Measured compression ratios** on ArkFlow's own WAL segment payloads
(Arrow IPC frames with repetitive schema metadata):

| Algorithm | Ratio (smaller is better) |
|-----------|---------------------------|
| `lz4` | ~108× |
| `zstd-3` | ~181× |
| `zstd-9` | ~200× (slower) |

**Min-size threshold:** Segments smaller than 10 KB are uploaded
uncompressed regardless of this setting — the CPU overhead of compressing
a small payload outweighs the storage savings.

**CPU cost vs. savings:** Compression trades CPU for network and storage.
On a modern CPU at zstd-3, expect ~5-10% single-core utilization for
~70% storage reduction. Use `lz4` for CPU-bound workloads, `zstd-9` for
storage-bound workloads.

## Dimension 4: Segment Retention & Reclamation

Without reclamation, sealed S3 WAL segments and `manifest.json` grow without
bound, and restarts spend increasing time listing and downloading historical
segments. The `retention` block controls automatic reclamation of sealed
segments that are wholly behind the committed reclaim floor.

```yaml validate=fragment wrap=durability
durability:
  backend:
    type: object_store
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
    retention:
      enabled: true        # default: true
      max_segments: 16     # optional: max acknowledged sealed segments to keep
      max_age: "24h"       # optional: max age (supports ms, s, m, h, d, e.g. "1h30m")
      max_bytes: 67108864  # optional: max total bytes across kept acknowledged segments
```

### Default Policy & Multi-Cap Semantics

- **Default (`retention` omitted, or `enabled: true` with all three caps unset):**
  ArkFlow reclaims **every** eligible acknowledged sealed segment immediately on
  the background reclaim loop (matching local `redb` WAL reclamation).
- **Explicit caps (`max_segments`, `max_age`, `max_bytes`):** When one or more
  caps are configured, only those configured caps apply. Eligible segments are
  evaluated newest-to-oldest and retention stops at the first violated cap,
  keeping a contiguous newest suffix without leaving history gaps.
- **Disabled (`enabled: false`):** Disables ArkFlow's own segment deletions
  (keeping sealed segments in place). Note that S3 bucket lifecycle expiration
  rules are **not cursor-aware** — **do not** configure current-version
  expiration on the live WAL prefix, as a stalled downstream consumer can fall
  behind the expiration age and lose unacknowledged WAL segments. Restrict
  age-based lifecycle expiration to separately archived copies (or noncurrent
  object versions on versioned buckets).

### Safety & Crash-Recovery Guarantees

- **Rewind-safe boundary:** Only sealed segments in `sealed_segments` whose
  `last_seq <= min(manifest.cursor, reclaimable_hwm)` are eligible.
  `reclaimable_hwm` advances to `seq - 1` only after the wrapped source commit
  succeeds (`mark_committed(seq)`), matching the local `redb` backend so an
  in-flight `rewind_cursor` or `Wal::undo(seq)` can never lose its segment.
  Unacknowledged segments and the newest `active_segment` are never deleted.
- **Manifest-first crash safety:** Reclamation runs on a dedicated, cancellable
  background loop (decoupled from segment sealing so slow S3 deletes never
  delay acknowledgements or stall `close()`):
  1. ETag-coordinated manifest update moves eligible segments from
     `sealed_segments` into `pending_deletes`.
  2. Best-effort bounded S3 `DELETE` removes queued objects (`NotFound` is
     idempotent).
  3. ETag-coordinated manifest update removes confirmed deletions from
     `pending_deletes`.
- **Fast recovery:** `manifest.json` records per-segment sequence bounds, size,
  and seal time (`segment_meta`, backward-compatible schema v2). On restart,
  `recover()`, `read_after_cursor()`, and `read_after()` skip `GET` requests
  for `pending_deletes` and for any segment with `last_seq <= manifest.cursor`,
  downloading only unacknowledged (or unindexed legacy/LIST-fallback) segments.

### Object-Storage Cost Model

| Cost Component | Behavior with Retention Enabled |
| --- | --- |
| **Stored bytes** | Retention caps (`max_segments`, `max_age`, `max_bytes`) bound only the **eligible acknowledged history selected for retention** among **current** WAL objects, not total bucket occupancy. Actual current-object bytes = unacknowledged segments + `active_segment` (1 sealed segment) + undo-protected segment (containing the latest committed sequence) + retained eligible history + `pending_deletes` (sweeps delete at most 64 objects per pass, and failed deletes continue consuming storage until retried). On **versioned** S3 buckets, key-only deletes create delete markers and noncurrent versions continue incurring storage charges until a noncurrent-version expiration lifecycle rule removes them. |
| **DELETE requests** | Standard `DELETE` requests are free on AWS S3 (and most S3-compatible stores); capped at 64 objects per sweep so backlogs drain smoothly. |
| **Manifest PUTs** | Active reclaim sweeps that deregister and delete objects add 2 small manifest PUTs (deregister + cleanup). Idle sweeps perform **0** manifest PUTs. |
| **Restart GETs** | Drops from `O(total historical segments)` to `O(unacknowledged segments only)`, eliminating linear restart degradation on long-running streams. |

## Putting It All Together

### High-Throughput Batch Job

Minimize S3 PUT requests; tolerate up to ~10K messages re-delivered on
node loss at a 10K msg/s ingest (the `max_entries`/`max_bytes` caps bind
first) and an acknowledgement latency of up to ~10s.

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive
    parallel_put:
      workers: 4
    compression:
      type: zstd
      level: 3
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

### Real-Time Stream with Tight Replay Window

Minimize replay window and acknowledgement latency; throughput is secondary.

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: low_latency
    parallel_put:
      workers: 2       # still useful for occasional bursts
    compression:
      type: lz4        # faster than zstd; less CPU
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

### Cost-Sensitive Cold Storage

Maximum compression, minimal PUTs, accept a larger replay window and
longer acknowledgement latency.

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  backend:
    type: object_store
    segment_tuning:
      strategy: aggressive
      flush_interval: "60s"   # even longer
    compression:
      type: zstd
      level: 9                # maximum compression
    node_id: "${ARKFLOW_NODE_ID}"
    stream_id: main
    s3:
      bucket: my-bucket
```

## Validation

All configurations are validated at startup:

- `parallel_put.workers` must be 1-8 (zero is rejected)
- `compression.level` must be in the algorithm's range
  (zstd 0-22, lz4 1-16)
- `retention.max_age` must be greater than zero when set (use
  `retention.max_segments: 0` or omit all caps for immediate reclaim)
- Existing checks: `node_id` and `stream_id` non-empty, `bucket`
  non-empty, `sync: per_entry` rejected for `object_store`

Invalid configs cause `Stream::run` to return `Err` at startup, so
the engine fails fast rather than silently degrading.

## Backward Compatibility

Old configs (without `segment_tuning`, `parallel_put`, `compression`, or
`retention`) remain syntactically valid — these fields default to:

- `segment_tuning.strategy: balanced` (same params as legacy `segment:`)
- `parallel_put.workers: 1`
- `compression.type: none`
- `retention.enabled: true` (with all three caps unset — automatic reclamation
  of eligible acknowledged sealed segments)

**Upgrade note on retention behavior:** while existing configs parse unchanged,
omitting `retention` now enables automatic deletion of eligible acknowledged
sealed segments behind the committed reclaim floor. Operators who rely on the
previous no-deletion behavior (for example, external inspection of historical
segments in the WAL prefix) should explicitly set `retention.enabled: false`
before upgrading.

## Example Configs

See the `examples/` directory for ready-to-use configurations:

- `durability_example_s3.yaml` — baseline (balanced, no compression)
- `durability_example_aggressive.yaml` — high-throughput
- `durability_example_parallel.yaml` — 4 PUT workers declared (accepted;
  currently no effect — see the [Parallel PUT Workers](#dimension-2-parallel-put-workers) note)
- `durability_example_compressed.yaml` — zstd compression
- `durability_smoke_test.yaml` — all optimizations combined

## Monitoring

The WAL exports background flush-failure counters — watch for any increase:

- `arkflow_stream_wal_flush_failures_total` — background flush failures of
  the stream's input WAL (0 without durability)
- `arkflow_job_wal_flush_failures_total` — background flush failures of a
  Job's input WAL (0 without a WAL)

See [Observability](../operate/observability.md) for the full metrics list.
