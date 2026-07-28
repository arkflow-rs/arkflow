# Capability: Input Durability (Delta)

## MODIFIED Requirements

### Requirement: Pluggable WAL storage backend
The WAL SHALL support a configurable storage backend selected per stream via a `backend` setting. The `local` backend (the existing embedded store) SHALL be the default. An `object_store` (S3-compatible) backend SHALL be available as an opt-in alternative. The object-store backend SHALL support additional performance tuning parameters: `segment_tuning` (flush strategies), `parallel_put_workers` (concurrency), and `compression` (storage optimization).

#### Scenario: Local backend is the default
- **WHEN** a stream has `durability.enabled: true` with no `backend` field (or `backend: local`)
- **THEN** the WAL persists to a local embedded store exactly as before — process-crash recovery, single-node, no behavioral change

#### Scenario: Object-store backend is opt-in
- **WHEN** a stream has `backend: s3` (or another registered object-store backend)
- **THEN** the WAL persists segments and a manifest to the configured object store

#### Scenario: Segment tuning on object-store backend
- **WHEN** a stream has `backend: s3` and configures `segment_tuning.strategy: aggressive`
- **THEN** segments are flushed with aggressive parameters (`max_entries: 10000`, `max_bytes: 10MB`, `flush_interval: 10s`)

#### Scenario: Parallel PUT workers on object-store backend
- **WHEN** a stream has `backend: s3` and configures `parallel_put_workers: 4`
- **THEN** segments are uploaded by up to 4 concurrent PUT workers

#### Scenario: Compression on object-store backend
- **WHEN** a stream has `backend: s3` and configures `compression: zstd`
- **THEN** segments are compressed with zstd before upload and decompressed during recovery

### Requirement: Segment-based batching with a bounded loss window
The object-store backend SHALL persist entries as immutable segment objects written in batches. The loss window (entries at risk on node loss) SHALL be bounded by configurable segment flush triggers (`max_entries`, `max_bytes`, `flush_interval`) which can be set via `segment_tuning` presets or custom parameters. The `per-entry` sync policy SHALL be rejected for the object-store backend. Segment flush triggers SHALL support fine-tuning via `segment_tuning.max_entries`, `segment_tuning.max_bytes`, and `segment_tuning.flush_interval`.

#### Scenario: Loss window is configurable
- **WHEN** the segment flush triggers are set via `segment_tuning`
- **THEN** the maximum number of entries at risk on node loss is bounded by those triggers

#### Scenario: Preset strategy sets triggers
- **WHEN** a stream configures `segment_tuning.strategy: low-latency`
- **THEN** the loss window is bounded by `max_entries: 100`, `max_bytes: 100KB`, `flush_interval: 100ms`

#### Scenario: Custom triggers override preset
- **WHEN** a stream configures `segment_tuning.strategy: aggressive` and `segment_tuning.max_entries: 5000`
- **THEN** the loss window is bounded by `max_entries: 5000` with other aggressive defaults

#### Scenario: per-entry sync is rejected on the object-store backend
- **WHEN** a stream is configured with `backend: s3` and `sync: per_entry`
- **THEN** the configuration is rejected at load time with an error
