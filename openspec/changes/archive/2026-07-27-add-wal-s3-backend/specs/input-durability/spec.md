## ADDED Requirements

### Requirement: Pluggable WAL storage backend
The WAL SHALL support a configurable storage backend selected per stream via a `backend` setting. The `local` backend (the existing embedded store) SHALL be the default. An `object_store` (S3-compatible) backend SHALL be available as an opt-in alternative.

#### Scenario: Local backend is the default
- **WHEN** a stream has `durability.enabled: true` with no `backend` field (or `backend: local`)
- **THEN** the WAL persists to a local embedded store exactly as before — process-crash recovery, single-node, no behavioral change

#### Scenario: Object-store backend is opt-in
- **WHEN** a stream has `backend: s3` (or another registered object-store backend)
- **THEN** the WAL persists segments and a manifest to the configured object store

### Requirement: Per-node namespace isolation
When the object-store backend is in use, the WAL SHALL isolate its object namespace by a node identity (`node_id`) and a stream identity (`stream_id`) in the object key prefix. Multiple arkflow nodes sharing one bucket SHALL NOT read or overwrite each other's WAL. The `node_id` SHALL be an explicit configuration value.

#### Scenario: Nodes sharing a bucket are isolated
- **WHEN** two arkflow nodes are configured with the same object-store bucket and root prefix but different `node_id` values
- **THEN** each node reads and writes only its own `{node_id}/` namespace and neither observes the other's WAL

#### Scenario: node_id is explicit and stable across restarts
- **WHEN** a node restarts after being lost
- **THEN** recovery uses the same configured `node_id` to locate the node's prior WAL in object storage

### Requirement: Object-store WAL survives node loss
When the object-store backend is in use, every entry that has been flushed to a segment object SHALL be recoverable after the node (pod/host) is lost — not only after a process crash. Only entries still in the in-memory staging queue (not yet flushed to a segment) are at risk on node loss.

#### Scenario: Flushed entries survive pod disappearance
- **WHEN** a node has flushed entries to segment objects and then the node/pod disappears
- **THEN** on restart (same `node_id`) those flushed entries are present in object storage and are replayed during recovery

#### Scenario: Un-flushed entries are the loss window
- **WHEN** a node disappears with entries still in the in-memory staging queue
- **THEN** those un-flushed entries are lost, while all previously flushed entries are recovered

### Requirement: Segment-based batching with a bounded loss window
The object-store backend SHALL persist entries as immutable segment objects written in batches. The loss window (entries at risk on node loss) SHALL be bounded by configurable segment flush triggers (`max_entries`, `max_bytes`, `flush_interval`). The `per-entry` sync policy SHALL be rejected for the object-store backend.

#### Scenario: Loss window is configurable
- **WHEN** the segment flush triggers are set
- **THEN** the maximum number of entries at risk on node loss is bounded by those triggers

#### Scenario: per-entry sync is rejected on the object-store backend
- **WHEN** a stream is configured with `backend: s3` and `sync: per_entry`
- **THEN** the configuration is rejected at load time with an error

### Requirement: Recovery is consistent under partial writes
Recovery from the object-store backend SHALL NOT rely solely on the manifest. It SHALL enumerate the actual segment objects (LIST) as a fallback, SHALL include segments present on the store but absent from the manifest, and SHALL verify each entry's checksum to discard a torn tail of a partially-written active segment.

#### Scenario: Segment present but manifest not updated
- **WHEN** a segment object was written but the manifest was not yet updated before a crash
- **THEN** recovery enumerates the segment via LIST and replays its entries past the cursor

#### Scenario: Torn active-segment tail is discarded
- **WHEN** the active segment's final entry is truncated by a mid-write crash
- **THEN** recovery detects the bad checksum, truncates at the last good entry, and replays only intact entries

### Requirement: Segment reclaim
The object-store backend SHALL reclaim (delete) sealed segment objects whose entries are all behind the committed cursor. Reclaim SHALL be best-effort and SHALL NOT block ingestion. A segment referenced by the manifest but missing on the store SHALL be ignored during recovery.

#### Scenario: Reclaimed segments are behind the cursor
- **WHEN** the cursor advances past the last sequence of a sealed segment
- **THEN** that segment object is deleted and removed from the manifest on the next manifest write

#### Scenario: Missing segment is ignored
- **WHEN** recovery reads a manifest that references a segment that no longer exists on the store
- **THEN** recovery skips that segment without error
