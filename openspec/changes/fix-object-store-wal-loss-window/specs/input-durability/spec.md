## MODIFIED Requirements

### Requirement: Object-store WAL survives node loss
When the object-store backend is in use, every entry that has been flushed to a segment object SHALL be recoverable after the node (pod/host) is lost — not only after a process crash. Entries not yet sealed into a segment object SHALL be re-delivered by their source after restart (the source acknowledgement for such entries cannot have completed), preserving at-least-once semantics; no acknowledged entry SHALL be lost on node loss.

#### Scenario: Flushed entries survive pod disappearance
- **WHEN** a node has flushed entries to segment objects and then the node/pod disappears
- **THEN** on restart (same `node_id`) those flushed entries are present in object storage and are replayed during recovery

#### Scenario: Un-sealed entries are redelivered by the source
- **WHEN** a node disappears with entries staged in memory but not yet sealed to a segment object
- **THEN** those entries' source acknowledgements have not completed, and the source re-delivers them after restart (at-least-once), while all previously sealed entries are recovered from object storage

### Requirement: Segment-based batching with a bounded replay window
The object-store backend SHALL persist entries as immutable segment objects written in batches. A source acknowledgement SHALL complete only after the acknowledged entry's sequence is contained in a sealed segment object: un-sealed work is redone from source re-delivery on restart (replay window), never silently lost. The replay window SHALL be bounded by the configurable segment flush triggers (`max_entries`, `max_bytes`, `flush_interval`), which also bound the acknowledgement latency added by this gating. The `per-entry` sync policy SHALL be rejected for the object-store backend.

#### Scenario: Acknowledged entries are always sealed
- **WHEN** a source acknowledgement for sequence N completes on the object-store backend
- **THEN** a sealed segment object containing sequence N exists in object storage, and a crash immediately after the acknowledgement replays at most from N (never loses N)

#### Scenario: Replay window is configurable
- **WHEN** the segment flush triggers are set
- **THEN** the maximum number of entries redone on node loss — and the maximum acknowledgement latency added by seal gating — are bounded by those triggers

#### Scenario: per-entry sync is rejected on the object-store backend
- **WHEN** a stream is configured with `backend: s3` and `sync: per_entry`
- **THEN** the configuration is rejected at load time with an error

## ADDED Requirements

### Requirement: WAL flusher failures SHALL be observable
The WAL background flusher SHALL NOT silently swallow flush failures on its wake path: each failed flush SHALL be counted in a flush-failure metric and reported via a rate-limited warning, and a persistently failing flusher SHALL escalate to error-level logging. The shutdown path SHALL continue to surface the final flush result through `close()` as today.

#### Scenario: Persistent store failure is visible
- **WHEN** the WAL flusher's store writes fail persistently on the wake path
- **THEN** a flush-failure counter increments per failed attempt, warnings are emitted at a bounded rate, and sustained failure is logged at error level (no silent hot retry)

#### Scenario: Graceful close still surfaces the final flush
- **WHEN** the WAL is closed while the flusher has pending entries
- **THEN** `close()` performs the final flush and propagates its error, unchanged from the existing behavior
