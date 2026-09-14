# Design: event-time and recovery correctness follow-up

## Context

The unified stream runtime now combines event-time gates, window state, source
acknowledgements, WAL recovery, local checkpoints, and Hub/Agent reconciliation.
The latest review found that these boundaries still use incompatible notions of
progress: a watermark can be local to one source chain while a window is fed by
several physical partitions, and an acknowledgement can commit one part of a
logical delivery while another part fails.  The control plane has the same
problem when operation identity and persisted state do not include the Job
generation.

## Goals / Non-Goals

### Goals

- Represent event-time progress by the complete physical input identity
  (topic, when present, and partition), seed known assignments, and use the
  slowest active input for gating and recovery.
- Keep event-time gates local to their delivery channel while sharing the
  downstream watermark frontier where multiple event-time inputs feed one
  window.  Session windows retain dynamic boundaries in the window operator;
  processing-time windows are never event-time gated.
- Make grouped source acknowledgements compensatable and keep WAL, state, and
  source progress at one acknowledgement boundary.  WAL replay reconstructs a
  source-position acknowledgement where the connector supports it.
- Make window re-key cleanup, emitted/update state, and journal snapshots
  durable as one logical mutation without unbounded staged growth.
- Preserve barrier progress and lifecycle cleanup when inputs end, workers fail,
  startup fails, or shutdown times out.
- Make Hub operations generation-aware and restart-safe, including expired
  commands and late results.
- Preserve legacy window semantics by carrying supported joins or rejecting an
  unsupported legacy shape explicitly.

### Non-Goals

- Changing the external checkpoint format incompatibly.  New per-partition
  fields are additive and old task-level watermark fields remain readable as a
  fallback.
- Providing exactly-once guarantees for connectors that cannot compensate a
  committed source acknowledgement.  Such connectors continue to expose their
  existing at-least-once behavior and are not silently treated as atomic.
- Replacing the existing Job graph or introducing a new scheduler protocol.
- Making an event-time session gate infer keys after arbitrary processor schema
  changes.  Dynamic session ownership remains in the window operator.

## Decisions

### 1. Physical watermark identity and ownership

Add a serializable physical partition key consisting of an optional topic and a
numeric partition.  `WatermarkTracker` stores progress by that key, exposes a
compatibility wrapper for the old numeric API, and can seed entries from a
connector assignment.  A seeded, non-idle partition participates in the
minimum until its configured idle timeout elapses.  A session timing is not
converted to a synthetic `event_time + gap` deadline in the source gate; the
window operator maintains the per-key dynamic session end and emitted/update
state.  Processing-time window triggers are excluded from source event-time
timings.

Each event-time source gate keeps its own held rows and acknowledgements, but
gates that feed the same downstream event-time window share a tracker.  This
avoids releasing one chain's buffered rows through another chain while still
ensuring that late classification sees the downstream minimum.  Checkpoint
reports persist all known physical watermark entries, and Agent restore seeds
all assigned entries rather than synthesizing partition zero.

### 2. Acknowledgement and WAL boundary

Extend `Ack` with a compensating undo hook.  `ConcurrentAck` acknowledges source
children as a logical group: it records successful children, invokes undo on
those children if a sibling fails, and reports the original failure.  Durable
source acknowledgements implement the hook by restoring their in-memory and
durable frontier as far as the backend permits.  WAL acknowledgement advances
its cursor one delivery at a time, never drains unrelated later source acks in
the caller, and blocks the frontier behind a failed earlier source commit.

`WalInput::read` flushes the appended entry before returning.  Recovery entries
use the wrapped input's source-position acknowledgement when available instead
of an unconditional `NoopAck`.  Window journal commit is ordered with the
source acknowledgement as one `CommitGroupOnAck` boundary; source failures
therefore invoke the same rollback path.  This is still at-least-once for
non-compensatable external sinks, but it no longer claims a successful grouped
source commit after a sibling failure.

### 3. Window journal and compatibility

Journal writes for a live window replace the previous mutation for the same
state key, keeping staged bytes proportional to the live snapshot.  A session
re-key stages deletion of the old backend key in the new fired transaction;
that deletion is not independently committed by stale-key cleanup.  Merging
buffers propagates whether any member has already been emitted, so a merged
session produces an update.  Legacy `join` configuration is rejected with a
clear migration error until an equivalent join node is available in the new
graph; other legacy window payload fields retain their established mapping.

### 4. Graph and lifecycle fencing

The barrier aligner tracks active inputs, implicitly removes a bounded input on
EOS, and releases buffered EOS/data once the remaining active inputs align.
Worker pools are cancelled and joined before processors close; collector and
worker errors are propagated.  Startup failures close the real adapter, source
reconnect waits observe cancellation, and shutdown timeout transitions the
runtime entry out of `Stopping`/`Restarting` after aborting the task.

### 5. Control-plane generation and recovery

The active-operation key includes Job generation.  Reconciliation computes
stale placements across generations, restores persisted operations before
reconciling Jobs, and requeues expired leased commands.  Command results are
applied only to non-terminal operations with a matching generation; late or
old results are ignored.  This prevents a previous rollout from resurrecting a
cancelled or superseded Job.

### 6. State backend accounting

Redb metric seeding excludes expired entries.  All byte-budget checks and
counter updates use one write-side critical section shared by competing
mutations, so concurrent puts cannot pass the same stale budget observation.

## Risks / Trade-offs

- Sharing a tracker requires source chains that feed the same window to agree on
  the event-time configuration.  The graph builder only shares compatible
  groups; incompatible configurations remain independently validated.
- Compensating Kafka commits is best effort because broker commit APIs do not
  provide a universal transaction spanning arbitrary sources.  Failures are
  surfaced and the cursor is never reported as fully successful without the
  connector operation.
- Keeping old checkpoint fields increases format surface area.  Restore gives
  precedence to per-partition data and falls back to the old task watermark so
  existing artifacts remain usable.
- Explicitly rejecting legacy joins may require a user migration, but is safer
  than silently changing joined rows into unjoined input.

## Migration / Rollout

The implementation is additive for serialized checkpoint manifests and uses
the existing runtime and reconciliation APIs.  New manifests write physical
watermark entries; old manifests continue through the compatibility fallback.
No data migration is needed for Redb state.  Rollout validation covers unit
tests for tracker, WAL, ACK, window, aligner, and control-plane state-machine
paths, followed by the workspace test suite where external Kafka/Docker
dependencies are available.
