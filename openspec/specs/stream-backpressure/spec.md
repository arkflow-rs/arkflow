# stream-backpressure Specification

## Purpose
TBD - created by archiving change redesign-backpressure-notify. Update Purpose after archive.
## Requirements
### Requirement: In-flight messages stay bounded
Pipeline stages SHALL be connected by bounded channels, and the number of in-flight envelopes a stage holds — those sent but not yet consumed downstream — SHALL NOT exceed the edge's configured capacity (default 1024). A stage whose downstream edge is full SHALL await send rather than buffer without bound, so that a slow downstream propagates backpressure up to the source chain, which stops reading new input until capacity is released.

#### Scenario: In-flight held near the edge capacity under a slow output
- **WHEN** the terminal sink writes persistently slower than the source produces and a downstream edge keeps accumulating envelopes
- **THEN** the producing chain awaits send once the edge reaches capacity, stops reading its upstream, and resumes only after the consumer takes envelopes and releases capacity

#### Scenario: No backpressure under a fast output
- **WHEN** the sink keeps up so that every edge stays below its capacity
- **THEN** producing chains await nothing and read and process input continuously

### Requirement: Backpressure release is signal-driven
Releasing backpressure SHALL be event-driven: when a consumer takes an envelope and edge capacity becomes available, the awaiting producer SHALL be woken by the bounded channel's own capacity notification rather than by periodic polling or a fixed sleep.

#### Scenario: Producer resumes immediately after the consumer advances
- **WHEN** a producing chain is awaiting send on a full edge and the consumer takes one envelope
- **THEN** the producer is woken promptly by the release of capacity and sends the next envelope without waiting for a fixed sleep interval

### Requirement: Liveness under input end and cancellation
Every chain SHALL terminate on input end (EOF/EOS) or cancellation without deadlocking on a backpressure wait: a producer awaiting send on a full edge SHALL be released when the downstream consumer stops and closes its endpoint, and consumers SHALL keep draining in order until the edge is empty so that pending envelopes and EOS can flow through.

#### Scenario: Drains and exits despite backpressure at input EOF
- **WHEN** the source reaches EOF while edges are at capacity and producers are awaiting send
- **THEN** consumers keep draining in order, producers send the remaining envelopes as capacity is released, EOS is forwarded through every chain, and all chains exit

#### Scenario: Cancellation unblocks a full edge
- **WHEN** a producing chain is awaiting send on a full edge and the job is cancelled so that the consumer stops and its channel endpoint closes
- **THEN** the blocked send is released because the endpoint closed, the chain unwinds its event loop, and the task set terminates without hanging or leaking tasks

### Requirement: Ordered-output semantics unaffected by the backpressure mechanism
The ordered-delivery guarantee SHALL be independent of backpressure: data envelopes SHALL be delivered on each edge in send order (FIFO), forward chains SHALL preserve source-to-sink batch order, and acknowledgments SHALL flow only after the terminal sink writes successfully.

#### Scenario: Still written in order across repeated backpressure cycles
- **WHEN** backpressure is entered and exited repeatedly during a run
- **THEN** the sink still observes batches in source order, with no reordering or loss

