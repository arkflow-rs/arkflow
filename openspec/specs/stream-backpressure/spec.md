# stream-backpressure Specification

## Purpose
TBD - created by archiving change redesign-backpressure-notify. Update Purpose after archive.
## Requirements
### Requirement: In-flight messages stay bounded
Stream SHALL limit, before each processor worker reads its next input, the number of in-flight messages — those with an assigned sequence number not yet written out in order by the output — so that it does not exceed an upper bound of `BACKPRESSURE_THRESHOLD` plus the processor concurrency, ensuring `do_output`'s reorder buffer (`BTreeMap`) does not grow without bound.

#### Scenario: In-flight held near the threshold under a slow output
- **WHEN** the output writes persistently slower than the input and processors produce, so in-flight keeps accumulating
- **THEN** the processor worker stops reading new input once in-flight reaches the upper bound, and does not resume until the output writes one message in order and advances `next_seq` to release a slot

#### Scenario: No backpressure under a fast output
- **WHEN** the output writes fast and in-flight stays below `BACKPRESSURE_THRESHOLD`
- **THEN** the processor worker enters no wait and continuously reads and processes input

### Requirement: Backpressure release is signal-driven
When in-flight drops below the threshold because the output advanced `next_seq`, Stream SHALL immediately wake the blocked processor worker via an explicit signal (`tokio::sync::Notify`), rather than relying on periodic polling or a fixed sleep.

#### Scenario: Processor resumes immediately after the output advances
- **WHEN** a processor worker is waiting under backpressure and the output writes one message in order, bringing in-flight back below the threshold
- **THEN** the processor worker is woken immediately by the signal emitted when the output advances `next_seq`, without waiting for a fixed sleep interval

### Requirement: Liveness under input end and cancellation
Stream SHALL guarantee that all processor and output workers eventually exit on input end (EOF) or cancellation, without deadlocking on a backpressure wait.

#### Scenario: Drains and exits despite backpressure at input EOF
- **WHEN** the input emits EOF while more than the threshold in-flight messages remain unwritten and a processor worker is waiting under backpressure
- **THEN** the output worker keeps draining the remaining in-flight in order, advancing `next_seq` and emitting a signal per message written so that the processor worker is woken incrementally; once in-flight drops below the threshold, the processor worker observes the input channel closing and exits, after which the output worker flushes any residue left in the reorder buffer and exits

### Requirement: Ordered-output semantics unaffected by the backpressure mechanism
Changing the backpressure signaling mechanism SHALL NOT alter the semantics that the output writes messages in ascending sequence-number order.

#### Scenario: Still written in order across repeated backpressure cycles
- **WHEN** multiple processor workers process concurrently and backpressure is entered and exited repeatedly during the run
- **THEN** the output worker still writes messages strictly in ascending `next_seq` order, with no reordering or loss

