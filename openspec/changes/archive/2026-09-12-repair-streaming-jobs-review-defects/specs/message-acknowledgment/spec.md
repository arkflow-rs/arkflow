## MODIFIED Requirements

### Requirement: Composite frontier waits are cancellation-aware

Any acknowledgement waiting for an earlier contiguous frontier SHALL observe the owning input or job cancellation, SHALL be woken when the input closes, and SHALL NOT hold a shared per-input serialization lock or the input's connection guard while it waits for an external condition — an assignment to return, a gap to close, or a broker round trip — so sibling acknowledgements on other partitions continue to settle. Shutdown SHALL NOT depend on an abandoned earlier acknowledgement completing, and a wait serving an external condition SHALL be bounded: when it expires the input SHALL either fail the delivery explicitly or record the documented at-least-once degradation.

#### Scenario: Kafka gap closes during shutdown

- **WHEN** offset N+1 is waiting for offset N and the Kafka input is closed or the job is cancelled
- **THEN** the waiting acknowledgement returns a cancellation/closed error and the shutdown path can finish

#### Scenario: Rebalance while an acknowledgement is in flight

- **WHEN** a partition is unassigned during a rebalance and an in-flight acknowledgement for that partition waits for the assignment to return
- **THEN** acknowledgements for the input's other partitions keep advancing, the checkpoint drain is not blocked behind the wait, and the wait ends on reassignment, cancellation, or its bound

## ADDED Requirements

### Requirement: Delivery settlement SHALL NOT block the source loop

Settling a delivery SHALL NOT require the input's read loop to wait on a durable commit, a broker round trip, or an acknowledgement that depends on later input. An input producing a delivery it does not forward — a tombstone, an empty batch, or a filtered record — SHALL hand the settlement to the same compensation path as a forwarded delivery instead of committing inline in `read`, so the source keeps polling for records and injected control events, and a settlement failure surfaces against that delivery for retry rather than as an input error that terminates the source.

#### Scenario: Tombstone delivery is settled out of band

- **WHEN** a compacted topic delivers a record with a null payload
- **THEN** the input acknowledges its position without invoking the acknowledgement inline in the read loop, and the source keeps polling for the next record and for injected control events

#### Scenario: Settlement failure is retryable

- **WHEN** settling a non-forwarded delivery fails because of a frontier fence, a broker offset store failure, or a closed connection
- **THEN** the failure is reported against that delivery so replay or the input's reconnect path can retry it, instead of failing the source permanently
