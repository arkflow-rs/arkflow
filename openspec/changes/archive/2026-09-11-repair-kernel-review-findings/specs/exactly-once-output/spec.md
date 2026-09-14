## MODIFIED Requirements

### Requirement: Transaction boundary equals the buffer aggregation unit
When deliveries are aggregated before one output call — a window operator emission batch or a batched composite delivery — that batch's composite ack (e.g. `VecAck` / `ArrayAck`) SHALL be delivered to a single `write_batch` call. A transactional output SHALL treat one `write_batch` call as one atomic transaction unit covering all constituent input acks. No aggregation point SHALL drop, split, or silently merge acks in a way that breaks the one-`write_batch`-per-ack-range invariant.

#### Scenario: Window aggregation is one transaction unit
- **WHEN** a tumbling window operator emits one aggregate covering messages from three input reads whose acks are combined into a single composite ack
- **THEN** the aggregated batch is delivered to exactly one `write_batch` call, and a transactional sink commits the whole window atomically
