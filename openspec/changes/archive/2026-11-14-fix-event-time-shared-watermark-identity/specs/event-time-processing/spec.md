## ADDED Requirements

### Requirement: Shared watermark trackers SHALL keep per-source-chain identity

When several event-time source chains share one downstream watermark frontier, the runtime SHALL track each chain's physical partition progress independently, even when two chains observe the same `(topic, partition)` identity. The shared frontier SHALL remain the minimum across all chains, so a fast chain cannot close windows that a slow chain still has events for. Checkpoints SHALL persist watermark positions under the raw `(topic, partition)` shape, restore SHALL map them into the restoring chain's identity, and single-chain groups SHALL keep raw keys unchanged.

#### Scenario: Two chains read the same topic and partition

- **WHEN** two source chains feeding the same window observe the same topic and partition, and the fast chain advances to 4,000 while the slow chain is still at 100
- **THEN** the slow chain's event is held rather than classified late, and the shared frontier reflects the slower chain

#### Scenario: Checkpoint a shared frontier

- **WHEN** a chain within a shared group writes its watermark positions to a checkpoint
- **THEN** it persists only its own partitions under the raw `(topic, partition)` shape, and restoring that checkpoint into a gate re-applies the chain identity without changing the stored format

#### Scenario: Single-chain group keeps raw keys

- **WHEN** exactly one source chain owns a watermark group
- **THEN** tracker keys, persisted positions, and restored positions remain the raw `(topic, partition)` identity
