## MODIFIED Requirements

### Requirement: Channel backpressure

Edges SHALL be bounded channels. When a downstream channel is full, the upstream
vertex SHALL await send and thereby propagate backpressure to its source.

#### Scenario: Full channel blocks upstream

- **WHEN** a bounded edge reaches capacity and its consumer stops receiving
- **THEN** the producing vertex blocks on send instead of buffering unboundedly

#### Scenario: Capacity is configurable

- **WHEN** a graph is built through the builder API with a non-default channel capacity
- **THEN** edges are constructed with that capacity, defaulting to 1024
