## ADDED Requirements

### Requirement: Buffering stage retained state stays bounded

A stage that retains messages between its write and read sides (the memory buffer) SHALL bound its retained message count by its configured `capacity`: once the retained count is at capacity, `write` SHALL await downstream drain instead of accumulating further, so backpressure propagates upstream through the bounded edges. An awaiting `write` SHALL be released when the buffer closes so shutdown stays live.

#### Scenario: Write awaits at capacity and resumes after drain

- **WHEN** the memory buffer holds `capacity` messages and a producer calls `write` while no reader is draining
- **THEN** the `write` does not return and no further messages are retained; once a reader drains the accumulated messages below capacity, the awaiting `write` completes

#### Scenario: Awaiting write is released on close

- **WHEN** a producer's `write` is awaiting capacity release and the buffer is closed
- **THEN** the awaiting `write` returns rather than hanging, and shutdown completes without deadlock
