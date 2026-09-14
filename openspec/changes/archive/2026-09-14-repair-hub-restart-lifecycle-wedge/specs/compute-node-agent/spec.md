## ADDED Requirements

### Requirement: Redundant lifecycle starts settle within a bounded wait

When the Agent receives a lifecycle start for a Job whose previous kernel has not finished tearing down, it SHALL wait for the previous kernel's teardown for a bounded period. If the teardown exceeds the bound, the Agent SHALL record a warning and proceed with the new start; the redundant start SHALL then either complete or terminate with an explicit failure result, so the Hub operation record always settles instead of blocking indefinitely behind a wedged teardown.

#### Scenario: Previous kernel teardown completes in time

- **WHEN** a redundant start's previous kernel finishes its WAL-safe teardown within the bounded wait
- **THEN** the new start proceeds normally and reports its terminal result as usual

#### Scenario: Previous kernel teardown exceeds the bound

- **WHEN** the previous kernel's teardown does not finish within the bounded wait
- **THEN** the Agent records a warning, proceeds with the new start, and the Hub operation settles to a terminal outcome through completion or an explicit failure rather than remaining pending forever
