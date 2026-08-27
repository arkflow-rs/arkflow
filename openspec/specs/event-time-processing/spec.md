# event-time-processing Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Sources SHALL declare time semantics
Each event-time Job source SHALL declare an event timestamp expression or explicitly select processing time, together with a watermark strategy when event time is used.

#### Scenario: Validate an event-time source
- **WHEN** a Job selects event-time processing without a valid timestamp expression or watermark strategy
- **THEN** validation fails before deployment with the source and missing configuration identified

### Requirement: Watermarks SHALL reflect partition progress
The runtime SHALL track watermark progress per input partition and SHALL advance an operator watermark only from eligible upstream partition progress, including configured idle-partition handling.

#### Scenario: One partition becomes idle
- **WHEN** an input partition is marked idle according to the configured policy
- **THEN** it does not permanently hold back the operator watermark, and the observation identifies the idle partition

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

