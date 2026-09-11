# event-time-processing Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Sources SHALL declare time semantics

Each event-time Job source SHALL declare an event timestamp expression or explicitly select processing time, together with a watermark strategy when event time is used. A nullable or invalid timestamp routed by policy SHALL carry a marker distinct from a late-event marker.

#### Scenario: Validate an event-time source

- **WHEN** a Job selects event-time processing without a valid timestamp expression or watermark strategy
- **THEN** validation fails before deployment with the source and missing configuration identified

#### Scenario: Route an invalid timestamp

- **WHEN** a nullable timestamp cannot be converted to an event timestamp and the invalid-event policy routes the row
- **THEN** the routed row contains a dedicated invalid-timestamp marker and is not labeled only as a late event

### Requirement: Watermarks SHALL reflect partition progress
The runtime SHALL track watermark progress by the complete physical input identity (topic, when present, and partition), SHALL seed all known assigned partitions before data arrives, SHALL exclude only partitions that are configured idle, and SHALL advance an operator watermark from the minimum active upstream progress. Event-time gates that feed the same downstream window SHALL use the same downstream watermark frontier. Session windows SHALL use dynamic per-key boundaries owned by the window operator, and processing-time windows SHALL not be event-time gated.

#### Scenario: One partition becomes idle
- **WHEN** an input partition is marked idle according to the configured policy
- **THEN** it does not permanently hold back the operator watermark, and the observation identifies the topic and partition that became idle

#### Scenario: An unobserved assigned partition is slow
- **WHEN** one assigned partition has not emitted a row while another partition advances
- **THEN** the unobserved partition remains active until the idle timeout and the operator watermark does not advance past its progress

#### Scenario: Topics reuse a partition number
- **WHEN** topic A partition 0 and topic B partition 0 report different progress
- **THEN** they maintain independent watermark entries and neither can advance the other topic's windows

#### Scenario: Multiple source edges feed one window
- **WHEN** two event-time source edges feed the same downstream window
- **THEN** late classification and window closure use their shared slowest active frontier rather than a source-local watermark

#### Scenario: Processing-time window receives event-time input
- **WHEN** an event-time source feeds a window whose trigger is processing time
- **THEN** the source event-time gate forwards the row immediately and the processing-time trigger remains responsible for emission

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. Session windows SHALL retain and merge dynamic per-key session boundaries; a late row that bridges an emitted session SHALL produce an update rather than a new initial result.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy, including the dynamic session membership for session windows

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

### Requirement: Event timestamps SHALL accept supported Arrow units
An event-time source SHALL accept Int64 timestamps and Arrow timestamp columns in seconds, milliseconds, microseconds, or nanoseconds, normalize them to checked millisecond values, and reject overflow or unsupported types before processing.

#### Scenario: Normalize an Arrow timestamp column
- **WHEN** the configured timestamp field is a nullable `TimestampSecond`, `TimestampMillisecond`, `TimestampMicrosecond`, or `TimestampNanosecond` column
- **THEN** the gate converts each non-null value to milliseconds, preserves null information, and applies the same window-boundary rules to the normalized value

#### Scenario: Timestamp conversion overflows
- **WHEN** a timestamp value cannot be represented in milliseconds
- **THEN** event-time processing fails with an actionable field/type error and does not silently wrap the value

### Requirement: Event-time gates SHALL classify current rows consistently
The gate SHALL evaluate current rows against a watermark that is consistent with the batch's recorded progress. Rows that become late because the batch advances the watermark SHALL receive the configured Drop, Route, or Update action, while future rows MAY remain held.

#### Scenario: A batch contains a future row and an old row
- **WHEN** a first batch contains event times `[2100, 100]` for a `[0,1000)` window and the batch advances the watermark beyond that window
- **THEN** the old row is classified by the configured late-event policy and only the future row remains held for a later window

### Requirement: Invalid event timestamps SHALL not be held indefinitely
A null or otherwise invalid event timestamp SHALL NOT be retained as an ordinary held event because it cannot produce a window end. The runtime SHALL route it to the configured invalid/late side output when one exists, otherwise drop and acknowledge it.

#### Scenario: Null timestamp with a route configured
- **WHEN** a nullable timestamp column contains a null row and the Job has a late-event route
- **THEN** the row is sent to that side output with an invalid-timestamp marker and its acknowledgement is completed after the route write

#### Scenario: Null timestamp without a route
- **WHEN** a nullable timestamp column contains a null row and no side output is configured
- **THEN** the row is dropped, the invalid/late metric is incremented, and its acknowledgement is completed without retaining it in the gate

### Requirement: Sliding windows SHALL include every containing window
For a sliding window, the runtime SHALL enumerate every valid window start whose interval contains the event timestamp, including when the window size is not divisible by the slide.

#### Scenario: Non-divisible sliding window
- **WHEN** `size=5`, `slide=2`, and an event has timestamp 4
- **THEN** the event contributes to windows starting at 4, 2, and 0, and no containing start is omitted because of integer division truncation

### Requirement: Sliding-window exclusions accumulate across releases

When a held row belongs to multiple sliding windows that close on different watermark advances, the runtime SHALL merge newly closed memberships into the existing exclusion metadata. Re-evaluating a held row SHALL NOT discard exclusions already recorded.

#### Scenario: A held row sees two successive window closures

- **WHEN** a held row is first marked for an ending-5 window and later the ending-9 window also closes before release
- **THEN** the final row carries both exclusions and is not reintroduced into either already-emitted window

### Requirement: Window state and source acknowledgement share rollback semantics

For a fired window backed by staged state, state finalization and source/WAL acknowledgement SHALL form one retryable processing unit. If any source acknowledgement fails, the runtime SHALL conditionally compensate or retain the state transaction so replay cannot double-count or lose the window update.

#### Scenario: Replay after a failed fired-window acknowledgement

- **WHEN** a fired window is emitted successfully but its source acknowledgement fails before the durable cut completes
- **THEN** replaying the source row restores the pre-commit state or resumes the same staged transaction rather than applying a second aggregate

### Requirement: Event-time compatibility markers remain row-local

Sliding-window late-membership metadata SHALL be attached and merged per input row. A row whose open membership remains eligible SHALL still be processed for that membership even when another containing membership has already closed.

#### Scenario: Mixed open and closed memberships

- **WHEN** a late row belongs to one closed sliding window and one still-open sliding window
- **THEN** the closed membership is excluded or routed according to policy while the open membership receives the row exactly once

