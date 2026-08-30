## MODIFIED Requirements

### Requirement: Watermarks SHALL reflect partition progress
The runtime SHALL track watermark progress per input partition and SHALL advance an operator watermark only from eligible upstream partition progress, including configured idle-partition handling. An operator receiving multiple active upstream partitions or edges SHALL use the minimum active progress, not the fastest input's maximum.

#### Scenario: One partition becomes idle
- **WHEN** an input partition is marked idle according to the configured policy
- **THEN** it does not permanently hold back the operator watermark, and the observation identifies the idle partition

#### Scenario: One active partition lags
- **WHEN** two active partitions report watermarks 2,000 and 1,000 respectively
- **THEN** the operator watermark remains at or below 1,000 until the lagging partition advances or becomes idle

#### Scenario: Restore a non-zero partition watermark
- **WHEN** a source subtask assigned to physical partition 3 restores a checkpointed watermark
- **THEN** the watermark is installed for partition 3 and no synthetic partition 0 participates in the calculation

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. A late Update within the allowed-lateness deadline SHALL modify the already emitted window result rather than create an unrelated partial window.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the same window result according to the Job policy, preserving the window key and identity

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

#### Scenario: Update targets an already emitted window
- **WHEN** a late event marked for Update arrives before the window's allowed-lateness deadline
- **THEN** the runtime reopens the retained closed aggregate, emits a complete corrected result with an update marker, and acknowledges the input only after that result is written

## ADDED Requirements

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
