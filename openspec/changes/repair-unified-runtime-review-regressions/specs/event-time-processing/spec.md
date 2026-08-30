# event-time-processing Specification

## MODIFIED Requirements

### Requirement: Sources SHALL declare time semantics

Each event-time Job source SHALL declare an event timestamp expression or explicitly select processing time, together with a watermark strategy when event time is used. A nullable or invalid timestamp routed by policy SHALL carry a marker distinct from a late-event marker.

#### Scenario: Validate an event-time source

- **WHEN** a Job selects event-time processing without a valid timestamp expression or watermark strategy
- **THEN** validation fails before deployment with the source and missing configuration identified

#### Scenario: Route an invalid timestamp

- **WHEN** a nullable timestamp cannot be converted to an event timestamp and the invalid-event policy routes the row
- **THEN** the routed row contains a dedicated invalid-timestamp marker and is not labeled only as a late event

### Requirement: Watermarks SHALL reflect partition progress

The runtime SHALL track watermark progress per physical input partition and SHALL advance an operator watermark only from the minimum eligible progress across active upstream partitions, including configured idle-partition handling. A partition SHALL become active before it contributes to the minimum, and a newly observed slower partition SHALL NOT be hidden by a provisional earlier global maximum.

#### Scenario: One partition becomes idle

- **WHEN** an input partition is marked idle according to the configured policy
- **THEN** it does not permanently hold back the operator watermark, and the observation identifies the idle partition

#### Scenario: A slower partition is first observed

- **WHEN** partition 0 reports watermark 2,000 and partition 1 is then first observed at 1,000
- **THEN** the active watermark reflects the minimum active progress and does not remain at 2,000 solely because that value was previously observed

#### Scenario: One connector task multiplexes partitions

- **WHEN** a single source task consumes physical partitions 0 and 1 and their deliveries carry those partition identities
- **THEN** event-time progress for partition 0 cannot advance or close windows on behalf of partition 1

### Requirement: Windows SHALL define lateness behavior

Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. For sliding windows, each row SHALL be classified against every containing window membership rather than a single latest window end. Runtime metrics SHALL count each late or invalid row regardless of whether the policy drops, routes, or updates it.

#### Scenario: A late event arrives within allowed lateness

- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy

#### Scenario: A late event exceeds allowed lateness

- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

#### Scenario: A sliding row belongs to multiple windows

- **WHEN** a row belongs to sliding windows ending at 5 and 9 and the first membership has already expired
- **THEN** the first membership is classified as late independently and is not reintroduced as a new on-time aggregate merely because the later membership remains open

#### Scenario: Dropped rows are counted

- **WHEN** a batch contains multiple late rows and the policy drops them, including rows with invalid timestamps
- **THEN** the late-event metric increases by the number of affected rows, not by one per action group
