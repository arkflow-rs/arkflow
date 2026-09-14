## MODIFIED Requirements

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

#### Scenario: A slower partition is first observed
- **WHEN** partition 0 reports watermark 2,000 and partition 1 is then first observed at 1,000
- **THEN** the active watermark reflects the minimum active progress and does not remain at 2,000 solely because that value was previously observed

#### Scenario: One connector task multiplexes partitions
- **WHEN** a single source task consumes physical partitions 0 and 1 and their deliveries carry those partition identities
- **THEN** event-time progress for partition 0 cannot advance or close windows on behalf of partition 1

#### Scenario: Restore a non-zero partition watermark
- **WHEN** a source subtask assigned to physical partition 3 restores a checkpointed watermark
- **THEN** the watermark is installed for partition 3 and no synthetic partition 0 participates in the calculation

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. A late Update within the allowed-lateness deadline SHALL modify the already emitted window result rather than create an unrelated partial window. For sliding windows, each row SHALL be classified against every containing window membership rather than a single latest window end. Runtime metrics SHALL count each late or invalid row regardless of whether the policy drops, routes, or updates it. Session windows SHALL retain and merge dynamic per-key session boundaries; a late row that bridges an emitted session SHALL produce an update rather than a new initial result.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy, including the dynamic session membership for session windows

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

#### Scenario: Update targets an already emitted window
- **WHEN** a late event marked for Update arrives before the window's allowed-lateness deadline
- **THEN** the runtime reopens the retained closed aggregate, emits a complete corrected result with an update marker, and acknowledges the input only after that result is written

#### Scenario: A sliding row belongs to multiple windows
- **WHEN** a row belongs to sliding windows ending at 5 and 9 and the first membership has already expired
- **THEN** the first membership is classified as late independently and is not reintroduced as a new on-time aggregate merely because the later membership remains open

#### Scenario: Dropped rows are counted
- **WHEN** a batch contains multiple late rows and the policy drops them, including rows with invalid timestamps
- **THEN** the late-event metric increases by the number of affected rows, not by one per action group
