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

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. Session windows SHALL retain and merge dynamic per-key session boundaries; a late row that bridges an emitted session SHALL produce an update rather than a new initial result.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy, including the dynamic session membership for session windows

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome
