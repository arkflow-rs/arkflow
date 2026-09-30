## MODIFIED Requirements

### Requirement: Stable Stream identity
Each configured Stream SHALL have a stable unique ID for runtime commands, API resources, metrics, and events. Legacy configurations without IDs SHALL receive deterministic `stream-<index>` IDs and a migration warning. The runtime registry SHALL assign stream indices from a monotonically increasing counter (never reusing an index after de-registration), so that two concurrently active Streams can never share a derived job ID, state namespace prefix, or checkpoint directory — even after `replace_config` removes and re-registers entries.

#### Scenario: Duplicate IDs are rejected
- **WHEN** a candidate configuration contains two Streams with the same ID
- **THEN** validation fails before any runtime Stream is stopped or replaced

#### Scenario: Legacy configuration is loaded
- **WHEN** a configuration contains Streams without IDs
- **THEN** the Engine assigns deterministic IDs and emits a migration warning

#### Scenario: De-registration and re-registration never reuse an index

- **WHEN** a Stream is de-registered (via `replace_config`) and a new or surviving Stream is subsequently registered
- **THEN** the new registration receives a strictly greater index than every previously assigned index, and its derived job ID / state namespace prefix cannot collide with any concurrently active Stream

### Requirement: Stream start, stop, and restart
The runtime manager SHALL support starting, stopping, and restarting individual Streams with correct lifecycle semantics: start launches a fresh supervised task; stop requests shutdown and awaits the task; restart stops then starts with one cancellation cycle. A `stop` request issued while a `restart` is in progress SHALL NOT produce an inconsistent state where `stop` reports success while the Stream is left Running.

#### Scenario: Concurrent stop during restart does not leave a Running stream

- **WHEN** a `restart` is initiated and a concurrent `stop` arrives during the restart's stop phase
- **THEN** the `stop` completes without error (the restart's internal stop fulfils the stop intent), and the final state reflects the last-initiated intent — the `stop` never silently resurrects a stopped Stream into Running

#### Scenario: Stop and restart are individually correct

- **WHEN** `stop` is called on a Running Stream (no concurrent restart)
- **THEN** the Stream transitions to Stopped and its task has exited

- **WHEN** `restart` is called on a Running Stream (no concurrent stop)
- **THEN** the Stream's previous task exits, a fresh task launches, and restart metrics increment
