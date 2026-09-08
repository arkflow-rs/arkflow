## MODIFIED Requirements

### Requirement: Per-Stream lifecycle supervision
The runtime manager SHALL track each Stream independently with state, a per-Stream cancellation mechanism, and a supervised task handle. Startup failures SHALL be reported before readiness, and shutdown timeouts SHALL leave the Stream in a recoverable terminal or failed state rather than permanently stopping lifecycle commands in `Stopping` or `Restarting`.

#### Scenario: One Stream fails
- **WHEN** one Stream task exits with an error
- **THEN** that Stream becomes `failed`, its latest error is retained, and unrelated Streams continue running

#### Scenario: Engine shuts down
- **WHEN** the Engine receives its shutdown signal
- **THEN** the runtime manager requests shutdown for every Stream and waits for their tasks to finish or reports a bounded shutdown failure while updating each Stream state

#### Scenario: Startup fails
- **WHEN** graph construction or resource connection fails before a local Job starts
- **THEN** readiness is not reported and the temporary adapter/resources are closed

#### Scenario: Shutdown times out
- **WHEN** a Stream task does not exit within the shutdown timeout
- **THEN** the manager aborts it, records the timeout, and transitions the entry to a recoverable `failed` or `stopped` state
