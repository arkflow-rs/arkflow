## MODIFIED Requirements

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. A late Update within the allowed-lateness deadline SHALL modify the already emitted window result rather than create an unrelated partial window. For sliding windows, each row SHALL be classified against every containing window membership rather than a single latest window end. Runtime metrics SHALL count each late or invalid row regardless of whether the policy drops, routes, or updates it. Session windows SHALL retain and merge dynamic per-key session boundaries; a late row that bridges an emitted session SHALL produce an update rather than a new initial result. Expired-window cleanup SHALL NOT reclaim an emitted buffer that still owes an unemitted correction, including at end-of-stream; the correction SHALL be emitted before the buffer's state is reclaimed.

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

#### Scenario: End-of-stream emits pending corrections before reclaim

- **WHEN** a session window's emitted buffer was merged or extended by a late update and the source reaches end-of-stream before the watermark advances past the merged end
- **THEN** the pending correction is emitted before the expired-buffer cleanup reclaims the buffer, and neither the corrected rows nor the new rows are lost

### Requirement: Event-time gates SHALL classify current rows consistently
The gate SHALL evaluate current rows against a watermark that is consistent with the batch's recorded progress. Rows that become late because the batch advances the watermark SHALL receive the configured Drop, Route, or Update action, while future rows MAY remain held. At end-of-stream the gate SHALL classify each held row per window membership against that window's real lateness deadline derived from the last observed watermark; a sentinel end-of-stream watermark SHALL NOT by itself exclude memberships that are still within their deadline.

#### Scenario: A batch contains a future row and an old row
- **WHEN** a first batch contains event times `[2100, 100]` for a `[0,1000)` window and the batch advances the watermark beyond that window
- **THEN** the old row is classified by the configured late-event policy and only the future row remains held for a later window

#### Scenario: End-of-stream does not truncate live sliding memberships
- **WHEN** the source ends while a held sliding row has one membership past its deadline and one membership still within its deadline
- **THEN** the still-live membership receives the row under the same Update/Drop/Route policy as mid-stream, and the emitted aggregates are not permanently truncated by the end-of-stream classification

### Requirement: Window state and source acknowledgement share rollback semantics

For a fired window backed by staged state, state finalization and source/WAL acknowledgement SHALL form one retryable processing unit. If any source acknowledgement fails, the runtime SHALL conditionally compensate or retain the state transaction so replay cannot double-count or lose the window update. Every window operator construction path SHALL register its fired-buffer writes with that retryable unit: a path that writes the backend before the output acknowledgement SHALL roll back the backend state, not only its in-memory buffers, when the acknowledgement fails.

#### Scenario: Replay after a failed fired-window acknowledgement

- **WHEN** a fired window is emitted successfully but its source acknowledgement fails before the durable cut completes
- **THEN** replaying the source row restores the pre-commit state or resumes the same staged transaction rather than applying a second aggregate

#### Scenario: A direct-construction path rolls back its backend writes

- **WHEN** a window operator built without the state journal emits a fired buffer, its acknowledgement fails, and the process replays the source rows before the next emission
- **THEN** the replay does not double-count into the emitted aggregate because the backend's emitted state was rolled back together with the in-memory buffers

## ADDED Requirements

### Requirement: Timestamped rows with a NULL key SHALL follow an explicit policy

A row with a convertible event timestamp but a NULL key SHALL NOT be silently skipped by keyed window aggregation. The runtime SHALL count it in the invalid/late metrics, SHALL route it to the late/invalid side output when one is configured, and SHALL otherwise drop it and complete its acknowledgement — mirroring the invalid-timestamp policy. The row's delivery SHALL NOT be acknowledged while it is silently unaccounted.

#### Scenario: NULL key with a side output configured

- **WHEN** a windowed Job receives a row whose timestamp converts but whose key column is NULL and a late/invalid side output is configured
- **THEN** the row is routed to that side output with an invalid marker, counted in the metrics, and acknowledged only after the route write

#### Scenario: NULL key without a side output

- **WHEN** a windowed Job receives a row whose key column is NULL and no side output is configured
- **THEN** the row is dropped, the invalid/late metric is incremented, and the acknowledgement completes without the row being counted into any aggregate
