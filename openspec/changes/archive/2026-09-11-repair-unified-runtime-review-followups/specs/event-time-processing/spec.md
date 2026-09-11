## ADDED Requirements

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
