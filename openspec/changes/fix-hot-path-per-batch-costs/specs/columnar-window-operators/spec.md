## ADDED Requirements

### Requirement: Journaled persist cost SHALL NOT grow with open-window count
Per-batch journal persistence SHALL touch only the window keys the batch actually mutated (dirty transactions) and SHALL NOT clone the whole open-buffer map per batch. The stale-key backend scan SHALL run only when buffers were removed since the last persistence (fired-window expiry deletions or `max_buffered_keys` evictions) or on the first persistence after a backend restore, and SHALL NOT run on batches that removed nothing. Stale-entry cleanup semantics are unchanged: backend entries of removed keys SHALL be deleted by a subsequent persistence (a restart MUST NOT resurrect an expired window that was already fired), and the persisted state format SHALL be unchanged.

#### Scenario: Steady-state batches skip the stale scan
- **WHEN** consecutive batches mutate existing window keys and remove no buffers
- **THEN** no backend scan runs for those batches, and per-batch persist cost stays flat as committed backend entries accumulate

#### Scenario: Buffer removal triggers backend cleanup on the next persist
- **WHEN** a window fires past its allowed lateness and its buffer is removed
- **THEN** the next persistence deletes the corresponding backend entry, so a restart does not resurrect the expired window

#### Scenario: Restore keeps the cleanup safety net
- **WHEN** the operator restores its buffers from the backend and the first persistence runs
- **THEN** a stale scan may run once (safety net), after which steady-state batches skip it again
