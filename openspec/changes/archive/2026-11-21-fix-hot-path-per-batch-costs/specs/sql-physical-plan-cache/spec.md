## ADDED Requirements

### Requirement: The cached fast path SHALL NOT deep-clone the session state per batch
On the fully-cached fast path (analyzed/optimized/physical plan caches all hit for the batch schema, query not time-dependent), the SQL processor SHALL NOT call `SessionContext::state()` — a full `SessionState` deep clone in DataFusion 54 — per batch; execution SHALL reuse the existing physical plan with the context's task context. Session-state cloning SHALL happen only on cache misses and on time-dependent queries that re-optimize per batch (where the refreshed execution start time keeps folded timestamps current), including the temporary-table path. Output SHALL be identical to the per-batch-clone behavior for the same input batch.

#### Scenario: Cached batches skip the session-state clone
- **WHEN** a same-schema batch is processed repeatedly with all plan caches warm and the query is not time-dependent
- **THEN** no session-state clone occurs on those batches and the outputs are identical to the per-batch-clone behavior

#### Scenario: Time-dependent queries keep fresh folded timestamps
- **WHEN** a query folding `now`/`current_date`/`current_time`/`current_timestamp` is processed repeatedly
- **THEN** the query still re-optimizes per batch with a refreshed execution start time (excluded from plan reuse by the existing reuse requirement), unaffected by this change
