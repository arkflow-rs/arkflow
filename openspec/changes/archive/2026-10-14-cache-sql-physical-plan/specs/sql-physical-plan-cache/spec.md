## ADDED Requirements

### Requirement: SQL processor SHALL reuse the physical plan while the batch schema is unchanged

The SQL processor's fast path (no temporary tables) SHALL plan each (query, batch schema) pair's physical plan at most once and reuse it for subsequent batches with the same schema: the table scan node the plan was built from SHALL NOT capture batch data at planning time and SHALL read the processor's current batch at execution time. Reuse SHALL be keyed per pooled session context and invalidated on schema change, and the output of the reused plan SHALL be identical to per-batch re-planning (values, column order, and types) for the same input batch. Queries whose optimization folds time expressions (`now`, `current_date`, `current_time`, `current_timestamp`) SHALL be excluded from physical-plan reuse and re-optimize per batch, so the folded timestamps stay current.

#### Scenario: Cached plan serves each batch's own data

- **WHEN** the same schema arrives in consecutive batches and an aggregate query runs against each
- **THEN** every batch's result reflects exactly that batch's rows, identical to per-batch re-planning

#### Scenario: Schema drift invalidates the cached physical plan

- **WHEN** a batch arrives whose schema differs from the cached plan's schema
- **THEN** the processor re-plans for the new schema and the query succeeds with correct results instead of executing a plan bound to the old schema

#### Scenario: Physical planning happens once per schema

- **WHEN** many batches of one fixed schema flow through the processor
- **THEN** physical plan construction occurs at most once per pooled context for that schema, not once per batch

#### Scenario: Stateful operators reset per execution

- **WHEN** a query whose physical plan embeds per-execution node state (parallel aggregation repartitioning, TopK sort filters) runs across batches
- **THEN** each batch's execution derives from the cached plan with fresh per-execution state, and outputs equal a full independent re-plan for the same batch

#### Scenario: Recursive queries bypass the cache

- **WHEN** a `WITH RECURSIVE` query runs across batches
- **THEN** the physical plan is re-built every batch (its one-shot work-table state cannot be reset), and every batch returns the full recursion result

#### Scenario: Time-folding queries keep fresh timestamps

- **WHEN** a query selecting `now()` runs across batches with time passing between them
- **THEN** the second batch's timestamp advances rather than freezing at the first batch's plan time

### Requirement: The lazy scan source SHALL fail closed on execution-time schema mismatch

The execution-time scan source SHALL read the processor's current batch when its stream is produced and SHALL apply the planning-time projection to it; if the current batch's schema differs from the schema the plan was built for, the execution SHALL fail with an explicit error rather than producing misaligned output.

#### Scenario: Mismatched execution batch errors instead of misreading columns

- **WHEN** (defensively) the scan source opens with a batch whose schema differs from its planning-time schema
- **THEN** the execution returns an error naming the schema mismatch and no partial or column-shifted output is produced
