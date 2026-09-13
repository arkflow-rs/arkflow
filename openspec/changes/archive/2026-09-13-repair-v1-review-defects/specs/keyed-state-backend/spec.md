## MODIFIED Requirements

### Requirement: State journal version fences SHALL gate every replayed mutation

The state journal SHALL prevent a retried transaction from replaying a mutation whose staged snapshot predates a newer committed value, for every mutation kind that can overwrite state, including absolute writes and deletes. A mutation whose compensation was skipped because a later transaction owns the key SHALL NOT be replayed as a fresh effect either: a relative mutation (such as `Increment`) whose compensation was skipped SHALL be fenced on retry so the replay does not apply its delta a second time, either by marking the staged mutation as already applied or by failing the apply explicitly. A fenced mutation SHALL be treated as never applied for compensation and completion accounting, a mutation kind that cannot be partially skipped SHALL fail the whole apply with an explicit error, and the journal SHALL bound its pending transactions with a validated limit.

#### Scenario: Stale delete against a newer commit

- **WHEN** a transaction carrying a delete is compensated and later retried after another transaction committed the same key
- **THEN** the delete does not erase the newer committed value and the apply either skips the mutation or fails explicitly

#### Scenario: A skipped compensation does not double-count a retried increment

- **WHEN** transaction A applied an `Increment` to a key, its rollback skipped restoring the previous value because transaction B had already committed a later value that includes A's delta, and A is then restaged and retried
- **THEN** the retried `Increment` is fenced out as already applied (or fails explicitly), and the key's value remains B's committed value instead of counting A's delta twice

#### Scenario: A forward increment still composes in any order

- **WHEN** two uncompensated transactions increment the same key and are applied in either order
- **THEN** both deltas take effect exactly once without a version conflict

#### Scenario: Pending-bound exhaustion

- **WHEN** the number of simultaneously staged transactions reaches the configured `state.max_pending_transactions`
- **THEN** the journal rejects the new transaction with an error naming the bound and the configuration key, and a Job that leaves the bound unset keeps the documented default

#### Scenario: An invalid pending bound is rejected

- **WHEN** a Job declares `state.max_pending_transactions` as zero
- **THEN** Job validation rejects the spec before the Job starts instead of failing every journal begin at runtime
