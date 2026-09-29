# distributed-job-runtime Delta — console-hub-alignment

## ADDED Requirements

### Requirement: Job detail reflects observed task attempt state

The Job detail task listing SHALL report the observed state of each task attempt as last reported by the executing compute node, and SHALL distinguish observed state from desired placement. When no observation exists for a task (for example before first dispatch), the listing SHALL fall back to the desired placement state and mark the entry as not observed. The listing SHALL NOT present desired placement state as if it were observed runtime state.

#### Scenario: Running task shows its observed state

- **WHEN** a compute node reports a task attempt as running and an operator opens the Job detail
- **THEN** the task entry shows the observed running state and is marked observed

#### Scenario: Undispatched task falls back to placement

- **WHEN** a Job detail is requested for a task that has never been reported by any node
- **THEN** the task entry carries the desired placement state and is marked not observed
