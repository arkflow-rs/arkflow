## MODIFIED Requirements

### Requirement: Failure classification and retry
The Hub SHALL distinguish rejected requests, transient failures, unavailable nodes, permanent execution failures, ambiguous results, and superseded commands, and SHALL apply bounded retry with recorded attempt count and next retry time to retryable failures. An expired command lease is a retryable or ambiguous attempt, not a silently discarded operation. A retryable failure SHALL always converge to a fresh queued command after its backoff expires, even when the prior operation record for the intent is terminal.

#### Scenario: Temporary node failure
- **WHEN** a command fails because the node or transport is temporarily unavailable
- **THEN** the desired state remains unchanged, the Intent enters retrying or degraded, and the Hub schedules a backoff retry

#### Scenario: Ambiguous command result
- **WHEN** the Agent disconnects after receiving a command but before reporting its result
- **THEN** the Hub marks the Attempt outcome ambiguous or expired, requests a fresh report, and does not retry until the observed state is evaluated

#### Scenario: A retried intent produces a fresh command
- **WHEN** the retry backoff of an intent whose last attempt failed transiently expires and reconciliation runs
- **THEN** the Hub replaces the terminal operation record with a fresh queued operation carrying the new attempt's command id, and the next command poll delivers that command

### Requirement: Job operation state SHALL be aggregated by assignment
The Hub SHALL persist or derive one operation result per expected assignment and SHALL calculate the Job-level observed state using all results for the current generation/action. Terminal Job states SHALL not be written until the aggregate has enough information to distinguish success, retryable degradation, and permanent failure. The observation write SHALL be a compare-and-set on the generation the caller read: a report whose generation no longer matches SHALL be ignored without regressing the Job's generation, desired state, or convergence.

#### Scenario: All assignments succeed
- **WHEN** every expected assignment reaches `running` or `succeeded` for a start operation
- **THEN** the Job-level observed state becomes `running`

#### Scenario: A permanent failure is confirmed
- **WHEN** the complete assignment set has been evaluated and an assignment reports a non-retryable execution failure
- **THEN** the Job-level state becomes failed/blocked according to the operation policy and retains the assignment failure details

#### Scenario: A stale observation cannot regress a newer generation
- **WHEN** a desired-state change bumps the Job generation between the observation's read and write, and a report captured at the older generation is then applied
- **THEN** the storage rejects the write with a generation conflict, the Hub returns the current record, and the newer generation with its desired state survives
