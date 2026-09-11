## MODIFIED Requirements

### Requirement: Composite acknowledgement propagates errors
Composite acknowledgements SHALL return `Err` if any constituent acknowledgement fails, SHALL not report success for a partially committed logical source delivery, and SHALL compensate already-successful durable constituents before returning a sibling failure whenever the constituent supports compensation. Retries SHALL remain possible after a transient failure.

#### Scenario: One constituent fails
- **WHEN** a composite acknowledgement acks multiple constituents and one returns `Err`
- **THEN** the composite acknowledgement returns `Err`, compensates successful durable constituents, and leaves the logical delivery retryable

#### Scenario: A retry follows a failed fan-out acknowledgement
- **WHEN** a fan-out acknowledgement is retried after a transient wrapped acknowledgement failure
- **THEN** the staged state transaction and every child acknowledgement are still available for the retry

#### Scenario: No-op acknowledgement succeeds
- **WHEN** a `NoopAck` is acked
- **THEN** it returns `Ok(())` without side effect
