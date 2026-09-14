## MODIFIED Requirements

### Requirement: Legacy behavior equivalence

Compiled streams SHALL preserve the legacy runtime's observable behavior:
at-least-once delivery, per-edge ordering, WAL recovery, error routing, and
processing-time window batching cadence. Duration fields that the legacy runtime
parsed SHALL keep accepting the legacy grammar — microsecond, nanosecond, and
compound forms such as `1h30m` — and a value outside every accepted grammar SHALL
fail compilation with an error that names the accepted grammar.

#### Scenario: Example regression

- **WHEN** every runnable example under `examples/` runs under the unified kernel
- **THEN** sink outputs are equivalent to the legacy Stream runtime for the same inputs

#### Scenario: Legacy duration strings still compile

- **WHEN** a migrated Stream config declares a duration as `500us`, `1h30m`, or another form the legacy `humantime` parser accepted
- **THEN** compilation parses it to the same millisecond value the legacy runtime used instead of rejecting it as unsupported
