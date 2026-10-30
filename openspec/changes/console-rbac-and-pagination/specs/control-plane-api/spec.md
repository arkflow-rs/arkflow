# Delta Spec: control-plane-api

## ADDED Requirements

### Requirement: System identity echoes the authenticated principal

The `/system` endpoint on both the Hub plane and the local plane SHALL include an optional `principal` object `{ id, roles, actions }` describing the principal authenticated for the current request, where `actions` lists every operator action the principal is authorized for under the same predicate that enforces mutations. Unauthenticated requests SHALL echo `principal: null`. The field SHALL be additive: consumers that do not know it MUST be able to ignore it without behavior change.

#### Scenario: Structured credential session

- **WHEN** a request authenticates with a structured operator credential holding the viewer role
- **THEN** `/system` echoes that credential's principal id, the viewer role, and exactly the actions the viewer role authorizes

#### Scenario: Unauthenticated probe

- **WHEN** `/system` is requested without valid credentials on a plane that requires them
- **THEN** the response echoes `principal: null` and the rest of the system identity is unchanged

#### Scenario: Echo and enforcement cannot diverge

- **WHEN** an echoed `actions` list includes an action
- **THEN** authorizing a mutation that requires that action uses the same principal predicate, so the echo is never stale relative to enforcement for the same request's principal
