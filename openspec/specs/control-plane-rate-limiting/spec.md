# control-plane-rate-limiting Specification

## Purpose
TBD - created by archiving change add-control-plane-rate-limiting. Update Purpose after archive.
## Requirements
### Requirement: Request rate limiting on the versioned control API

Both control planes (local standalone and Hub) SHALL enforce a per-credential-identity request rate limit over every route under the versioned API prefix, implemented as an in-process token bucket that admits bursts up to a configured capacity and refills at a configured rate. Health probes, metrics export, and the OIDC login flow SHALL remain outside the limiter, matching the existing authentication exemption boundary. When rate limiting is disabled by configuration, request handling SHALL be byte-for-byte identical to the pre-change behavior.

#### Scenario: Burst within capacity is admitted

- **WHEN** a single credential issues a burst of at most the configured capacity requests to the versioned API after an idle period
- **THEN** every request is handled normally with no rate-limit response

#### Scenario: Disabled limiter preserves behavior

- **WHEN** rate limiting is disabled by configuration
- **THEN** no request receives a rate-limit rejection regardless of request rate

### Requirement: Credential-identity bucketing

The limiter SHALL key buckets on a truncated hash of the presented credential, never storing credential material: static and structured operator credentials are keyed per credential (one credential equals one principal), OIDC browser sessions are keyed per session, agent requests are keyed per node credential with the legacy query-token channel and the Bearer header resolving to the same bucket, and unauthenticated local mode shares a single anonymous bucket. The bucket table SHALL be capacity-bounded: when the bound is reached, unrecognized identities share one catch-all bucket governed by the principal rate, and idle buckets are reclaimed lazily on the request path without background tasks.

#### Scenario: Per-credential isolation

- **WHEN** credential A exhausts its bucket while credential B has capacity
- **THEN** requests authenticated with B are served while requests authenticated with A are rejected

#### Scenario: Agent channels share one bucket

- **WHEN** the same node authenticates once via the Bearer header and once via the legacy query-token channel within the same window
- **THEN** both requests draw from that node's single bucket

#### Scenario: Forged-credential flood degrades instead of growing memory

- **WHEN** requests present more distinct credentials than the bucket-table bound
- **THEN** the excess identities share the catch-all bucket and the process memory attributable to the limiter stays bounded

#### Scenario: Credentials are not disclosed by the limiter

- **WHEN** any request is evaluated by the limiter
- **THEN** no credential material appears in logs, responses, or stored limiter state

### Requirement: Rejection response is machine-actionable

A request exceeding its bucket SHALL be rejected with HTTP 429 before reaching any handler, with a problem body naming the rate-limit condition and a `Retry-After` header carrying the whole-second refill estimate. Rejections SHALL NOT create audit events or operation records. The limiter SHALL export a cumulative rejection counter per plane and key kind as a Prometheus metric family with a closed, low-cardinality label set.

#### Scenario: Over-limit request is rejected with retry guidance

- **WHEN** a request arrives and its bucket has no tokens
- **THEN** the response is 429 with a problem body naming the rate-limit condition and a `Retry-After` header, and no handler logic runs

#### Scenario: Rejections are observable

- **WHEN** rejections occur on either plane
- **THEN** the exported metric family increases with the plane and key kind labeled, without exposing credential-derived label values

### Requirement: Rate limit configuration

The control-plane server configuration SHALL carry a rate-limit section with an enable switch and per-identity-class rate and burst parameters, defaulted to enabled with limits that exceed legitimate deployment traffic by at least an order of magnitude (including the default Agent polling cadence). The section SHALL be additive under serde: existing configurations without it parse unchanged.

#### Scenario: Existing configuration parses unchanged

- **WHEN** a control-plane configuration predating this capability is loaded
- **THEN** the rate-limit section takes its defaults and startup succeeds

#### Scenario: Agent polling cadence is never throttled

- **WHEN** agents operate at the default polling interval with heartbeat, report, and observation traffic
- **THEN** no agent request is rejected by the default node rate

