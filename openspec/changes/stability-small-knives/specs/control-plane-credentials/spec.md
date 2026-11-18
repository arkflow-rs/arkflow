## MODIFIED Requirements

### Requirement: Credential material SHALL never be disclosed

The Hub SHALL NOT store reversible credential-directory material (user token plaintext and other credential secrets), log it, emit it in audit records, metrics labels, or list/detail responses, and SHALL compare presented secrets in constant time. Storing a non-reversible verifier (the SHA-256 value over the credential id and secret) is required by the constant-time verification design and is not a disclosure; what the prohibition forbids is anything the verifier could be inverted or replayed from. This storage prohibition is about the credential directory's own material; it does not forbid a Job specification from carrying connector credentials inside its `spec_json`, which is persisted verbatim by design and disclosed only through the redacted read/mutation paths below. With that one-time issuance exception (the plaintext token the issuer receives exactly once, per the managed-lifecycle requirement), responses, logs, and audit messages SHALL expose only stable identifiers and scalar metadata. Job-record mutation responses (create, update, upgrade, rollback, restore, recovery, trigger, desired-state, and lifecycle actions) SHALL pass the returned record through the same redaction applied to read paths, so a caller cannot read back credential material it just wrote.

#### Scenario: List responses carry no secrets

- **WHEN** any granted principal lists a user's credentials
- **THEN** the response contains no token plaintext, no secret hash, and no secret-derived value

#### Scenario: Audit records carry no secrets

- **WHEN** a credential is issued, rotated, or revoked
- **THEN** the audit record names the actor, action, user, and credential identifiers without token material

#### Scenario: Job mutation responses carry no secrets

- **WHEN** a mutation that returns a Job record (for example create, upgrade, rollback, or desired-state change) contains credential-looking fields in its specification
- **THEN** the response carries the same redacted values the corresponding GET detail would return, and no plaintext secret appears in the response body
