## MODIFIED Requirements

### Requirement: Secret redaction

Configuration read APIs and diagnostic responses SHALL redact configured credential, token, password, and secret fields by default. Redaction SHALL additionally cover credentials embedded in URL-shaped string values: for a value of the form `scheme://user:password@host/...`, the password segment SHALL be replaced by the redaction marker while scheme, user, and host remain readable. URLs without an embedded userinfo component SHALL NOT be altered.

#### Scenario: Read sensitive configuration

- **WHEN** a client requests the current configuration
- **THEN** sensitive values are replaced by a redaction marker and are not returned in plaintext

#### Scenario: URL with embedded credentials is masked

- **WHEN** the configuration contains a field whose value is `postgres://admin:hunter2@db.internal:5432/vectors` and the field is returned by a read API
- **THEN** the response contains `postgres://admin:******@db.internal:5432/vectors` and the literal password never appears

#### Scenario: Plain URL passes through untouched

- **WHEN** the configuration contains a field whose value is `http://qdrant.internal:6333`
- **THEN** the value is returned unchanged

## ADDED Requirements

### Requirement: Version identifiers SHALL be validated

The configuration version store SHALL validate caller-supplied version identifiers before using them in filesystem paths: an identifier SHALL be rejected (not-found error class) when it is empty, longer than 128 characters, contains characters outside `[A-Za-z0-9._-]`, or contains a `..` sequence. Generated identifiers (timestamp-sequence) SHALL satisfy the same constraint.

#### Scenario: Traversal identifier is rejected

- **WHEN** a client requests a configuration diff or rollback with a version id containing a path separator or `..` (e.g. `../../secrets`)
- **THEN** the request fails with a not-found class error and no file outside the version store root is read or written

#### Scenario: Legitimate identifier resolves

- **WHEN** a client requests a previously stored version by its generated id (e.g. `1737500000000-0`)
- **THEN** the stored version is returned
