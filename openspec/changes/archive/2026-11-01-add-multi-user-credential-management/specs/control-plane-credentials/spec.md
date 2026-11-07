# Delta Spec: control-plane-credentials

## ADDED Requirements

### Requirement: The Hub SHALL maintain a durable user directory

The Hub SHALL persist operator users as first-class records (`cp_users`) with an immutable user id, a display name, exactly one role from the existing role enumeration, a resource-scope set using the existing `type=id` grammar, an optional OIDC subject binding, a disabled flag, and a per-user token invalidation threshold. User administration SHALL be authorized by a dedicated `ManageUsers` action held by the admin role only. Creating, updating, disabling, enabling, and deleting users SHALL be supported; deleting a user SHALL revoke its credentials and sessions while leaving previously written audit records intact.

#### Scenario: Create and list users

- **WHEN** an admin creates a user with a valid id, role, and scopes and then lists users
- **THEN** the new user appears with the stored role and scopes and without credential material

#### Scenario: Non-admin cannot administer users

- **WHEN** a viewer- or operator-role principal calls any user administration route
- **THEN** the Hub returns 403, performs no mutation, and audits the denial

#### Scenario: Invalid user definition is rejected

- **WHEN** a user is created with a malformed id, an unknown role, or malformed scopes
- **THEN** the Hub returns 400 with a stable problem code and creates no user

#### Scenario: User ids are immutable

- **WHEN** an update attempts to change an existing user's id
- **THEN** the Hub rejects the update and the user record is unchanged

### Requirement: The directory SHALL preserve administrative reachability

The Hub SHALL refuse to disable, delete, or demote the last enabled admin user unless a static operator credential is configured as break-glass. User ids SHALL be unique and validated, OIDC subject bindings SHALL be unique, and the directory SHALL enforce configured bounds on user count.

#### Scenario: Last admin cannot be disabled

- **WHEN** an admin attempts to disable the only remaining enabled admin user while no static operator credential is configured
- **THEN** the Hub returns 409 with `last_admin_required` and the user remains enabled

#### Scenario: Duplicate OIDC subject is rejected

- **WHEN** a user update assigns an OIDC subject already bound to another user
- **THEN** the Hub returns 409 and neither user record changes

### Requirement: Credentials SHALL have a managed lifecycle

The Hub SHALL issue opaque operator credentials for a user on demand, returning the plaintext token exactly once in the creation or rotation response and storing only a hash thereafter. A credential SHALL carry an optional expiry, be listable by metadata only, be rotatable atomically (a new credential issued and the old one revoked in one storage mutation), and be revocable immediately. Revoked, expired, or unknown credentials SHALL fail authentication with 401 on their next use. Per-user credential count SHALL be bounded.

#### Scenario: Issue a credential

- **WHEN** an admin issues a credential for a user
- **THEN** the response carries the plaintext token and metadata, and subsequent list responses expose only metadata

#### Scenario: Rotate a credential

- **WHEN** an admin rotates a credential
- **THEN** the new token authenticates as that user while the old token is immediately rejected with 401

#### Scenario: Revoked credential stops working

- **WHEN** an admin revokes a credential and the holder retries with it
- **THEN** the Hub returns 401 and performs no control-plane mutation

#### Scenario: Credential limit is enforced

- **WHEN** issuing a credential would exceed the per-user credential bound
- **THEN** the Hub returns 409 with a stable limit problem code and issues nothing

### Requirement: Credential material SHALL never be disclosed

The Hub SHALL NOT store credential plaintext, log it, emit it in audit records, metrics labels, or list/detail responses, and SHALL compare presented secrets in constant time. Responses, logs, and audit messages SHALL expose only stable identifiers and scalar metadata.

#### Scenario: List responses carry no secrets

- **WHEN** any granted principal lists a user's credentials
- **THEN** the response contains no token plaintext, no secret hash, and no secret-derived value

#### Scenario: Audit records carry no secrets

- **WHEN** a credential is issued, rotated, or revoked
- **THEN** the audit record names the actor, action, user, and credential identifiers without token material

### Requirement: Authentication SHALL resolve principals in a fixed, fail-closed order

The Hub SHALL resolve operator credentials in a fixed order — static operator credential, managed credential, browser session (cookie channel), OIDC bearer token — preserving the existing precedence of static credentials over federation. A managed credential SHALL authenticate as its owning user's principal (role and scopes from the directory). Unknown, malformed, revoked, expired, or disabled-user credentials SHALL produce the same 401 response as an absent credential. When durable storage is configured and a lookup fails, that authentication source SHALL fail closed rather than granting access; on a volatile Hub without durable storage the managed-credential source SHALL be unavailable and all pre-existing authentication paths SHALL behave exactly as before.

#### Scenario: Static credential precedence is preserved

- **WHEN** a presented credential matches the configured static operator credential
- **THEN** it resolves through the static path and no managed or OIDC lookup runs

#### Scenario: Storage failure denies instead of granting

- **WHEN** a managed credential is presented while the identity store is unreachable
- **THEN** the request is rejected with 401 and no operator action is authorized

#### Scenario: Volatile Hub keeps existing behavior

- **WHEN** the Hub runs without durable storage and no managed credentials exist
- **THEN** static and OIDC authentication behave exactly as before this change

### Requirement: Sessions SHALL be durable and revocable

On a Hub with durable storage, the Hub SHALL persist browser sessions (`cp_sessions`) as random session identifiers with a principal snapshot, an optional owning user, and an expiry; the session cookie SHALL continue to carry only the opaque identifier with the existing attributes and lifetime. Sessions SHALL survive Hub restart and leadership change, and SHALL be revoked immediately when their owning user is disabled, deleted, or has tokens revoked. Sessions SHALL NOT be restored by control-plane backup/restore. On a Hub without durable storage the Hub MAY keep sessions in process memory, and the interface SHALL state that such sessions cannot be revoked.

#### Scenario: Session survives Hub restart

- **WHEN** a browser logs in and the Hub process restarts with the same durable storage
- **THEN** the existing session cookie still authenticates until its expiry

#### Scenario: Session revocation is immediate

- **WHEN** an admin disables a user or revokes that user's tokens
- **THEN** requests carrying the user's existing session cookie return 401 on their next use

#### Scenario: Restore does not resurrect sessions

- **WHEN** a control-plane snapshot is restored into an empty store
- **THEN** the user and credential records are present and no browser session is restored

### Requirement: User-level token revocation SHALL invalidate issued credentials and sessions

The Hub SHALL provide a user-level revocation operation that immediately invalidates that user's browser sessions and raises the user's token invalidation threshold, so that federated tokens issued before the revocation no longer authenticate. The operation SHALL be idempotent and audited.

#### Scenario: Revoke all tokens for a user

- **WHEN** an admin revokes a user's tokens
- **THEN** the user's sessions are deleted and previously issued federated tokens are rejected with 401, while other users are unaffected

#### Scenario: Revocation is audited and idempotent

- **WHEN** the same revoke-tokens request is repeated
- **THEN** the operation succeeds without error and a second audit record is written

### Requirement: Identity state SHALL be durable, fenced, and HA-consistent

Managed identity SHALL require durable storage: on a Hub without durable storage the identity administration API SHALL return a stable `identity_store_unavailable` error and SHALL NOT create in-memory users or credentials. Identity mutations SHALL be persisted through the fenced storage path so a stale leader cannot write. Identity state SHALL be read from the shared store on every authentication so revocation is effective immediately across leader changes, without a cache whose staleness bound is unspecified.

#### Scenario: Volatile Hub rejects identity administration

- **WHEN** an admin calls a user or credential route on a Hub configured without durable storage
- **THEN** the Hub returns 503 with `identity_store_unavailable` and creates nothing

#### Scenario: Revocation survives leadership change

- **WHEN** a credential is revoked on the leader and another Hub instance becomes leader against the same store
- **THEN** the revoked credential still fails with 401

#### Scenario: Stale leader cannot write identity

- **WHEN** a Hub that lost its lease attempts an identity mutation
- **THEN** the fenced storage path rejects the write and no identity record changes

### Requirement: Identity administration SHALL be audited and observable

Every accepted or rejected user, credential, and token-revocation mutation SHALL be audited with the real actor principal, action, target resource, correlation id, outcome, and a stable failure code where applicable, without credential material or secret configuration values. The Hub SHALL export a closed-label authentication outcome counter distinguishing static, managed, OIDC, and session sources. Session and credential expiry cleanup SHALL run under the existing maintenance sweep and remain bounded.

#### Scenario: Accepted mutation is attributed to its actor

- **WHEN** an admin creates a user
- **THEN** the audit record identifies that admin's principal id as the actor and the created user as the target

#### Scenario: Rejected mutation is audited

- **WHEN** an operator without `ManageUsers` attempts to issue a credential
- **THEN** the audit records the denial with the requesting principal, action, target user, and outcome rejected

#### Scenario: Authentication outcomes are measurable

- **WHEN** operator authentication succeeds or fails
- **THEN** the exported counter increases under a closed `source`/`outcome` label set without credential-derived label values

### Requirement: Identity administration SHALL preserve pre-existing behavior when unused

When no managed users or credentials exist, the Hub SHALL behave exactly as before this change: static and OIDC authentication, sessions, audit, and rate limiting are byte-identical, the additional administrative routes exist but require `ManageUsers`, and no additional storage reads occur on the static-credential path beyond the existing behavior. The startup guard for a secure Hub SHALL continue to require the configured static operator credential as bootstrap and break-glass.

#### Scenario: No managed identities configured

- **WHEN** a Hub is configured only with a static operator credential and no managed users exist
- **THEN** operator requests authenticate exactly as before and no identity-table read occurs for static credentials

#### Scenario: Secure startup guard is unchanged

- **WHEN** a secure Hub is configured without a static operator credential
- **THEN** startup still fails with the existing missing-credential error even if the identity tables contain admins
