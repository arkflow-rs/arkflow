## MODIFIED Requirements

### Requirement: The directory SHALL preserve administrative reachability

The Hub SHALL refuse to disable, delete, or demote the last enabled admin user unless a static operator credential is configured as break-glass. User ids SHALL be unique and validated, OIDC subject bindings SHALL be unique, and the directory SHALL enforce configured bounds on user count. User creation SHALL be serialized against concurrent directory writes and persisted with insert-only semantics (never overwriting an existing user on conflict); attempting to create a user whose id already exists SHALL fail with `409 Conflict` (`user_already_exists`) and leave the existing user unchanged.

#### Scenario: Last admin cannot be disabled

- **WHEN** an admin attempts to disable the only remaining enabled admin user while no static operator credential is configured
- **THEN** the Hub returns 409 with `last_admin_required` and the user remains enabled

#### Scenario: Duplicate OIDC subject is rejected

- **WHEN** a user update assigns an OIDC subject already bound to another user
- **THEN** the Hub returns 409 and neither user record changes

#### Scenario: Concurrent duplicate user creation does not overwrite

- **WHEN** two concurrent requests attempt to create the same `user_id`
- **THEN** exactly one succeeds with `201 Created`, the other fails with `409 Conflict` (`user_already_exists`), and the created user record is not overwritten

### Requirement: Sessions SHALL be durable and revocable

On a Hub with durable storage, the Hub SHALL persist browser sessions (`cp_sessions`) as random session identifiers with a principal snapshot, an optional owning user, and an expiry; the session cookie SHALL continue to carry only the opaque identifier with the existing attributes and lifetime. Sessions SHALL survive Hub restart and leadership change, and SHALL be revoked immediately and atomically inside the same storage transaction as the user write when their owning user is disabled, deleted, or has authorization or OIDC binding changed, and immediately when the user has tokens revoked. When an OIDC browser session was created before its subject was bound to a directory user (`user_id` unset), binding that subject to a directory user (`create_user` or `save_user`), changing or clearing the binding, disabling or deleting that user, or revoking that user's tokens SHALL immediately revoke all matching unbound sessions for that OIDC subject (atomically inside the user-write transaction for `create_user`, `save_user`, and `delete_user`); additionally, session resolution SHALL fail closed for any unbound session whose subject is now bound to a directory user or when `managed_users_only` is enabled. An unbound OIDC subject whose `sub` string matches an unrelated local directory `user_id` SHALL NOT be mis-associated with that local user. Sessions SHALL NOT be restored by control-plane backup/restore. On a Hub without durable storage the Hub MAY keep sessions in process memory, and the interface SHALL state that such sessions cannot be revoked.

#### Scenario: Session survives Hub restart

- **WHEN** a browser logs in and the Hub process restarts with the same durable storage
- **THEN** the existing session cookie still authenticates until its expiry

#### Scenario: Session revocation is immediate

- **WHEN** an admin disables a user or revokes that user's tokens
- **THEN** requests carrying the user's existing session cookie return 401 on their next use

#### Scenario: Unbound OIDC session is revoked when subject is bound or disabled

- **WHEN** an unbound OIDC subject holds an active browser session and an admin binds that subject to a directory user (or disables/deletes that user or revokes its tokens)
- **THEN** requests carrying the old session cookie immediately return 401 on their next use

#### Scenario: Restore does not resurrect sessions

- **WHEN** a control-plane snapshot is restored into an empty store
- **THEN** the user and credential records are present and no browser session is restored
