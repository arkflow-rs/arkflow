# node-identity Specification

## Purpose

Durable per-node credentials (`cp_node_credentials`), strict node-id binding, bootstrap-only shared token enrollment, managed rotation/revocation, and Agent credential retention for Hub-managed compute nodes.

## Requirements

### Requirement: Durable per-node credentials SHALL bind strictly to their owning node

On a Hub with durable storage, each compute-node credential (`cp_node_credentials`, `arkn_<credential_id>_<secret>`) SHALL be bound to exactly one `node_id` and persisted as a non-reversible verifier (`SHA-256(credential_id ‖ 0x00 ‖ secret)`) compared in constant time. Presenting node A's credential when registering or operating as node B MUST fail closed with `401 Unauthorized` and audit the cross-node impersonation attempt without updating node B's session or state. Revoked, expired, or unknown credentials MUST fail with `401 Unauthorized`. Active (unrevoked) credentials per node SHALL be bounded (`MAX_CREDENTIALS_PER_NODE = 8`).

#### Scenario: Cross-node impersonation with another node's credential is rejected

- **WHEN** a caller presents node A's `arkn_` credential in a registration request with `node_id = "node-b"`
- **THEN** the Hub rejects registration with `401 Unauthorized`, leaves node B's live session untouched, and records a `node_identity_mismatch` audit event

#### Scenario: Node registers with its own pre-provisioned credential

- **WHEN** an operator issues a per-node credential for `"node-a"` and `"node-a"` registers with that token
- **THEN** the Hub authenticates `"node-a"` under that `credential_id` and issues a session token

#### Scenario: Revoked or expired per-node credential is rejected

- **WHEN** an operator revokes a node credential (or its `expires_at_ms` passes) and the node attempts to register with it
- **THEN** the Hub rejects registration with `401 Unauthorized`

### Requirement: Shared node_token SHALL be limited to bootstrap enrollment and explicit break-glass

When durable storage is configured, the fleet-shared `node_token` SHALL authenticate registration for a `node_id` ONLY when that `node_id` has no prior per-node credentials recorded in `cp_node_credentials` (including revoked or expired historical credentials), except for a single same-boot unconfirmed bootstrap retry (`boot_id` matching an active, unrevoked, unexpired `"bootstrap"` registration that has not yet completed any post-register authenticated call or `arkn_` registration) or when `allow_shared_node_token_break_glass` is explicitly enabled. On successful initial bootstrap enrollment (when `node_bootstrap_disabled` is false), the Hub SHALL mint and persist a new per-node `arkn_` credential (`label = "bootstrap"`) and return its plaintext once in `RegisterResponse.node_credential`. Once a `node_id` has any recorded per-node credential (other than the same-boot unconfirmed retry path above), subsequent registrations presenting the shared `node_token` for that `node_id` MUST fail with `401 Unauthorized` (`bootstrap_blocked_enrolled_node`) — revoking a node's last active credential MUST NOT re-open shared `node_token` enrollment. Volatile Hubs without durable storage SHALL preserve pre-existing shared-token behavior.

#### Scenario: First-time node bootstrap mints a per-node credential

- **WHEN** `"node-a"` has no recorded per-node credentials and registers against a durable Hub with the shared bootstrap `node_token`
- **THEN** the Hub stores a hashed per-node credential for `"node-a"`, returns the plaintext once in `RegisterResponse.node_credential`, and subsequent re-registrations by `"node-a"` succeed using that `arkn_` credential

#### Scenario: Shared node_token cannot re-enroll an already-enrolled or revoked node

- **WHEN** `"node-a"` has any recorded per-node credential (even after its last active credential is revoked) and a caller registers with `node_id = "node-a"` using the shared `node_token` (with break-glass disabled and outside a same-boot unconfirmed bootstrap retry)
- **THEN** the Hub rejects the request with `401 Unauthorized` and audits `bootstrap_blocked_enrolled_node`

#### Scenario: Break-glass override allows shared node_token when explicitly enabled

- **WHEN** `allow_shared_node_token_break_glass` is enabled and `"node-a"` registers with the shared `node_token`
- **THEN** the Hub permits registration for recovery and records `auth_mode=break_glass` in the audit trail

#### Scenario: Disabled bootstrap rejects un-enrolled shared token registrations

- **WHEN** `node_bootstrap_disabled` is enabled and an un-enrolled `"node-c"` registers with the shared `node_token`
- **THEN** the Hub rejects registration with `401 Unauthorized` (`bootstrap_disabled`)

### Requirement: Operators SHALL manage node credentials through scoped routes with immediate revocation

The Hub SHALL expose `GET /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/node-credentials/{credential_id}/rotate`, and `DELETE /api/v1/node-credentials/{credential_id}` gated by `OperatorAction::ManageNodes` on resource `("node", Some(node_id))`. Listing responses SHALL return metadata only (never plaintext tokens or hashes). Issuance and rotation responses SHALL return the plaintext token exactly once; rotation SHALL revoke the old credential and insert the new credential atomically in a single storage transaction. Revoking or rotating a credential SHALL immediately invalidate any active in-memory Hub session (`NodeRecord`) that authenticated with that `credential_id`. On a volatile Hub without durable storage, node-credential routes SHALL fail with `503 identity_store_unavailable`.

#### Scenario: Node-scoped operator administers only its permitted node

- **WHEN** an operator scoped to `node=node-a` issues or lists credentials for `"node-a"`, then attempts to list or revoke credentials for `"node-b"`
- **THEN** `"node-a"` succeeds and `"node-b"` fails with `403 Forbidden`

#### Scenario: Revocation terminates an active node session immediately

- **WHEN** `"node-a"` holds an active session authenticated via `cred-1` and an operator revokes (or rotates) `cred-1`
- **THEN** `"node-a"`'s next `heartbeat`, `report`, `commands`, `command_result`, or `job_observation` call using the old session token fails with `401 Unauthorized`

#### Scenario: Credential metadata and audit never disclose secrets

- **WHEN** an operator lists node credentials after issuing or rotating one
- **THEN** no response field, log, or audit record contains the token plaintext or `secret_hash`

### Requirement: Agents SHALL retain enrolled per-node credentials across re-registrations and optional disk persistence

When an Agent receives `RegisterResponse.node_credential`, it SHALL replace its in-memory registration token with that per-node credential for subsequent re-registrations in the same process. When `node_credential_file` (`ARKFLOW_NODE_CREDENTIAL_FILE` or `health_check.node_credential_file`) is configured, the Agent SHALL load an existing non-empty credential from that path at startup in preference to the bootstrap `node_token` and SHALL atomically persist (`0600` on Unix) any newly received `node_credential`.

#### Scenario: Agent persists and reuses enrolled credential across restart

- **WHEN** an Agent configured with `node_credential_file` enrolls with a bootstrap `node_token`, receives `node_credential`, and then restarts with the same `node_id` and `node_credential_file`
- **THEN** the restarted Agent presents the persisted `arkn_` credential and re-registers successfully even while the Hub blocks shared-token reuse for enrolled nodes
