---
sidebar_position: 8
title: Users and credentials
description: Manage the Hub user directory, issue and rotate bearer credentials, bind OIDC subjects, and revoke sessions.
---

# Users and credentials

The Hub can hold its own **user directory**: named users with a role and
resource scopes, opaque bearer **credentials** issued per user, and durable
browser **sessions**. This is the storage-backed middle layer between the
single static operator credential and full OIDC federation — it lets you give
each human or automation its own revocable identity without every caller
sharing `ARKFLOW_OPERATOR_TOKEN`.

The directory is a Hub-mode feature and requires durable storage
(`ARKFLOW_HUB_STORAGE`). A volatile Hub has nowhere to keep users, so the whole
management API answers `503 identity_store_unavailable` and managed
credentials never authenticate. Unconfigured deployments keep working exactly
as before: the static operator credential is still the priority path.

## Concepts

| Concept | Meaning |
|---------|---------|
| **User** | A stable identity `user_id` with a single `role` (`admin`, `operator`, or `viewer`), optional resource `scopes`, a `display_name`, an optional bound OIDC subject, and a `disabled` flag. |
| **Credential** | An opaque bearer token issued to one user. The Hub stores only a hash; the plaintext is shown exactly once at issuance or rotation. A credential can carry a `label` and an optional expiry. |
| **Session** | A browser login created by the OIDC authorization-code flow. When the Hub has durable storage the session is persisted (`cp_sessions`), so it survives a Hub restart and can be revoked centrally. |
| **Scope** | `{resource_type, resource_id?}` pairs that narrow what a user may act on. Roles are global; scopes refine them. |
| **Revocation threshold** | `tokens_valid_after_ms` on the user. Federated tokens whose `iat` predates it are rejected — this is the JWT revocation mechanism. |

The user id is part of URLs and audit records: 1–64 characters of lowercase
letters, digits, `.`, `_`, `-`, starting with a lowercase letter or digit.
`display_name` and credential `label` are limited to 128 characters. A user
holds exactly one role (there is no role set per user); `admin` is the only
role that holds the `manage_users` action, so only an admin can administer the
directory.

The directory is bounded: at most **1024 users**, and at most **16 active
credentials per user** (revoked credentials no longer count). Each OIDC subject
can be bound to at most one user.

## Authentication resolution order

When a request presents a bearer credential, the Hub resolves it in this
order:

1. **Static operator credential** (`ARKFLOW_OPERATOR_TOKEN`) — the
   compatibility/break-glass path. A match wins immediately; a non-match falls
   through to the next layer (although a malformed structured credential never
   matches).
2. **Managed credential** — any token beginning with `ark_` is parsed as
   `ark_<credential_id>_<secret>` and resolved against the user directory.
   Managed tokens **never fall back** to OIDC: an unknown, revoked, expired,
   disabled-user, or storage-unreachable lookup is a hard `401`. The Hub stores
   `SHA-256(credential_id ‖ 0x00 ‖ secret)` and compares in constant time.
3. **Browser session** — the `arkflow_session` cookie resolves to a persisted
   session (or the in-memory map on a volatile Hub); revoked, expired,
   disabled-user, and deleted-user sessions are rejected.
4. **OIDC JWT** — a validated bearer JWT from the configured identity
   provider, then mapped through the managed directory rules described below.

Every identity lookup failure mode is **fail-closed**: a storage error during
credential resolution denies the request instead of skipping authentication.

## Bootstrap and break-glass

There is no built-in first user. Bootstrap with either of these paths:

- **Static operator credential (recommended).** Start the Hub with
  `ARKFLOW_OPERATOR_TOKEN` set, then use it to create the first admin user and
  issue that user a credential:

  ```bash
  export HUB=https://hub.example.com
  export BOOTSTRAP="$ARKFLOW_OPERATOR_TOKEN"

  curl -fsS -X POST "$HUB/api/v1/users" \
    -H "Authorization: Bearer $BOOTSTRAP" -H 'Content-Type: application/json' \
    -d '{"user_id":"alice","display_name":"Alice","role":"admin","scopes":[]}'

  curl -fsS -X POST "$HUB/api/v1/users/alice/credentials" \
    -H "Authorization: Bearer $BOOTSTRAP" -H 'Content-Type: application/json' \
    -d '{"label":"alice-laptop"}'
  ```

  Keep the static credential configured as a **break-glass** credential:
  while it exists, the "last enabled admin" invariant is waived, so you can
  still recover from a mis-edit that would otherwise lock every directory
  admin out.

- **OIDC bootstrap.** With no static credential, a federated subject that is
  **not yet bound** to a directory user keeps its claim-mapped role (see
  below). An `admin` subject can therefore create the first users and bind
  them; from then on the directory is authoritative for those subjects.

If you remove the static credential, the last-admin invariant becomes
enforced: the Hub rejects disabling, deleting, or demoting the only remaining
enabled admin with `409 last_admin_required`. There is no other way back in
except storage-level recovery — so either keep the static credential, or
guarantee that at least one enabled admin user always exists.

On a **volatile Hub** the management API is unavailable
(`identity_store_unavailable`); use the static operator credential until the
Hub has durable storage.

## Issuing, rotating, and revoking credentials

Credentials are managed per user; the plaintext token is returned **only** in
the issuance/rotation response and is never readable again.

| Operation | Route | Behavior |
|-----------|-------|----------|
| List metadata | `GET /api/v1/users/{id}/credentials` | `credential_id`, `label`, timestamps, expiry, revocation — never the token or hash. Answers `404 user_not_found` when the user does not exist. |
| Issue | `POST /api/v1/users/{id}/credentials` | `201` with `{credential, token}`. Optional `label` and `expires_at_ms`. |
| Rotate | `POST /api/v1/credentials/{credential_id}/rotate` | `200` with a fresh `{credential, token}`; the old credential is revoked in the same storage mutation. Rotation of an already-revoked credential is rejected. |
| Revoke | `DELETE /api/v1/credentials/{credential_id}` | Idempotent. Answers `200 {"revoked": true\|false}`; a second call keeps the original revocation timestamp. |

Use the token as `Authorization: Bearer <token>`, exactly like the static
credential. Revocation takes effect immediately: the next request with that
token is `401`. `last_used_at_ms` is `null` on this release — usage timestamps
are reserved in the response shape but not yet recorded.

Recommendations:

- Issue one credential per client/host so revoking one does not lock out the
  others; label it with where it lives.
- Rotate on a schedule and immediately when a token may have leaked. Rotation
  revokes the old credential and creates the new one in a single storage
  mutation, so the old token stops working at once.
- Setting `expires_at_ms` bounds exposure for short-lived automation; an
  expired credential behaves like a revoked one.
- Revoking a credential does **not** revoke the user's other credentials.

## OIDC binding and revocation

With OIDC configured (see
[OIDC JWT federation](./deploy.md#oidc-jwt-federation)) a user can bind a
directory login to an identity-provider subject by setting `oidc_subject` on
the user record (it is unique across the directory; an empty string on a
`PATCH` clears the binding). Once bound, the directory is authoritative:

- The **role and scopes come from the directory**, not from the token's
  claims. Editing the user changes the next request's authorization
  immediately. Changing a user's role or scopes also revokes that user's
  durable browser sessions (a stored session carries the principal it was
  created with), so the next request re-authenticates against the updated
  record.
- A **disabled** bound user is rejected with `401` even with a valid JWT.
- The token's `iat` is compared with `tokens_valid_after_ms`; a token minted
  before the threshold (with a 60-second clock-skew tolerance) is rejected.
  This is how `POST /api/v1/users/{id}/revoke-tokens` invalidates already
  issued JWTs without waiting for expiry.
- A bound user whose token carries **no `iat`** is rejected — without an
  issue time the Hub cannot evaluate revocation, so it fails closed.

Unbound OIDC subjects keep the plain claims mapping (role claim and scopes
claim), so the directory is opt-in. Set
`ARKFLOW_OIDC_MANAGED_USERS_ONLY=true` to require every federated subject to
be bound to a directory user; an unbound subject then gets `401`. The default
is `false`, preserving claim-mapped behavior.

`POST /api/v1/users/{id}/revoke-tokens` does two things: it raises
`tokens_valid_after_ms` to now (invalidating older federated tokens) and
deletes every stored browser session for that user. It returns
`{"revoked_sessions": n}`. It does **not** revoke managed credentials — those
are individually revocable and rotatable, so revoke or rotate them
individually.

Disabling a user, deleting a user, and `revoke-tokens` all take effect
immediately: sessions resolve through the directory on every request, so there
is no cache to flush. Deleting a user also deletes its credentials and
sessions.

## Browser session persistence

The OIDC login flow creates an 8-hour server-side session cookie
(`arkflow_session`). When the Hub has durable storage the session row is
persisted, which changes the operational story from earlier releases:

- **Restart survival**: a Hub restart no longer signs everyone out; the cookie
  keeps working against the same store.
- **Central revocation**: logout deletes the session row; disabling/deleting
  the user or calling `revoke-tokens` invalidates the user's sessions on their
  next request.
- **Maintenance**: the Hub's periodic maintenance task prunes expired and
  revoked session rows, so the table stays bounded.
- **HA**: multiple Hub instances sharing one database share the session table;
  a session created on the leader resolves on any instance that can serve
  operator traffic (a standby still answers `503 hub_standby`).
- **Backup/restore deliberately excludes `cp_sessions`**: a session id is a
  live bearer secret, and resurrecting logins after a disaster recovery is a
  security anti-pattern. After a restore, users sign in again.

On a volatile Hub (no durable storage) sessions stay in memory exactly as
before: restarting the Hub signs everyone out.

## HA, backup, and restore

- **HA**: the directory lives in the shared control-plane database, so every
  Hub instance in a lease-based HA deployment sees the same users,
  credentials, and sessions. Identity mutations are fenced like every other
  control-plane write (a stale leader gets `503 stale_leader`).
- **Backup**: `cp_users` and `cp_credentials` are part of the backup/migrate/
  restore table order, so a disaster-recovery snapshot preserves users,
  roles, scopes, OIDC bindings, revocation thresholds, and credential hashes.
  `cp_sessions` is intentionally excluded (see above). Snapshot files contain
  no credential plaintext (only hashes) but do contain user metadata and audit
  history — handle them with the same care as the database.
- **Schema**: managed identity is storage schema **v5**. Opening an older
  store creates the identity tables and stamps the new version automatically;
  a database stamped by a newer binary is refused at startup.

## Audit trail

Identity mutations are audited to the Hub audit log with scalar metadata
only — never token material or secret hashes. Accepted mutations produce
these records:

| Action | When |
|--------|------|
| `user.create` | A user is created. |
| `user.update` | `PATCH /users/{id}` changes a user. |
| `user.delete` | A user is deleted. |
| `user.revoke_tokens` | Federated tokens and sessions are revoked. |
| `credential.create` | A credential is issued. |
| `credential.rotate` | A credential is rotated (the new credential id is recorded). |
| `credential.revoke` | A credential is revoked. |

Rejected mutations are audited too: the same action is written with
`outcome=rejected`, the real actor, the target user/credential, and the stable
problem code as `failure_code` (for example `last_admin_required`,
`credential_limit`, `user_not_found`, `oidc_subject_conflict`). A mutation that
fails before reaching the identity store — validation errors such as an
invalid user id or an unknown role — is also recorded as rejected. Authorization
denials (missing credential, or a principal without `manage_users`) are
audited by the generic authorization rejection path instead. Query the trail
at `GET /api/v1/audit`.

## Error codes

| Status | Code | When |
|--------|------|------|
| `400` | `invalid_user` | Malformed user id, unknown role, oversized display name/label, unparsable scopes, or an empty `PATCH`. |
| `400` | `invalid_credential` | Rotating an already-revoked credential. |
| `401` | `unauthorized` | No valid credential. |
| `403` | `forbidden` | Authenticated, but the principal lacks `manage_users` (only admin holds it). |
| `404` | `user_not_found` / `credential_not_found` | Unknown user or credential — including a credentials listing for a user that does not exist. |
| `409` | `last_admin_required` | The last enabled admin would be disabled, deleted, or demoted while no static operator credential exists. |
| `409` | `user_limit` | 1024 users already exist. |
| `409` | `credential_limit` | The user already has 16 active credentials. |
| `409` | `oidc_subject_conflict` | The OIDC subject is bound to another user. |
| `503` | `identity_store_unavailable` | The Hub has no durable storage. |

The full route table, request/response shapes, and the `manage_users` action
are in the
[HTTP API reference](../../reference/api.md#users-and-credentials).

## Console

The web console exposes the same surface on its standalone **Users** page
(`/users`), listed in the navigation only for principals with `manage_users`
in Hub mode: list/create/edit/disable/delete users, inspect credential
metadata, issue a credential with a one-time plaintext reveal, rotate, revoke,
and revoke all of a user's tokens. Runtime bearer credentials are entered on
the **Settings** page, and the console shell prompts for a token when the API
answers `401` and no OIDC login flow is configured — see
[web console](./console.md#users-and-credentials) for the interaction details
and the token storage boundary.

## Sensitivity and operational notes

- **Plaintext is one-shot.** Treat an issued token like a password: copy it
  into the target's secret store at once. The Hub cannot show it again; if it
  is lost, rotate.
- **Only hashes are stored.** The credential hash is
  `SHA-256(credential_id ‖ 0x00 ‖ secret)`; a stolen database contains no
  usable bearer token. Session rows store a hash of the cookie value, not the
  cookie.
- **TLS matters more than before.** Bearer credentials cross the wire on every
  request; terminate TLS at the Hub or a trusted proxy, and do not put
  credentials in query parameters.
- **Fail-closed identity.** A storage outage during authentication denies
  requests rather than silently dropping to an unauthenticated or
  static-token-only mode.
- **Static token hygiene.** If you keep `ARKFLOW_OPERATOR_TOKEN` as a
  break-glass credential, store it in a secret manager and rotate it after
  using it for recovery; it is all-powerful and bypasses the directory.
- **Bootstrap is the risky window.** Create the first admin, verify login with
  its managed credential, and only then decide whether to keep or remove the
  static credential.

## Related pages

- [Control plane overview](./overview.md) — Hub architecture and authentication
  layers.
- [Deployment](./deploy.md) — OIDC configuration, storage, backup/restore.
- [HTTP API reference](../../reference/api.md#users-and-credentials) — routes
  and problem codes.
- [Web console](./console.md) — the administrative UI.
