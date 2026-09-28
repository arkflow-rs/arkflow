---
sidebar_position: 2
---

# Control plane deployment

Run `cargo run -p arkflow-server --bin arkflow-server` as the Hub and start each compute node
with `health_check.hub_url`, `node_id`, and `node_token`. Then build the console with
`cd console && npm ci && npm run build`, and serve
`console/dist` from a protected reverse proxy. The development Vite server
proxies `/api` and `/metrics` to `127.0.0.1:8080`; production should preserve
the same-origin paths and set `VITE_API_BASE` only when the API prefix differs.
Set `VITE_API_TOKEN` only in a controlled build environment when the ArkFlow
listener has an operator credential configured. The compatibility credential
may be a raw token (admin) or `principal|role|secret`, for example
`readonly|viewer|viewer-secret`; viewer credentials can read resources and
audit history but cannot mutate Streams, nodes, or rollouts.

### Storage backends

The Hub persists its control-plane state (nodes, jobs, intents, operations,
audit history) in SQLite by default. `ARKFLOW_HUB_STORAGE` selects the backend
by scheme:

- A filesystem path (or unset) opens the SQLite store — WAL journal,
  `synchronous=NORMAL`, `busy_timeout=5s` — with zero additional setup.
- A URL starting with `postgres://` or `postgresql://` opens the PostgreSQL
  backend: a sqlx pool (8 connections, 5s acquire timeout) that probes
  connectivity and applies the idempotent `cp_*` DDL at startup, so an empty
  database converges on first boot. A unreachable database fails Hub startup
  rather than surfacing on the first command.

Both backends implement the same storage contract behind one FIFO actor, so
reconciliation, rollout, and outbox ordering semantics are backend-independent.
PostgreSQL is also the prerequisite for the Hub HA lease election described
next.

To move an existing SQLite deployment onto PostgreSQL, stop the Hub first
(migration requires the source to be quiescent), then run:

```bash
arkflow-server migrate --from sqlite:/var/lib/arkflow/hub.sqlite \
                       --to postgres://user:pass@db/hub
```

The tool copies every `cp_*` table in foreign-key order in 1000-row
transactions, resets identity sequences above the migrated max ids, and exits
non-zero on any row-count mismatch. Point `ARKFLOW_HUB_STORAGE` at the
PostgreSQL URL and restart the Hub only after a successful migration. Fresh
PostgreSQL deployments never need the tool — startup DDL creates the schema.

### TLS

**Control plane.** Set both `ARKFLOW_HUB_TLS_CERT` and `ARKFLOW_HUB_TLS_KEY`
(PEM file paths) and the Hub serves every request over TLS — routes, auth,
and readiness semantics are unchanged. Only one of the two fails startup.
Agents reach a TLS Hub with an `https://` `hub_url` and no extra
configuration. Without both variables the Hub binds plaintext exactly as
before.

**Data plane (cross-node shuffle).** Set `ARKFLOW_DATA_PLANE_TLS_CERT`,
`ARKFLOW_DATA_PLANE_TLS_KEY`, and `ARKFLOW_DATA_PLANE_TLS_CA` together — the
node's certificate, its private key, and the fleet CA — and every
cross-node connection runs mTLS: each side must present a certificate
chaining to the fleet CA before any frame (including the HMAC session
handshake) is exchanged. Node certificates must carry the SAN
`DNS:arkflow-data-plane` (the fixed verification name; node identity itself
is still proven by the HMAC handshake). A partial set of variables is
ignored with a warning. Generate a fleet CA and node certificates with
openssl, for example:

```bash
# Fleet CA
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem   -subj "/CN=arkflow-fleet-ca" -days 3650
# Per node (repeat per compute node)
openssl req -newkey rsa:2048 -nodes -keyout node.key -out node.csr   -subj "/CN=arkflow-node"
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -out node.pem   -days 365 -extfile <(echo "subjectAltName=DNS:arkflow-data-plane")
```

Enable TLS on every compute node before relying on split placement:
during a rolling enable, plaintext and TLS nodes cannot talk to each other
(connections fail closed). Certificate rotation means restarting the
process (automatic renewal is out of scope).

### Hub high availability (lease election)

Multiple Hub processes can share one PostgreSQL database; a singleton lease
row (`cp_hub_lease`) elects exactly one leader through compare-and-swap with a
monotonic fencing epoch. Enable it with environment variables on every Hub
instance pointed at the same database:

| Variable | Required | Description |
|----------|----------|-------------|
| `ARKFLOW_HUB_HA_ENABLED` | yes | Set `true` to join the election. Off by default; a disabled Hub is a plain single instance. |
| `ARKFLOW_HUB_STORAGE` | yes | Must be a PostgreSQL URL for multi-instance HA (SQLite works for development and testing only and logs a warning). |
| `ARKFLOW_HUB_HA_LEASE_TTL_MS` | no | Lease lifetime, default `15000`. Renewal runs at TTL/3; the failover window is bounded by the TTL plus one probe. Minimum 1000. |
| `ARKFLOW_HUB_HA_HOLDER_ID` | no | Explicit holder identity; defaults to `host:pid:boot-ms`. Must be unique per Hub process: two Hubs sharing one holder id would renew each other's lease and both act as leader — leave it unset unless you have a naming scheme that guarantees uniqueness. |

Behavior:

- The **leader** renews the lease every TTL/3 and runs all periodic work
  (node sweeps, reconciliation, retention). On graceful shutdown it releases
  the lease immediately so a standby can take over without waiting out the
  TTL.
- A **standby** serves only `/health`, `/readiness`, `/liveness`, and the
  metrics export; every operator and agent route answers `503 hub_standby`.
  Its readiness reports not-ready with the role. Put a load balancer or VIP
  in front of the instances and route to the backend whose `/readiness` is
  healthy — Agents keep their single `hub_url` and re-register with whichever
  instance leads.
- On takeover the promoted standby **reloads the durable control-plane view
  (jobs, versions, checkpoints, operations, rollouts) before serving** and
  clears the node registry; Agents re-register through their existing
  reconnect loop. A leader that loses the lease (renewal failure or storage
  outage) steps down at once and stops dispatching.

Operational assumptions: clocks must be NTP-aligned (the TTL must dwarf the
skew), and the failover window is bounded by the lease TTL plus one probe
(default ≈ 15s + 5s). Fencing epochs make each takeover observable
(`/api/v1/system` reports `ha.role` and `ha.epoch`; readiness carries the same
block; transitions appear in the event stream as `hub.leadership`). Writes
already in flight when a leader loses the lease are fenced only by this
window — full storage-level write fencing is a later HA stage.

### OIDC JWT federation

Instead of (or alongside) the static operator credential, the Hub accepts
bearer JWTs issued by your organization's OIDC identity provider. Configure
it with environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `ARKFLOW_OIDC_ISSUER` | yes | Token issuer; must match the token's `iss` claim. |
| `ARKFLOW_OIDC_AUDIENCE` | yes | Expected `aud` claim for the Hub. |
| `ARKFLOW_OIDC_JWKS_URL` | no | JWKS endpoint; defaults to `{issuer}/.well-known/jwks.json`. |
| `ARKFLOW_OIDC_ROLE_CLAIM` | no | Claim carrying the role(s); defaults to `roles`. |
| `ARKFLOW_OIDC_SCOPES_CLAIM` | no | Claim carrying resource scopes; defaults to `scopes`. |
| `ARKFLOW_OIDC_CLIENT_ID` | login | OAuth2 client id for the browser authorization-code flow. |
| `ARKFLOW_OIDC_CLIENT_SECRET` | login | OAuth2 client secret. |
| `ARKFLOW_OIDC_REDIRECT_URI` | login | Registered redirect URI, e.g. `https://hub.example.com/api/v1/auth/oidc/callback`. |

When the three client variables are configured, the Hub discovers the IdP's
authorization/token endpoints at startup and serves a browser login flow:

- `GET /api/v1/auth/oidc/login` — redirects to the identity provider with a
  random `state`, a PKCE S256 `code_challenge`, and an `nonce`, all bound to
  an HttpOnly cookie. The IdP must support PKCE (RFC 7636, `S256`) and echo
  the `nonce` claim in the id_token.
- `GET /api/v1/auth/oidc/callback?code&state` — exchanges the code (with the
  PKCE verifier) for an id_token, checks the `nonce` claim, validates the
  token through the same pipeline as bearer JWTs, creates an 8-hour
  server-side session, and sets an HttpOnly `arkflow_session` cookie
  (`Secure` is added when the redirect URI is https).
- `GET /api/v1/auth/oidc/logout` — deletes the server-side session and
  clears the cookie.

Browser sessions participate in the same RBAC model as every other
credential (the role comes from the role claim). The Console integrates
automatically: it probes `GET /api/v1/auth/oidc/status` (a public endpoint
reporting `login_enabled`, `authenticated`, and the principal), redirects
to the login flow on a 401, and shows a sign-out control for
authenticated sessions. `VITE_API_TOKEN` deployments keep working
unchanged — with a static token configured the console never redirects. Sessions live in memory:
restarting the Hub signs everyone out, and there is no revocation list —
prefer short-lived tokens at the identity provider. Without the client
variables the Hub stays bearer-only and the login routes are absent.

Claims map onto the existing RBAC model: `sub` becomes the principal id,
the role claim accepts an array or a single string (`admin`, `operator`, or
`viewer` — the highest matching role wins), and the scopes claim (array or
comma-separated) uses the same `type=id` grammar as static credentials,
for example `["node=node-a", "stream"]`. Only asymmetric algorithms
(ES256/RS256) are accepted; signature, expiry, issuer, and audience are all
validated, and the JWKS is cached with on-demand refresh when an unknown key
id appears. Prefer short-lived tokens at your identity provider — there is
no revocation list. The static credential still works when configured, so
you can keep break-glass automation tokens while human access moves to the
identity provider.

The included `console/Dockerfile` builds static assets and serves them through
Nginx. Its `/api/` and `/metrics` locations proxy to an `arkflow-hub:8080`
service; deploy it on a private network with TLS and an authentication layer.
Do not expose the API or the token-bearing console directly to the public
internet. The ArkFlow default bind address is local-only.

## Migration from the health-centric console

The old UI treated the backend as a health and aggregate-stream monitor. The
resource-oriented console uses `/api/v1/system`, `/nodes`, `/streams`,
`/operations`, `/events`, `/configuration`, and `/components`. Lifecycle
requests are asynchronous and must be polled by operation ID. Existing
`/health`, `/readiness`, `/liveness`, `/metrics`, `/status`, and `/config*`
routes remain as compatibility aliases, but new integrations should use the
resource endpoints. Configuration versions, operations, audit records, and
bounded event history are durable in Hub mode. Rollouts are available under
`/api/v1/rollouts`; use the action endpoint to pause, resume, cancel, or create
a rollback. The authenticated `/api/v1/events/stream` SSE endpoint supports
filters and `Last-Event-ID`; clients must reload REST snapshots after a
`resync` event.

For reverse proxies, preserve `Authorization`, `X-Correlation-ID`, and the
SSE `text/event-stream` response without buffering. Do not put credentials in
query parameters.

## Upgrading a mixed fleet

Agents present their session credential only in the `Authorization: Bearer`
header. The Hub still accepts the legacy query parameter so Agents from older
releases keep polling after the Hub is upgraded, but an upgraded Agent requires
a Hub from the same release or later: in a rolling upgrade, upgrade the Hub
before upgrading any Agent. Agent reconnects use randomized backoff, so a Hub
restart does not produce synchronized re-registration bursts. The Hub
session TTL (`health_check.agent_session_ttl_ms`, one hour by default) bounds
how long a leaked session credential can authenticate; Agents re-register
transparently when it elapses, so keep it comfortably above the longest
expected command (for example a long checkpoint) to avoid result resubmission.
