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
  random `state` bound to an HttpOnly cookie.
- `GET /api/v1/auth/oidc/callback?code&state` — exchanges the code for an
  id_token, validates it through the same pipeline as bearer JWTs, creates
  an 8-hour server-side session, and sets an HttpOnly `arkflow_session`
  cookie.
- `GET /api/v1/auth/oidc/logout` — deletes the server-side session and
  clears the cookie.

Browser sessions participate in the same RBAC model as every other
credential (the role comes from the role claim). Sessions live in memory:
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
