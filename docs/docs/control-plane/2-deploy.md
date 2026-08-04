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
