---
sidebar_position: 4
---

# Control plane deployment

Build the console with `cd console && npm ci && npm run build`, then serve
`console/dist` from a protected reverse proxy. The development Vite server
proxies `/api` and `/metrics` to `127.0.0.1:8080`; production should preserve
the same-origin paths and set `VITE_API_BASE` only when the API prefix differs.
Set `VITE_API_TOKEN` only in a controlled build environment when the ArkFlow
listener has `health_check.api_token` configured.

The included `console/Dockerfile` builds static assets and serves them through
Nginx. Its `/api/` and `/metrics` locations proxy to an `arkflow:8080`
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
resource endpoints. Operation and event history is process-local; configuration
versions remain durable under `.arkflow/config-history`.
