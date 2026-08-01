---
sidebar_position: 4
---

# Control plane

ArkFlow exposes a single-node control API and an independent web console. The
API is enabled by default on `127.0.0.1:8080`; configure `health_check.address`
and `health_check.api_prefix` to change the bind address and versioned prefix.
Keep the listener local or put it behind an authenticated reverse proxy. An
optional `health_check.api_token` enables Bearer authentication for lifecycle
commands and configuration reads/writes.

Read-only discovery endpoints include `/api/v1/system`, `/status`,
`/streams`, `/components`, `/schema`, and `/events`. Use
`POST /api/v1/streams/{id}/start|stop|restart` for lifecycle operations and
`/metrics` for Prometheus text exposition.

Configuration changes are parsed and validated before reconciliation. Unchanged
Streams retain their running task; changed or removed Streams are stopped and
new/changed Streams are built before they are started. Versions are written
atomically under `.arkflow/config-history`, and rollback creates a new child
version rather than rewriting history. Stream IDs must contain only ASCII
letters, digits, `-`, or `_`; legacy configurations receive deterministic
`stream-0`, `stream-1`, … IDs.

The existing WAL remains at-least-once: restarting a Stream replays unacknowledged
entries according to its configured WAL cursor semantics. The console can be
run locally with `cd console && npm install && npm run dev`; set
`VITE_API_BASE` when the API is served under a different prefix. The Vite
development proxy targets `127.0.0.1:8080` and production deployments should
serve the built static files through the same protected origin as the API.
