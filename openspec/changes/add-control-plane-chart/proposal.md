## Why

The engine chart (`deploy/charts/arkflow`, archived change `add-helm-chart`) can only install the engine: the Hub has no container image at all (`docker/Dockerfile:40` copies only the `arkflow` binary) and the Console image (`console/Dockerfile`) is never published by CI (`.github/workflows/docker.yml:97` builds only `docker/Dockerfile`). The control-plane deployment docs (`docs/docs/operate/control-plane/deploy.md:5-9`) prescribe `cargo run` for the Hub and a hand-built console behind a manual reverse proxy — no repeatable container path exists for the distributed runtime.

## What Changes

- Add `docker/Dockerfile.server`: cargo-chef staged build of the `arkflow-server` binary (Hub), runtime image mirroring the engine image's dependency set, defaulting `ARKFLOW_HUB_ADDRESS=0.0.0.0:8080`.
- Make the Console image deployable with a configurable Hub upstream: its nginx config becomes an envsubst template (`ARKFLOW_HUB_UPSTREAM`, default `arkflow-hub:8080`) using nginx's built-in templates mechanism, preserving the same-origin `/api` and `/metrics` proxy contract.
- Publish both images from CI: matrix-ize the existing Docker workflow over engine / server / console images (per-image buildcache refs; disk-space and swap steps only on Rust builds).
- Extend the existing chart with `mode: control-plane`, rendering: Hub Deployment (+Service, SQLite-on-PVC storage by default with optional `postgres://` URL), Console Deployment (+Service), and an optional in-chart agent joining the in-chart Hub. Shared node credential: generated Secret by default or user-supplied reference.
- CI render assertions extend to the control-plane mode; `arkflow --validate` covers the agent-side rendered config.
- Docs: extend `operate/helm.md` (en + zh-Hans) with the control-plane mode; the control-plane deploy page links the chart as the recommended path.

## Capabilities

### New Capabilities

<!-- none -->

### Modified Capabilities

- `kubernetes-helm-chart`: adds requirements for the control-plane install mode (Hub/Console workloads, storage contract, shared node credential, configurable console upstream) and for publishing/CI coverage of the two new images.

## Impact

- **Images**: two new ghcr.io artifacts (`arkflow-server`, `arkflow-console`) alongside `arkflow`; `docker.yml` becomes a three-image matrix with per-image cache refs.
- **Chart**: `deploy/charts/arkflow` gains `controlPlane.*` values and templates (hub/console services and deployments, credential Secret); existing standalone/agent renders unchanged (asserted by CI).
- **Console build**: `console/nginx.conf` becomes `console/nginx.default.conf.template` (envsubst via nginx entrypoint); behavior with default env is byte-equivalent to today.
- **Rust code**: none. The Hub consumes its existing env surface (`crates/arkflow-server/src/bin/arkflow-server.rs:26-46`).

## Non-goals

- **No CRD/operator** — unchanged from `add-helm-chart`: deferred until a real GitOps demand signal; when built, translation-only.
- **No cert-manager / fleet mTLS wiring** — the data-plane TLS env surface (`ARKFLOW_DATA_PLANE_TLS_*`) remains user-managed; chart values only pass references through. A follow-up can integrate cert-manager for fleet CA/node certs.
- **No Hub HA topology management** — `ARKFLOW_HUB_HA_*` values are passed through where trivial, but multi-replica Hub leader election orchestration is out of scope; the Hub Deployment stays single-replica with Postgres as the HA prerequisite when users opt in manually.
- **No console image build-args for API base/token** beyond the existing `VITE_API_TOKEN` channel; OIDC stays the recommended auth.
