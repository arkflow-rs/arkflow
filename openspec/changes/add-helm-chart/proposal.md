## Why

Issue #1225 asks for a Helm chart and none exists: the only Kubernetes delivery path today is a set of hand-written raw manifests in `docs/docs/operate/kubernetes.md:37-106` (ConfigMap + Deployment + Service) that each operator must copy, edit, and maintain by hand. The engine image is already published to ghcr.io on every `v*.*.*` tag (`.github/workflows/docker.yml:12`), so the packaging prerequisite is met but never consumed by an installable artifact. The chart also fixes real footguns that raw manifests invite: unbounded `replicas` on a stateful streaming engine (duplicate consumption), missing data-plane port for agent mode (`crates/arkflow-core/src/config.rs:185`), and hand-inlined secrets (the `${env:VAR}` expansion at `crates/arkflow-core/src/secret.rs:18` already provides the right mechanism).

## What Changes

- Add a Helm chart at `deploy/charts/arkflow` that installs the ArkFlow engine in two modes:
  - **standalone**: one stateful engine instance driven by a ConfigMap-rendered config.
  - **agent**: engine joining an existing Hub via `health_check.hub_urls`, adding the data-plane shuffle port and a headless Service for cross-node discovery.
- Values follow a pass-through philosophy: `config:` renders the full engine config document verbatim into the ConfigMap; image, resources, probes, and service settings are structured values. Secrets enter through existing `${env:VAR}` / `${env:VAR:-default}` expansion backed by K8s Secret env injection — the chart does not invent a parallel templating layer.
- Pin standalone mode to a single replica (Deployment with `replicas` locked, rendering more than one is a hard error or requires an explicit override acknowledging duplicate consumption) and document why.
- Publish the chart as an OCI artifact to `ghcr.io/arkflow-rs/charts/arkflow` on releases.
- CI: `helm lint` + `helm template` for both modes (plus `arkflow --validate` against the rendered config where feasible offline).
- Docs: new chart page in both `docs/docs/` and the zh-Hans tree, registered in the sidebar; the existing raw-manifest page links to the chart as the recommended path.
- Community: reply to #1225 with the delivery roadmap; close #768 (`/readiness` already shipped as a config default, `crates/arkflow-core/src/config.rs:365`).

## Capabilities

### New Capabilities

- `kubernetes-helm-chart`: Requirements for the official Helm chart's install surface — modes, config injection contract, replica semantics for the stateful engine, port/service exposure (HTTP health + data-plane shuffle), secret handling, and chart publication/CI gates.

### Modified Capabilities

<!-- None: existing specs' requirements are unchanged; the chart is a new delivery surface. -->

## Impact

- **New files only**: `deploy/charts/arkflow/**` (Chart.yaml, values.yaml, templates/), a chart workflow job or workflow file, two docs pages (en + zh-Hans), sidebar entries in `docs/sidebars.ts`.
- **No Rust code changes**: the chart consumes the existing binary image, config schema, health endpoints, and env expansion as-is.
- **New tooling dependency**: `helm` in CI (lint/template only; no cluster required for the baseline gate).
- **Registry**: new OCI artifact path under the existing ghcr.io namespace.

## Non-goals

- **No Kubernetes operator or CRDs.** Roadmap layer 3, gated on a real GitOps demand signal. When built, it must be a thin CRD→Hub-API translation layer; it must never make placement/scheduling decisions (the Hub is the single reconciliation brain per `control-plane-reconciliation`).
- **No Hub or Console containerization.** `docker/Dockerfile:40` copies only the engine binary; Hub (`arkflow-server`) and Console have no published images. The umbrella `mode: standalone | control-plane` chart is a separate follow-up change.
- **No cert-manager / fleet mTLS integration in this change** — planned with the control-plane chart so data-plane TLS applies to a multi-node fleet, not a single-instance install.
- **No structured/stream-per-value chart API**: the chart will not decompose engine config into Helm values beyond operational concerns (image, resources, probes, service). Schema evolution stays in the engine's JSON Schema, not in chart values.
