## Context

The engine image is published to `ghcr.io/arkflow-rs/arkflow` on every release tag (`.github/workflows/docker.yml`), and the documented Kubernetes path is copy-paste raw manifests (`docs/docs/operate/kubernetes.md`). The engine binary supports two deployable roles from one config document: a standalone pipeline, and a compute-node agent that joins a Hub (`health_check.hub_urls`, `node_id`, `node_token` — `crates/arkflow-core/src/config.rs:167-178`) and optionally binds a data-plane shuffle port (`config.rs:185`). Config supports `${env:VAR}` expansion (`secret.rs`), and the binary exposes `/health` and `/readiness` by default. The control plane (Hub/Console) has no published images yet — that constrains this chart to the engine.

## Goals / Non-Goals

**Goals:**

- One-command install of the engine (`helm install`) in standalone or agent mode, using the already-published image.
- Make the safe things default and the unsafe things explicit (single replica, plaintext data plane, exposure).
- Keep the chart thin: no parallel schema, no secrets templating, values only for operational concerns.
- Chart artifact published to ghcr OCI and lint/template-gated in CI.

**Non-Goals:**

- Operator/CRDs, Hub/Console chart, cert-manager/mTLS integration, structured per-component values (see proposal Non-goals).
- Running or managing a Kubernetes cluster in CI (no kind smoke test in the baseline gate).

## Decisions

### D1: One chart, `mode: standalone | agent` values switch

Templates render a nearly identical workload for both modes; agent mode adds the data-plane container port and switches the Service to headless (`clusterIP: None`) for cross-node discovery. Alternative — two charts — rejected: it duplicates 90% of templates and forks the values surface before there is demand.

### D2: Pass-through config, structured operations

`values.config` (string, full YAML document) renders verbatim into the ConfigMap mounted at `/app/etc/config.yaml`. Image, tag, resources, probes, service type, pod annotations, env are structured values. Rationale: the engine's config schema is already versioned and validated (`arkflow --validate`, JSON Schema); lifting it into chart values would create a second schema that always lags. Alternatives considered: fully structured values (rejected — permanent drift), or a `--set`-friendly flattened map (rejected — Helm maps are lossy for nested YAML).

### D3: Secrets ride existing `${env:}` expansion

The chart renders `envFrom`/`env` entries from `values.env` plus Secret references; users reference them inside `config` as `${env:MY_TOKEN}`. The chart never receives secret values itself. Alternative — chart-side secret templating (`{{ .Values.password }}` into config) — rejected: it puts secrets into values files and Helm release metadata.

### D4: Single replica enforced by Deployment + `Recreate` strategy

Standalone mode renders `replicas: 1` and `strategy.type: Recreate`. `Recreate` is the load-bearing part: with `RollingUpdate`, a config change briefly runs two pods against the same sources → duplicate consumption, which violates input-durability assumptions. Surging the replica count requires an explicit `unsafe.allowMultipleReplicas: true` which renders a loud comment and is documented as unsupported for partition-unaware sources. Alternative — StatefulSet — rejected for now: stable network identity buys nothing for a single replica and complicates config-only updates; it becomes relevant only with control-plane-managed multi-node fleets.

### D5: Agent identity from pod metadata

In agent mode, `node_id` defaults to the pod name via `fieldRef: metadata.name` when `config` does not set one, and `node_token` is expected as `${env:...}` from a user-provided Secret. Headless Service + data-plane port make the node reachable for shuffle. The engine already refuses to bind the data-plane listener without credentials (`authenticated-network-shuffle` spec), so the chart cannot accidentally ship an unauthenticated fleet listener.

### D6: Publish as OCI artifact, gate with lint + template

`helm push` to `ghcr.io/arkflow-rs/charts/arkflow` in the release workflow (reusing the existing ghcr login). CI job runs `helm lint` and `helm template` for both modes; where the runner can pull the image offline, the rendered `config.yaml` is additionally fed to `arkflow --validate`. Alternative — GitHub Pages chart repo — rejected: ghcr credentials and namespace already exist.

## Risks / Trade-offs

- [Pass-through config can bind `health_check.address` to loopback → kubelet probes fail] → Mitigation: values comments + chart `NOTES.txt` warning; cannot hard-fail inside `helm template` without parsing engine config.
- [`Recreate` causes a stop-the-world window on updates] → Accepted for a single-instance engine; multi-instance availability is the control-plane umbrella chart's problem, not this one's.
- [Docs drift between raw-manifest page and chart] → Mitigation: raw-manifest page stays as the "what the chart generates" reference and links the chart page as the recommended path; both pages updated in the same change.
- [Chart versioning vs engine versioning confusion] → Mitigation: `appVersion` tracks the engine tag; chart `version` starts at `0.1.0` and bumps independently, documented in the chart README.

## Migration Plan

New artifact only — nothing to migrate. Rollback = `helm uninstall`; existing raw-manifest users are unaffected.

## Open Questions

- Default for a `persistence` value (optional PVC for WAL/state store): ship disabled in v1 with a documented value to enable, since the engine's WAL is opt-in today. Revisit with the umbrella chart.
