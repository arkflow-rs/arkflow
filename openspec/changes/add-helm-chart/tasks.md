## 1. Chart scaffold and standalone mode

- [ ] 1.1 Create `deploy/charts/arkflow` scaffold: `Chart.yaml` (`appVersion` from engine version, `version: 0.1.0`), `values.yaml` with pass-through `config` (string), image/resources/probes/service/env/persistence values and explanatory comments, `.helmignore`, chart `README.md`
- [ ] 1.2 Render ConfigMap from `values.config` verbatim (preserve document byte-for-byte) and mount read-only at the engine's default config path; verify with `helm template | diff` against the input document
- [ ] 1.3 Render Deployment: `replicas: 1`, `strategy.type: Recreate`, env from `values.env` (with Secret refs), liveness `/health` and readiness `/readiness` on the HTTP port, overridable paths/ports
- [ ] 1.4 Render ClusterIP Service for the HTTP port; optional PVC values (disabled by default) wiring a `persistence` volume when enabled
- [ ] 1.5 Enforce single-replica semantics: clamp or fail on `replicas > 1` unless `unsafe.allowMultipleReplicas: true`, and render the warning marker when opted in (assert via `helm template` test renders)
- [ ] 1.6 Write `NOTES.txt` including the `health_check.address: 0.0.0.0` reachability warning and install-verification commands

## 2. Agent mode

- [ ] 2.1 Add `mode: agent` switch: render data-plane container port and switch Service to headless (`clusterIP: None`); standalone render MUST NOT contain the data-plane port (assert in test renders)
- [ ] 2.2 Default `node_id` to pod name via downward API (`fieldRef: metadata.name`) when `config` does not set it; document `node_token` as `${env:...}` from a user Secret
- [ ] 2.3 Create example values files (`values-agent.yaml` with a minimal hub-joining config using `${env:...}` references) and verify the rendered config passes `arkflow --validate` offline

## 3. CI gates and publication

- [ ] 3.1 Add a chart CI job: `helm lint` + `helm template` for both modes on every change under `deploy/charts/**`; include mode-surface assertions (data-plane port/headless only in agent mode, Recreate strategy, replica clamp)
- [ ] 3.2 Add render-validate step: extract the rendered `config.yaml` and run it through `arkflow --validate` (release image or built binary) for both example value sets
- [ ] 3.3 Extend the release workflow to `helm package` + `helm push` the chart as an OCI artifact to `ghcr.io/arkflow-rs/charts/arkflow`, chart version bumped independently of `appVersion`

## 4. Documentation (en + zh-Hans) and community

- [ ] 4.1 Add chart page under `docs/docs/` (deploy path, both modes, secrets via `${env:}`, single-replica rationale, OCI install command) with required front matter and yaml fence markers; register in `docs/sidebars.ts`
- [ ] 4.2 Add the zh-Hans counterpart page with matching content and register it in the zh-Hans sidebar
- [ ] 4.3 Update `docs/docs/operate/kubernetes.md` (and zh-Hans counterpart) to present the chart as the recommended path and keep raw manifests as the "what the chart generates" reference; keep `pnpm docs:check` green
- [ ] 4.4 Reply to issue #1225 with the roadmap (chart now, umbrella control-plane chart next, operator gated on GitOps demand) and the OCI install command; close #768 citing the existing `/readiness` default (`crates/arkflow-core/src/config.rs:365`)
- [ ] 4.5 Full verification: `cargo test --workspace --all-targets` unaffected (no Rust changes), chart CI job green, `pnpm docs:check` green
