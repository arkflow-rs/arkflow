## 1. Hub image

- [ ] 1.1 Create `docker/Dockerfile.server` (cargo-chef staged, `cargo chef cook -p arkflow-server`, build the `arkflow-server` binary, runtime stage mirroring engine deps; `ENV ARKFLOW_HUB_ADDRESS=0.0.0.0:8080`, `EXPOSE 8080`) and build it locally with docker to verify it compiles
- [ ] 1.2 Convert `console/nginx.conf` to `console/nginx.default.conf.template` with `${ARKFLOW_HUB_UPSTREAM}` (default `http://arkflow-hub:8080`), update `console/Dockerfile` to install the template, and verify `docker build` of the console image succeeds

## 2. CI image matrix

- [ ] 2.1 Matrix-ize `.github/workflows/docker.yml` over engine/server/console (per-image buildcache refs; disk/swap/cosign steps conditional on Rust images; console context `./console`), keeping the existing engine path byte-equivalent in behavior
- [ ] 2.2 Verify the matrix locally with `actionlint` (or equivalent YAML schema check) and dry-run the build contexts (`docker build` for all three Dockerfiles succeeds)

## 3. Chart control-plane mode

- [ ] 3.1 Add `controlPlane.*` values (hub image/storage/postgresURL/resources, console image/replica, agent enablement, nodeToken secretRef) and `mode: control-plane`; standalone/agent templates unchanged
- [ ] 3.2 Render Hub Deployment + Service (env surface: address, node token, operator token opt-in, storage; SQLite-on-PVC default with `Recreate`, PVC disabled when postgres URL set)
- [ ] 3.3 Render console Deployment + Service with `ARKFLOW_HUB_UPSTREAM` pointing at the in-chart Hub Service
- [ ] 3.4 Render the shared node-credential Secret (generated `randAlphaNum 32` + `helm.sh/resource-policy: keep`, or user secretRef passthrough) and wire it into Hub and in-chart agent
- [ ] 3.5 Render optional in-chart agent Deployment reusing agent templates with `hub_urls` defaulting to the in-chart Hub; agent headless Service and data-plane port included
- [ ] 3.6 Update `NOTES.txt` for control-plane mode (console access, credential Secret lifecycle warning, storage mode)

## 4. Gates and docs

- [ ] 4.1 Extend `deploy/charts/assert-renders.sh`: control-plane render assertions (hub/console services present, PVC default on, PVC off with postgres, generated vs user secret, modes-unchanged regression) and agent-config `arkflow --validate` for the in-chart agent values
- [ ] 4.2 Update `helm.yml` validate step to cover the control-plane agent config render
- [ ] 4.3 Extend `docs/docs/operate/helm.md` with the control-plane section (values example, storage modes, credential lifecycle, roadmap note now pointing at umbrella delivered) and sync the zh-Hans counterpart (yaml blocks byte-identical); update `operate/control-plane/deploy.md` (en + zh) to link the chart as the recommended path; `pnpm docs:check` green
- [ ] 4.4 Full verification: `cargo test -p arkflow --test docs_snippets_validate --test examples_validate` green, chart assertion script green, no Rust source changes (`cargo build -p arkflow-server` still green via the Docker build in 1.1)
