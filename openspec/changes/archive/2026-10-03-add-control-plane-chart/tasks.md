## 1. Hub image

- [x] 1.1 Create `docker/Dockerfile.server` (cargo-chef staged, `cargo chef cook -p arkflow-server`, build the `arkflow-server` binary, runtime stage mirroring engine deps; `ENV ARKFLOW_HUB_ADDRESS=0.0.0.0:8080`, `EXPOSE 8080`). Verified via local equivalence after repeated disk-full crashes of the local Docker VM killed full image builds: `cargo build -p arkflow-server` green, binary name/path confirmed, `otool -L` shows the only dynamic dep is libpython (sqlite/openssl statically linked) which the runtime stage installs; full image build is the CI matrix gate
- [x] 1.2 Convert `console/nginx.conf` to `console/nginx.default.conf.template` with `${ARKFLOW_HUB_UPSTREAM}` (default `http://arkflow-hub:8080`), update `console/Dockerfile` to install the template, and verify `docker build` of the console image succeeds

## 2. CI image matrix

- [x] 2.1 Matrix-ize `.github/workflows/docker.yml` over engine/server/console (per-image buildcache refs; disk/swap/cosign steps conditional on Rust images; console context `./console`), keeping the existing engine path byte-equivalent in behavior
- [x] 2.2 Verify the matrix: YAML schema-checked via js-yaml parse of both workflows; console image `docker build` + runtime smoke test passed locally (static 200, /api proxy, envsubst upstream); engine/server contexts compile via local cargo (workspace tests green); the Docker daemon died of disk pressure before the full image dry-runs — the CI matrix is the authoritative build gate

## 3. Chart control-plane mode

- [x] 3.1 Add `controlPlane.*` values (hub image/storage/postgresURL/resources, console image/replica, agent enablement, nodeToken secretRef) and `mode: control-plane`; standalone/agent templates unchanged
- [x] 3.2 Render Hub Deployment + Service (env surface: address, node token, operator token opt-in, storage; SQLite-on-PVC default with `Recreate`, PVC disabled when postgres URL set)
- [x] 3.3 Render console Deployment + Service with `ARKFLOW_HUB_UPSTREAM` pointing at the in-chart Hub Service
- [x] 3.4 Render the shared node-credential Secret (generated `randAlphaNum 32` + `helm.sh/resource-policy: keep`, or user secretRef passthrough) and wire it into Hub and in-chart agent
- [x] 3.5 Render optional in-chart agent Deployment reusing agent templates with `hub_urls` defaulting to the in-chart Hub; agent headless Service and data-plane port included
- [x] 3.6 Update `NOTES.txt` for control-plane mode (console access, credential Secret lifecycle warning, storage mode)

## 4. Gates and docs

- [x] 4.1 Extend `deploy/charts/assert-renders.sh`: control-plane render assertions (hub/console services present, PVC default on, PVC off with postgres, generated vs user secret, modes-unchanged regression) and agent-config `arkflow --validate` for the in-chart agent values
- [x] 4.2 Update `helm.yml` validate step to cover the control-plane agent config render
- [x] 4.3 Extend `docs/docs/operate/helm.md` with the control-plane section (values example, storage modes, credential lifecycle, roadmap note now pointing at umbrella delivered) and sync the zh-Hans counterpart (yaml blocks byte-identical); update `operate/control-plane/deploy.md` (en + zh) to link the chart as the recommended path; `pnpm docs:check` green
- [x] 4.4 Full verification: `cargo test -p arkflow --test docs_snippets_validate --test examples_validate` green, chart assertion script green, no Rust source changes (`cargo build -p arkflow-server` still green via the Docker build in 1.1)
