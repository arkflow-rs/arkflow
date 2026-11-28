# kubernetes-helm-chart Specification

## Purpose

Keep the official `deploy/charts/arkflow` chart a truthful, single-source
install surface for standalone, agent, and control-plane modes: CI must lint
and render every mode the chart claims to support, secrets must enter only
through env expansion, and the published OCI artifact must track the engine
release it deploys (chart version independent, `appVersion` gated). This
exists because the chart ships infrastructure defaults operators cannot
easily audit — wrong probes, leaked secrets, or a stale `appVersion` become
production incidents, not documentation errors.

## Requirements
### Requirement: Single chart with standalone and agent install modes

The official Helm chart SHALL install the ArkFlow engine image in exactly two modes selected by the `mode` value: `standalone` (a self-contained pipeline) and `agent` (an engine joining an existing Hub). Agent mode SHALL additionally render the data-plane shuffle container port and a headless Service so other nodes can reach this node for cross-node shuffle.

#### Scenario: Standalone install
- **WHEN** a user runs `helm install` with `mode=standalone` and a complete `config` document
- **THEN** the release renders a ConfigMap, a single-replica workload, and a ClusterIP Service exposing the HTTP health port, with no data-plane port

#### Scenario: Agent install
- **WHEN** a user runs `helm install` with `mode=agent` and a `config` document that sets `health_check.hub_urls`
- **THEN** the release renders the data-plane container port and a headless Service in addition to the standalone surface

### Requirement: Pass-through configuration injection

The chart SHALL render the `config` value verbatim as the engine's configuration document mounted at the engine's default config path, and SHALL NOT decompose engine configuration fields into structured chart values. The chart SHALL keep only operational concerns (image, tag, resources, probes, service type, pod annotations, env, persistence) as structured values.

#### Scenario: Verbatim config
- **WHEN** `helm template` renders a release with a `config` value containing engine YAML (including comments and field ordering)
- **THEN** the ConfigMap data contains that document byte-for-byte, and the workload mounts it read-only at the path the engine binary reads by default

#### Scenario: Schema drift containment
- **WHEN** the engine gains new configuration fields in a later release
- **THEN** the chart requires no values changes to pass them through, and `arkflow --validate` remains the authority on whether the rendered config is valid

### Requirement: Secrets enter only through env expansion

The chart SHALL inject user-referenced Kubernetes Secrets as environment variables on the workload, and documentation SHALL direct users to reference them inside `config` via the engine's `${env:VAR}` expansion. The chart MUST NOT template secret values into the ConfigMap, into chart values defaults, or into any rendered resource that stores them as plaintext.

#### Scenario: Token via environment
- **WHEN** a user sets `values.env` to reference a Secret and writes `node_token: "${env:NODE_TOKEN}"` in `config`
- **THEN** the rendered ConfigMap contains only the literal `${env:NODE_TOKEN}` string and the workload receives the secret value only as an environment variable

### Requirement: Single-replica semantics for standalone mode

In standalone mode the chart SHALL render exactly one workload replica with a strategy that never runs two engine pods concurrently against the same sources (update strategy `Recreate`). Rendering more than one replica SHALL require an explicit opt-in value that the chart's documentation marks as unsupported for partition-unaware sources.

#### Scenario: Config update does not double-consume
- **WHEN** a standalone release is upgraded with a changed `config`
- **THEN** the old pod terminates before the new pod starts, so at most one engine instance consumes the configured sources at any time

#### Scenario: Multi-replica is explicit
- **WHEN** a user renders the chart without the multi-replica opt-in and requests replicas greater than one
- **THEN** the render fails or is clamped to one, and with the opt-in set the render includes a warning marker naming the duplicate-consumption risk

### Requirement: Probes wired to engine health endpoints

The chart SHALL configure liveness and readiness probes against the engine's default `/health` and `/readiness` paths on the HTTP health port, with paths and ports overridable as values, and chart documentation SHALL require `health_check.address` to bind a pod-reachable address such as `0.0.0.0`.

#### Scenario: Default probes
- **WHEN** a release renders with default probe values
- **THEN** liveness targets `/health` and readiness targets `/readiness` on the rendered HTTP port

### Requirement: Agent identity and reachability defaults

In agent mode, when `config` does not set `node_id`, the chart SHALL default the node identity to the pod name via the Kubernetes downward API. The chart MUST NOT render any data-plane credential material itself; credentials remain engine config resolved from user-provided Secrets.

#### Scenario: Node identity from pod
- **WHEN** agent mode renders without a `node_id` in `config`
- **THEN** the workload exposes the pod name as an environment/downward-API value the engine expansion resolves for `node_id`

### Requirement: Chart publication and CI gates

Every change touching the chart SHALL be gated by CI running `helm lint` and `helm template` for both modes. On release, the chart SHALL be published as a versioned OCI artifact to the project's registry namespace, with `appVersion` tracking the engine release tag. The release pipeline SHALL enforce that `Chart.yaml` `appVersion` equals the engine workspace version before publishing, so a forgotten bump fails the release instead of shipping a chart that references stale images.

#### Scenario: CI gate
- **WHEN** a pull request modifies files under the chart directory
- **THEN** CI renders both modes and fails on lint errors, invalid templates, or mode-specific surface violations (data-plane port present only in agent mode)

#### Scenario: Release publication
- **WHEN** a release tag is cut
- **THEN** the chart is pushed to the registry as an OCI artifact installable via `helm install oci://…/charts/arkflow --version <chart-version>`

#### Scenario: appVersion drift fails the release

- **WHEN** the release pipeline runs and `Chart.yaml` `appVersion` differs from the workspace version in the root `Cargo.toml`
- **THEN** the pipeline fails with an error naming both values, and no chart artifact is published

### Requirement: Control-plane install mode renders Hub and Console

With `mode: control-plane`, the chart SHALL render a Hub Deployment and Service (image `arkflow-server`, listening on `0.0.0.0:8080` via its env surface), a Console Deployment and Service serving the static console with the same-origin `/api` and `/metrics` reverse proxy, and SHALL default `controlPlane.tls.enabled` to `true` so multi-node control-plane deployments run with Hub TLS and Agent mTLS out of the box (using a chart-generated, `keep`-annotated PKI Secret unless `controlPlane.tls.existingSecret` is provided). Operators MAY set `controlPlane.tls.enabled=false` to explicitly disable chart-managed TLS/mTLS for local development or external TLS termination. The chart SHALL keep the existing `standalone` and `agent` renders unchanged when `controlPlane.tls` only targets `control-plane` mode.

#### Scenario: Control-plane install enables mTLS by default
- **WHEN** a user runs `helm install` with `mode=control-plane` and default `controlPlane.tls.enabled=true`
- **THEN** the release renders a `keep`-policy TLS Secret (CA, Hub cert/key, Agent cert/key), configures `ARKFLOW_HUB_TLS_CERT`, `ARKFLOW_HUB_TLS_KEY`, and `ARKFLOW_HUB_TLS_CA` on the Hub with HTTPS probes, points `ARKFLOW_HUB_UPSTREAM` at `https://<hub>:8080`, and leaves standalone/agent modes unaffected

#### Scenario: Control-plane TLS opt-out restores plaintext HTTP
- **WHEN** a user renders `mode=control-plane` with `--set controlPlane.tls.enabled=false`
- **THEN** no TLS Secret is generated, Hub/Console/in-chart Agent omit TLS mounts/envs, Hub probes use HTTP, and `ARKFLOW_HUB_UPSTREAM` targets `http://<hub>:8080`

#### Scenario: Existing modes unchanged
- **WHEN** `helm template` renders standalone and agent modes
- **THEN** the output contains no Hub, Console, or control-plane resources, and differs from the pre-control-plane renders only by the added `app.kubernetes.io/component: engine` selector label

### Requirement: Configurable console upstream

The Console image SHALL resolve its Hub upstream at container start from an environment variable (default `http://arkflow-hub:8080`) via an nginx template, and the chart SHALL set that variable to the in-chart Hub Service.

#### Scenario: Default upstream outside the chart
- **WHEN** the Console container starts without the upstream env set
- **THEN** its nginx configuration proxies `/api` and `/metrics` to `http://arkflow-hub:8080`, matching the previous hard-coded behavior

#### Scenario: Chart-set upstream
- **WHEN** the chart renders the Console Deployment
- **THEN** the upstream env points at the release's Hub Service, and the rendered proxy target is that address

### Requirement: Hub storage contract

In control-plane mode the chart SHALL default the Hub to SQLite on a PVC (`ARKFLOW_HUB_STORAGE` pointing at a path on the persistence volume) and SHALL offer a value that switches `ARKFLOW_HUB_STORAGE` to a `postgres://` URL (env-expansion references allowed), disabling the PVC when set. The Hub Deployment SHALL run a single replica with the `Recreate` update strategy in either case.

#### Scenario: SQLite default
- **WHEN** control-plane mode renders with default storage values
- **THEN** the Hub env names a SQLite path under the persistence mount and the release includes the PVC

#### Scenario: Postgres opt-in
- **WHEN** the user sets the Postgres URL value
- **THEN** the Hub env carries that URL, the PVC is not rendered, and the Deployment remains single-replica `Recreate`

### Requirement: Shared node credential

The chart SHALL feed the same node credential to the Hub (`ARKFLOW_NODE_TOKEN`) and to any in-chart agent. By default the chart SHALL generate the credential into a Secret on first install with a keep policy so upgrades do not rotate it; a user-supplied Secret reference SHALL replace the generated one.

#### Scenario: Generated credential
- **WHEN** control-plane mode renders with the default credential values
- **THEN** one Secret is rendered and both the Hub and the in-chart agent reference the same key from it

#### Scenario: User-supplied credential
- **WHEN** the user provides a Secret reference
- **THEN** no credential Secret is generated and both workloads reference the user's Secret key

### Requirement: Optional in-chart agent

Control-plane mode SHALL support an opt-in agent Deployment joining the in-chart Hub, reusing the agent-mode rendering contract (data-plane port, headless Service, pod-name node identity) with `hub_urls` defaulting to the in-chart Hub Service (`https://<release-name>-hub:8080` in `values-controlplane.yaml`). When `controlPlane.tls.enabled` is `true`, the in-chart agent SHALL mount the shared fleet TLS Secret and receive `ARKFLOW_AGENT_TLS_{CERT,KEY,CA}` and `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}` so both control-plane and data-plane traffic use mTLS by default. When `persistence.enabled` is `true`, the chart SHALL set `ARKFLOW_NODE_CREDENTIAL_FILE` inside `persistence.mountPath` so enrolled per-node credentials survive pod restarts.

#### Scenario: Agent enabled with default mTLS
- **WHEN** the user enables the control-plane agent with default values
- **THEN** the release renders the agent workload mounted with the TLS Secret, configured with `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*`, and `values-controlplane.yaml` points `hub_urls` at `https://arkflow-hub:8080`

### Requirement: Agent mode supports mTLS material from Secrets

In `mode: agent`, when `agent.tls.existingSecret` is configured, the chart SHALL mount that Secret read-only and inject `ARKFLOW_AGENT_TLS_{CERT,KEY,CA}` and `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}` into the agent container without embedding certificate material in ConfigMaps or chart values.

#### Scenario: Standalone agent chart references external mTLS Secret
- **WHEN** `helm template` renders `mode=agent` with `--set agent.tls.existingSecret=fleet-mtls`
- **THEN** the Deployment mounts `fleet-mtls` read-only and injects all six `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*` environment variables while keeping ConfigMap data free of secrets

### Requirement: Server and console images are published

CI SHALL build and publish the Hub image (`arkflow-server`) and Console image (`arkflow-console`) from the same repository on main pushes and release tags, with per-image build caches, alongside the existing engine image; CI render assertions SHALL cover the control-plane mode on every chart change.

#### Scenario: Release publishes three images
- **WHEN** a release tag is cut
- **THEN** engine, server, and console images are published to the registry namespace and the chart's control-plane default values resolve to published tags

#### Scenario: Control-plane render gates
- **WHEN** a pull request modifies the chart
- **THEN** CI renders control-plane mode and asserts the Hub/Console surfaces, the storage contract, and the shared-credential wiring

