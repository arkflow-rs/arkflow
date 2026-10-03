# kubernetes-helm-chart Specification

## Purpose
TBD - created by archiving change add-helm-chart. Update Purpose after archive.
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

Every change touching the chart SHALL be gated by CI running `helm lint` and `helm template` for both modes. On release, the chart SHALL be published as a versioned OCI artifact to the project's registry namespace, with `appVersion` tracking the engine release tag.

#### Scenario: CI gate
- **WHEN** a pull request modifies files under the chart directory
- **THEN** CI renders both modes and fails on lint errors, invalid templates, or mode-specific surface violations (data-plane port present only in agent mode)

#### Scenario: Release publication
- **WHEN** a release tag is cut
- **THEN** the chart is pushed to the registry as an OCI artifact installable via `helm install oci://…/charts/arkflow --version <chart-version>`

