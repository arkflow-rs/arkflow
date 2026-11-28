## MODIFIED Requirements

### Requirement: Control-plane install mode renders Hub and Console

With `mode: control-plane`, the chart SHALL render a Hub Deployment and Service (image `arkflow-server`, listening on `0.0.0.0:8080` via its env surface), a Console Deployment and Service serving the static console with the same-origin `/api` and `/metrics` reverse proxy, and SHALL default `controlPlane.tls.enabled` to `true` so multi-node control-plane deployments run with Hub TLS and Agent mTLS out of the box (using a chart-generated, `keep`-annotated PKI Secret unless `controlPlane.tls.existingSecret` is provided). Operators MAY set `controlPlane.tls.enabled=false` to explicitly disable chart-managed TLS/mTLS for local development or external TLS termination. The chart SHALL keep the existing `standalone` and `agent` renders unchanged when `controlPlane.tls` only targets `control-plane` mode.

#### Scenario: Control-plane install enables mTLS by default

- **WHEN** a user runs `helm install` with `mode=control-plane` and default `controlPlane.tls.enabled=true`
- **THEN** the release renders a `keep`-policy TLS Secret (CA, Hub cert/key, Agent cert/key), configures `ARKFLOW_HUB_TLS_CERT`, `ARKFLOW_HUB_TLS_KEY`, and `ARKFLOW_HUB_TLS_CA` on the Hub with HTTPS probes, points `ARKFLOW_HUB_UPSTREAM` at `https://<hub>:8080`, and leaves standalone/agent modes unaffected

#### Scenario: Control-plane TLS opt-out restores plaintext HTTP

- **WHEN** a user renders `mode=control-plane` with `--set controlPlane.tls.enabled=false`
- **THEN** no TLS Secret is generated, Hub/Console/in-chart Agent omit TLS mounts/envs, Hub probes use HTTP, and `ARKFLOW_HUB_UPSTREAM` targets `http://<hub>:8080`

#### Scenario: Existing modes unchanged

- **WHEN** `helm template` renders `standalone` mode
- **THEN** the output contains no Hub, Console, or control-plane TLS resources

### Requirement: Optional in-chart agent

Control-plane mode SHALL support an opt-in agent Deployment joining the in-chart Hub, reusing the agent-mode rendering contract (data-plane port, headless Service, pod-name node identity) with `hub_urls` defaulting to the in-chart Hub Service (`https://<release-name>-hub:8080` in `values-controlplane.yaml`). When `controlPlane.tls.enabled` is `true`, the in-chart agent SHALL mount the shared fleet TLS Secret and receive `ARKFLOW_AGENT_TLS_{CERT,KEY,CA}` and `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}` so both control-plane and data-plane traffic use mTLS by default. When `persistence.enabled` is `true`, the chart SHALL set `ARKFLOW_NODE_CREDENTIAL_FILE` inside `persistence.mountPath` so enrolled per-node credentials survive pod restarts.

#### Scenario: Agent enabled with default mTLS

- **WHEN** the user enables the control-plane agent with default values
- **THEN** the release renders the agent workload mounted with the TLS Secret, configured with `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*`, and `values-controlplane.yaml` points `hub_urls` at `https://arkflow-hub:8080`

## ADDED Requirements

### Requirement: Agent mode supports mTLS material from Secrets

In `mode: agent`, when `agent.tls.existingSecret` is configured, the chart SHALL mount that Secret read-only and inject `ARKFLOW_AGENT_TLS_{CERT,KEY,CA}` and `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}` into the agent container without embedding certificate material in ConfigMaps or chart values.

#### Scenario: Standalone agent chart references external mTLS Secret

- **WHEN** `helm template` renders `mode=agent` with `--set agent.tls.existingSecret=fleet-mtls`
- **THEN** the Deployment mounts `fleet-mtls` read-only and injects all six `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*` environment variables while keeping ConfigMap data free of secrets
