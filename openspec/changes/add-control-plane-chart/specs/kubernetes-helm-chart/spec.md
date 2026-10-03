## ADDED Requirements

### Requirement: Control-plane install mode renders Hub and Console

With `mode: control-plane`, the chart SHALL render a Hub Deployment and Service (image `arkflow-server`, listening on `0.0.0.0:8080` via its env surface), a Console Deployment and Service serving the static console with the same-origin `/api` and `/metrics` reverse proxy, and SHALL keep the existing standalone/agent renders unchanged in all other modes.

#### Scenario: Control-plane install
- **WHEN** a user runs `helm install` with `mode=control-plane` and default values
- **THEN** the release renders Hub and Console Deployments with their Services, a shared node-credential Secret, and SQLite-on-PVC Hub storage, and the rendered console proxy targets the in-chart Hub Service

#### Scenario: Existing modes unchanged
- **WHEN** `helm template` renders standalone and agent modes
- **THEN** the output contains no Hub, Console, or control-plane resources, identical to the pre-control-plane renders

### Requirement: Configurable console upstream

The Console image SHALL resolve its Hub upstream at container start from an environment variable (default `http://arkflow-hub:8080`) via an nginx template, and the chart SHALL set that variable to the in-chart Hub Service address.

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

Control-plane mode SHALL support an opt-in agent Deployment joining the in-chart Hub, reusing the agent-mode rendering contract (data-plane port, headless Service, pod-name node identity) with `hub_urls` defaulting to the in-chart Hub Service.

#### Scenario: Agent enabled
- **WHEN** the user enables the control-plane agent
- **THEN** the release renders the agent workload whose config references the in-chart Hub, the shared node credential, and the data-plane surface

### Requirement: Server and console images are published

CI SHALL build and publish the Hub image (`arkflow-server`) and Console image (`arkflow-console`) from the same repository on main pushes and release tags, with per-image build caches, alongside the existing engine image; CI render assertions SHALL cover the control-plane mode on every chart change.

#### Scenario: Release publishes three images
- **WHEN** a release tag is cut
- **THEN** engine, server, and console images are published to the registry namespace and the chart's control-plane default values resolve to published tags

#### Scenario: Control-plane render gates
- **WHEN** a pull request modifies the chart
- **THEN** CI renders control-plane mode and asserts the Hub/Console surfaces, the storage contract, and the shared-credential wiring
