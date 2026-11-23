## MODIFIED Requirements

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
