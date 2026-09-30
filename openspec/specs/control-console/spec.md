# control-console Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Console system dashboard
The Web Console SHALL display Engine state, Stream counts by lifecycle state, aggregate activity metrics, and recent errors using the Control API.

#### Scenario: Open dashboard
- **WHEN** the operator opens the console against a reachable Engine
- **THEN** the dashboard renders current system and Stream summary data and indicates API errors clearly

### Requirement: Stream inspection and controls
The Web Console SHALL provide Stream list and detail views with topology summary, state, metrics, recent errors, and start/stop/restart actions.

#### Scenario: Restart from Stream detail
- **WHEN** the operator confirms a restart action
- **THEN** the console submits the versioned API command, shows the operation state, and refreshes the Stream status

### Requirement: Schema-driven configuration editing
The Web Console SHALL provide configuration text editing and SHALL use the API-provided JSON Schema and component metadata for validation, completion, and component guidance.

#### Scenario: Invalid configuration feedback
- **WHEN** the operator edits an invalid configuration and requests validation
- **THEN** the console displays structured validation errors with their configuration paths and does not offer a publish action as successful

### Requirement: Configuration publishing and rollback
The Web Console SHALL allow an operator to inspect configuration versions, review validation results, publish a valid configuration, and request rollback to a prior version.

#### Scenario: Publish valid configuration
- **WHEN** the operator submits a validated configuration
- **THEN** the console displays the API application result, affected Streams, and any recovery failure

### Requirement: Safe display of secrets
The Web Console SHALL render redacted values returned by the API and SHALL NOT expose plaintext credentials in browser logs, URLs, or client-side error messages.

#### Scenario: View configured connector credentials
- **WHEN** the operator opens a connector configuration containing credentials
- **THEN** the console displays redaction markers rather than secret values

### Requirement: Credential material SHALL NOT enter the image build context or the repository
The console build chain SHALL prevent accidental credential spread: the `console/` Docker build context SHALL exclude `.env*` files (and `node_modules`, `dist`) via `.dockerignore`; repository ignore rules SHALL cover `.env` and `.env.*` while keeping `.env.example` tracked. Injecting the static token on a controlled build SHALL go through the explicit `ARG VITE_API_TOKEN` channel rather than a `.env` file placed in the build directory. The example configuration SHALL warn that the static token is inlined into the public JavaScript bundle, is for trusted networks only, and that production deployments should use OIDC.

#### Scenario: Local .env stays out of the image
- **WHEN** a `.env` file containing `VITE_API_TOKEN` exists in `console/` and a Docker image build runs
- **THEN** the file is excluded from the build context and appears in no image layer

#### Scenario: Static token injection is explicit
- **WHEN** a controlled build needs the static token
- **THEN** it is passed via `--build-arg VITE_API_TOKEN=...` (the Dockerfile declares the `ARG`), leaving an auditable build command with no dependency on files in the build directory

#### Scenario: A real .env is never committed
- **WHEN** a developer creates `console/.env` (or any `.env.*` other than `.env.example`) and runs `git add`
- **THEN** ignore rules keep the credential out of the repository

#### Scenario: The example file carries the exposure warning
- **WHEN** someone consults `console/.env.example`
- **THEN** it states that the static token is inlined into the public bundle, is for trusted networks only, and that production should use OIDC

