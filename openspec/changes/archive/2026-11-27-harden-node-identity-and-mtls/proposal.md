## Why

`PRODUCT-ITERATION-2026-11-22.md` registers PA-P1-07 — three structural gaps in compute-node authentication, control-plane/data-plane TLS, and fleet deployment defaults:

1. **Fleet-shared `node_token` allows any node to impersonate any other `node_id`**: `Hub::register` (`crates/arkflow-server/src/hub/nodes.rs:263-280`) verifies `request.node_token` against the single fleet-wide `self.config.node_token` (`crates/arkflow-server/src/hub.rs:77`) and immediately replaces `NodeRecord` for `request.node_id` (`nodes.rs:384-465`). Any holder of the shared token can register as an existing online node, hijack its session/commands/labels/jobs, and forge its reports. There is no durable per-node credential store, no rotation/revocation path, and `register_after_auth` only audits `incompatible_protocol` (`nodes.rs:291-308`), leaving normal registrations, auth rejections, and command completions without node-attributed audit records.
2. **Hub TLS disables client authentication, and Agent has no Hub client mTLS surface**: `serve_hub` (`crates/arkflow-server/src/api/mod.rs:1092-1095`) hard-codes `rustls::ServerConfig::builder().with_no_client_auth()` with no `tls_ca` / client-auth option, and `build_agent_client` (`crates/arkflow-server/src/agent/session.rs:295-307`) creates a bare `reqwest::Client` without a custom CA root or client identity certificate. Meanwhile, the Helm chart's multi-node `control-plane` mode (`deploy/charts/arkflow/values-controlplane.yaml:34`, `templates/hub-deployment.yaml:38-78`, `templates/deployment.yaml:56-71`) deploys plaintext HTTP between Hub, Console, and Agents and leaves data-plane mTLS unconfigured.
3. **Data-plane shuffle authentication relies on a single fleet-wide HMAC secret and lacks peer-node certificate identity binding**: `agent::session::run` (`crates/arkflow-server/src/agent/session.rs:50-92`) requires `ARKFLOW_DATA_PLANE_SECRET` (shared across every node in the fleet) and builds `DataPlaneCredentials::new(node_id, data_secret)`, while `authorize_inbound` (`crates/arkflow-core/src/executor/remote/manager.rs:2083`) verifies all inbound handshakes against that single shared secret. Any participating node holding `ARKFLOW_DATA_PLANE_SECRET` can forge `source_node` for another node on a split-placed edge. `DataPlaneTlsConfig` (`crates/arkflow-core/src/executor/remote/transport.rs:476-545`) verifies only fleet CA trust + fixed `DNS:arkflow-data-plane` SAN without binding peer certificate node identity when present.

## What Changes

- **Durable per-node credentials (`cp_node_credentials`) & bootstrap-only shared `node_token`**:
  - Add `cp_node_credentials` table in SQLite and PostgreSQL (`SCHEMA_VERSION` 6 -> 7, included in `TABLE_ORDER` for `backup`/`restore`/`migrate`), storing `SHA-256(credential_id ‖ 0x00 ‖ secret)` hashes (constant-time `ct_eq` verification, max 8 active credentials per `node_id`) for `arkn_<credential_id>_<secret>` tokens.
  - Add Hub operator endpoints (`GET /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/node-credentials/{credential_id}/rotate`, `DELETE /api/v1/node-credentials/{credential_id}`) gated by `OperatorAction::ManageNodes` on `("node", Some(node_id))`. Revoking or rotating a credential immediately invalidates active in-memory sessions authenticated by that credential.
  - Enforce strict node binding in `Hub::register`: a per-node credential (`arkn_*`) is accepted ONLY when `record.node_id == request.node_id` and not revoked/expired.
  - Restrict shared `node_token` to bootstrap/break-glass on durable Hubs: on initial registration of a `node_id` with no active per-node credentials (and `node_bootstrap_disabled == false`), Hub mints and stores a per-node credential (`label = "bootstrap"`) and returns it once in `RegisterResponse.node_credential`; once `node_id` holds an active per-node credential, further registrations with the shared `node_token` fail closed (`401`) unless `allow_shared_node_token_break_glass` is explicitly enabled. Volatile (`insecure_local`) Hubs preserve pre-existing shared-token behavior.
  - Update Agent (`NodeAgentConfig` / `agent::session`) to retain `RegisterResponse.node_credential` in memory for subsequent re-registrations and optionally persist it (`0600`) to `node_credential_file` / `ARKFLOW_NODE_CREDENTIAL_FILE`.
- **Node-attributed control-plane auditing**:
  - Audit accepted and rejected `agent.register`, `node_credential.{create,enroll,rotate,revoke}`, `agent.auth_rejected`, and terminal `agent.command_result` with `actor = "node:<node_id>"` (or acting operator id for admin mutations), `node_id`, `credential_id` / cert subject metadata — never token plaintexts or hashes.
- **Hub control-plane mTLS & Agent client TLS/mTLS**:
  - Extend `ServerConfig` / `HubStartupConfig` with `tls_ca` (`ARKFLOW_HUB_TLS_CA`) and `tls_client_auth` (`ARKFLOW_HUB_TLS_CLIENT_AUTH`: `agent_required` default when `tls_ca` is set, or `required` / `none`).
  - Use `WebPkiClientVerifier` in `HubTlsListener` (`agent_required` uses `.allow_unauthenticated()` so `/health`, `/readiness`, `/liveness`, `/metrics`, and Console proxy traffic remain TLS-encrypted without forcing browser/kubelet client certs, while `/api/v1/agent/*` strictly requires a verified peer certificate).
  - Parse optional `URI:urn:arkflow:node:<node_id>` SAN from verified peer certificates via `x509-parser`; when present, enforce `cert_node_id == node_id` on `/api/v1/agent/*` (both `register` and authenticated session calls).
  - Extend `AgentConfig` / `NodeAgentConfig` with `tls_ca`, `tls_cert`, `tls_key` (and `ARKFLOW_AGENT_TLS_CA`, `ARKFLOW_AGENT_TLS_CERT`, `ARKFLOW_AGENT_TLS_KEY`) wired into `build_agent_client`; `(tls_cert, tls_key)` enforce all-or-none fail-closed startup validation.
- **Per-edge data-plane shuffle keys & optional cert node-identity binding**:
  - In Hub split-placement dispatch (`crates/arkflow-server/src/hub/placement.rs`), generate a fresh 32-byte secret per directed cross-node pair `(source_node, destination_node)` and deliver each node ONLY the `edge_secrets` touching that node in `job_start`.
  - Extend `RemoteEdgeContext`, `PeerExpectation`, and `NetworkManager` (`crates/arkflow-core/src/executor/remote/`) to authenticate handshakes and derive `FrameCrypto` session keys from per-edge secrets when present, and add `DataPlaneCredentials::hub_provisioned(local_node)` so Agents running data-plane mTLS (`ARKFLOW_DATA_PLANE_TLS_*`) can participate in split placement without a fleet-wide `ARKFLOW_DATA_PLANE_SECRET` (keeping the missing-secret colocated-only fallback unchanged when mTLS is absent).
  - Bind optional `URI:urn:arkflow:node:<node_id>` SAN on `DataPlaneTlsConfig` peer certificates to `expected.source_node` (inbound) and `destination_node` (outbound), while keeping certificates carrying only `DNS:arkflow-data-plane` backwards compatible.
- **Helm multi-node mTLS by default + documentation**:
  - Update `deploy/charts/arkflow` `mode: control-plane` to default `controlPlane.tls.enabled: true`: generate and keep a Fleet CA + Hub/Agent certs Secret (`genCA` / `genSignedCert`, or `controlPlane.tls.existingSecret`), configure Hub `ARKFLOW_HUB_TLS_{CERT,KEY,CA}` + HTTPS probes, Console `https://` upstream, and in-chart Agent `ARKFLOW_AGENT_TLS_*` + `ARKFLOW_DATA_PLANE_TLS_*`, plus `values-controlplane.yaml` `https://arkflow-hub:8080`. Provide `controlPlane.tls.enabled=false` as the explicit opt-out switch, and support `agent.tls.existingSecret` in `mode: agent`.
  - Update EN and zh-Hans documentation (`helm.md`, `control-plane/deploy.md`, `tls-matrix.md`, `reference/cli.md`, `reference/api.md`, `CHANGELOG.md`).

## Non-goals

- Do not build an online ACME/CA signing daemon or automatic background certificate rotation; certificate generation stays with Helm (`genCA`/`genSignedCert`), `cert-manager`, or operator PKI tooling, and rotation remains a process restart (or Hub credential rotation API for token credentials).
- Do not bind `ARKFLOW_NODE_TOKEN` or operator tokens to Kubernetes ServiceAccount TokenReview APIs (keep ArkFlow self-contained outside Kubernetes too).
- Do not break existing `unsafe` / local `insecure_local` single-node development workflows (`mode: standalone` and loopback `insecure_local` Hub remain plaintext by default).
- Do not audit every high-frequency `agent/heartbeat`, `agent/report`, or `agent/commands` poll on success (only registrations, credential mutations, auth rejections, and terminal `command_result` deliveries are audited to avoid flooding `cp_audit_events`).
- Do not alter the v2 data-plane wire framing format or `FrameCrypto` HKDF/HMAC construction (`arkflow-data-plane/v2`); per-edge secrets drop cleanly into the existing secret input of `SessionAuth` / `PeerExpectation`.

### New Capabilities
- `node-identity`: Durable per-node credentials (`cp_node_credentials`), strict `node_id` binding, bootstrap-only shared token transition, operator node-credential lifecycle API, and Agent credential retention.

### Modified Capabilities
- `secure-durable-control-plane`: Hub mTLS (`tls_ca`, `tls_client_auth`), Agent client TLS/mTLS (`ARKFLOW_AGENT_TLS_*`), SAN node binding on `/agent/*`, and fail-closed per-node/bootstrap rejection.
- `control-plane-identity`: Node-attributed auditing for agent registration, node-credential lifecycle, auth rejections, and terminal command results.
- `authenticated-network-shuffle`: Hub-provisioned per-edge shuffle secrets (`edge_secrets`), `hub_provisioned` data-plane mode under mTLS, and optional peer certificate `urn:arkflow:node:<id>` SAN binding.
- `kubernetes-helm-chart`: Multi-node `control-plane` mode enables control-plane + data-plane mTLS by default with explicit `controlPlane.tls.enabled=false` opt-out and `agent.tls.existingSecret` support.
- `control-plane-deployment`: Document new `arkflow-server` / Agent TLS & node-bootstrap flags/env vars, Helm mTLS toggle, and node/data-plane threat model in EN and zh-Hans docs.

## Impact

- **Code**:
  - `Cargo.toml`, `crates/arkflow-core/Cargo.toml`, `crates/arkflow-server/Cargo.toml` (`x509-parser` already in `Cargo.lock`).
  - `crates/arkflow-core/src/config.rs`, `crates/arkflow-core/src/executor/graph.rs`, `crates/arkflow-core/src/executor/remote/{auth,manager,transport}.rs`.
  - `crates/arkflow-server/src/storage/{mod,sqlite,postgres,postgres_methods,table_copier}.rs`.
  - `crates/arkflow-server/src/hub.rs`, `crates/arkflow-server/src/hub/{wire,error,nodes,operations,placement,identity,observability}.rs`.
  - `crates/arkflow-server/src/api/{mod,nodes,agent_endpoints}.rs`, `crates/arkflow-server/src/api_contract.rs`, `crates/arkflow-server/src/startup_config.rs`, `crates/arkflow-server/src/bin/arkflow-server.rs`.
  - `crates/arkflow-server/src/agent/{config,session,resources,commands,kernel}.rs`.
  - `deploy/charts/arkflow/{values.yaml,values-controlplane.yaml,values-agent.yaml,templates/*,assert-renders.sh}`.
- **Tests**: Unit, cross-node impersonation attack tests, Hub/Agent mTLS integration tests, per-edge shuffle isolation tests, storage migration/copy tests, and Helm render assertions.
- **Docs**: EN + zh-Hans updates to `operate/helm.md`, `operate/control-plane/deploy.md`, `operate/tls-matrix.md`, `reference/cli.md`, `reference/api.md`, `docs/static/config-schema.json` (if regenerated), and `CHANGELOG.md`.
- **Compatibility**:
  - `SCHEMA_VERSION` bumps from `6` to `7` (additive `CREATE TABLE IF NOT EXISTS cp_node_credentials`).
  - Pre-existing Agents sharing `node_token` against a volatile Hub or single-boot durable Hub enroll seamlessly; re-registering an already-enrolled `node_id` with the shared `node_token` is intentionally blocked unless the returned `node_credential` is used or `allow_shared_node_token_break_glass` is enabled.
  - Helm `mode: control-plane` defaults to mTLS enabled (`controlPlane.tls.enabled: true`); upgrades or local tests expecting plaintext HTTP must set `controlPlane.tls.enabled=false` or use `https://`.
