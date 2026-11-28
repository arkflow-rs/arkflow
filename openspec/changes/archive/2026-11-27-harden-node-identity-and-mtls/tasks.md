## Phase 1: Storage & Per-Node Credentials (`cp_node_credentials`)

- [x] 1.1 Bump `SCHEMA_VERSION` from 6 to 7 in `crates/arkflow-server/src/storage/mod.rs`, add `NodeCredentialRecord`, `cp_node_credentials` DDL + `cp_node_credentials_node` index in `sqlite.rs` and `postgres.rs`, and add `"cp_node_credentials"` to `TABLE_ORDER` in `table_copier.rs` right after `"cp_nodes"`.
- [x] 1.2 Add fenced storage methods (`create_node_credential`, `revoke_node_credential`, `rotate_node_credential`) and read methods (`list_node_credentials`, `get_node_credential`, `count_active_node_credentials`) in `storage_ops!`, `StorageBackend`, `SqliteBackend`, and `PostgresBackend`, plus SQLite/table-copier unit tests.
- [x] 1.3 Implement `arkn_<credential_id>_<secret>` parsing, hashing (`SHA-256(credential_id ‖ 0x00 ‖ secret)`), `MAX_CREDENTIALS_PER_NODE` (8), `HubConfig` bootstrap/break-glass flags (`node_bootstrap_disabled`, `allow_shared_node_token_break_glass`), and strict `node_id` binding + bootstrap enrollment in `Hub::register` (`hub/nodes.rs`).
- [x] 1.4 Implement Hub node-credential management methods (`list_node_credentials`, `create_node_credential`, `rotate_node_credential`, `revoke_node_credential`) with immediate in-memory session invalidation on revoke/rotate, and wire routes `/api/v1/nodes/{node_id}/credentials`, `/api/v1/node-credentials/{credential_id}/rotate`, and `/api/v1/node-credentials/{credential_id}` in `api/{mod,nodes}.rs` + `api_contract.rs` gated by `OperatorAction::ManageNodes`.
- [x] 1.5 Wire `RegisterResponse.node_credential`, `AgentConfig.node_credential_file` / `ARKFLOW_NODE_CREDENTIAL_FILE` (`0600` atomic write) in `agent/{config,session}.rs` and `arkflow-core/src/config.rs`, and add cross-node impersonation, bootstrap lock-out, rotation, and revocation tests in `hub/tests.rs` and `api/tests.rs` (`cargo test -p arkflow-server`).

## Phase 2: Node-Attributed Auditing

- [x] 2.1 Add `node` category audit events in `hub/nodes.rs` and `hub/operations.rs` for `agent.register` (accepted and rejected, including `node_identity_mismatch` and `bootstrap_blocked_enrolled_node`), `node_credential.{create,enroll,rotate,revoke}`, `agent.auth_rejected`, and terminal `agent.command_result`, ensuring no token plaintexts or hashes ever enter audit records.
- [x] 2.2 Add unit and API tests verifying audit attribution (`node:<id>` actor, `node_id`, `credential_id` / cert subject, failure codes, secret absence) across SQLite storage (`cargo test -p arkflow-server`).

## Phase 3: Hub Control-Plane mTLS & Agent Client TLS/mTLS

- [x] 3.1 Add `x509-parser = { workspace = true }` (0.18, already in `Cargo.lock`) to `crates/arkflow-core` and `crates/arkflow-server`; add `tls_ca` and `tls_client_auth` (`none` | `agent_required` | `required`) to `ServerConfig` / `HubStartupConfig` / `startup_config.rs` (`ARKFLOW_HUB_TLS_CA`, `ARKFLOW_HUB_TLS_CLIENT_AUTH`) with fail-closed startup validation.
- [x] 3.2 Upgrade `HubTlsListener` (`api/mod.rs`) to configure `WebPkiClientVerifier` when `tls_ca` is set, extract verified peer leaf certificate metadata (`PeerClientCert` including `urn:arkflow:node:<id>` SAN) into Axum `ConnectInfo`, and enforce client cert presence + optional node-id SAN match on `/api/v1/agent/*` (`api/agent_endpoints.rs`).
- [x] 3.3 Add `tls_ca`, `tls_cert`, `tls_key` to `AgentConfig` / `NodeAgentConfig` (`ARKFLOW_AGENT_TLS_CA`, `ARKFLOW_AGENT_TLS_CERT`, `ARKFLOW_AGENT_TLS_KEY`) with all-or-none `(cert, key)` startup validation and wire custom root CA + client `Identity` into `build_agent_client` (`agent/session.rs`).
- [x] 3.4 Add Hub mTLS & Agent mTLS tests (valid mTLS registration, missing client cert rejected on `/agent/*` while `/health` remains accessible in `agent_required`, untrusted CA rejected, `urn:arkflow:node:` SAN mismatch rejected, partial agent TLS config fails startup) (`cargo test -p arkflow-server`).

## Phase 4: Per-Edge Data-Plane Keys & Cert Node Binding

- [x] 4.1 Extend `DataPlaneCredentials` with `hub_provisioned(local_node)`, add `inbound_edge_secrets` + `register_inbound_for_session_with_secret` + `open_edge_deferred_for_job_with_secret` to `NetworkManager`, and extend `RemoteEdgeContext` (`graph.rs`) with per-edge secrets (`edge_secrets`) while keeping `PeerExpectation`'s existing public fields backwards compatible.
- [x] 4.2 Update `authorize_inbound` (`remote/manager.rs`) to resolve per-edge secrets before `claim_handshake_nonce` and fail closed when neither per-edge secret nor `shared_secret` exists; add optional `URI:urn:arkflow:node:<id>` SAN extraction/verification in `DataPlaneTlsConfig` (`remote/transport.rs`) and `authorize_inbound`.
- [x] 4.3 Update Hub split-placement dispatch (`hub/placement.rs`) to generate per-directed-edge `(src_node, dst_node)` secrets and deliver only touching `edge_secrets` per node; update `agent/kernel.rs` and `agent/session.rs` to consume `edge_secrets` and allow `hub_provisioned` data plane when `data_plane_tls` is active without `ARKFLOW_DATA_PLANE_SECRET`.
- [x] 4.4 Add core + server tests for per-edge secret isolation (a third node in the same Job cannot forge an edge between A and B), SAN node-id mismatch rejection on data-plane mTLS, and hub-provisioned split placement without `ARKFLOW_DATA_PLANE_SECRET` (`cargo test -p arkflow-core`, `cargo test -p arkflow-server`).

## Phase 5: Helm Chart Multi-Node mTLS Default

- [x] 5.1 Update `deploy/charts/arkflow` (`values.yaml`, `values-controlplane.yaml`, `values-agent.yaml`, `templates/_helpers.tpl`, `templates/tls-secret.yaml`, `templates/hub-deployment.yaml`, `templates/console-deployment.yaml`, `templates/deployment.yaml`, `templates/NOTES.txt`) so `mode: control-plane` enables control-plane + data-plane mTLS by default (`controlPlane.tls.enabled: true`), supports `controlPlane.tls.enabled=false` opt-out, supports `agent.tls.existingSecret` in `mode: agent`, and wires `ARKFLOW_NODE_CREDENTIAL_FILE` when persistence is enabled.
- [x] 5.2 Update `deploy/charts/assert-renders.sh` and `deploy/charts/arkflow/README.md` to assert default mTLS rendering, opt-out rendering, and `agent.tls.existingSecret`; run `./deploy/charts/assert-renders.sh` and `arkflow --validate` on extracted configs.

## Phase 6: Documentation, Snapshots & Final Verification

- [x] 6.1 Update English docs (`docs/docs/operate/helm.md`, `docs/docs/operate/control-plane/deploy.md`, `docs/docs/operate/tls-matrix.md`, `docs/docs/reference/cli.md`, `docs/docs/reference/api.md`) covering per-node credentials, bootstrap/break-glass semantics, Hub/Agent/data-plane mTLS, opt-out switches, and the threat model.
- [x] 6.2 Update zh-Hans docs (`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/operate/{helm.md,control-plane/deploy.md,tls-matrix.md}`, `reference/{cli.md,api.md}`) and `CHANGELOG.md` (`[Unreleased]`).
- [x] 6.3 Regenerate config schema/inventory snapshots if `EngineConfig` schema changed (`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`), and run `cargo test --workspace --all-targets`, `cargo clippy --workspace --all-targets`, `cargo fmt --check`, and `pnpm --dir docs docs:check`.
