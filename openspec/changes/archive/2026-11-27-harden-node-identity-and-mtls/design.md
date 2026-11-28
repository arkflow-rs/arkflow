## Design Decisions

### D1. Durable Per-Node Credentials (`cp_node_credentials`) & `arkn_` Token Format

- **Schema (`SCHEMA_VERSION` 6 -> 7)**:
  - Add `cp_node_credentials` in both `crates/arkflow-server/src/storage/sqlite.rs` and `postgres.rs` with columns:
    `credential_id TEXT PRIMARY KEY`, `node_id TEXT NOT NULL`, `secret_hash TEXT NOT NULL`, `label TEXT NOT NULL DEFAULT ''`, `created_at_ms INTEGER NOT NULL`, `expires_at_ms INTEGER`, `last_used_at_ms INTEGER`, `revoked_at_ms INTEGER`, plus index `cp_node_credentials_node ON cp_node_credentials(node_id)`.
  - Add `"cp_node_credentials"` to `TABLE_ORDER` in `crates/arkflow-server/src/storage/table_copier.rs` right after `"cp_nodes"` so `backup`, `restore`, and `migrate` copy per-node credential hashes cleanly without dropping enrolled identities.
  - Note: Unlike operator `cp_credentials` (which references `cp_users`), a node credential MAY be pre-provisioned for a `node_id` before that node's first `register()` inserts a `cp_nodes` row, so `cp_node_credentials.node_id` has no foreign key constraint against `cp_nodes`.
- **Token format & verification**:
  - Format: `arkn_<credential_id>_<secret>`, where `<credential_id>` is a 16-hex-char random identifier and `<secret>` is 32 OS-random bytes encoded as 64 hex chars. Distinct `arkn_` prefix guarantees zero ambiguity with operator `ark_` tokens or bootstrap `node_token` strings.
  - Hash: reuse `SHA-256(credential_id ‖ 0x00 ‖ secret)` (`hub::identity::hash_secret`) and constant-time equality (`subtle::ConstantTimeEq`).
  - Cap: `MAX_CREDENTIALS_PER_NODE = 8` active (unrevoked, i.e., `revoked_at_ms IS NULL`) credentials per `node_id` so rotation overlap is effortless while bounding storage per node.
- **Storage actor operations** (`storage/mod.rs`, `sqlite.rs`, `postgres_methods.rs`):
  - Add fenced `create_node_credential`, `revoke_node_credential` (returning `Option<String>` = owning `node_id` when newly revoked), `rotate_node_credential` (atomic revoke old + insert new in one transaction), and plain `list_node_credentials(node_id)` / `get_node_credential(credential_id)` / `count_active_node_credentials(node_id)`.
  - Serialization lock: add `node_identity_write_lock: Arc<tokio::sync::Mutex<()>>` on `Hub` to serialize credential issuance/enrollment/rotation per Hub alongside the DB transaction's atomicity.

### D2. Registration, Bootstrap Enrollment, and Immediate Revocation

- **`Hub::register` decision tree (`crates/arkflow-server/src/hub/nodes.rs`)**:
  1. Verify mTLS peer certificate constraint first (D4) when Hub mTLS is enabled.
  2. Inspect `request.node_token`:
     - **Case A (`arkn_<cred_id>_<secret>`)**:
       - Requires `self.storage.is_some()`; volatile Hubs fail closed (`401`).
       - Look up `credential_id` in `cp_node_credentials`. Reject with `401` if absent, revoked (`revoked_at_ms.is_some()`), expired (`expires_at_ms <= Some(now)`), secret hash mismatch, OR **`record.node_id != request.node_id`** (cross-node impersonation check, audited with `failure_code = "node_identity_mismatch"`).
       - Accept and record `authenticated_credential_id = Some(cred_id)` on `NodeRecord`.
     - **Case B (shared `node_token` or `insecure_local` without `node_token`)**:
       - If `self.config.node_token` is `None`: allow only when `self.config.insecure_local == true` (loopback dev mode).
       - Otherwise verify `ct_eq(request.node_token, expected)`. If mismatch -> `401` + audit.
       - If `self.storage` is `None` (volatile Hub): accept as before (`node_credential: None`).
       - If `self.storage` is `Some(storage)`:
         - If `self.config.node_bootstrap_disabled`: reject with `401` + audit (`failure_code = "bootstrap_disabled"`).
         - Check active per-node credentials for `request.node_id` (`count_active_node_credentials` excluding expired/revoked):
           - If `active_count == 0`: under `node_identity_write_lock` (re-checking `active_count == 0` inside lock so concurrent boot races cannot mint duplicates), mint a fresh `arkn_` credential (`label = "bootstrap"`), persist it, return `RegisterResponse.node_credential = Some(plaintext)`, and audit `node_credential.enroll`.
           - If `active_count > 0`:
             - If `self.config.allow_shared_node_token_break_glass`: accept (break-glass override, audited with `auth_mode=break_glass`).
             - Otherwise: **reject** with `401` + audit (`failure_code = "bootstrap_blocked_enrolled_node"`).
- **Immediate session invalidation on revoke/rotate**:
  - `NodeRecord` records `credential_id: Option<String>` and `cert_node_id: Option<String>`.
  - When `Hub::revoke_node_credential` or `Hub::rotate_node_credential` revokes `cred_id` for `node_id`, it acquires `self.nodes.write().await` and clears/expires (`session_expires_at_ms = 0`, randomized/wiped `session_token`) any `NodeRecord` for `node_id` whose `credential_id.as_deref() == Some(cred_id)`. Subsequent `heartbeat`, `report`, `commands`, `command_result`, and `job_observation` requests immediately fail with `401 Unauthorized`.
- **Agent credential retention (`crates/arkflow-server/src/agent/{config,session}.rs`)**:
  - `AgentConfig` / `NodeAgentConfig` gain `#[serde(default)] pub node_credential_file: Option<String>` (env override `ARKFLOW_NODE_CREDENTIAL_FILE`).
  - At Agent startup, if `node_credential_file` exists and is non-empty (trimmed), Agent loads it as initial `node_token` instead of the bootstrap `node_token`.
  - When `RegisterResponse.node_credential` is `Some(token)`, Agent updates `active_config.node_token = token` for all subsequent re-registrations and, if `node_credential_file` is configured, writes it atomically (temp file + rename, `0600` permissions on Unix).
- **Operator API (`crates/arkflow-server/src/api/nodes.rs`, `api_contract.rs`)**:
  - Add `GET /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/nodes/{node_id}/credentials`, `POST /api/v1/node-credentials/{credential_id}/rotate`, `DELETE /api/v1/node-credentials/{credential_id}`.
  - Authorize with `require_operator_action(&hub, &headers, OperatorAction::ManageNodes, "node", Some(node_id))` (resolving owning `node_id` first on rotate/delete before authorizing, returning `404` for unknown `credential_id` only after verifying `ManageNodes` capability at fleet/wildcard level or resolving owning node so unauthorized callers get `403`).

### D3. Node-Attributed Auditing (`crates/arkflow-server/src/hub/`)

- Audit events use `category = "node"` via `record_audit_best_effort` (counting failures in `arkflow_audit_write_failures_total{category="node"}` without blocking security responses):
  - `agent.register` (`accepted` / `rejected`): `actor = Some(format!("node:{}", node_id))`, `resource_type = "node"`, `resource_id = Some(node_id)`, `node_id = Some(node_id)`, `failure_code` on rejection, scalar `message` containing `auth_mode=...` and `credential_id=...` / `cert_identity=...` (never token/hash).
  - `node_credential.create`, `node_credential.enroll`, `node_credential.rotate`, `node_credential.revoke`: `actor` = acting operator id (or `node:<id>` for enroll), `resource_type = "node"`, `resource_id = Some(node_id)`, `node_id = Some(node_id)`, `message` containing `credential_id=...`.
  - `agent.auth_rejected`: emitted when an authenticated agent endpoint (`heartbeat`, `report`, `commands`, `command_result`, `job_observation`) fails mTLS peer cert verification or cert-to-node binding.
  - `agent.command_result`: emitted when `Hub::command_result` applies a **terminal** state transition (`Succeeded`, `Failed`, `TimedOut`, `Cancelled`), with `actor = Some(format!("node:{}", auth.node_id))`, `resource_type = "operation"`, `resource_id = Some(operation.id)`, `node_id = Some(auth.node_id)`, `outcome`, `message` naming `operation`, `job/stream` id, and `credential_id` when present.

### D4. Hub Control-Plane mTLS & Agent Client TLS/mTLS

- **Hub listener (`crates/arkflow-server/src/api/mod.rs`, `startup_config.rs`)**:
  - Add `pub tls_ca: Option<String>` (`ARKFLOW_HUB_TLS_CA`) and `pub tls_client_auth: HubTlsClientAuth` (`agent_required` | `required` | `none`, env `ARKFLOW_HUB_TLS_CLIENT_AUTH`, defaulting to `agent_required` when `tls_ca` is set and `none` when `tls_ca` is unset) to `ServerConfig`.
  - Reject partial/invalid configurations at startup:
    - `tls_ca` set without `tls_cert` + `tls_key` -> fail startup.
    - `tls_client_auth != none` without `tls_ca` -> fail startup.
    - Unreadable or unparsable `tls_ca` -> fail startup.
  - When `tls_ca` is configured:
    - `agent_required`: `WebPkiClientVerifier::builder(ca_store).allow_unauthenticated().build()`. TLS handshake succeeds without client cert (for `/health`, `/readiness`, `/liveness`, `/metrics`, Console proxy), rejects any invalid/untrusted client cert at TLS handshake, and attaches `PeerClientCert` (parsed from `TlsStream::get_ref().1.peer_certificates()` via `x509-parser` 0.18) to `HubTlsConnectInfo` via Axum `Connected`.
    - `required`: `WebPkiClientVerifier::builder(ca_store).build()` (rejects unauthenticated clients at TLS handshake).
  - Middleware / extractor on `/agent/*` routes (`agent_endpoints.rs`): when `hub.mtls_enforced()` (i.e. `tls_ca` is set), every `/agent/*` request MUST carry a verified `PeerClientCert` (`401 Unauthorized` + audit otherwise). If the leaf cert carries one or more `URI:urn:arkflow:node:<cert_node_id>` SANs, verify `<cert_node_id>` matches the request's `node_id` (`401 Unauthorized` + audit `cert_node_identity_mismatch` otherwise); certs without a `urn:arkflow:node:` SAN still prove fleet CA membership while `arkn_` proves per-node identity.
- **Agent HTTP client (`crates/arkflow-core/src/config.rs`, `crates/arkflow-server/src/agent/{config,session}.rs`)**:
  - Add optional `tls_ca`, `tls_cert`, `tls_key` to `AgentConfig` and `NodeAgentConfig` with env overrides `ARKFLOW_AGENT_TLS_CA`, `ARKFLOW_AGENT_TLS_CERT`, `ARKFLOW_AGENT_TLS_KEY` (env overrides file value if set).
  - Validate `(tls_cert, tls_key)` all-or-none at Agent startup: setting only one of `cert` or `key`, or unreadable/invalid PEM material for `ca`/`cert`/`key`, returns an explicit configuration error before starting the session loop.
  - Wire into `build_agent_client`: `.add_root_certificate(...)` when `tls_ca` is set, and `.identity(reqwest::Identity::from_pem(...))` when `(tls_cert, tls_key)` are set.

### D5. Per-Edge Data-Plane Shuffle Keys & Optional Cert Node Binding

- **Hub per-edge secret distribution (`crates/arkflow-server/src/hub/placement.rs`)**:
  - When building a split-placement `job_start` payload (`spec.placement == Split`), the Hub determines all distinct cross-node pairs `(src_node, dst_node)` (`src_node != dst_node`) present in the Job plan's `edges` under `assignments` (or all ordered pairs among `targets` where `src != dst`? Deriving from actual `plan.edges` under `assignments` is exact; if an operator connects multiple subtasks across `(A, B)`, one fresh 32-byte random secret per `(A, B)` covers every quad on `A -> B`).
  - Wait: `placement.rs` currently builds one `split_payload` JSON shared across all nodes in the job! To avoid sending Node A's secrets for `B -> C` to Node A, `placement.rs` injects per-target `edge_secrets` when constructing each target node's `job_start` command payload: only keys `"<src>-><dst>"` where `src == target_node || dst == target_node` are included!
- **Core remote runtime (`crates/arkflow-core/src/executor/remote/{auth,manager,transport}.rs`, `graph.rs`)**:
  - `DataPlaneCredentials` supports both `DataPlaneCredentials::new(local_node, shared_secret)` (`shared_secret: Some(...)`) and `DataPlaneCredentials::hub_provisioned(local_node)` (`shared_secret: None`).
  - `PeerExpectation` gains `#[cfg_attr(...)] pub edge_secret: Option<Arc<[u8]>>` (default `None` — existing struct literals can use `PeerExpectation::new(source_node, job_id, generation)` or keep `edge_secret: None` if we provide a helper or check tests; wait: `PeerExpectation` is `pub` in `arkflow-core` and constructed with struct literal syntax in `tests.rs` ~18 times, so keeping `pub edge_secret: Option<Arc<[u8]>>` or adding `edge_secrets: BTreeMap<EdgeSessionKey, Arc<[u8]>>` on `NetworkManager` via `register_inbound_for_session_with_secret` avoids touching 18 test struct literals!).
    - **Surgical choice**: Keep `PeerExpectation`'s existing 3 public fields untouched (`source_node`, `job_id`, `generation`) and store `inbound_edge_secrets: RwLock<BTreeMap<EdgeSessionKey, Arc<[u8]>>>` on `NetworkManager` (populated by `register_inbound_for_session_with_secret` and cleaned up in `remove_inbound_session` / `remove_job_session`), plus `open_edge_deferred_for_job_with_secret` on `NetworkManager` and `edge_secrets: BTreeMap<(String, String), Arc<[u8]>>` on `RemoteEdgeContext`! Zero churn to existing `PeerExpectation` callers.
  - In `authorize_inbound`: look up `expected` and optional `edge_secret` first (after structural header/version/destination checks), pick `edge_secret` if present else `credentials.shared_secret` (failing closed if neither exists!), verify HMAC BEFORE `claim_handshake_nonce`, and construct `SessionAuth` using that resolved secret so `FrameCrypto` derives session keys from the per-edge secret.
  - In `agent/session.rs`: if `ARKFLOW_DATA_PLANE_SECRET` is unset AND `data_plane_tls` (`ARKFLOW_DATA_PLANE_TLS_*`) IS configured, instantiate `DataPlaneCredentials::hub_provisioned(node_id)` instead of disabling shuffle! If both `ARKFLOW_DATA_PLANE_SECRET` and `data_plane_tls` are absent, preserve the existing colocated-only fallback (`data_plane_setup_advertises_or_degrades_gracefully` Case 5).
- **Optional data-plane cert SAN binding (`transport.rs`, `manager.rs`)**:
  - Extract `peer_node_id_from_der(cert_der)` using `x509-parser` (scanning `SubjectAlternativeName` for `GeneralName::URI("urn:arkflow:node:<id>")`).
  - In `DataPlaneTlsConfig::connect_for_node(stream, expected_peer)` / `accept_with_peer_id(stream)`: if the peer certificate carries `urn:arkflow:node:<id>`, return the extracted node id so `TcpEdgeTransport` rejects if `<id> != destination_node` and `authorize_inbound` rejects if `<id> != request.source_node`. Certs without a `urn:arkflow:node:` SAN (carrying only `DNS:arkflow-data-plane`) stay accepted.

### D6. Helm Chart Multi-Node mTLS Default (`deploy/charts/arkflow/`)

- Add `controlPlane.tls`:
  ```yaml
  tls:
    enabled: true
    existingSecret: ""
    caKey: ca.crt
    hubCertKey: hub.crt
    hubKeyKey: hub.key
    agentCertKey: agent.crt
    agentKeyKey: agent.key
  ```
- Add `templates/tls-secret.yaml`: when `mode == "control-plane"` and `controlPlane.tls.enabled` and not `controlPlane.tls.existingSecret`, generate and keep (`lookup` + `helm.sh/resource-policy: keep`) a Fleet CA via `genCA` and signed Hub + Agent certs via `genSignedCert` (Hub SANs include `<hubName>`, `<hubName>.<ns>`, `<hubName>.<ns>.svc`, `localhost`, `127.0.0.1`; Agent SANs include `arkflow-data-plane`, `<workloadName>`, `*.<workloadName>.<ns>.svc.cluster.local`, `localhost`).
- Update `hub-deployment.yaml`: when `controlPlane.tls.enabled`, mount `/etc/arkflow/tls` read-only, set `ARKFLOW_HUB_TLS_CERT`, `ARKFLOW_HUB_TLS_KEY`, `ARKFLOW_HUB_TLS_CA`, and set `scheme: HTTPS` on liveness/readiness `httpGet` probes.
- Update `console-deployment.yaml`: set `ARKFLOW_HUB_UPSTREAM` to `https://<hubName>:8080` when `controlPlane.tls.enabled` (`http://...` when `false`).
- Update `deployment.yaml`:
  - In `mode: control-plane` with `controlPlane.agent.enabled` and `controlPlane.tls.enabled`, mount `/etc/arkflow/tls` and inject `ARKFLOW_AGENT_TLS_{CERT,KEY,CA}` + `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}`. Also, when `persistence.enabled` is true on the in-chart agent, set `ARKFLOW_NODE_CREDENTIAL_FILE` under `persistence.mountPath`.
  - In `mode: agent`, add `agent.tls.existingSecret` (with `caKey`, `certKey`, `keyKey` defaulting to `ca.crt`, `agent.crt`, `agent.key`): when set, mount `/etc/arkflow/tls` and inject `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*`.
- Update `values-controlplane.yaml`: set `health_check.hub_urls: ["https://arkflow-hub:8080"]` so default `control-plane` renders match Hub TLS out of the box.
- Update `deploy/charts/assert-renders.sh` to assert mTLS enabled by default in `control-plane`, opt-out via `--set controlPlane.tls.enabled=false`, and `agent.tls.existingSecret` in `mode: agent`.
