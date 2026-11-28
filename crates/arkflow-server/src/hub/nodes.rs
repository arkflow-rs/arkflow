//! Node registry and the Agent pull protocol: register, heartbeat, report, commands.

use super::error::HubError;
use super::identity::{generate_credential_id, generate_secret, hash_secret};
use super::placement::node_under_pressure;
use super::wire::{
    default_protocol_version, AgentAuth, AgentCommand, AgentOperation, HeartbeatRequest, HubEvent,
    HubNode, HubOperation, HubOperationState, NodeConnectionState, NodeReport, RegisterRequest,
    RegisterResponse,
};
use super::{
    now_ms, persist_operation, Hub, NodeRecord, PeerClientCert, MAX_EVENTS, MAX_NODES,
    SUPPORTED_PROTOCOL_VERSION,
};
use crate::api_contract::{OperatorRole, ResourceScope};
use crate::storage::{AuditRecord, NodeCredentialRecord, NodeMutation, ObservedMutation};
use arkflow_core::control::NodeMaintenanceState;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use subtle::ConstantTimeEq;

pub(crate) const NODE_TOKEN_PREFIX: &str = "arkn_";
pub(crate) const MAX_CREDENTIALS_PER_NODE: usize = 8;

pub(crate) fn parse_node_managed_token(token: &str) -> Option<(&str, &str)> {
    let rest = token.strip_prefix(NODE_TOKEN_PREFIX)?;
    let (credential_id, secret) = rest.split_once('_')?;
    if credential_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((credential_id, secret))
}

const ALLOWED_NODE_METRICS: &[&str] = &[
    "input_batches",
    "input_messages",
    "processing_errors",
    "output_batches",
    "output_messages",
    "input_errors",
    "input_reconnects",
    "output_errors",
    "restarts",
    "streams_total",
    "streams_running",
    "kernel_batches_in",
    "kernel_batches_out",
    "kernel_rows",
    "kernel_errors",
    "in_flight",
    "mean_latency_us",
    "checkpoint_duration_ms",
    "checkpoint_failures",
    "watermark_lag_ms",
    "late_events",
    "jobs_total",
    "jobs_running",
    "jobs_ephemeral_state",
    "jobs_recovery_required",
    // Host resource gauges sampled by the Agent (see agent.rs
    // ResourceSampler): ephemeral registry state, exported as-is.
    "node_cpu_usage_percent",
    "node_cpu_cores",
    "node_memory_used_bytes",
    "node_memory_total_bytes",
    "node_memory_available_bytes",
];

fn authenticated_node<'a>(
    nodes: &'a mut BTreeMap<String, NodeRecord>,
    auth: &AgentAuth,
) -> Result<&'a mut NodeRecord, HubError> {
    let node = nodes.get_mut(&auth.node_id).ok_or(HubError::Unauthorized)?;
    if !bool::from(
        auth.session_token
            .as_bytes()
            .ct_eq(node.session_token.as_bytes()),
    ) || now_ms() > node.session_expires_at_ms
        || node.bootstrap_rotation_pending
        || node
            .cert_node_id
            .as_deref()
            .is_some_and(|bound| bound != auth.node_id)
    {
        return Err(HubError::Unauthorized);
    }
    node.bootstrap_unconfirmed = false;
    Ok(node)
}

fn find_reclaimable_node(
    nodes: &BTreeMap<String, NodeRecord>,
    node_id: &str,
    now: u64,
) -> Result<Option<String>, HubError> {
    if nodes.len() < MAX_NODES || nodes.contains_key(node_id) {
        return Ok(None);
    }
    // Retired nodes used to occupy the fixed registry forever: the expiry
    // sweep only changes their state, never removes the map entry. Reclaim
    // one expired/offline slot instead of rejecting a live node for the
    // whole process lifetime.
    nodes
        .iter()
        .find(|(id, record)| {
            id.as_str() != node_id
                && (record.resource.state != NodeConnectionState::Online
                    || record.resource.lease_expires_at_ms <= now)
        })
        .map(|(id, _)| Some(id.clone()))
        .ok_or(HubError::Capacity)
}

pub(crate) fn sanitize_metrics(metrics: BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    metrics
        .into_iter()
        .filter(|(key, value)| {
            ALLOWED_NODE_METRICS.contains(&key.as_str()) && value.is_finite() && *value >= 0.0
        })
        .collect()
}

/// Upper bound on per-Job snapshots kept for one node. Jobs are
/// operator-configured so this is generous; a misbehaving Agent cannot grow
/// Hub memory without bound.
const MAX_REPORTED_JOBS_PER_NODE: usize = 256;

/// Keep the bounded set of reported Job snapshots, discarding finite values
/// only: series cardinality stays at O(jobs x chains) per node.
fn bounded_job_snapshots(
    jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
) -> BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot> {
    jobs.into_iter().take(MAX_REPORTED_JOBS_PER_NODE).collect()
}

/// Version metadata is small and finite, but the same bounding discipline as
/// job snapshots applies: a misbehaving Agent cannot grow Hub memory.
const MAX_REPORTED_VERSIONS_PER_NODE: usize = 128;
const MAX_REPORTED_TASKS_PER_JOB: usize = 1024;

fn bounded_config_versions(
    versions: Vec<arkflow_core::configuration::ConfigVersion>,
) -> Vec<arkflow_core::configuration::ConfigVersion> {
    versions
        .into_iter()
        .take(MAX_REPORTED_VERSIONS_PER_NODE)
        .collect()
}

fn bounded_job_tasks(map: BTreeMap<String, Vec<String>>) -> BTreeMap<String, Vec<String>> {
    map.into_iter()
        .take(MAX_REPORTED_JOBS_PER_NODE)
        .map(|(job, tasks)| {
            (
                job,
                tasks
                    .into_iter()
                    .filter(|task| !task.is_empty())
                    .take(MAX_REPORTED_TASKS_PER_JOB)
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

pub(crate) fn sanitize_capabilities(capabilities: Vec<String>) -> Vec<String> {
    capabilities
        .into_iter()
        .filter(|capability| {
            !capability.is_empty()
                && capability.len() <= 64
                && capability
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        })
        .take(32)
        .collect()
}

/// Maximum labels per node (see the `node-labels` capability).
pub(crate) const MAX_NODE_LABELS: usize = 16;
/// Maximum length of a label key or label value.
pub(crate) const MAX_NODE_LABEL_PART_LEN: usize = 63;

/// A label key or value is 1-63 characters, starts and ends with an
/// alphanumeric character, and allows `-`, `_`, `.` in between — the same
/// shape Kubernetes uses for label values.
fn valid_label_part(part: &str) -> bool {
    if part.is_empty() || part.len() > MAX_NODE_LABEL_PART_LEN {
        return false;
    }
    let alnum = |character: char| character.is_ascii_alphanumeric();
    part.chars()
        .all(|character| alnum(character) || matches!(character, '-' | '_' | '.'))
        && part.chars().next().is_some_and(alnum)
        && part.chars().last().is_some_and(alnum)
}

/// Validate a full label map. The error names the offending entry so the
/// operator can fix it in one round trip.
pub fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), String> {
    if labels.len() > MAX_NODE_LABELS {
        return Err(format!(
            "label map exceeds {MAX_NODE_LABELS} entries (got {})",
            labels.len()
        ));
    }
    for (key, value) in labels {
        if !valid_label_part(key) {
            return Err(format!(
                "invalid label key '{key}': 1-{MAX_NODE_LABEL_PART_LEN} chars, alphanumeric start and end, only '-', '_', '.' inside"
            ));
        }
        if !valid_label_part(value) {
            return Err(format!(
                "invalid label value for '{key}': 1-{MAX_NODE_LABEL_PART_LEN} chars, alphanumeric start and end, only '-', '_', '.' inside"
            ));
        }
    }
    Ok(())
}

/// Parse one `key=value` label filter term.
pub fn parse_label_term(term: &str) -> Result<(String, String), String> {
    let (key, value) = term
        .split_once('=')
        .ok_or_else(|| format!("invalid label filter '{term}': expected key=value"))?;
    if !valid_label_part(key) || !valid_label_part(value) {
        return Err(format!(
            "invalid label filter '{term}': key and value must be 1-{MAX_NODE_LABEL_PART_LEN} chars, alphanumeric start and end, only '-', '_', '.' inside"
        ));
    }
    Ok((key.to_owned(), value.to_owned()))
}

pub(crate) fn bounded_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

/// Parse a structured operator credential `id|role|secret[|scopes]`.
///
/// A string without any `|` is the plain static-token mode and stays a
/// valid Admin credential (the whole string is the secret). A string that
/// LOOKS structured (contains `|`) but fails to parse — unknown role, empty
/// id/secret, or too few fields — returns `None`: it must never silently
/// degrade into an Admin credential keyed by the whole malformed string.
pub(crate) fn parse_operator_credential(
    configured: &str,
) -> Option<(&str, OperatorRole, &str, Vec<ResourceScope>)> {
    if !configured.contains('|') {
        // Plain static token: the whole string is an Admin secret.
        return Some(("operator", OperatorRole::Admin, configured, Vec::new()));
    }
    let mut fields = configured.splitn(4, '|');
    let id = fields.next()?;
    let role = fields.next()?;
    let secret = fields.next()?;
    let role = match role {
        "admin" => OperatorRole::Admin,
        "operator" => OperatorRole::Operator,
        "viewer" => OperatorRole::Viewer,
        _ => return None,
    };
    if id.trim().is_empty() || secret.is_empty() {
        return None;
    }
    let scopes = fields
        .next()
        .into_iter()
        .flat_map(|value| value.split(','))
        .filter_map(parse_resource_scope)
        .collect();
    Some((id, role, secret, scopes))
}

fn parse_resource_scope(value: &str) -> Option<ResourceScope> {
    let (resource_type, resource_id) = value.split_once('=')?;
    if resource_type.trim().is_empty() {
        return None;
    }
    Some(ResourceScope {
        resource_type: resource_type.to_owned(),
        resource_id: (!resource_id.is_empty()).then(|| resource_id.to_owned()),
    })
}

pub(crate) fn required_capabilities(operation: &str) -> Vec<String> {
    match operation {
        "start" | "stop" | "restart" => vec!["stream_lifecycle".into()],
        "job_start"
        | "job_stop"
        | "job_restart"
        | "job_checkpoint"
        | "job_savepoint"
        | "job_checkpoint_commit"
        | "job_savepoint_commit" => {
            vec!["job_runtime".into(), "state_backend".into()]
        }
        "apply_configuration" | "rollback_configuration" => vec!["configuration".into()],
        _ => Vec::new(),
    }
}

impl Hub {
    fn node_storage(&self) -> Result<&crate::storage::StorageActor, HubError> {
        self.storage
            .as_ref()
            .ok_or(HubError::IdentityStoreUnavailable)
    }

    async fn record_node_audit(
        &self,
        actor: Option<String>,
        action: &str,
        node_id: &str,
        outcome: &str,
        failure_code: Option<&str>,
        message: Option<String>,
    ) {
        self.record_audit_best_effort(
            "node",
            AuditRecord {
                event_id: 0,
                actor,
                action: action.to_owned(),
                resource_type: "node".into(),
                resource_id: Some(node_id.to_owned()),
                node_id: Some(node_id.to_owned()),
                stream_id: None,
                correlation_id: None,
                outcome: outcome.to_owned(),
                failure_code: failure_code.map(str::to_owned),
                message,
                occurred_at_ms: now_ms(),
            },
        )
        .await;
    }

    pub(crate) async fn audit_agent_auth_rejection(
        &self,
        node_id: &str,
        failure_code: &str,
        detail: &str,
    ) {
        let target_node = if node_id.trim().is_empty() {
            "unknown"
        } else {
            node_id
        };
        self.record_node_audit(
            Some(format!("node:{target_node}")),
            "agent.auth_rejected",
            target_node,
            "rejected",
            Some(failure_code),
            Some(bounded_text(detail, 256)),
        )
        .await;
    }

    async fn audit_register_rejected(&self, node_id: &str, failure_code: &str, message: String) {
        let target_node = if node_id.trim().is_empty() {
            "unknown"
        } else {
            node_id
        };
        self.record_node_audit(
            Some(format!("node:{target_node}")),
            "agent.register",
            target_node,
            "rejected",
            Some(failure_code),
            Some(bounded_text(&message, 256)),
        )
        .await;
    }

    /// Issue a durable per-node credential (`arkn_<credential_id>_<secret>`).
    /// Returns the stored record and the one-time plaintext token.
    pub async fn create_node_credential(
        &self,
        node_id: &str,
        label: &str,
        expires_at_ms: Option<u64>,
        actor: &str,
    ) -> Result<(NodeCredentialRecord, String), HubError> {
        if node_id.trim().is_empty() || node_id.len() > 128 {
            return Err(HubError::Invalid("node_id must be 1-128 characters".into()));
        }
        if label.len() > 128 {
            return Err(HubError::Invalid(
                "credential label must be at most 128 characters".into(),
            ));
        }
        let storage = self.node_storage()?.clone();
        let _guard = self.node_identity_write_lock.lock().await;
        let existing = storage
            .list_node_credentials(node_id)
            .await
            .map_err(HubError::from)?;
        if existing
            .iter()
            .filter(|record| record.revoked_at_ms.is_none())
            .count()
            >= MAX_CREDENTIALS_PER_NODE
        {
            return Err(HubError::CredentialLimit);
        }
        let credential_id = generate_credential_id()?;
        let secret = generate_secret()?;
        let record = NodeCredentialRecord {
            credential_id: credential_id.clone(),
            node_id: node_id.to_owned(),
            secret_hash: hash_secret(&credential_id, &secret),
            label: label.to_owned(),
            created_at_ms: now_ms(),
            expires_at_ms,
            last_used_at_ms: None,
            revoked_at_ms: None,
        };
        storage
            .create_node_credential(record.clone())
            .await
            .map_err(HubError::from)?;
        self.record_node_audit(
            Some(actor.to_owned()),
            "node_credential.create",
            node_id,
            "accepted",
            None,
            Some(format!("node={node_id} credential={credential_id}")),
        )
        .await;
        let token = format!("{NODE_TOKEN_PREFIX}{credential_id}_{secret}");
        Ok((record, token))
    }

    pub async fn list_node_credentials(
        &self,
        node_id: &str,
    ) -> Result<Vec<NodeCredentialRecord>, HubError> {
        self.node_storage()?
            .list_node_credentials(node_id)
            .await
            .map_err(HubError::from)
    }

    pub async fn get_node_credential(
        &self,
        credential_id: &str,
    ) -> Result<Option<NodeCredentialRecord>, HubError> {
        self.node_storage()?
            .get_node_credential(credential_id)
            .await
            .map_err(HubError::from)
    }

    /// Expire any active in-memory session authenticated by `credential_id` on
    /// `node_id` so revocation/rotation takes effect on the very next poll,
    /// heartbeat, or report.
    async fn invalidate_node_session_for_credential(&self, node_id: &str, credential_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            if node.credential_id.as_deref() == Some(credential_id) {
                node.session_expires_at_ms = 0;
                node.session_token.clear();
            }
        }
    }

    pub async fn expected_cert_node_id(&self, node_id: &str) -> Option<String> {
        self.nodes
            .read()
            .await
            .get(node_id)
            .and_then(|record| record.cert_node_id.clone())
    }

    pub async fn revoke_node_credential(
        &self,
        credential_id: &str,
        actor: &str,
    ) -> Result<bool, HubError> {
        let storage = self.node_storage()?.clone();
        let hub = self.clone();
        let credential_id = credential_id.to_owned();
        let actor = actor.to_owned();
        tokio::spawn(async move {
            let _guard = hub.node_identity_write_lock.lock().await;
            let Some((record, newly_revoked)) = storage
                .revoke_node_credential(&credential_id, now_ms())
                .await
                .map_err(HubError::from)?
            else {
                return Err(HubError::NotFound);
            };
            hub.invalidate_node_session_for_credential(&record.node_id, &credential_id)
                .await;
            if newly_revoked {
                hub.record_node_audit(
                    Some(actor),
                    "node_credential.revoke",
                    &record.node_id,
                    "accepted",
                    None,
                    Some(format!(
                        "node={} credential={credential_id}",
                        record.node_id
                    )),
                )
                .await;
            }
            Ok(newly_revoked)
        })
        .await
        .map_err(|error| {
            HubError::Invalid(format!("node credential revoke task failed: {error}"))
        })?
    }

    pub async fn rotate_node_credential(
        &self,
        credential_id: &str,
        actor: &str,
    ) -> Result<(NodeCredentialRecord, String), HubError> {
        let storage = self.node_storage()?.clone();
        let hub = self.clone();
        let credential_id = credential_id.to_owned();
        let actor = actor.to_owned();
        tokio::spawn(async move {
            let _guard = hub.node_identity_write_lock.lock().await;
            let current = storage
                .get_node_credential(&credential_id)
                .await
                .map_err(HubError::from)?
                .ok_or(HubError::NotFound)?;
            if current.revoked_at_ms.is_some() {
                return Err(HubError::Invalid(
                    "cannot rotate an already revoked node credential".into(),
                ));
            }
            let new_id = generate_credential_id()?;
            let secret = generate_secret()?;
            let now = now_ms();
            let record = NodeCredentialRecord {
                credential_id: new_id.clone(),
                node_id: current.node_id.clone(),
                secret_hash: hash_secret(&new_id, &secret),
                label: current.label.clone(),
                created_at_ms: now,
                expires_at_ms: current.expires_at_ms,
                last_used_at_ms: None,
                revoked_at_ms: None,
            };
            let rotated = storage
                .rotate_node_credential(record.clone(), &credential_id, now)
                .await
                .map_err(HubError::from)?;
            if !rotated {
                return Err(HubError::Invalid(
                    "cannot rotate an already revoked node credential".into(),
                ));
            }
            hub.invalidate_node_session_for_credential(&current.node_id, &credential_id)
                .await;
            hub.record_node_audit(
                Some(actor),
                "node_credential.rotate",
                &current.node_id,
                "accepted",
                None,
                Some(format!(
                    "node={} old_credential={credential_id} new_credential={new_id}",
                    current.node_id
                )),
            )
            .await;
            let token = format!("{NODE_TOKEN_PREFIX}{new_id}_{secret}");
            Ok((record, token))
        })
        .await
        .map_err(|error| {
            HubError::Invalid(format!("node credential rotate task failed: {error}"))
        })?
    }

    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse, HubError> {
        self.register_with_peer_cert(request, None).await
    }

    pub async fn register_with_peer_cert(
        &self,
        request: RegisterRequest,
        peer_cert: Option<&PeerClientCert>,
    ) -> Result<RegisterResponse, HubError> {
        if let Some(cert) = peer_cert {
            if !cert.node_ids.is_empty() && !cert.node_ids.iter().any(|id| id == &request.node_id) {
                self.audit_register_rejected(
                    &request.node_id,
                    "cert_node_identity_mismatch",
                    format!(
                        "client certificate node SAN mismatch (cert_subject={})",
                        cert.subject
                    ),
                )
                .await;
                return Err(HubError::Unauthorized);
            }
        }
        // Hold `node_identity_write_lock` across credential verification/minting
        // AND `register_after_auth`'s session installation whenever durable
        // storage is active so concurrent `revoke`/`rotate` cannot invalidate an
        // in-memory session just before this registration installs it.
        // Lock order stays strictly: `node_identity_write_lock -> storage -> nodes`.
        let _identity_guard = if self.storage.is_some() {
            Some(self.node_identity_write_lock.lock().await)
        } else {
            None
        };
        let now = now_ms();
        let (auth_mode, credential_id, minted_credential) = if let Some((cred_id, secret)) =
            parse_node_managed_token(&request.node_token)
        {
            let Some(storage) = self.storage.as_ref() else {
                self.audit_register_rejected(
                    &request.node_id,
                    "invalid_node_credential",
                    "per-node credential presented to volatile Hub".into(),
                )
                .await;
                return Err(HubError::Unauthorized);
            };
            let Some(record) = storage
                .get_node_credential(cred_id)
                .await
                .map_err(HubError::from)?
            else {
                self.audit_register_rejected(
                    &request.node_id,
                    "invalid_node_credential",
                    format!("unknown node credential {cred_id}"),
                )
                .await;
                return Err(HubError::Unauthorized);
            };
            if record.node_id != request.node_id {
                self.audit_register_rejected(
                    &request.node_id,
                    "node_identity_mismatch",
                    format!(
                        "credential={cred_id} belongs to node={}, attempted node={}",
                        record.node_id, request.node_id
                    ),
                )
                .await;
                return Err(HubError::Unauthorized);
            }
            if record.revoked_at_ms.is_some() {
                self.audit_register_rejected(
                    &request.node_id,
                    "revoked_node_credential",
                    format!("credential={cred_id} is revoked"),
                )
                .await;
                return Err(HubError::Unauthorized);
            }
            if record.expires_at_ms.is_some_and(|exp| now >= exp) {
                self.audit_register_rejected(
                    &request.node_id,
                    "expired_node_credential",
                    format!("credential={cred_id} is expired"),
                )
                .await;
                return Err(HubError::Unauthorized);
            }
            let presented_hash = hash_secret(cred_id, secret);
            if !bool::from(
                presented_hash
                    .as_bytes()
                    .ct_eq(record.secret_hash.as_bytes()),
            ) {
                self.audit_register_rejected(
                    &request.node_id,
                    "invalid_node_credential",
                    format!("credential={cred_id} secret mismatch"),
                )
                .await;
                return Err(HubError::Unauthorized);
            }
            ("per_node", Some(cred_id.to_owned()), None)
        } else {
            let auth_mode = match self.config.node_token.as_deref() {
                None => {
                    if !self.config.insecure_local {
                        self.audit_register_rejected(
                            &request.node_id,
                            "missing_node_token",
                            "node_token not configured".into(),
                        )
                        .await;
                        return Err(HubError::Unauthorized);
                    }
                    let _ = &request.node_token;
                    "insecure_local"
                }
                Some(expected) => {
                    if expected.trim().is_empty()
                        || !bool::from(request.node_token.as_bytes().ct_eq(expected.as_bytes()))
                    {
                        self.audit_register_rejected(
                            &request.node_id,
                            "invalid_node_token",
                            "shared node_token mismatch".into(),
                        )
                        .await;
                        return Err(HubError::Unauthorized);
                    }
                    "shared"
                }
            };
            if let Some(storage) = self.storage.as_ref() {
                if auth_mode == "shared" {
                    if self.node_bootstrap_disabled.load(Ordering::Acquire) {
                        self.audit_register_rejected(
                            &request.node_id,
                            "bootstrap_disabled",
                            "shared node_token bootstrap is disabled".into(),
                        )
                        .await;
                        return Err(HubError::Unauthorized);
                    }
                    // Validate structural fields and fleet capacity BEFORE
                    // minting any durable credential so rejected registrations
                    // never leave orphan bootstrap rows behind.
                    if request.node_id.trim().is_empty() {
                        return Err(HubError::Invalid("node_id must not be empty".into()));
                    }
                    if request.protocol_version != SUPPORTED_PROTOCOL_VERSION {
                        let message =
                            format!("unsupported protocol version: {}", request.protocol_version);
                        self.audit_register_rejected(
                            &request.node_id,
                            "incompatible_protocol",
                            message.clone(),
                        )
                        .await;
                        return Err(HubError::Invalid(message));
                    }
                    {
                        let nodes = self.nodes.read().await;
                        let _ = find_reclaimable_node(&nodes, &request.node_id, now)?;
                    }
                    let history = storage
                        .list_node_credentials(&request.node_id)
                        .await
                        .map_err(HubError::from)?;
                    let req_boot_id = request
                        .boot_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|boot| !boot.is_empty());
                    // Same-boot retry recovery: if the bootstrap response (or a
                    // previous rotation commit before register_after_auth) was lost
                    // before the Agent confirmed the session, let the same
                    // unconfirmed boot_id re-roll the single active/unexpired
                    // bootstrap credential cleanly instead of wedging the node.
                    let all_bootstrap = history.iter().all(|c| c.label == "bootstrap");
                    let active_bootstrap = history
                        .iter()
                        .filter(|c| c.revoked_at_ms.is_none())
                        .collect::<Vec<_>>();
                    let same_boot_unconfirmed_cred = match req_boot_id {
                        Some(req_boot)
                            if all_bootstrap
                                && active_bootstrap.len() == 1
                                && active_bootstrap[0]
                                    .expires_at_ms
                                    .is_none_or(|exp| now < exp) =>
                        {
                            let existing_cred_id = active_bootstrap[0].credential_id.clone();
                            let mut nodes = self.nodes.write().await;
                            let eligible = nodes.get(&request.node_id).is_some_and(|record| {
                                record.bootstrap_unconfirmed
                                    && !record.bootstrap_rotation_pending
                                    && record.boot_id.as_deref() == Some(req_boot)
                                    && record.credential_id.as_deref()
                                        == Some(existing_cred_id.as_str())
                            });
                            if eligible {
                                if let Some(record) = nodes.get_mut(&request.node_id) {
                                    record.bootstrap_rotation_pending = true;
                                }
                                Some(existing_cred_id)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    if !history.is_empty() && same_boot_unconfirmed_cred.is_none() {
                        if self
                            .allow_shared_node_token_break_glass
                            .load(Ordering::Acquire)
                        {
                            ("break_glass", None, None)
                        } else {
                            self.audit_register_rejected(
                                &request.node_id,
                                "bootstrap_blocked_enrolled_node",
                                "shared node_token rejected for already-enrolled node".into(),
                            )
                            .await;
                            return Err(HubError::Unauthorized);
                        }
                    } else if let Some(previous_cred_id) = same_boot_unconfirmed_cred {
                        let cred_id = generate_credential_id()?;
                        let secret = generate_secret()?;
                        let record = NodeCredentialRecord {
                            credential_id: cred_id.clone(),
                            node_id: request.node_id.clone(),
                            secret_hash: hash_secret(&cred_id, &secret),
                            label: "bootstrap".into(),
                            created_at_ms: now,
                            expires_at_ms: None,
                            last_used_at_ms: None,
                            revoked_at_ms: None,
                        };
                        let rotated = match storage
                            .rotate_node_credential(record, &previous_cred_id, now)
                            .await
                        {
                            Ok(rotated) => rotated,
                            Err(error) => {
                                let mut nodes = self.nodes.write().await;
                                if let Some(existing) = nodes.get_mut(&request.node_id) {
                                    if existing.credential_id.as_deref()
                                        == Some(previous_cred_id.as_str())
                                    {
                                        existing.bootstrap_rotation_pending = false;
                                    }
                                }
                                return Err(HubError::from(error));
                            }
                        };
                        if !rotated {
                            let mut nodes = self.nodes.write().await;
                            if let Some(existing) = nodes.get_mut(&request.node_id) {
                                if existing.credential_id.as_deref()
                                    == Some(previous_cred_id.as_str())
                                {
                                    existing.bootstrap_rotation_pending = false;
                                }
                            }
                            return Err(HubError::Unauthorized);
                        }
                        // Rotation committed in durable storage: immediately
                        // invalidate the prior session BEFORE any fallible
                        // register_after_auth work so the revoked credential's
                        // session cannot remain active if registration errors,
                        // while preserving recoverability on the next retry.
                        {
                            let mut nodes = self.nodes.write().await;
                            if let Some(existing) = nodes.get_mut(&request.node_id) {
                                existing.session_token = String::new();
                                existing.session_expires_at_ms = 0;
                                existing.credential_id = Some(cred_id.clone());
                                existing.bootstrap_unconfirmed = true;
                                existing.bootstrap_rotation_pending = false;
                            }
                        }
                        self.record_node_audit(
                            Some(format!("node:{}", request.node_id)),
                            "node_credential.enroll",
                            &request.node_id,
                            "accepted",
                            None,
                            Some(format!(
                                "node={} credential={cred_id} previous_unconfirmed={previous_cred_id}",
                                request.node_id
                            )),
                        )
                        .await;
                        let token = format!("{NODE_TOKEN_PREFIX}{cred_id}_{secret}");
                        ("bootstrap_enroll", Some(cred_id), Some(token))
                    } else {
                        let cred_id = generate_credential_id()?;
                        let secret = generate_secret()?;
                        let record = NodeCredentialRecord {
                            credential_id: cred_id.clone(),
                            node_id: request.node_id.clone(),
                            secret_hash: hash_secret(&cred_id, &secret),
                            label: "bootstrap".into(),
                            created_at_ms: now,
                            expires_at_ms: None,
                            last_used_at_ms: None,
                            revoked_at_ms: None,
                        };
                        storage
                            .create_node_credential(record)
                            .await
                            .map_err(HubError::from)?;
                        self.record_node_audit(
                            Some(format!("node:{}", request.node_id)),
                            "node_credential.enroll",
                            &request.node_id,
                            "accepted",
                            None,
                            Some(format!("node={} credential={cred_id}", request.node_id)),
                        )
                        .await;
                        let token = format!("{NODE_TOKEN_PREFIX}{cred_id}_{secret}");
                        ("bootstrap_enroll", Some(cred_id), Some(token))
                    }
                } else {
                    (auth_mode, None, None)
                }
            } else {
                (auth_mode, None, None)
            }
        };

        let cert_node_id = peer_cert.and_then(|cert| {
            cert.node_ids
                .iter()
                .find(|id| id.as_str() == request.node_id)
                .cloned()
        });
        let cert_subject = peer_cert.map(|cert| cert.subject.clone());
        self.register_after_auth(
            request,
            auth_mode,
            credential_id,
            cert_node_id,
            cert_subject,
            minted_credential,
        )
        .await
    }

    async fn register_after_auth(
        &self,
        request: RegisterRequest,
        auth_mode: &str,
        credential_id: Option<String>,
        cert_node_id: Option<String>,
        cert_subject: Option<String>,
        minted_credential: Option<String>,
    ) -> Result<RegisterResponse, HubError> {
        if request.node_id.trim().is_empty() {
            return Err(HubError::Invalid("node_id must not be empty".into()));
        }
        if request.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            let message = format!("unsupported protocol version: {}", request.protocol_version);
            self.record_audit_best_effort(
                "node",
                crate::storage::AuditRecord {
                    event_id: 0,
                    actor: Some(format!("node:{}", request.node_id)),
                    action: "agent.register".into(),
                    resource_type: "node".into(),
                    resource_id: Some(request.node_id.clone()),
                    node_id: Some(request.node_id.clone()),
                    stream_id: None,
                    correlation_id: None,
                    outcome: "rejected".into(),
                    failure_code: Some("incompatible_protocol".into()),
                    message: Some(message.clone()),
                    occurred_at_ms: now_ms(),
                },
            )
            .await;
            return Err(HubError::Invalid(message));
        }
        let now = now_ms();
        // Session tokens authenticate every agent request after registration,
        // so they MUST come from a CSPRNG: a sequential counter would be
        // enumerable by anyone who can reach the Hub and defeat the
        // constant-time comparisons downstream.
        let session_token: String = {
            use rand::TryRngCore;
            let mut bytes = [0u8; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut bytes)
                .expect("OS RNG cannot fail");
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        };
        // Older clients do not send a process identity. Keep them compatible
        // by treating the fresh session token as their boot identity; the
        // built-in Agent sends its stable `NodeAgentConfig::boot_id`.
        let registered_boot_id = request
            .boot_id
            .as_deref()
            .map(str::trim)
            .filter(|boot_id| !boot_id.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| session_token.clone());
        // Labels are durable operator state: re-hydrate them from storage so
        // they survive Hub restarts (the in-memory registry is rebuilt from
        // re-registrations). The registration upsert itself never touches the
        // labels column — see the `node-labels` capability.
        let labels = match self.storage.as_ref() {
            Some(storage) => storage
                .get_node_labels(&request.node_id)
                .await
                .map_err(HubError::from)?
                .unwrap_or_default(),
            None => BTreeMap::new(),
        };
        let resource = HubNode {
            id: request.node_id.clone(),
            protocol_version: request.protocol_version.clone(),
            version: "unknown".into(),
            state: NodeConnectionState::Online,
            capabilities: sanitize_capabilities(request.capabilities.clone()),
            last_seen_at_ms: now,
            lease_expires_at_ms: now + self.config.lease_ttl_ms,
            report_seq: 0,
            streams_total: 0,
            streams_running: 0,
            streams_failed: 0,
            maintenance_state: NodeMaintenanceState::Active,
            data_address: request
                .data_address
                .clone()
                .filter(|address| !address.trim().is_empty()),
            labels,
        };
        let mut nodes = self.nodes.write().await;
        if let Some(reclaim_id) = find_reclaimable_node(&nodes, &request.node_id, now)? {
            nodes.remove(&reclaim_id);
        }
        let old = nodes.remove(&request.node_id);
        let boot_changed = old
            .as_ref()
            .is_some_and(|record| record.boot_id.as_deref() != Some(registered_boot_id.as_str()));
        // Commands queued for an old process belong to a runtime that no
        // longer exists. Reconciliation below will enqueue the desired
        // state for the new boot.
        //
        // A same-boot session refresh keeps its leases AS leases. Re-queuing
        // a still-valid lease looked attractive (a poll response issued under
        // the expired token might never have reached the Agent), but a
        // command the Agent already executed but whose result post raced the
        // session expiry would then be re-executed under the SAME command and
        // checkpoint id. Checkpoint rounds are not idempotent — the kernel
        // rejects the repeated barrier ("stale duplicate barrier") — so the
        // pause capture would fail. The expiry sweep is the recovery path: it
        // settles the timed-out operation and the caller re-dispatches a
        // FRESH capture id.
        let (commands, leased_commands) = match &old {
            Some(record) if !boot_changed => {
                (record.commands.clone(), record.leased_commands.clone())
            }
            _ => (VecDeque::new(), BTreeMap::new()),
        };
        nodes.insert(
            request.node_id.clone(),
            NodeRecord {
                resource,
                session_token: session_token.clone(),
                session_expires_at_ms: now.saturating_add(self.config.session_ttl_ms),
                credential_id: credential_id.clone(),
                cert_node_id,
                bootstrap_unconfirmed: auth_mode == "bootstrap_enroll",
                bootstrap_rotation_pending: false,
                boot_id: Some(registered_boot_id.clone()),
                report_seq: 0,
                last_report_at_ms: 0,
                // Pressure history is deliberately not carried across
                // registrations: a reconnecting node re-earns its streak
                // within a few report intervals (bounded rebalance delay).
                pressure_streak: 0,
                commands,
                leased_commands,
                streams: old
                    .as_ref()
                    .map(|record| record.streams.clone())
                    .unwrap_or_default(),
                operations: old
                    .as_ref()
                    .map(|record| record.operations.clone())
                    .unwrap_or_default(),
                events: old
                    .as_ref()
                    .map(|record| record.events.clone())
                    .unwrap_or_default(),
                configuration: old.as_ref().and_then(|record| record.configuration.clone()),
                // A boot change invalidates the previous process's local Jobs
                // (their start operations are marked unavailable below), so
                // their metric snapshots must not survive the re-registration.
                jobs: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.jobs.clone())
                        .unwrap_or_default()
                },
                // Version metadata lives in the node's durable store, so it
                // survives re-registration like the redacted configuration.
                config_versions: old
                    .as_ref()
                    .map(|record| record.config_versions.clone())
                    .unwrap_or_default(),
                job_tasks: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.job_tasks.clone())
                        .unwrap_or_default()
                },
                // Registration re-hydrates labels from storage further down;
                // the revision baseline comes from the record this insert
                // replaced (or 0 for a fresh node).
                labels_rev: old.as_ref().map(|record| record.labels_rev).unwrap_or(0),
                metrics: old.map(|record| record.metrics).unwrap_or_default(),
            },
        );
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&request.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            storage
                .upsert_node(NodeMutation {
                    node_id: request.node_id.clone(),
                    version: "unknown".into(),
                    state: "online".into(),
                    capabilities_json: serde_json::to_string(&sanitize_capabilities(
                        request.capabilities,
                    ))
                    .unwrap_or_else(|_| "[]".into()),
                    boot_id: Some(registered_boot_id.clone()),
                    report_seq: Some(0),
                    last_seen_at_ms: now,
                    lease_expires_at_ms: now + self.config.lease_ttl_ms,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
            // The Agent restarts report_seq from 1 on every session rebuild,
            // so the previous session's stored per-stream cursors would
            // silently drop every new observation. Reset them together with
            // the in-memory cursor above.
            storage
                .reset_observed_cursors(request.node_id.clone())
                .await
                .map_err(HubError::from)?;
            // A concurrent label replacement may have committed between the
            // pre-insert hydration read and this point. Re-sync the labels
            // with a read taken BEFORE the write lock: `replace_node_labels`
            // writes storage first and takes this same write lock for its
            // memory update afterwards. The storage round trip stays out of
            // the lock — a slow label query must not stall every writer
            // queued on this lock (registration, reports) — so the read is
            // paired with an in-memory labels-revision snapshot taken before
            // it: the pre-read is applied under the write lock only when no
            // replacement has landed since. Value comparison would be
            // unsound (a replacement back to the same map is invisible);
            // `labels_rev` advances on every replacement, so any replacement
            // landing during the read window suppresses the stale pre-read —
            // its own storage write precedes its memory update, making it at
            // least as new as anything this registration read. A replacement
            // whose memory update lands after our release overwrites us
            // regardless. The map can never end this registration stale.
            let labels_rev_before = self
                .nodes
                .read()
                .await
                .get(&request.node_id)
                .map(|node| node.labels_rev);
            let fresh_labels = storage
                .get_node_labels(&request.node_id)
                .await
                .ok()
                .flatten();
            {
                let mut nodes = self.nodes.write().await;
                if let Some(node) = nodes.get_mut(&request.node_id) {
                    // The revision guard binds to THIS registration's record:
                    // if the storage read was delayed past lease expiry, a
                    // capacity reclaim could have dropped the node and a
                    // fresh registration re-created it (labels_rev reset to
                    // 0) — without the session-token check this
                    // registration's stale completion would pass the
                    // revision guard and regress that new record.
                    if node.session_token == session_token {
                        if let Some(fresh) = fresh_labels {
                            if labels_rev_before == Some(node.labels_rev) {
                                node.resource.labels = fresh;
                            }
                        }
                    }
                }
            }
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            storage
                .wake_node(&request.node_id, now)
                .await
                .map_err(HubError::from)?;
            if let Some(state) = storage
                .get_node_maintenance(&request.node_id)
                .await
                .map_err(HubError::from)?
            {
                let maintenance_state = match state.as_str() {
                    "draining" => NodeMaintenanceState::Draining,
                    "maintenance" => NodeMaintenanceState::Maintenance,
                    _ => NodeMaintenanceState::Active,
                };
                if let Some(node) = self.nodes.write().await.get_mut(&request.node_id) {
                    node.resource.maintenance_state = maintenance_state;
                }
            }
        }
        let mut details = format!("auth_mode={auth_mode}");
        if let Some(cred_id) = credential_id.as_deref() {
            details.push_str(&format!(" credential={cred_id}"));
        }
        if let Some(subject) = cert_subject.as_deref() {
            details.push_str(&format!(" cert_subject={subject}"));
        }
        self.record_node_audit(
            Some(format!("node:{}", request.node_id)),
            "agent.register",
            &request.node_id,
            "accepted",
            None,
            Some(bounded_text(&details, 256)),
        )
        .await;
        for job in self.jobs().await? {
            if job.desired_state != "stopped"
                && (job.node_ids.is_empty() || job.node_ids.iter().any(|id| id == &request.node_id))
            {
                self.reconcile_job(&job).await?;
            }
        }
        Ok(RegisterResponse {
            node_id: request.node_id,
            session_token,
            session_ttl_ms: self.config.session_ttl_ms,
            lease_ttl_ms: self.config.lease_ttl_ms,
            poll_interval_ms: self.config.poll_interval_ms,
            protocol_version: default_protocol_version(),
            node_credential: minted_credential,
        })
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<(), HubError> {
        if let Some(protocol_version) = request.protocol_version.as_deref() {
            if protocol_version != SUPPORTED_PROTOCOL_VERSION {
                return Err(HubError::Invalid(format!(
                    "unsupported protocol version: {protocol_version}"
                )));
            }
        }
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &request.auth)?;
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = match request.state.as_str() {
            "draining" => NodeConnectionState::Draining,
            _ => NodeConnectionState::Online,
        };
        if let Some(version) = request.software_version {
            node.resource.version = version;
        }
        if !request.capabilities.is_empty() {
            node.resource.capabilities = sanitize_capabilities(request.capabilities);
        }
        Ok(())
    }

    /// A fresh Agent process starts with an empty local JobRuntime. Mark every
    /// previous start attempt for this node unavailable before reconciliation,
    /// otherwise a persisted successful operation would suppress the new start
    /// command even though no local Job exists. Shared by registration and
    /// report so the invalidation semantics cannot drift between them.
    async fn invalidate_job_starts_on_boot_change(
        &self,
        node_id: &str,
        boot_changed: bool,
        now: u64,
    ) -> Vec<HubOperation> {
        if !boot_changed {
            return Vec::new();
        }
        let mut operations = self.operations.write().await;
        operations
            .values_mut()
            .filter(|operation| {
                operation.node_id == node_id
                    && operation.operation == AgentOperation::JobStart
                    && !matches!(
                        operation.state,
                        HubOperationState::Failed
                            | HubOperationState::TimedOut
                            | HubOperationState::NodeUnavailable
                            | HubOperationState::Cancelled
                            | HubOperationState::Superseded
                    )
            })
            .map(|operation| {
                let was_succeeded = operation.state == HubOperationState::Succeeded;
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                if was_succeeded {
                    operation.failure_class = Some("recovery_required".into());
                    operation.error = Some(
                        "previous successful Job start invalidated by a new Agent process boot"
                            .into(),
                    );
                } else {
                    operation.error =
                        Some("in-flight Job start invalidated by a new Agent process boot".into());
                }
                operation.clone()
            })
            .collect()
    }

    pub async fn report(&self, report: NodeReport) -> Result<(), HubError> {
        let reported_streams = report.streams.clone();
        let reported_configuration = report.configuration.clone();
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &report.auth)?;
        let boot_changed = report
            .boot_id
            .as_deref()
            .is_some_and(|boot_id| node.boot_id.as_deref() != Some(boot_id));
        if let Some(boot_id) = report.boot_id.as_deref() {
            match node.boot_id.as_deref() {
                Some(current) if current == boot_id => {
                    // Same session: the sequence cursor rejects replays.
                    if report.report_seq <= node.report_seq {
                        return Ok(());
                    }
                    node.report_seq = report.report_seq;
                }
                Some(_) => {
                    // A delayed report from an older session (the node has
                    // re-registered since): acknowledge without regressing
                    // the new session's observed state.
                    return Ok(());
                }
                None => {
                    node.boot_id = Some(boot_id.to_owned());
                    node.report_seq = report.report_seq;
                }
            }
        }
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        // The node view reports which state report it reflects; heartbeats
        // refresh last_seen but never this.
        node.resource.report_seq = node.report_seq;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = if report.state == "draining" {
            NodeConnectionState::Draining
        } else {
            NodeConnectionState::Online
        };
        node.resource.version = report.version;
        node.resource.capabilities = sanitize_capabilities(report.capabilities);
        node.resource.streams_total = report.streams.len();
        node.resource.streams_running = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Running)
            .count();
        node.resource.streams_failed = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Failed)
            .count();
        node.streams = report.streams;
        node.operations = report.operations;
        node.events = report.events.clone();
        node.metrics = sanitize_metrics(report.metrics);
        node.last_report_at_ms = now;
        node.pressure_streak = if node_under_pressure(&node.metrics) {
            node.pressure_streak.saturating_add(1)
        } else {
            0
        };
        node.jobs = bounded_job_snapshots(report.jobs);
        node.configuration = report.configuration;
        node.config_versions = bounded_config_versions(report.config_versions);
        node.job_tasks = bounded_job_tasks(report.job_tasks);
        let persisted_version = node.resource.version.clone();
        let persisted_state = format!("{:?}", node.resource.state).to_lowercase();
        let persisted_capabilities =
            serde_json::to_string(&node.resource.capabilities).unwrap_or_else(|_| "[]".into());
        let persisted_boot_id = node.boot_id.clone();
        let persisted_report_seq = Some(node.report_seq);
        let persisted_lease = node.resource.lease_expires_at_ms;
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&report.auth.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            storage
                .upsert_node(NodeMutation {
                    node_id: report.auth.node_id.clone(),
                    version: persisted_version,
                    state: persisted_state,
                    capabilities_json: persisted_capabilities,
                    boot_id: persisted_boot_id,
                    report_seq: persisted_report_seq,
                    last_seen_at_ms: now,
                    lease_expires_at_ms: persisted_lease,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
        }
        let mut events = self.events.write().await;
        for mut event in report.events {
            if events.len() >= MAX_EVENTS {
                events.pop_front();
            }
            event.message = event.message.map(|message| bounded_text(&message, 512));
            events.push_back(HubEvent {
                event_id: None,
                node_id: report.auth.node_id.clone(),
                event,
            });
            if let Some(event) = events.back().cloned() {
                let _ = self.updates.send(event);
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for stream in &reported_streams {
                let observed_state = serde_json::to_value(stream.state)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                let last_error_code = stream.last_error.as_ref().map(|error| error.stage.clone());
                let last_error_message = stream
                    .last_error
                    .as_ref()
                    .map(|error| error.message.clone());
                storage
                    .record_observed(ObservedMutation {
                        node_id: report.auth.node_id.clone(),
                        stream_id: stream.id.clone(),
                        boot_id: report.boot_id.clone(),
                        report_seq: report.report_seq,
                        observed_generation: stream.observed_generation,
                        observed_state,
                        config_version_id: stream.observed_config_version.clone(),
                        action_id: stream.last_completed_action_id.clone(),
                        snapshot_json: serde_json::to_string(&stream)
                            .unwrap_or_else(|_| "{}".into()),
                        last_error_code,
                        last_error_message,
                    })
                    .await
                    .map_err(HubError::from)?;
            }
            if let Some(config_target) = storage
                .get_desired(&report.auth.node_id, "__configuration__")
                .await
                .map_err(HubError::from)?
            {
                let observed_version = report.configuration_version.clone().or_else(|| {
                    reported_streams
                        .iter()
                        .find_map(|stream| stream.observed_config_version.clone())
                });
                if observed_version.is_some() {
                    storage
                        .record_observed(ObservedMutation {
                            node_id: report.auth.node_id.clone(),
                            stream_id: "__configuration__".into(),
                            boot_id: report.boot_id.clone(),
                            report_seq: report.report_seq,
                            observed_generation: Some(config_target.generation),
                            observed_state: "configured".into(),
                            config_version_id: observed_version,
                            action_id: None,
                            snapshot_json: serde_json::to_string(&reported_configuration)
                                .unwrap_or_else(|_| "null".into()),
                            last_error_code: None,
                            last_error_message: None,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        if boot_changed {
            for job in self.jobs().await? {
                if job.desired_state != "stopped"
                    && (job.node_ids.is_empty()
                        || job.node_ids.iter().any(|id| id == &report.auth.node_id))
                {
                    self.reconcile_job(&job).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn commands(&self, auth: AgentAuth) -> Result<Vec<AgentCommand>, HubError> {
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &auth)?;
        let now = now_ms();
        let mut commands = Vec::new();
        let mut expired = Vec::new();
        while let Some(command) = node.commands.pop_front() {
            if command.expires_at_ms > now {
                node.leased_commands
                    .insert(command.id.clone(), command.clone());
                commands.push(command);
            } else {
                expired.push(command);
            }
        }
        let expired_leases = node
            .leased_commands
            .iter()
            .filter(|(_, command)| command.expires_at_ms <= now)
            .map(|(command_id, _)| command_id.clone())
            .collect::<Vec<_>>();
        for command_id in expired_leases {
            if let Some(command) = node.leased_commands.remove(&command_id) {
                expired.push(command);
            }
        }
        drop(nodes);
        // A queued command is leased even before an Agent receives it. Do not
        // silently discard an expired lease while leaving its operation in an
        // active deduplication state: mark it retryable/terminal so the normal
        // Job reconciler can enqueue a fresh command.  Non-Job commands do
        // not have a desired-state reconciler, so retain enough of the
        // expired command to enqueue a replacement below.
        let mut expired_operations = Vec::new();
        let mut expired_job_ids = BTreeSet::new();
        let mut expired_retries = Vec::new();
        if !expired.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &expired {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    if matches!(
                        operation.state,
                        HubOperationState::Queued
                            | HubOperationState::Dispatched
                            | HubOperationState::Acknowledged
                            | HubOperationState::Running
                    ) {
                        operation.state = HubOperationState::TimedOut;
                        operation.finished_at_ms = Some(now);
                        operation.next_retry_at_ms = Some(now);
                        operation.error = Some("command lease expired before execution".into());
                        if matches!(
                            operation.operation,
                            AgentOperation::JobStart | AgentOperation::JobStop
                        ) {
                            expired_job_ids.insert(operation.resource_id.clone());
                        } else {
                            expired_retries.push(command.clone());
                        }
                        expired_operations.push(operation.clone());
                    }
                }
            }
        }
        if !commands.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &commands {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    operation.state = HubOperationState::Dispatched;
                    operation.dispatched_at_ms = Some(now);
                }
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for operation in &expired_operations {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            for command in &commands {
                if let Some(attempt_id) = command.attempt_id.as_deref() {
                    storage
                        .mark_attempt_dispatched(attempt_id, command.expires_at_ms)
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        // A command can expire after being queued, or after an Agent polled it
        // and lost the response before execution.  Marking its old operation
        // terminal is not enough for operations without a desired-state
        // reconciler: the old active record would otherwise be the next
        // deduplication hit forever.  Re-enqueue a fresh command with the same
        // payload and generation.  Job start/stop uses the canonical
        // reconciliation path below so a changed Job spec/placement is
        // rebuilt instead of replaying stale command data.
        for command in expired_retries {
            if command.operation.is_job() {
                let Some(job) = self.job(&command.resource_id).await? else {
                    continue;
                };
                if job.generation != command.generation {
                    continue;
                }
            }
            if let Err(error) = self
                .enqueue_with_metadata(
                    command.node_id.clone(),
                    command.operation.clone(),
                    command.resource_id.clone(),
                    command.correlation_id.clone(),
                    command.payload.clone(),
                    command.generation,
                    command.action_id.clone(),
                    command.config_version_id.clone(),
                    None,
                    None,
                    command.attempt_id.clone(),
                )
                .await
            {
                // The node may have gone offline while the expired command
                // was being requeued.  The next heartbeat/reconciliation can
                // retry it; polling the current command queue should still
                // succeed and return the non-expired commands.
                tracing::warn!(
                    node_id = %command.node_id,
                    operation = %command.operation,
                    resource_id = %command.resource_id,
                    %error,
                    "failed to requeue expired Hub command"
                );
            }
        }
        for job_id in expired_job_ids {
            if let Some(job) = self.job(&job_id).await? {
                self.reconcile_job(&job).await?;
            }
        }
        Ok(commands)
    }

    pub async fn nodes(&self) -> Vec<HubNode> {
        self.nodes
            .read()
            .await
            .values()
            .map(|node| node.resource.clone())
            .collect()
    }

    pub async fn set_node_maintenance(
        &self,
        node_id: &str,
        state: NodeMaintenanceState,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<HubNode, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let state_name = match state {
            NodeMaintenanceState::Active => "active",
            NodeMaintenanceState::Draining => "draining",
            NodeMaintenanceState::Maintenance => "maintenance",
        };
        if !storage
            .set_node_maintenance(
                crate::storage::NodeMaintenanceMutation {
                    node_id: node_id.into(),
                    state: state_name.into(),
                    actor,
                    correlation_id,
                },
                now_ms(),
            )
            .await
            .map_err(HubError::from)?
        {
            return Err(HubError::NotFound);
        }
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(node_id).ok_or(HubError::NotFound)?;
        node.resource.maintenance_state = state;
        Ok(node.resource.clone())
    }

    /// Full replacement of a node's label map (see the `node-labels`
    /// capability). Validation rejects oversized or malformed maps without
    /// partially applying; authorization and denial auditing live at the API
    /// layer; the accepted replacement is audited by the storage layer in the
    /// same transaction. Returns the stored labels.
    pub async fn replace_node_labels(
        &self,
        node_id: &str,
        labels: BTreeMap<String, String>,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<BTreeMap<String, String>, HubError> {
        validate_labels(&labels).map_err(HubError::Invalid)?;
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .replace_node_labels(
                crate::storage::NodeLabelsMutation {
                    node_id: node_id.into(),
                    labels: labels.clone(),
                    actor,
                    correlation_id,
                },
                now_ms(),
            )
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        // The storage row existed, so the labels are durable even when the
        // in-memory record is absent (Hub restarted, Agent not yet
        // re-registered) — re-registration re-hydrates them.
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.resource.labels = labels.clone();
            // Every replacement advances the revision — including same-value
            // ones — so registration's re-sync can tell "nothing happened
            // during my read window" from "a replacement landed that wrote
            // the same map I snapshotted".
            node.labels_rev = node.labels_rev.wrapping_add(1);
        }
        Ok(labels)
    }

    /// Resolve a label selector to the sorted ids of nodes whose labels
    /// match every selector pair (equality, AND semantics). Resolution reads
    /// the DURABLE node records (`cp_nodes.labels_json`), not the in-memory
    /// registration map: a node that has not re-registered since a Hub
    /// restart still resolves, exactly like an unreachable-but-registered
    /// node — consistent with explicit node lists, whose ids the direct
    /// rollout path also never filters by registration state. The rollout
    /// health gates handle delivery to unreachable targets.
    pub async fn resolve_label_selector(
        &self,
        selector: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let records = storage.list_node_labels().await.map_err(HubError::from)?;
        let mut matched: Vec<String> = records
            .into_iter()
            .filter(|(_, labels)| {
                selector
                    .iter()
                    .all(|(key, value)| labels.get(key) == Some(value))
            })
            .map(|(node_id, _)| node_id)
            .collect();
        matched.sort();
        Ok(matched)
    }
    pub async fn configuration(&self, node_id: &str) -> Option<serde_json::Value> {
        self.nodes
            .read()
            .await
            .get(node_id)
            .and_then(|node| node.configuration.clone())
    }

    pub async fn mark_stale(&self) {
        let now = now_ms();
        let mut nodes = self.nodes.write().await;
        let stale_ids: Vec<String> = nodes
            .values_mut()
            .filter_map(|node| {
                if node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms <= now
                {
                    node.resource.state = NodeConnectionState::Stale;
                    Some(node.resource.id.clone())
                } else {
                    None
                }
            })
            .collect();
        if stale_ids.is_empty() {
            return;
        }
        let mut operations = self.operations.write().await;
        for operation in operations.values_mut() {
            if stale_ids.iter().any(|id| id == &operation.node_id)
                && matches!(
                    operation.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                )
            {
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                operation.error = Some("Node lease expired".into());
            }
        }
    }
}

#[cfg(test)]
mod credential_tests {
    use super::parse_operator_credential;
    use crate::api_contract::OperatorRole;

    /// Spec: a plain token (no '|') stays a valid Admin credential, and a
    /// structured credential that fails to parse is rejected instead of
    /// silently degrading into an Admin credential keyed by the whole
    /// malformed string.
    #[test]
    fn plain_static_token_stays_admin() {
        let (id, role, secret, scopes) = parse_operator_credential("plain-token").unwrap();
        assert_eq!(id, "operator");
        assert_eq!(role, OperatorRole::Admin);
        assert_eq!(secret, "plain-token");
        assert!(scopes.is_empty());
    }

    #[test]
    fn structured_credential_parses_with_scopes() {
        let (id, role, secret, scopes) =
            parse_operator_credential("alice|viewer|hush|node=node-a,stream=orders").unwrap();
        assert_eq!(id, "alice");
        assert_eq!(role, OperatorRole::Viewer);
        assert_eq!(secret, "hush");
        assert_eq!(scopes.len(), 2);
    }

    #[test]
    fn malformed_structured_credentials_are_rejected() {
        // Unknown role — the CR's privilege-escalation typo.
        assert!(parse_operator_credential("alice|typo|secret").is_none());
        // Too few fields to be structured.
        assert!(parse_operator_credential("alice|admin").is_none());
        // Empty id / secret.
        assert!(parse_operator_credential("|admin|secret").is_none());
        assert!(parse_operator_credential("alice|admin|").is_none());
    }
}
