//! Stable v1 operator HTTP wire shapes.
//!
//! These types intentionally contain no Axum extractors. They are shared by
//! handlers, contract tests, and future generated OpenAPI descriptions.

use crate::storage::IntentRecord;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct OperatorPrincipal {
    pub id: String,
    pub roles: Vec<OperatorRole>,
    #[serde(default)]
    pub scopes: Vec<ResourceScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ResourceScope {
    pub resource_type: String,
    #[serde(default)]
    pub resource_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorRole {
    Admin,
    Operator,
    Viewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAction {
    Read,
    Operate,
    Configure,
    ManageNodes,
    ManageRollouts,
    /// Replace the webhook subscription set and fire test deliveries (see
    /// the `webhook-alerting` capability). Held by admin and operator:
    /// webhooks are an outbound control-plane channel carrying signing
    /// secrets, but they are day-to-day operational tooling.
    ManageWebhooks,
    /// Decide configuration approval requests. Held by admin only: the
    /// operator role can submit requests but must not approve them, keeping
    /// submission and approval in different hands (see
    /// `configuration-approval`).
    ApproveRollouts,
    ReadAudit,
    /// Administer the managed user directory and its credentials. Held by
    /// admin only: issuing credentials is equivalent to issuing identity
    /// (see `control-plane-credentials`).
    ManageUsers,
}

impl OperatorAction {
    /// Every action in the v1 wire contract, in declaration order. The
    /// `/system` principal echo filters this against `can` so the console's
    /// gating view stays complete when an action is added later.
    pub const ALL: [OperatorAction; 9] = [
        OperatorAction::Read,
        OperatorAction::Operate,
        OperatorAction::Configure,
        OperatorAction::ManageNodes,
        OperatorAction::ManageRollouts,
        OperatorAction::ManageWebhooks,
        OperatorAction::ApproveRollouts,
        OperatorAction::ReadAudit,
        OperatorAction::ManageUsers,
    ];
}

impl OperatorPrincipal {
    pub fn legacy_operator() -> Self {
        Self {
            id: "operator".into(),
            roles: vec![OperatorRole::Admin],
            scopes: Vec::new(),
        }
    }

    pub fn can(&self, action: OperatorAction) -> bool {
        self.roles.iter().any(|role| match role {
            OperatorRole::Admin => true,
            OperatorRole::Operator => matches!(
                action,
                OperatorAction::Read
                    | OperatorAction::Operate
                    | OperatorAction::Configure
                    | OperatorAction::ManageNodes
                    | OperatorAction::ManageRollouts
                    | OperatorAction::ManageWebhooks
            ),
            OperatorRole::Viewer => {
                matches!(action, OperatorAction::Read | OperatorAction::ReadAudit)
            }
        })
    }

    pub fn can_scope(
        &self,
        action: OperatorAction,
        resource_type: &str,
        resource_id: Option<&str>,
    ) -> bool {
        self.can(action)
            && (self.scopes.is_empty()
                || self.scopes.iter().any(|scope| {
                    let same_resource = scope.resource_type == resource_type
                        && (scope.resource_id.is_none()
                            || scope.resource_id.as_deref() == resource_id);
                    let node_contains_stream = scope.resource_type == "node"
                        && resource_type != "node"
                        && scope.resource_id.as_ref().is_some_and(|node_id| {
                            resource_id.is_some_and(|resource| {
                                resource == node_id || resource.starts_with(&format!("{node_id}/"))
                            })
                        });
                    same_resource || node_contains_stream
                }))
    }

    /// Identity echo for the console: who is calling, their roles, and the
    /// role-level actions this principal holds. `actions` lists role-level
    /// capabilities (`can`), not per-resource decisions — a scoped
    /// credential can hold an action here and still be denied on a specific
    /// resource, because every mutation additionally enforces resource
    /// scopes server-side (`can_scope`); the echo carries no resource
    /// context from which to precompute that decision.
    pub fn echo(&self) -> OperatorPrincipalEcho {
        OperatorPrincipalEcho {
            id: self.id.clone(),
            roles: self.roles.clone(),
            actions: OperatorAction::ALL
                .iter()
                .copied()
                .filter(|action| self.can(*action))
                .collect(),
        }
    }
}

/// The `principal` field of `/system`. Informational identity only — it
/// never carries scopes or credential material, and authorizing a mutation
/// always re-resolves the principal server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperatorPrincipalEcho {
    pub id: String,
    pub roles: Vec<OperatorRole>,
    pub actions: Vec<OperatorAction>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DesiredStateRequest {
    pub state: String,
    #[serde(default)]
    pub config_version: Option<String>,
    #[serde(default)]
    pub action_id: Option<String>,
}

/// Incoming webhook subscription (full-replace payload entry). The signing
/// secret is write-only: responses never echo it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebhookSubscriptionInput {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub secret: Option<String>,
}

/// Response representation of a stored webhook subscription. Never contains
/// the signing secret.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookSubscriptionResponse {
    pub id: String,
    pub url: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
    pub created_by: Option<String>,
    pub created_at_ms: u64,
    /// Set when the signing secret is configured; the value itself never
    /// leaves the Hub.
    pub has_secret: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RestartActionRequest {
    #[serde(default)]
    pub action_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateRolloutRequest {
    pub config_version: String,
    #[serde(default)]
    pub node_ids: Vec<String>,
    #[serde(default = "default_rollout_batch_size")]
    pub batch_size: u32,
    /// Label selector resolved to an explicit node set once, at request time
    /// (mutually exclusive with `node_ids`; see the `node-labels` capability).
    #[serde(default)]
    pub target_selector: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubmitApprovalRequest {
    pub config_version: String,
    #[serde(default)]
    pub node_ids: Vec<String>,
    #[serde(default = "default_rollout_batch_size")]
    pub batch_size: u32,
    /// Label selector resolved to an explicit node set once, at request time
    /// (mutually exclusive with `node_ids`; see the `node-labels` capability).
    #[serde(default)]
    pub target_selector: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApprovalDecisionRequest {
    /// `approve` or `reject`.
    pub decision: String,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RolloutActionRequest {
    pub action: String,
    #[serde(default)]
    pub config_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateJobRequest {
    pub spec: serde_json::Value,
    #[serde(default)]
    pub node_ids: Vec<String>,
    #[serde(default = "default_job_desired_state")]
    pub desired_state: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidateJobRequest {
    pub spec: serde_json::Value,
    #[serde(default)]
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobUpgradeRequest {
    pub spec: serde_json::Value,
    #[serde(default)]
    pub node_ids: Vec<String>,
    pub expected_generation: u64,
    /// `"stopped"` (default, the classic stop-the-world flow) or `"atomic"`
    /// (savepoint → version commit → recovery start, supervised by the Hub).
    #[serde(default)]
    pub mode: Option<String>,
    /// Required for the stopped mode; the atomic mode takes its own savepoint.
    #[serde(default)]
    pub savepoint_id: Option<String>,
    /// Atomic mode: per-orchestration verification timeout override.
    #[serde(default)]
    pub verify_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobUpgradeActionRequest {
    pub action: String,
}

fn default_job_desired_state() -> String {
    "stopped".into()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobDesiredStateRequest {
    pub state: String,
}

fn default_rollout_batch_size() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize)]
pub struct AcceptedIntentResponse {
    pub operation_id: String,
    pub intent_id: String,
    pub node_id: String,
    pub stream_id: String,
    pub generation: u64,
    pub desired_state: String,
    pub config_version: Option<String>,
    pub action_id: Option<String>,
    pub intent_state: String,
    pub convergence: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReconciliationDetailResponse {
    pub intent_id: String,
    pub attempt_id: Option<String>,
    pub generation: u64,
    pub intent_state: String,
    pub attempt_state: Option<String>,
    pub convergence: String,
    pub retry_count: u32,
    pub next_retry_at_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub observed_generation: Option<u64>,
    pub observed_state: Option<String>,
}

impl From<IntentRecord> for AcceptedIntentResponse {
    fn from(intent: IntentRecord) -> Self {
        Self {
            operation_id: intent.intent_id.clone(),
            intent_id: intent.intent_id,
            node_id: intent.node_id,
            stream_id: intent.stream_id,
            generation: intent.generation,
            desired_state: intent.desired_state,
            config_version: intent.config_version_id,
            action_id: intent.action_id,
            intent_state: intent.state,
            convergence: intent.convergence_state,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserRequest {
    pub user_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub role: String,
    #[serde(default)]
    pub scopes: Vec<ResourceScope>,
    #[serde(default)]
    pub oidc_subject: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub scopes: Option<Vec<ResourceScope>>,
    #[serde(default)]
    pub disabled: Option<bool>,
    /// Empty string clears the OIDC binding.
    #[serde(default)]
    pub oidc_subject: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserResponse {
    pub user_id: String,
    pub display_name: String,
    pub role: String,
    pub scopes: Vec<ResourceScope>,
    pub disabled: bool,
    pub oidc_subject: Option<String>,
    pub tokens_valid_after_ms: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateCredentialRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialView {
    pub credential_id: String,
    pub label: String,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub last_used_at_ms: Option<u64>,
    pub revoked_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedCredentialResponse {
    pub credential: CredentialView,
    /// Plaintext token; returned exactly once at issuance/rotation.
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokedTokensResponse {
    pub revoked_sessions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_request_and_response_have_stable_wire_names() {
        let request: DesiredStateRequest =
            serde_json::from_str(r#"{"state":"running","config_version":"cfg-1"}"#).unwrap();
        assert_eq!(request.state, "running");
        assert_eq!(request.action_id, None);

        let response = AcceptedIntentResponse {
            operation_id: "intent-1".into(),
            intent_id: "intent-1".into(),
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            generation: 1,
            desired_state: "running".into(),
            config_version: None,
            action_id: None,
            intent_state: "accepted".into(),
            convergence: "pending".into(),
        };
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["intent_state"], "accepted");
        assert_eq!(value["convergence"], "pending");

        let detail = ReconciliationDetailResponse {
            intent_id: "intent-1".into(),
            convergence: "applying".into(),
            ..Default::default()
        };
        let detail = serde_json::to_value(detail).unwrap();
        assert_eq!(detail["intent_id"], "intent-1");
        assert_eq!(detail["convergence"], "applying");
    }

    #[test]
    fn operator_roles_have_explicit_action_boundaries() {
        let viewer = OperatorPrincipal {
            id: "viewer".into(),
            roles: vec![OperatorRole::Viewer],
            scopes: vec![ResourceScope {
                resource_type: "node".into(),
                resource_id: Some("node-a".into()),
            }],
        };
        assert!(viewer.can(OperatorAction::Read));
        assert!(viewer.can(OperatorAction::ReadAudit));
        assert!(!viewer.can(OperatorAction::Operate));
        assert!(viewer.can_scope(OperatorAction::Read, "node", Some("node-a")));
        assert!(!viewer.can_scope(OperatorAction::Read, "node", Some("node-b")));
        assert!(OperatorPrincipal::legacy_operator().can(OperatorAction::ManageRollouts));
    }

    #[test]
    fn manage_webhooks_is_admin_and_operator_but_not_viewer() {
        assert!(OperatorPrincipal::legacy_operator().can(OperatorAction::ManageWebhooks));
        let operator = OperatorPrincipal {
            id: "ops".into(),
            roles: vec![OperatorRole::Operator],
            scopes: Vec::new(),
        };
        assert!(operator.can(OperatorAction::ManageWebhooks));
        let viewer = OperatorPrincipal {
            id: "viewer".into(),
            roles: vec![OperatorRole::Viewer],
            scopes: Vec::new(),
        };
        assert!(!viewer.can(OperatorAction::ManageWebhooks));
        // Fleet-level resource: a node-scoped principal cannot reach the
        // webhook routes even with the right role actions elsewhere.
        let node_scoped = OperatorPrincipal {
            id: "scoped".into(),
            roles: vec![OperatorRole::Operator],
            scopes: vec![ResourceScope {
                resource_type: "node".into(),
                resource_id: Some("node-a".into()),
            }],
        };
        assert!(!node_scoped.can_scope(OperatorAction::ManageWebhooks, "webhook", None));
        assert!(operator.can_scope(OperatorAction::ManageWebhooks, "webhook", None));
    }

    #[test]
    fn approve_rollouts_is_admin_only() {
        // Admin (legacy static token and structured credentials) can decide
        // approval requests.
        assert!(OperatorPrincipal::legacy_operator().can(OperatorAction::ApproveRollouts));
        let admin = OperatorPrincipal {
            id: "root".into(),
            roles: vec![OperatorRole::Admin],
            scopes: Vec::new(),
        };
        assert!(admin.can(OperatorAction::ApproveRollouts));
        // The operator role can submit requests but never approve them, and
        // the viewer role can do neither.
        let operator = OperatorPrincipal {
            id: "alice".into(),
            roles: vec![OperatorRole::Operator],
            scopes: Vec::new(),
        };
        assert!(operator.can(OperatorAction::ManageRollouts));
        assert!(!operator.can(OperatorAction::ApproveRollouts));
        let viewer = OperatorPrincipal {
            id: "bob".into(),
            roles: vec![OperatorRole::Viewer],
            scopes: Vec::new(),
        };
        assert!(!viewer.can(OperatorAction::ApproveRollouts));
    }
}
