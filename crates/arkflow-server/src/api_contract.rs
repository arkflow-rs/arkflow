//! Stable v1 operator HTTP wire shapes.
//!
//! These types intentionally contain no Axum extractors. They are shared by
//! handlers, contract tests, and future generated OpenAPI descriptions.

use crate::storage::IntentRecord;
use serde::{Deserialize, Serialize};

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
    ReadAudit,
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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DesiredStateRequest {
    pub state: String,
    #[serde(default)]
    pub config_version: Option<String>,
    #[serde(default)]
    pub action_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RestartActionRequest {
    #[serde(default)]
    pub action_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateRolloutRequest {
    pub config_version: String,
    pub node_ids: Vec<String>,
    #[serde(default = "default_rollout_batch_size")]
    pub batch_size: u32,
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
}
