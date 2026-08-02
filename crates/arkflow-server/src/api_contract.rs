//! Stable v1 operator HTTP wire shapes.
//!
//! These types intentionally contain no Axum extractors. They are shared by
//! handlers, contract tests, and future generated OpenAPI descriptions.

use crate::storage::IntentRecord;
use serde::{Deserialize, Serialize};

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
}
