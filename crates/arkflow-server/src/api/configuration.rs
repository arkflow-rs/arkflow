//! Configuration report/draft/validate/diff/apply/rollback handlers for the
//! local control plane and the Hub's node-targeted configuration API.
use super::{problem, require_operator_action, DiffQuery};
use crate::api_contract::OperatorAction;
use crate::hub;
use crate::hub::AgentOperation;
use crate::storage;
use arkflow_core::configuration::{parse_and_validate, redacted_config, ConfigCandidate};
use arkflow_core::control_plane::ControlPlane;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

pub(super) async fn hub_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.configuration(&node_id).await {
        Some(configuration) => Json(configuration).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "node_configuration_unavailable",
            "Node has not reported configuration".into(),
        ),
    }
}

pub(super) async fn hub_configuration_versions(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    if hub.configuration(&node_id).await.is_none() {
        return problem(
            StatusCode::NOT_FOUND,
            "node_configuration_unavailable",
            "Node has not reported configuration".into(),
        );
    }
    Json(hub.config_versions(&node_id).await).into_response()
}

/// Read-only configuration reports (validation, version diff) dispatched to
/// the selected node. Unlike apply/rollback these never create versions or
/// rollouts: the report rides the terminal command result.
pub(super) async fn hub_readonly_configuration_command(
    hub: &hub::Hub,
    node_id: String,
    operation: &str,
    payload: serde_json::Value,
    headers: &HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        hub,
        headers,
        OperatorAction::Configure,
        "configuration",
        Some(node_id.clone()),
    )
    .await
    {
        return response;
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match hub
        .enqueue_with_payload(
            node_id,
            AgentOperation::parse(operation),
            "configuration".into(),
            correlation_id,
            Some(payload),
        )
        .await
    {
        Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
        Err(hub::HubError::NodeUnavailable) => problem(
            StatusCode::CONFLICT,
            "node_unavailable",
            "Target node is stale or offline".into(),
        ),
        Err(_) => problem(
            StatusCode::BAD_REQUEST,
            "command_rejected",
            "Invalid configuration command".into(),
        ),
    }
}

pub(super) async fn hub_validate_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    let payload = match serde_json::to_value(candidate) {
        Ok(payload) => payload,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_configuration",
                "Configuration payload is not serializable".into(),
            )
        }
    };
    hub_readonly_configuration_command(&hub, node_id, "validate_configuration", payload, &headers)
        .await
}

pub(super) async fn hub_configuration_diff(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<DiffQuery>,
) -> Response {
    hub_readonly_configuration_command(
        &hub,
        node_id,
        "diff_configuration",
        serde_json::json!({"from": query.from, "to": query.to}),
        &headers,
    )
    .await
}

pub(super) async fn hub_apply_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    hub_configuration_command(&hub, node_id, "apply_configuration", candidate, &headers).await
}

pub(super) async fn hub_rollback_configuration(
    State(hub): State<hub::Hub>,
    Path((node_id, version)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    hub_configuration_command(
        &hub,
        node_id,
        "rollback_configuration",
        serde_json::json!({"id": version}),
        &headers,
    )
    .await
}

pub(super) async fn hub_configuration_command<T: Serialize>(
    hub: &hub::Hub,
    node_id: String,
    operation: &str,
    payload: T,
    headers: &HeaderMap,
) -> Response {
    let principal = match require_operator_action(
        hub,
        headers,
        OperatorAction::Configure,
        "configuration",
        Some(node_id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let payload = match serde_json::to_value(payload) {
        Ok(payload) => payload,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_configuration",
                "Configuration payload is not serializable".into(),
            )
        }
    };
    if hub.has_storage() {
        let config_version_id = if operation == "apply_configuration" {
            headers
                .get("x-config-version")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_millis())
                        .unwrap_or_default();
                    format!("cfg-{timestamp}-{}", std::process::id())
                })
        } else {
            payload
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .unwrap_or_default()
        };
        let rollout = if operation == "apply_configuration" {
            hub.create_rollout_with_content(
                config_version_id,
                payload.to_string(),
                vec![node_id.clone()],
                1,
                Some(principal.id.clone()),
                correlation_id.clone(),
            )
            .await
        } else {
            hub.create_rollout(
                config_version_id,
                vec![node_id.clone()],
                1,
                Some(principal.id.clone()),
                correlation_id.clone(),
            )
            .await
        };
        match rollout {
            Ok(rollout) => {
                return (
                    StatusCode::ACCEPTED,
                    Json(single_node_rollout_response(&rollout, &node_id)),
                )
                    .into_response()
            }
            Err(hub::HubError::Invalid(message)) => {
                return problem(StatusCode::BAD_REQUEST, "invalid_configuration", message)
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(error) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    error.to_string(),
                )
            }
        }
    }
    match hub
        .enqueue_with_payload(
            node_id,
            AgentOperation::parse(operation),
            "configuration".into(),
            correlation_id,
            Some(payload),
        )
        .await
    {
        Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
        Err(hub::HubError::NodeUnavailable) => problem(
            StatusCode::CONFLICT,
            "node_unavailable",
            "Target node is stale or offline".into(),
        ),
        _ => problem(
            StatusCode::BAD_REQUEST,
            "command_rejected",
            "Invalid configuration command".into(),
        ),
    }
}

pub(super) fn single_node_rollout_response(
    rollout: &storage::RolloutRecord,
    node_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "rollout_id": rollout.rollout_id,
        "config_version_id": rollout.config_version_id,
        "config_version": rollout.config_version_id,
        "state": rollout.state,
        "batch_size": rollout.batch_size,
        "current_batch": rollout.current_batch,
        "total_targets": rollout.total_targets,
        "actor": rollout.actor,
        "correlation_id": rollout.correlation_id,
        "created_at_ms": rollout.created_at_ms,
        "updated_at_ms": rollout.updated_at_ms,
        "resource_type": "configuration",
        "resource_id": "__configuration__",
        "node_id": node_id,
        "intent_id": rollout.rollout_id,
        "generation": 1,
        "desired_state": "configured",
        "convergence_state": "pending",
    })
}

pub(super) async fn configuration(State(cp): State<ControlPlane>) -> Response {
    match redacted_config(&cp.configuration().await) {
        Ok(value) => Json(value).into_response(),
        Err(error) => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "configuration_failed",
            error.to_string(),
        ),
    }
}

pub(super) async fn configuration_draft(State(cp): State<ControlPlane>) -> Response {
    match cp.draft().await {
        Some(value) => Json(value).into_response(),
        None => (StatusCode::NO_CONTENT, ()).into_response(),
    }
}

pub(super) async fn save_configuration_draft(
    State(cp): State<ControlPlane>,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    Json(cp.set_draft(candidate).await).into_response()
}

pub(super) async fn configuration_diff(
    State(cp): State<ControlPlane>,
    Query(query): Query<DiffQuery>,
) -> Response {
    let from = match cp.version_store().load(&query.from) {
        Ok(value) => value,
        Err(error) => {
            return problem(
                StatusCode::NOT_FOUND,
                "configuration_version_not_found",
                error.to_string(),
            )
        }
    };
    let to = match cp.version_store().load(&query.to) {
        Ok(value) => value,
        Err(error) => {
            return problem(
                StatusCode::NOT_FOUND,
                "configuration_version_not_found",
                error.to_string(),
            )
        }
    };
    Json(serde_json::json!({"from": query.from, "to": query.to, "changed": from.content != to.content, "from_format": from.format, "to_format": to.format})).into_response()
}

pub(super) async fn validate_configuration(
    // The state extractor stays so the route keeps the shared handler shape;
    // validation itself is stateless (the auth decision lives in the route
    // middleware, not the handler).
    State(_cp): State<ControlPlane>,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    // Validation parses and resolves secret references and constructs every
    // component in the candidate, so it rides behind the same route-level
    // authentication as every other API endpoint: an open endpoint would
    // hand unauthenticated callers a node-local env/file oracle and a
    // per-request construction load.
    match parse_and_validate(&candidate) {
        Ok(report) => Json(report).into_response(),
        Err(issue) => Json(arkflow_core::configuration::ConfigValidationReport {
            valid: false,
            errors: vec![issue],
        })
        .into_response(),
    }
}

pub(super) async fn configuration_versions(State(cp): State<ControlPlane>) -> Response {
    match cp.versions() {
        Ok(value) => Json(value).into_response(),
        Err(error) => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "configuration_versions_failed",
            error.to_string(),
        ),
    }
}

pub(super) async fn apply_configuration(
    State(cp): State<ControlPlane>,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    match cp.apply_configuration(&candidate).await {
        Ok(value) => (StatusCode::ACCEPTED, Json(value)).into_response(),
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "configuration_apply_failed",
            error.to_string(),
        ),
    }
}

pub(super) async fn rollback_configuration(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
) -> Response {
    match cp.rollback_configuration(&id).await {
        Ok(value) => (StatusCode::ACCEPTED, Json(value)).into_response(),
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "configuration_rollback_failed",
            error.to_string(),
        ),
    }
}
