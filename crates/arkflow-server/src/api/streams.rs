//! Stream list/get/lifecycle handlers for the local plane, and the Hub's
//! node-targeted stream command / desired-state handlers.
use super::{problem, problem_with_details, require_operator_action, PageQuery};
use crate::api_contract::{
    AcceptedIntentResponse, DesiredStateRequest, OperatorAction, RestartActionRequest,
};
use crate::hub;
use crate::hub::AgentOperation;
use crate::storage::{self, DesiredMutation};
use arkflow_core::control::Page;
use arkflow_core::control_plane::ControlPlane;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub(super) async fn hub_streams(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    _headers: HeaderMap,
) -> Response {
    let all = hub.streams(query.node_id.as_deref()).await;
    let total = all.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    let items = all
        .into_iter()
        .map(|(node_id, stream)| {
            let mut value = serde_json::to_value(stream).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                object.insert("node_id".into(), serde_json::Value::String(node_id));
            }
            value
        })
        .skip(page.saturating_sub(1).saturating_mul(page_size))
        .take(page_size)
        .collect();
    Json(Page {
        items,
        page,
        page_size,
        total,
    })
    .into_response()
}

pub(super) async fn hub_stream(
    State(hub): State<hub::Hub>,
    Path((node_id, stream_id)): Path<(String, String)>,
    _headers: HeaderMap,
) -> Response {
    match hub.stream_resource(&node_id, &stream_id).await {
        Ok(Some(resource)) => Json(resource).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "stream_not_found",
            format!("Unknown stream {node_id}/{stream_id}"),
        ),
        Err(hub::HubError::StorageUnavailable) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            "Durable control-plane storage is unavailable".into(),
        ),
        Err(error) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            error.to_string(),
        ),
    }
}

pub(super) async fn hub_targeted_command(
    State(hub): State<hub::Hub>,
    Path((node_id, id, action)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "stream",
        Some(format!("{node_id}/{id}")),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if !matches!(action.as_str(), "start" | "stop" | "restart") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_operation",
            "Unsupported node operation".into(),
        );
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if hub.has_storage() && matches!(action.as_str(), "start" | "stop") {
        match hub
            .set_desired_state(DesiredMutation {
                node_id: node_id.clone(),
                stream_id: id.clone(),
                desired_state: if action == "start" {
                    "running".into()
                } else {
                    "stopped".into()
                },
                config_version_id: None,
                action_id: None,
                expected_generation: headers
                    .get("if-match")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_generation_etag),
                actor: Some(principal.id.clone()),
                correlation_id,
                idempotency_key: headers
                    .get("idempotency-key")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                intent_type: None,
                payload_json: None,
            })
            .await
        {
            Ok(intent) => return intent_response(StatusCode::ACCEPTED, intent),
            Err(hub::HubError::GenerationConflict { expected, current }) => {
                return problem(
                    StatusCode::PRECONDITION_FAILED,
                    "generation_conflict",
                    format!("Expected generation {expected}, current generation {current}"),
                )
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(hub::HubError::IdempotencyKeyReused) => {
                return problem(
                    StatusCode::CONFLICT,
                    "idempotency_key_reused",
                    "Idempotency-Key was already used for a different mutation".into(),
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
    if hub.has_storage() && action == "restart" {
        let action_id = headers
            .get("x-action-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or_default();
                format!("restart-{timestamp}-{}", std::process::id())
            });
        match hub
            .restart_state(
                node_id,
                id,
                action_id,
                headers
                    .get("if-match")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_generation_etag),
                Some(principal.id.clone()),
                correlation_id,
                headers
                    .get("idempotency-key")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            )
            .await
        {
            Ok(intent) => return intent_response(StatusCode::ACCEPTED, intent),
            Err(hub::HubError::GenerationConflict { expected, current }) => {
                return problem(
                    StatusCode::PRECONDITION_FAILED,
                    "generation_conflict",
                    format!("Expected generation {expected}, current generation {current}"),
                )
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(hub::HubError::IdempotencyKeyReused) => {
                return problem(
                    StatusCode::CONFLICT,
                    "idempotency_key_reused",
                    "Idempotency-Key was already used for a different mutation".into(),
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
        .enqueue(node_id, AgentOperation::parse(&action), id, correlation_id)
        .await
    {
        Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
        Err(hub::HubError::NodeUnavailable) => problem(
            StatusCode::CONFLICT,
            "node_unavailable",
            "Target node is stale or offline".into(),
        ),
        Err(error) => problem(
            StatusCode::BAD_REQUEST,
            "command_rejected",
            error.to_string(),
        ),
    }
}

pub(super) fn intent_response(status: StatusCode, intent: storage::IntentRecord) -> Response {
    let location = format!("/api/v1/operations/{}", intent.intent_id);
    let etag = format!("\"generation-{}\"", intent.generation);
    (
        status,
        [(header::LOCATION, location), (header::ETAG, etag)],
        Json(AcceptedIntentResponse::from(intent)),
    )
        .into_response()
}

pub(super) async fn hub_restart_action(
    State(hub): State<hub::Hub>,
    Path((node_id, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<RestartActionRequest>>,
) -> Response {
    let Some(action_id) = body.and_then(|Json(request)| request.action_id) else {
        return hub_targeted_command(State(hub), Path((node_id, id, "restart".into())), headers)
            .await;
    };
    let Ok(action_header) = HeaderValue::from_str(&action_id) else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "action_id must be a valid header value".into(),
        );
    };
    let mut headers = headers;
    headers.insert("x-action-id", action_header);
    hub_targeted_command(State(hub), Path((node_id, id, "restart".into())), headers).await
}

pub(super) async fn hub_desired_state(
    State(hub): State<hub::Hub>,
    Path((node_id, id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<DesiredStateRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "stream",
        Some(format!("{node_id}/{id}")),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if !matches!(request.state.as_str(), "running" | "stopped") {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "state must be running or stopped".into(),
        );
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let expected_generation = headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_generation_etag);
    match hub
        .set_desired_state(DesiredMutation {
            node_id: node_id.clone(),
            stream_id: id.clone(),
            desired_state: request.state,
            config_version_id: request.config_version,
            action_id: request.action_id,
            expected_generation,
            actor: Some(principal.id.clone()),
            correlation_id,
            idempotency_key: headers
                .get("idempotency-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            intent_type: None,
            payload_json: None,
        })
        .await
    {
        Ok(intent) => {
            let location = format!("/api/v1/operations/{}", intent.intent_id);
            let etag = format!("\"generation-{}\"", intent.generation);
            (
                StatusCode::ACCEPTED,
                [(header::LOCATION, location), (header::ETAG, etag)],
                Json(AcceptedIntentResponse::from(intent)),
            )
                .into_response()
        }
        Err(hub::HubError::GenerationConflict { expected, current }) => problem_with_details(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
            Some(serde_json::json!({
                "expected_generation": expected,
                "current_generation": current,
                "resource": { "node_id": node_id, "stream_id": id }
            })),
        ),
        Err(hub::HubError::StorageUnavailable) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            "Durable control-plane storage is unavailable".into(),
        ),
        Err(hub::HubError::IdempotencyKeyReused) => problem(
            StatusCode::CONFLICT,
            "idempotency_key_reused",
            "Idempotency-Key was already used for a different mutation".into(),
        ),
        Err(error) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            error.to_string(),
        ),
    }
}

pub(super) fn parse_generation_etag(value: &str) -> Option<u64> {
    value
        .trim_matches('"')
        .strip_prefix("generation-")
        .and_then(|value| value.parse().ok())
}

pub(super) async fn streams(
    State(cp): State<ControlPlane>,
    Query(query): Query<PageQuery>,
) -> Json<Page<arkflow_core::control::StreamStatus>> {
    Json(
        cp.streams(query.page.unwrap_or(1), query.page_size.unwrap_or(50))
            .await,
    )
}

pub(super) async fn stream(State(cp): State<ControlPlane>, Path(id): Path<String>) -> Response {
    match cp.stream(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "stream_not_found",
            format!("Unknown Stream: {id}"),
        ),
    }
}

pub(super) async fn start_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "start", headers).await
}

pub(super) async fn stop_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "stop", headers).await
}

pub(super) async fn restart_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "restart", headers).await
}

pub(super) async fn lifecycle(
    cp: ControlPlane,
    id: String,
    action: &str,
    headers: HeaderMap,
) -> Response {
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match cp.lifecycle(&id, action, correlation_id).await {
        Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
        Err(error) if error.to_string().contains("Unknown stream") => {
            problem(StatusCode::NOT_FOUND, "stream_not_found", error.to_string())
        }
        Err(error) => problem(
            StatusCode::CONFLICT,
            "operation_conflict",
            error.to_string(),
        ),
    }
}
