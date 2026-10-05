//! Configuration rollout handlers on the Hub API.
use super::{hub_problem, problem, require_operator_action};
use crate::api_contract::{CreateRolloutRequest, OperatorAction, RolloutActionRequest};
use crate::hub;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub(super) async fn create_rollout(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<CreateRolloutRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageRollouts,
        "rollout",
        None,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let actor = Some(principal.id);
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match hub
        .create_rollout(
            request.config_version,
            request.node_ids,
            request.batch_size,
            actor,
            correlation_id,
        )
        .await
    {
        Ok(rollout) => (StatusCode::ACCEPTED, Json(rollout)).into_response(),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::BAD_REQUEST, "invalid_rollout", message)
        }
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_rollouts(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    match hub.rollouts().await {
        Ok(rollouts) => Json(rollouts).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_rollout(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.rollout(&id).await {
        Ok(Some((rollout, targets))) => Json(serde_json::json!({
            "rollout": rollout,
            "targets": targets,
        }))
        .into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "Rollout not found".into(),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn rollout_action(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RolloutActionRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageRollouts,
        "rollout",
        Some(id.clone()),
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
    match hub
        .act_rollout(
            &id,
            &request.action,
            request.config_version,
            Some(principal.id),
            correlation_id,
        )
        .await
    {
        Ok(rollout) => (StatusCode::ACCEPTED, Json(rollout)).into_response(),
        Err(hub::HubError::Invalid(message)) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_rollout_action",
            message,
        ),
        Err(error) => hub_problem(error),
    }
}
