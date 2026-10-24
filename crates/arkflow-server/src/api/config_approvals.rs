//! Configuration approval request handlers on the Hub API (see the
//! `configuration-approval` capability).
use super::{hub_problem, problem, require_operator_action};
use crate::api_contract::{OperatorAction, SubmitApprovalRequest, ApprovalDecisionRequest};
use crate::hub;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub(super) struct ApprovalListQuery {
    status: Option<String>,
}

pub(super) async fn submit_approval(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<SubmitApprovalRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageRollouts,
        "config_approval",
        None,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    match hub
        .submit_config_approval(
            request.config_version,
            request.node_ids,
            request.batch_size,
            principal.id,
        )
        .await
    {
        Ok(submitted) => (StatusCode::CREATED, Json(submitted)).into_response(),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::BAD_REQUEST, "invalid_approval_request", message)
        }
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn list_approvals(
    State(hub): State<hub::Hub>,
    Query(query): Query<ApprovalListQuery>,
    _headers: HeaderMap,
) -> Response {
    let status = query.status.as_deref();
    if let Some(status) = status {
        if crate::storage::ConfigApprovalStatus::parse(status).is_none() {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_status",
                format!(
                    "unknown status '{status}'; expected one of pending, approving, approved, rejected, expired"
                ),
            );
        }
    }
    match hub.config_approvals(status).await {
        Ok(approvals) => Json(approvals).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn get_approval(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.config_approval(&id).await {
        Ok(Some(approval)) => Json(approval).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "Approval request not found".into(),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn decide_approval(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ApprovalDecisionRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ApproveRollouts,
        "config_approval",
        Some(id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let decision = request.decision.as_str();
    let approve = match decision {
        "approve" | "reject" => decision == "approve",
        other => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_decision",
                format!("unknown decision '{other}'; expected approve or reject"),
            )
        }
    };
    match hub
        .decide_config_approval(&id, approve, principal.id, request.note)
        .await
    {
        Ok(decided) => (StatusCode::ACCEPTED, Json(decided)).into_response(),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_decision", message)
        }
        Err(error) => hub_problem(error),
    }
}
