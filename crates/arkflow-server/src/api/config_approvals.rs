//! Configuration approval request handlers on the Hub API (see the
//! `configuration-approval` capability).
use super::{hub_problem, problem, require_operator_action};
use crate::api_contract::{ApprovalDecisionRequest, OperatorAction, SubmitApprovalRequest};
use crate::hub;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub(super) struct ApprovalListQuery {
    status: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

impl From<&ApprovalListQuery> for super::PageQuery {
    fn from(query: &ApprovalListQuery) -> Self {
        super::PageQuery {
            page: query.page,
            page_size: query.page_size,
            node_id: None,
        }
    }
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
    let node_ids = match super::resolve_target_selection(
        &hub,
        request.node_ids,
        request.target_selector,
    )
    .await
    {
        Ok(node_ids) => node_ids,
        Err(response) => return response,
    };
    match hub
        .submit_config_approval(
            request.config_version,
            node_ids,
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
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Read,
        "config_approval",
        None,
    )
    .await
    {
        return response;
    }
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
        Ok(approvals) => Json(super::page_items(approvals, &(&query).into())).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn get_approval(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Read,
        "config_approval",
        Some(id.clone()),
    )
    .await
    {
        return response;
    }
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
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageRollouts,
        "config_approval",
        Some(id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    // Approving always needs the second-principal right. Rejecting your own
    // pending request is a withdrawal and must stay available to the
    // submitting operator; rejecting someone else's request still needs the
    // approval right.
    let needs_approval_right = if approve {
        true
    } else {
        match hub.config_approval(&id).await {
            Ok(Some(approval)) => approval.requester_id != principal.id,
            _ => true,
        }
    };
    if needs_approval_right {
        if let Err(response) = require_operator_action(
            &hub,
            &headers,
            OperatorAction::ApproveRollouts,
            "config_approval",
            Some(id.clone()),
        )
        .await
        {
            return response;
        }
    }
    match hub
        .decide_config_approval(&id, approve, principal.id, request.note)
        .await
    {
        Ok(decided) => (StatusCode::ACCEPTED, Json(decided)).into_response(),
        Err(hub::HubError::Invalid(message)) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_decision",
            message,
        ),
        Err(error) => hub_problem(error),
    }
}
