//! Operation list/get/cancel handlers for the local control plane and the Hub.
use super::{problem, require_operator_action, OperationQuery};
use crate::api_contract::OperatorAction;
use crate::hub;
use arkflow_core::control::Page;
use arkflow_core::control_plane::ControlPlane;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub(super) async fn hub_operations(
    State(hub): State<hub::Hub>,
    Query(query): Query<OperationQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Read,
        "operation",
        query.node_id.clone(),
    )
    .await
    {
        return response;
    }
    let mut items = hub.operations(query.node_id.as_deref()).await;
    if let Some(resource_id) = query.resource_id.as_deref() {
        items.retain(|item| item.resource_id == resource_id);
    }
    if let Some(value) = query.operation {
        items.retain(|item| item.operation.as_str() == value);
    }
    // The Hub's own state set is a superset of the control-plane states the
    // query accepts; map onto it so the list and total honour the filter.
    if let Some(state) = query.state {
        let state = match state {
            arkflow_core::control::OperationState::Queued => hub::HubOperationState::Queued,
            arkflow_core::control::OperationState::Running => hub::HubOperationState::Running,
            arkflow_core::control::OperationState::Succeeded => hub::HubOperationState::Succeeded,
            arkflow_core::control::OperationState::Failed => hub::HubOperationState::Failed,
            arkflow_core::control::OperationState::Cancelled => hub::HubOperationState::Cancelled,
            arkflow_core::control::OperationState::TimedOut => hub::HubOperationState::TimedOut,
        };
        items.retain(|item| item.state == state);
    }
    if let Some(correlation_id) = query.correlation_id {
        items.retain(|item| item.correlation_id.as_deref() == Some(correlation_id.as_str()));
    }
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip(page.saturating_sub(1).saturating_mul(page_size))
            .take(page_size)
            .collect::<Vec<_>>(),
        page,
        page_size,
        total,
    })
    .into_response()
}

/// Authorize one operator action on a Hub operation and return the resolved
/// record. The operation id lives in its own namespace (`hop-42`), so a
/// node-scoped principal authorizes through the operation's owning node while
/// an explicit operation-ID scope still authorizes directly. `None` means the
/// operation does not exist (the caller reports 404).
#[allow(clippy::result_large_err)] // same ready-made Response idiom as require_operator_action
async fn authorize_operation(
    hub: &hub::Hub,
    headers: &HeaderMap,
    action: OperatorAction,
    id: &str,
) -> Result<Option<hub::HubOperation>, Response> {
    let operation = hub.operation(id).await;
    let supplied = super::bearer(headers);
    let principal = hub.operator_principal(supplied.as_deref()).await;
    let allowed = principal.as_ref().is_some_and(|principal| {
        principal.can_scope(action, "operation", Some(id))
            || operation.as_ref().is_some_and(|operation| {
                principal.can_scope(action, "operation", Some(&operation.node_id))
            })
    });
    if !allowed {
        // Same audit + denial path as every other endpoint.
        require_operator_action(hub, headers, action, "operation", Some(id.to_owned())).await?;
    }
    Ok(operation)
}

pub(super) async fn hub_operation(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let operation = match authorize_operation(&hub, &headers, OperatorAction::Read, &id).await {
        Ok(operation) => operation,
        Err(response) => return response,
    };
    match operation {
        Some(operation) => Json(operation).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

pub(super) async fn hub_cancel_operation(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let operation = match authorize_operation(&hub, &headers, OperatorAction::Operate, &id).await {
        Ok(operation) => operation,
        Err(response) => return response,
    };
    match operation {
        Some(_) => match hub.cancel_operation(&id).await {
            Some(operation) => Json(operation).into_response(),
            None => problem(
                StatusCode::NOT_FOUND,
                "operation_not_found",
                format!("Unknown operation: {id}"),
            ),
        },
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

pub(super) async fn operations(
    State(cp): State<ControlPlane>,
    Query(query): Query<OperationQuery>,
) -> Json<Page<arkflow_core::control::OperationRecord>> {
    let mut items = cp.operations().await;
    if let Some(resource_id) = query.resource_id {
        items.retain(|item| item.resource_id == resource_id);
    }
    if let Some(operation) = query.operation {
        items.retain(|item| item.operation.as_str() == operation.as_str());
    }
    if let Some(state) = query.state {
        items.retain(|item| item.state == state);
    }
    if let Some(correlation_id) = query.correlation_id {
        items.retain(|item| item.correlation_id.as_deref() == Some(correlation_id.as_str()));
    }
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    let items = items
        .into_iter()
        .skip(page.saturating_sub(1).saturating_mul(page_size))
        .take(page_size)
        .collect();
    Json(Page {
        items,
        page,
        page_size,
        total,
    })
}

pub(super) async fn operation(State(cp): State<ControlPlane>, Path(id): Path<String>) -> Response {
    match cp.operation(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

pub(super) async fn cancel_operation(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
) -> Response {
    match cp.cancel_operation(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}
