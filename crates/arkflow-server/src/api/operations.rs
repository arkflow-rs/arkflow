//! Operation list/get/cancel handlers for the local control plane and the Hub.
use super::{authorized, problem, require_operator_action, OperationQuery};
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
    _headers: HeaderMap,
) -> Response {
    let mut items = hub.operations(query.node_id.as_deref()).await;
    if let Some(resource_id) = query.resource_id.as_deref() {
        items.retain(|item| item.resource_id == resource_id);
    }
    if let Some(value) = query.operation {
        items.retain(|item| item.operation.as_str() == value);
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

pub(super) async fn hub_operation(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.operation(&id).await {
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
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "operation",
        Some(id.clone()),
    )
    .await
    {
        return response;
    }
    match hub.cancel_operation(&id).await {
        Some(operation) => Json(operation).into_response(),
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
    headers: HeaderMap,
) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    match cp.cancel_operation(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}
