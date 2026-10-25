//! Fleet overview (system/status/nodes) and node maintenance state handlers.
use super::{hub_problem, page_items, problem, require_operator_action, PageQuery};
use crate::api_contract::OperatorAction;
use crate::hub;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub(super) struct NodePageQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    node_id: Option<String>,
    /// Label filter: comma-joined `key=value` terms (Kubernetes-style),
    /// ANDed together. Label keys/values never contain commas, so the
    /// split is unambiguous.
    label: Option<String>,
}

impl From<&NodePageQuery> for PageQuery {
    fn from(query: &NodePageQuery) -> Self {
        Self {
            page: query.page,
            page_size: query.page_size,
            node_id: query.node_id.clone(),
        }
    }
}

pub(super) async fn hub_system(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    let nodes = hub.nodes().await;
    let leadership = hub.leadership().await;
    Json(
        serde_json::json!({"id":"arkflow-control-hub", "version":env!("CARGO_PKG_VERSION"), "state":"running", "node_count":nodes.len(), "online_nodes":nodes.iter().filter(|node| node.state == hub::NodeConnectionState::Online).count(), "capabilities":["node_registry","command_dispatch","fleet_aggregation"], "ha": {"enabled": hub.ha_config().enabled, "role": leadership.role(), "epoch": leadership.epoch(), "transitions": hub.leadership_transitions()}}),
    ).into_response()
}

/// Fleet-aggregated EngineStatus so console clients get one overview
/// contract in local and Hub mode. Only a lease-holding Hub reaches this
/// handler; standbys are rejected by the router's standby middleware.
pub(super) async fn hub_status(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    let nodes = hub.nodes().await;
    Json(arkflow_core::control::EngineStatus {
        version: env!("CARGO_PKG_VERSION").into(),
        state: "running".into(),
        uptime_seconds: hub.uptime_seconds(),
        streams_total: nodes.iter().map(|node| node.streams_total).sum(),
        streams_running: nodes.iter().map(|node| node.streams_running).sum(),
        streams_failed: nodes.iter().map(|node| node.streams_failed).sum(),
        approval_required: hub.approval_required(),
    })
    .into_response()
}

pub(super) async fn hub_nodes(
    State(hub): State<hub::Hub>,
    Query(query): Query<NodePageQuery>,
    _headers: HeaderMap,
) -> Response {
    let mut filters = Vec::new();
    if let Some(label) = &query.label {
        if !label.trim().is_empty() {
            for term in label.split(',') {
                match hub::parse_label_term(term.trim()) {
                    Ok(pair) => filters.push(pair),
                    Err(message) => {
                        return problem(StatusCode::BAD_REQUEST, "invalid_label_filter", message)
                    }
                }
            }
        }
    }
    let nodes = hub.nodes().await;
    let filtered: Vec<_> = nodes
        .into_iter()
        .filter(|node| {
            filters
                .iter()
                .all(|(key, value)| node.labels.get(key) == Some(value))
        })
        .collect();
    let pagination = PageQuery::from(&query);
    Json(page_items(filtered, &pagination)).into_response()
}

#[derive(Debug, Deserialize)]
pub(super) struct ReplaceLabelsRequest {
    pub labels: BTreeMap<String, String>,
}

/// Full replacement of a node's label map (see the `node-labels`
/// capability). The submitted map becomes the node's complete label set.
pub(super) async fn hub_replace_node_labels(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReplaceLabelsRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageNodes,
        "node",
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
    match hub
        .replace_node_labels(&node_id, request.labels, Some(principal.id), correlation_id)
        .await
    {
        Ok(labels) => Json(serde_json::json!({
            "node_id": node_id,
            "labels": labels,
        }))
        .into_response(),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::BAD_REQUEST, "invalid_labels", message)
        }
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_drain_node(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_set_maintenance(
        hub,
        node_id,
        arkflow_core::control::NodeMaintenanceState::Draining,
        headers,
    )
    .await
}

pub(super) async fn hub_maintain_node(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_set_maintenance(
        hub,
        node_id,
        arkflow_core::control::NodeMaintenanceState::Maintenance,
        headers,
    )
    .await
}

pub(super) async fn hub_resume_node(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_set_maintenance(
        hub,
        node_id,
        arkflow_core::control::NodeMaintenanceState::Active,
        headers,
    )
    .await
}

pub(super) async fn hub_set_maintenance(
    hub: hub::Hub,
    node_id: String,
    state: arkflow_core::control::NodeMaintenanceState,
    headers: HeaderMap,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageNodes,
        "node",
        Some(node_id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let correlation = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match hub
        .set_node_maintenance(&node_id, state, Some(principal.id), correlation)
        .await
    {
        Ok(node) => Json(node).into_response(),
        Err(error) => hub_problem(error),
    }
}
