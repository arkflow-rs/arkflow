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

pub(super) async fn hub_system(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    // The auth middleware guarantees an authorized caller by the time the
    // handler runs; resolving again (rather than trusting an extension)
    // keeps the echo on the exact enforcement path. A None here still
    // echoes null rather than omitting the field.
    let principal = hub
        .operator_principal(super::bearer(&headers).as_deref())
        .await;
    // Fleet-wide readings (node counts, HA leadership) are scoped like
    // `hub_status`. The identity echo stays available to every authenticated
    // principal — the console gates its own UI from it, including for
    // node-scoped credentials — so the fields are omitted, never faked.
    let fleet_visible = principal
        .as_ref()
        .is_some_and(|principal| principal.can_scope(OperatorAction::Read, "node", None));
    let mut payload = serde_json::json!({
        "id": "arkflow-control-hub",
        "version": env!("CARGO_PKG_VERSION"),
        "state": "running",
        "capabilities": ["node_registry", "command_dispatch", "fleet_aggregation"],
        "principal": principal.as_ref().map(|principal| principal.echo()),
    });
    if fleet_visible {
        let nodes = hub.nodes().await;
        let leadership = hub.leadership().await;
        payload["node_count"] = serde_json::json!(nodes.len());
        payload["online_nodes"] = serde_json::json!(nodes
            .iter()
            .filter(|node| node.state == hub::NodeConnectionState::Online)
            .count());
        payload["ha"] = serde_json::json!({
            "enabled": hub.ha_config().enabled,
            "role": leadership.role(),
            "epoch": leadership.epoch(),
            "transitions": hub.leadership_transitions(),
        });
    }
    Json(payload).into_response()
}

/// Fleet-aggregated EngineStatus so console clients get one overview
/// contract in local and Hub mode. Only a lease-holding Hub reaches this
/// handler; standbys are rejected by the router's standby middleware.
pub(super) async fn hub_status(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Read, "node", None).await
    {
        return response;
    }
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
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Read, "node", None).await
    {
        return response;
    }
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

fn node_credential_view(
    record: crate::storage::NodeCredentialRecord,
) -> crate::api_contract::NodeCredentialView {
    crate::api_contract::NodeCredentialView {
        credential_id: record.credential_id,
        node_id: record.node_id,
        label: record.label,
        created_at_ms: record.created_at_ms,
        expires_at_ms: record.expires_at_ms,
        last_used_at_ms: record.last_used_at_ms,
        revoked_at_ms: record.revoked_at_ms,
    }
}

fn node_credential_problem(error: hub::HubError) -> Response {
    match error {
        hub::HubError::NotFound => problem(
            StatusCode::NOT_FOUND,
            "credential_not_found",
            error.to_string(),
        ),
        hub::HubError::Invalid(message) => {
            problem(StatusCode::BAD_REQUEST, "invalid_credential", message)
        }
        hub::HubError::IdentityStoreUnavailable => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "identity_store_unavailable",
            error.to_string(),
        ),
        hub::HubError::CredentialLimit => {
            problem(StatusCode::CONFLICT, "credential_limit", error.to_string())
        }
        other => hub_problem(other),
    }
}

pub(super) async fn hub_node_credentials(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageNodes,
        "node",
        Some(node_id.clone()),
    )
    .await
    {
        return response;
    }
    match hub.list_node_credentials(&node_id).await {
        Ok(records) => {
            let views: Vec<_> = records.into_iter().map(node_credential_view).collect();
            Json(views).into_response()
        }
        Err(error) => node_credential_problem(error),
    }
}

pub(super) async fn hub_create_node_credential(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<crate::api_contract::CreateNodeCredentialRequest>,
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
    let label = request.label.unwrap_or_default();
    match hub
        .create_node_credential(&node_id, &label, request.expires_at_ms, &principal.id)
        .await
    {
        Ok((record, token)) => (
            StatusCode::CREATED,
            Json(crate::api_contract::IssuedNodeCredentialResponse {
                credential: node_credential_view(record),
                token,
            }),
        )
            .into_response(),
        Err(error) => node_credential_problem(error),
    }
}

pub(super) async fn hub_rotate_node_credential(
    State(hub): State<hub::Hub>,
    Path(credential_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let existing = match hub.get_node_credential(&credential_id).await {
        Ok(existing) => existing,
        Err(error) => return node_credential_problem(error),
    };
    let target_node = existing.as_ref().map(|record| record.node_id.clone());
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageNodes,
        "node",
        target_node,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if existing.is_none() {
        return node_credential_problem(hub::HubError::NotFound);
    }
    match hub
        .rotate_node_credential(&credential_id, &principal.id)
        .await
    {
        Ok((record, token)) => Json(crate::api_contract::IssuedNodeCredentialResponse {
            credential: node_credential_view(record),
            token,
        })
        .into_response(),
        Err(error) => node_credential_problem(error),
    }
}

pub(super) async fn hub_revoke_node_credential(
    State(hub): State<hub::Hub>,
    Path(credential_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let existing = match hub.get_node_credential(&credential_id).await {
        Ok(existing) => existing,
        Err(error) => return node_credential_problem(error),
    };
    let target_node = existing.as_ref().map(|record| record.node_id.clone());
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageNodes,
        "node",
        target_node,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if existing.is_none() {
        return node_credential_problem(hub::HubError::NotFound);
    }
    match hub
        .revoke_node_credential(&credential_id, &principal.id)
        .await
    {
        Ok(revoked) => Json(serde_json::json!({ "revoked": revoked })).into_response(),
        Err(error) => node_credential_problem(error),
    }
}
