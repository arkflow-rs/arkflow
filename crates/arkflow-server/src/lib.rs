//! ArkFlow's resource-oriented control-plane HTTP service.
//!
//! HTTP transport lives here; the domain facade consumed by this crate is
//! `arkflow_core::control_plane::ControlPlane` and contains no Axum types.

pub mod agent;
pub mod hub;

use arkflow_core::component::{self, ComponentKind};
use arkflow_core::configuration::redacted_config;
use arkflow_core::configuration::{parse_and_validate, ConfigCandidate};
use arkflow_core::control::{ApiError, Page};
use arkflow_core::control_plane::ControlPlane;
use axum::body::{to_bytes, Body};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub const API_VERSION: &str = "v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_api_prefix")]
    pub api_prefix: String,
    #[serde(default = "default_health_path")]
    pub health_path: String,
    #[serde(default = "default_readiness_path")]
    pub readiness_path: String,
    #[serde(default = "default_liveness_path")]
    pub liveness_path: String,
    #[serde(default)]
    pub cors_origins: Vec<String>,
    #[serde(default)]
    pub node_token: Option<String>,
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

impl ServerConfig {
    pub fn from_engine(config: &arkflow_core::config::EngineConfig) -> Self {
        let health = &config.health_check;
        Self {
            enabled: health.enabled,
            address: health.address.clone(),
            api_prefix: health.api_prefix.clone(),
            health_path: health.health_path.clone(),
            readiness_path: health.readiness_path.clone(),
            liveness_path: health.liveness_path.clone(),
            cors_origins: health.cors_origins.clone(),
            node_token: health.node_token.clone(),
            lease_ttl_ms: health.agent_lease_ttl_ms,
            poll_interval_ms: default_poll_interval_ms(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            address: default_address(),
            api_prefix: default_api_prefix(),
            health_path: default_health_path(),
            readiness_path: default_readiness_path(),
            liveness_path: default_liveness_path(),
            cors_origins: Vec::new(),
            node_token: None,
            lease_ttl_ms: default_lease_ttl_ms(),
            poll_interval_ms: default_poll_interval_ms(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
fn default_address() -> String {
    "127.0.0.1:8080".into()
}
fn default_api_prefix() -> String {
    "/api/v1".into()
}
fn default_health_path() -> String {
    "/health".into()
}
fn default_readiness_path() -> String {
    "/readiness".into()
}
fn default_liveness_path() -> String {
    "/liveness".into()
}
fn default_lease_ttl_ms() -> u64 {
    15_000
}
fn default_poll_interval_ms() -> u64 {
    1_000
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OperationQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    resource_id: Option<String>,
    operation: Option<String>,
    state: Option<arkflow_core::control::OperationState>,
    correlation_id: Option<String>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    event_type: Option<String>,
    stream_id: Option<String>,
    correlation_id: Option<String>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiffQuery {
    from: String,
    to: String,
}

fn page_items<T>(items: Vec<T>, query: &PageQuery) -> Page<T> {
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Page {
        items: items
            .into_iter()
            .skip((page - 1) * page_size)
            .take(page_size)
            .collect(),
        page,
        page_size,
        total,
    }
}

pub fn router(control_plane: ControlPlane, config: &ServerConfig) -> Router {
    let prefix = config.api_prefix.trim_end_matches('/');
    let api = Router::new()
        .route("/system", get(system))
        .route("/status", get(status))
        .route("/nodes", get(nodes))
        .route("/node", get(node))
        .route("/streams", get(streams))
        .route("/streams/{id}", get(stream))
        .route("/streams/{id}/start", post(start_stream))
        .route("/streams/{id}/stop", post(stop_stream))
        .route("/streams/{id}/restart", post(restart_stream))
        .route("/operations", get(operations))
        .route("/operations/{id}", get(operation))
        .route("/operations/{id}", delete(cancel_operation))
        .route("/events", get(events))
        .route("/configuration", get(configuration))
        .route("/configuration/validate", post(validate_configuration))
        .route(
            "/configuration/draft",
            get(configuration_draft).put(save_configuration_draft),
        )
        .route("/configuration/diff", get(configuration_diff))
        .route("/configuration/versions", get(configuration_versions))
        .route("/configuration/apply", post(apply_configuration))
        .route("/configuration/rollback/{id}", post(rollback_configuration))
        .route("/config", get(configuration))
        .route("/config/validate", post(validate_configuration))
        .route("/config/versions", get(configuration_versions))
        .route("/config/apply", post(apply_configuration))
        .route("/config/rollback/{id}", post(rollback_configuration))
        .route("/components", get(components))
        .route("/components/{kind}/{name}", get(component))
        .route("/schema", get(schema))
        .route("/metrics", get(metrics))
        .with_state(control_plane.clone());

    let mut app = Router::new()
        .route(&config.health_path, get(health))
        .route(&config.readiness_path, get(readiness))
        .route(&config.liveness_path, get(liveness))
        .route("/metrics", get(metrics))
        .nest(prefix, api)
        .with_state(control_plane)
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(middleware::from_fn(correlation_middleware))
        .layer(TraceLayer::new_for_http());
    if !config.cors_origins.is_empty() {
        let mut cors = CorsLayer::new();
        for origin in &config.cors_origins {
            if let Ok(value) = origin.parse::<HeaderValue>() {
                cors = cors.allow_origin(value);
            }
        }
        app = app.layer(cors.allow_methods(Any));
    }
    app
}

pub async fn serve(
    control_plane: ControlPlane,
    config: ServerConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        return Ok(());
    }
    let address: SocketAddr = config.address.parse()?;
    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, router(control_plane, &config).into_make_service())
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await?;
    Ok(())
}

/// Build the external Hub API. Unlike `router`, this router has no local
/// Engine state; all node-owned resources come from authenticated Agent
/// reports stored in `Hub`.
pub fn hub_router(hub: hub::Hub, config: &ServerConfig) -> Router {
    let prefix = config.api_prefix.trim_end_matches('/');
    let api = Router::new()
        .route("/system", get(hub_system))
        .route("/nodes", get(hub_nodes))
        .route("/streams", get(hub_streams))
        .route("/nodes/{node_id}/configuration", get(hub_configuration))
        .route(
            "/nodes/{node_id}/configuration/versions",
            get(hub_configuration_versions),
        )
        .route(
            "/nodes/{node_id}/configuration/apply",
            post(hub_apply_configuration),
        )
        .route(
            "/nodes/{node_id}/configuration/rollback/{version}",
            post(hub_rollback_configuration),
        )
        .route(
            "/nodes/{node_id}/streams/{id}/{action}",
            post(hub_targeted_command),
        )
        .route("/operations", get(hub_operations))
        .route("/operations/{id}", get(hub_operation))
        .route("/operations/{id}", delete(hub_cancel_operation))
        .route("/events", get(hub_events))
        .route("/metrics", get(hub_metrics))
        .route("/agent/register", post(agent_register))
        .route("/agent/heartbeat", post(agent_heartbeat))
        .route("/agent/report", post(agent_report))
        .route("/agent/commands", get(agent_commands))
        .route("/agent/commands/{id}/result", post(agent_command_result))
        .with_state(hub.clone());
    let app = Router::new()
        .route(&config.health_path, get(hub_health))
        .route(&config.readiness_path, get(hub_readiness))
        .route(&config.liveness_path, get(hub_liveness))
        .nest(prefix, api)
        .with_state(hub)
        .layer(RequestBodyLimitLayer::new(4 * 1024 * 1024))
        .layer(middleware::from_fn(correlation_middleware))
        .layer(TraceLayer::new_for_http());
    if config.cors_origins.is_empty() {
        app
    } else {
        let mut cors = CorsLayer::new();
        for origin in &config.cors_origins {
            if let Ok(value) = origin.parse::<HeaderValue>() {
                cors = cors.allow_origin(value);
            }
        }
        app.layer(cors.allow_methods(Any))
    }
}

pub async fn serve_hub(
    hub: hub::Hub,
    config: ServerConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        return Ok(());
    }
    let address: SocketAddr = config.address.parse()?;
    let listener = TcpListener::bind(address).await?;
    let sweep_hub = hub.clone();
    let sweep_cancel = cancellation.clone();
    let sweep_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tokio::select! { _ = interval.tick() => sweep_hub.mark_stale().await, _ = sweep_cancel.cancelled() => break }
        }
    });
    let result = axum::serve(listener, hub_router(hub, &config).into_make_service())
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await;
    sweep_task.abort();
    result?;
    Ok(())
}

async fn hub_system(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    let nodes = hub.nodes().await;
    Json(
        serde_json::json!({"id":"arkflow-control-hub", "version":env!("CARGO_PKG_VERSION"), "state":"running", "node_count":nodes.len(), "online_nodes":nodes.iter().filter(|node| node.state == hub::NodeConnectionState::Online).count(), "capabilities":["node_registry","command_dispatch","fleet_aggregation"]}),
    ).into_response()
}

async fn hub_nodes(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    Json(page_items(hub.nodes().await, &query)).into_response()
}

async fn hub_streams(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
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
        .skip((page - 1) * page_size)
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

async fn hub_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    match hub.configuration(&node_id).await {
        Some(configuration) => Json(configuration).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "node_configuration_unavailable",
            "Node has not reported configuration".into(),
        ),
    }
}

async fn hub_configuration_versions(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    if hub.configuration(&node_id).await.is_none() {
        return problem(
            StatusCode::NOT_FOUND,
            "node_configuration_unavailable",
            "Node has not reported configuration".into(),
        );
    }
    Json(Vec::<serde_json::Value>::new()).into_response()
}

async fn hub_apply_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    hub_configuration_command(&hub, node_id, "apply_configuration", candidate, &headers).await
}

async fn hub_rollback_configuration(
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

async fn hub_configuration_command<T: Serialize>(
    hub: &hub::Hub,
    node_id: String,
    operation: &str,
    payload: T,
    headers: &HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
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
    match hub
        .enqueue_with_payload(
            node_id,
            operation.into(),
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

async fn hub_targeted_command(
    State(hub): State<hub::Hub>,
    Path((node_id, id, action)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
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
    match hub.enqueue(node_id, action, id, correlation_id).await {
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

async fn hub_operations(
    State(hub): State<hub::Hub>,
    Query(query): Query<OperationQuery>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    let mut items = hub.operations(query.node_id.as_deref()).await;
    if let Some(resource_id) = query.resource_id.as_deref() {
        items.retain(|item| item.resource_id == resource_id);
    }
    if let Some(value) = query.operation {
        items.retain(|item| item.operation == value);
    }
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip((page - 1) * page_size)
            .take(page_size)
            .collect::<Vec<_>>(),
        page,
        page_size,
        total,
    })
    .into_response()
}

async fn hub_operation(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    match hub.operation(&id).await {
        Some(operation) => Json(operation).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

async fn hub_cancel_operation(
    State(hub): State<hub::Hub>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
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

async fn hub_events(
    State(hub): State<hub::Hub>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    let mut items = hub.events(query.node_id.as_deref()).await;
    if let Some(event_type) = query.event_type.as_deref() {
        items.retain(|item| item.event.event_type == event_type);
    }
    if let Some(stream_id) = query.stream_id.as_deref() {
        items.retain(|item| item.event.stream_id.as_deref() == Some(stream_id));
    }
    if let Some(correlation_id) = query.correlation_id.as_deref() {
        items.retain(|item| item.event.correlation_id.as_deref() == Some(correlation_id));
    }
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip((page - 1) * page_size)
            .take(page_size)
            .collect::<Vec<_>>(),
        page,
        page_size,
        total,
    })
    .into_response()
}

async fn hub_metrics(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if !hub.operator_authorized(bearer(&headers)) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        );
    }
    Json(serde_json::json!({
        "items": hub.metrics_by_node(query.node_id.as_deref()).await,
        "aggregate": hub.metrics(query.node_id.as_deref()).await,
    }))
    .into_response()
}

async fn agent_register(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::RegisterRequest>,
) -> Response {
    match hub.register(request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => hub_problem(error),
    }
}
async fn agent_heartbeat(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::HeartbeatRequest>,
) -> Response {
    match hub.heartbeat(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
async fn agent_report(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::NodeReport>,
) -> Response {
    match hub.report(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
async fn agent_commands(
    State(hub): State<hub::Hub>,
    Query(auth): Query<hub::AgentAuth>,
) -> Response {
    match hub.commands(auth).await {
        Ok(commands) => Json(commands).into_response(),
        Err(error) => hub_problem(error),
    }
}
async fn agent_command_result(
    State(hub): State<hub::Hub>,
    Path(_id): Path<String>,
    Query(auth): Query<hub::AgentAuth>,
    Json(result): Json<hub::CommandResult>,
) -> Response {
    match hub.command_result(auth, result).await {
        Ok(operation) => Json(operation).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_health(State(hub): State<hub::Hub>) -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"healthy","nodes":hub.nodes().await.len()}))
}
async fn hub_readiness(State(hub): State<hub::Hub>) -> Json<serde_json::Value> {
    let _ = hub;
    Json(serde_json::json!({"status":"ready","ready":true}))
}
async fn hub_liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"alive","alive":true}))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}
fn hub_problem(error: hub::HubError) -> Response {
    let status = match error {
        hub::HubError::Unauthorized => StatusCode::UNAUTHORIZED,
        hub::HubError::NodeUnavailable => StatusCode::CONFLICT,
        hub::HubError::NotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    };
    problem(status, "agent_request_rejected", error.to_string())
}

async fn system(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::SystemResource> {
    Json(cp.system().await)
}
async fn status(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::EngineStatus> {
    Json(cp.status().await)
}
async fn node(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::NodeResource> {
    Json(cp.node().await)
}

async fn nodes(
    State(cp): State<ControlPlane>,
    Query(query): Query<PageQuery>,
) -> Json<Page<arkflow_core::control::NodeResource>> {
    Json(page_items(vec![cp.node().await], &query))
}

async fn streams(
    State(cp): State<ControlPlane>,
    Query(query): Query<PageQuery>,
) -> Json<Page<arkflow_core::control::StreamStatus>> {
    Json(
        cp.streams(query.page.unwrap_or(1), query.page_size.unwrap_or(50))
            .await,
    )
}

async fn stream(State(cp): State<ControlPlane>, Path(id): Path<String>) -> Response {
    match cp.stream(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "stream_not_found",
            format!("Unknown Stream: {id}"),
        ),
    }
}

async fn start_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "start", headers).await
}

async fn stop_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "stop", headers).await
}

async fn restart_stream(
    State(cp): State<ControlPlane>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    lifecycle(cp, id, "restart", headers).await
}

async fn lifecycle(cp: ControlPlane, id: String, action: &str, headers: HeaderMap) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
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

async fn operations(
    State(cp): State<ControlPlane>,
    Query(query): Query<OperationQuery>,
) -> Json<Page<arkflow_core::control::OperationRecord>> {
    let mut items = cp.operations().await;
    if let Some(resource_id) = query.resource_id {
        items.retain(|item| item.resource_id == resource_id);
    }
    if let Some(operation) = query.operation {
        items.retain(|item| item.operation == operation);
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
        .skip((page - 1) * page_size)
        .take(page_size)
        .collect();
    Json(Page {
        items,
        page,
        page_size,
        total,
    })
}

async fn operation(State(cp): State<ControlPlane>, Path(id): Path<String>) -> Response {
    match cp.operation(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

async fn cancel_operation(State(cp): State<ControlPlane>, Path(id): Path<String>) -> Response {
    match cp.cancel_operation(&id).await {
        Some(value) => Json(value).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            format!("Unknown operation: {id}"),
        ),
    }
}

async fn events(
    State(cp): State<ControlPlane>,
    Query(query): Query<EventQuery>,
) -> Json<Page<arkflow_core::control::ControlEvent>> {
    let mut items = cp.events().await;
    if let Some(value) = query.event_type {
        items.retain(|item| item.event_type == value);
    }
    if let Some(value) = query.stream_id {
        items.retain(|item| item.stream_id.as_deref() == Some(value.as_str()));
    }
    if let Some(value) = query.correlation_id {
        items.retain(|item| item.correlation_id.as_deref() == Some(value.as_str()));
    }
    items.sort_by_key(|event| std::cmp::Reverse(event.occurred_at_ms));
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip((page - 1) * page_size)
            .take(page_size)
            .collect(),
        page,
        page_size,
        total,
    })
}

async fn configuration(State(cp): State<ControlPlane>, headers: HeaderMap) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    match redacted_config(&cp.configuration().await) {
        Ok(value) => Json(value).into_response(),
        Err(error) => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "configuration_failed",
            error.to_string(),
        ),
    }
}

async fn configuration_draft(State(cp): State<ControlPlane>, headers: HeaderMap) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    match cp.draft().await {
        Some(value) => Json(value).into_response(),
        None => (StatusCode::NO_CONTENT, ()).into_response(),
    }
}

async fn save_configuration_draft(
    State(cp): State<ControlPlane>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    Json(cp.set_draft(candidate).await).into_response()
}

async fn configuration_diff(
    State(cp): State<ControlPlane>,
    headers: HeaderMap,
    Query(query): Query<DiffQuery>,
) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
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

async fn validate_configuration(Json(candidate): Json<ConfigCandidate>) -> Response {
    match parse_and_validate(&candidate) {
        Ok(report) => Json(report).into_response(),
        Err(issue) => Json(arkflow_core::configuration::ConfigValidationReport {
            valid: false,
            errors: vec![issue],
        })
        .into_response(),
    }
}

async fn configuration_versions(State(cp): State<ControlPlane>, headers: HeaderMap) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    match cp.versions() {
        Ok(value) => Json(value).into_response(),
        Err(error) => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "configuration_versions_failed",
            error.to_string(),
        ),
    }
}

async fn apply_configuration(
    State(cp): State<ControlPlane>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
    match cp.apply_configuration(&candidate).await {
        Ok(value) => (StatusCode::ACCEPTED, Json(value)).into_response(),
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "configuration_apply_failed",
            error.to_string(),
        ),
    }
}

async fn rollback_configuration(
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
    match cp.rollback_configuration(&id).await {
        Ok(value) => (StatusCode::ACCEPTED, Json(value)).into_response(),
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "configuration_rollback_failed",
            error.to_string(),
        ),
    }
}

async fn components() -> Json<Vec<serde_json::Value>> {
    Json(component::list_components().into_iter().map(|(kind, item)| serde_json::json!({"kind": kind, "name": item.name, "description": item.description, "schema": item.config_schema, "example": item.config_example})).collect())
}

async fn component(Path((kind, name)): Path<(String, String)>) -> Response {
    let kind = match kind.parse::<ComponentKind>() {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::NOT_FOUND,
                "component_not_found",
                "Unknown component kind".into(),
            )
        }
    };
    match component::get_component_metadata(kind, &name) { Some(item) => Json(serde_json::json!({"kind": kind, "name": item.name, "description": item.description, "schema": item.config_schema, "example": item.config_example})).into_response(), None => problem(StatusCode::NOT_FOUND, "component_not_found", format!("Unknown component: {kind}/{name}")) }
}

async fn schema() -> Json<serde_json::Value> {
    Json(component::build_config_schema())
}

async fn metrics(State(cp): State<ControlPlane>) -> Response {
    let mut body = String::new();
    for stream in cp.runtime_manager().snapshots().await {
        body.push_str(&format!(
            "arkflow_stream_input_messages{{stream_id=\"{}\"}} {}\n",
            stream.id, stream.metrics.input_messages
        ));
        body.push_str(&format!(
            "arkflow_stream_output_messages{{stream_id=\"{}\"}} {}\n",
            stream.id, stream.metrics.output_messages
        ));
        body.push_str(&format!(
            "arkflow_stream_restarts{{stream_id=\"{}\"}} {}\n",
            stream.id, stream.metrics.restarts
        ));
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

async fn health(State(cp): State<ControlPlane>) -> Response {
    let healthy = cp.health().is_running();
    let body = serde_json::json!({"status": if healthy {"healthy"} else {"unhealthy"}, "running": healthy});
    (
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
        .into_response()
}
async fn readiness(State(cp): State<ControlPlane>) -> Response {
    let ready = cp.health().is_ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"status": if ready {"ready"} else {"not_ready"}, "ready": ready})),
    )
        .into_response()
}
async fn liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"alive", "alive":true}))
}

fn authorized(cp: &ControlPlane, headers: &HeaderMap) -> bool {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    cp.authorized(token)
}

fn problem(status: StatusCode, code: &str, message: String) -> Response {
    (
        status,
        Json(ApiError {
            code: code.into(),
            message,
            field: None,
            stream_id: None,
            correlation_id: None,
        }),
    )
        .into_response()
}

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

async fn correlation_middleware(mut request: Request<Body>, next: Next) -> Response {
    let correlation_id = request
        .headers()
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("req-{}", REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)));
    if let Ok(value) = HeaderValue::from_str(&correlation_id) {
        request.headers_mut().insert("x-correlation-id", value);
    }
    let response = next.run(request).await;
    let (mut parts, body) = response.into_parts();
    let body_bytes = to_bytes(body, 1024 * 1024).await.unwrap_or_default();
    let mut replacement = None;
    if parts.status.is_client_error() || parts.status.is_server_error() {
        match serde_json::from_slice::<ApiError>(&body_bytes) {
            Ok(mut error) => {
                if error.correlation_id.is_none() {
                    error.correlation_id = Some(correlation_id.clone());
                }
                replacement = serde_json::to_vec(&error).ok();
            }
            Err(_) if parts.status == StatusCode::BAD_REQUEST => {
                replacement = serde_json::to_vec(&ApiError {
                    code: "invalid_query".into(),
                    message: "Request query or body is invalid".into(),
                    field: None,
                    stream_id: None,
                    correlation_id: Some(correlation_id.clone()),
                })
                .ok();
            }
            Err(_) => {}
        }
    }
    if let Ok(value) = HeaderValue::from_str(&correlation_id) {
        parts.headers.insert("x-correlation-id", value);
    }
    if replacement.is_some() {
        parts.headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    Response::from_parts(
        parts,
        Body::from(replacement.unwrap_or_else(|| body_bytes.to_vec())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
    use arkflow_core::engine::Engine;
    use tower::ServiceExt;

    #[tokio::test]
    async fn resource_router_exposes_system_nodes_streams_and_health() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let cp = engine.control_plane();
        let app = router(cp, &ServerConfig::default());
        assert_eq!(
            app.clone()
                .oneshot(
                    axum::http::Request::get("/api/v1/system")
                        .body(axum::body::Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    axum::http::Request::get("/api/v1/nodes")
                        .body(axum::body::Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes?page=1&page_size=1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let nodes: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(nodes["total"], 1);
        assert_eq!(nodes["items"].as_array().unwrap().len(), 1);
        assert_eq!(nodes["items"][0]["id"], "local-node");
        assert_eq!(
            app.oneshot(
                axum::http::Request::get("/health")
                    .body(axum::body::Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn resource_contract_includes_pagination_and_correlation_id() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let app = router(engine.control_plane(), &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/streams?page=2&page_size=1")
                    .header("x-correlation-id", "test-correlation")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-correlation-id").unwrap(),
            "test-correlation"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["page"], 2);
        assert_eq!(value["page_size"], 1);
        assert!(value["items"].is_array());

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes?page=not-a-number")
                    .header("x-correlation-id", "invalid-page")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["code"], "invalid_query");
        assert_eq!(error["correlation_id"], "invalid-page");

        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/streams/missing")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["code"], "stream_not_found");
    }

    #[tokio::test]
    async fn protected_routes_reject_missing_credentials() {
        let health = HealthCheckConfig {
            api_token: Some("secret".into()),
            ..Default::default()
        };
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });
        let app = router(engine.control_plane(), &ServerConfig::default());
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/configuration")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn resource_api_integration_covers_routes_filters_redaction_and_aliases() {
        let health = HealthCheckConfig {
            api_token: Some("secret-token".into()),
            ..Default::default()
        };
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });
        let control_plane = engine.control_plane();
        control_plane.health().set_ready(true);
        control_plane.health().set_running(true);
        let app = router(control_plane, &ServerConfig::default());
        let auth = "Bearer secret-token";

        for path in [
            "/api/v1/system",
            "/api/v1/status",
            "/api/v1/nodes",
            "/api/v1/streams?page=1&page_size=10",
            "/api/v1/operations?page=1&page_size=10",
            "/api/v1/events?page=1&page_size=10",
            "/api/v1/components",
            "/api/v1/schema",
            "/api/v1/metrics",
            "/health",
            "/readiness",
            "/liveness",
        ] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(path)
                        .header("authorization", auth)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(!response.status().is_server_error(), "route failed: {path}");
        }

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/operations?state=not-a-real-state")
                    .header("authorization", auth)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/streams/missing")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/configuration")
                    .header("authorization", auth)
                    .header("x-correlation-id", "resource-contract")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-correlation-id").unwrap(),
            "resource-contract"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let configuration = String::from_utf8(body.to_vec()).unwrap();
        assert!(configuration.contains("******"));
        assert!(!configuration.contains("secret-token"));

        let draft = serde_json::json!({"format":"json","content":"{\"streams\":[]}"});
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/configuration/draft")
                    .header("authorization", auth)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(draft.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/configuration/draft")
                    .header("authorization", auth)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/configuration/validate")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"format":"json","content":"not-json"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let validation: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(validation["valid"], false);
        for path in ["/api/v1/config", "/api/v1/config/versions"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(path)
                        .header("authorization", auth)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "alias failed: {path}");
        }
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/config/validate")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"format":"json","content":"{\"streams\":[]}"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/configuration/diff?from=missing&to=missing")
                    .header("authorization", auth)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/components")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/components/input/generate")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            response.status(),
            StatusCode::OK | StatusCode::NOT_FOUND
        ));

        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/unknown-resource")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn hub_http_contract_registers_and_routes_a_targeted_command() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
        });
        let app = hub_router(hub, &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/register")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"node_id":"node-a","node_token":"node-secret"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let session: hub::RegisterResponse = serde_json::from_slice(&body).unwrap();

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = app
            .oneshot(
                axum::http::Request::get(format!(
                    "/api/v1/agent/commands?node_id=node-a&session_token={}",
                    session.session_token
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let commands: Vec<hub::AgentCommand> = serde_json::from_slice(&body).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].resource_id, "orders");
    }

    #[tokio::test]
    async fn hub_end_to_end_two_nodes_aggregate_target_reconnect_and_drain() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            lease_ttl_ms: 10_000,
            poll_interval_ms: 10,
        });
        let app = hub_router(hub.clone(), &ServerConfig::default());

        let node_a = register_hub_node(&app, "node-a").await;
        let node_b = register_hub_node(&app, "node-b").await;
        report_hub_node(&app, &node_a, "orders", "online").await;
        report_hub_node(&app, &node_b, "orders", "online").await;

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/system")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let system: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(system["node_count"], 2);
        assert_eq!(system["online_nodes"], 2);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/streams")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let streams: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stream_nodes: Vec<&str> = streams["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|stream| stream["node_id"].as_str().unwrap())
            .collect();
        assert_eq!(stream_nodes, vec!["node-a", "node-b"]);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes?page=2&page_size=1")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let nodes_page: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(nodes_page["total"], 2);
        assert_eq!(nodes_page["items"].as_array().unwrap().len(), 1);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                    .header("authorization", "Bearer operator")
                    .header("x-correlation-id", "two-node-start")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let operation: hub::HubOperation = serde_json::from_slice(&body).unwrap();
        assert_eq!(operation.node_id, "node-a");
        assert_eq!(operation.correlation_id.as_deref(), Some("two-node-start"));

        let commands_a = agent_commands(&app, &node_a).await;
        let commands_b = agent_commands(&app, &node_b).await;
        assert_eq!(commands_a.len(), 1);
        assert!(commands_b.is_empty());
        assert_eq!(commands_a[0].node_id, "node-a");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!(
                    "/api/v1/agent/commands/{}/result?node_id={}&session_token={}",
                    commands_a[0].id, node_a.node_id, node_a.session_token
                ))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&hub::CommandResult {
                        command_id: commands_a[0].id.clone(),
                        operation_id: operation.id.clone(),
                        state: hub::HubOperationState::Succeeded,
                        progress: 100,
                        error: None,
                        correlation_id: Some("two-node-start".into()),
                    })
                    .unwrap(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let operation_after_result = get_hub_operation(&app, &operation.id).await;
        assert_eq!(
            operation_after_result.state,
            hub::HubOperationState::Succeeded
        );

        let old_session = node_a.session_token.clone();
        let node_a_reconnected = register_hub_node(&app, "node-a").await;
        assert_ne!(old_session, node_a_reconnected.session_token);
        let old_session_response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!(
                    "/api/v1/agent/commands?node_id=node-a&session_token={old_session}"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(old_session_response.status(), StatusCode::UNAUTHORIZED);

        let streams_after_reconnect = get_hub_streams(&app).await;
        assert_eq!(streams_after_reconnect["total"], 2);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/heartbeat")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "node_id": "node-a",
                            "session_token": node_a_reconnected.session_token,
                            "state": "draining"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let nodes = get_hub_nodes(&app).await;
        assert_eq!(
            nodes.iter().find(|node| node.id == "node-a").unwrap().state,
            hub::NodeConnectionState::Draining
        );
        assert_eq!(
            nodes.iter().find(|node| node.id == "node-b").unwrap().state,
            hub::NodeConnectionState::Online
        );
    }

    async fn register_hub_node(app: &Router, node_id: &str) -> hub::RegisterResponse {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/register")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"node_id": node_id, "node_token": "node-secret"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn report_hub_node(
        app: &Router,
        session: &hub::RegisterResponse,
        stream_id: &str,
        state: &str,
    ) {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "node_id": session.node_id,
                            "session_token": session.session_token,
                            "version": "test-node",
                            "state": state,
                            "capabilities": ["stream_lifecycle", "metrics"],
                            "streams": [{
                                "id": stream_id,
                                "state": "running",
                                "desired_state": "running",
                                "started_at_ms": 1,
                                "last_error": null,
                                "metrics": {
                                    "input_batches": 1,
                                    "input_messages": 3,
                                    "processing_errors": 0,
                                    "output_batches": 1,
                                    "output_messages": 2,
                                    "input_errors": 0,
                                    "input_reconnects": 0,
                                    "output_errors": 0,
                                    "restarts": 0
                                }
                            }],
                            "operations": [],
                            "events": [],
                            "metrics": {"input_messages": 3.0},
                            "configuration": {"token": "[REDACTED]"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    async fn agent_commands(
        app: &Router,
        session: &hub::RegisterResponse,
    ) -> Vec<hub::AgentCommand> {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!(
                    "/api/v1/agent/commands?node_id={}&session_token={}",
                    session.node_id, session.session_token
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn get_hub_operation(app: &Router, id: &str) -> hub::HubOperation {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/operations/{id}"))
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn get_hub_streams(app: &Router) -> serde_json::Value {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/streams")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn get_hub_nodes(app: &Router) -> Vec<hub::HubNode> {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: Page<hub::HubNode> = serde_json::from_slice(&body).unwrap();
        page.items
    }
}
