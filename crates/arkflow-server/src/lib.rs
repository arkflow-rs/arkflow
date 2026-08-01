//! ArkFlow's resource-oriented control-plane HTTP service.
//!
//! HTTP transport lives here; the domain facade consumed by this crate is
//! `arkflow_core::control_plane::ControlPlane` and contains no Axum types.

use arkflow_core::component::{self, ComponentKind};
use arkflow_core::configuration::redacted_config;
use arkflow_core::configuration::{parse_and_validate, ConfigCandidate};
use arkflow_core::control::{ApiError, Page};
use arkflow_core::control_plane::ControlPlane;
use axum::body::Body;
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

#[derive(Debug, Deserialize)]
struct PageQuery {
    page: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OperationQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    resource_id: Option<String>,
    operation: Option<String>,
    state: Option<arkflow_core::control::OperationState>,
    correlation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    event_type: Option<String>,
    stream_id: Option<String>,
    correlation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiffQuery {
    from: String,
    to: String,
}

pub fn router(control_plane: ControlPlane, config: &ServerConfig) -> Router {
    let prefix = config.api_prefix.trim_end_matches('/');
    let api = Router::new()
        .route("/system", get(system))
        .route("/status", get(status))
        .route("/nodes", get(node))
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

async fn system(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::SystemResource> {
    Json(cp.system().await)
}
async fn status(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::EngineStatus> {
    Json(cp.status().await)
}
async fn node(State(cp): State<ControlPlane>) -> Json<arkflow_core::control::NodeResource> {
    Json(cp.node().await)
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
    items.sort_by(|left, right| right.occurred_at_ms.cmp(&left.occurred_at_ms));
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
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&correlation_id) {
        response.headers_mut().insert("x-correlation-id", value);
    }
    response
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
        let mut health = HealthCheckConfig::default();
        health.api_token = Some("secret".into());
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
}
