//! HTTP API surface: router builders, shared application configuration,
//! extractors, middleware, and the topical route-handler modules.
//!
//! The routers themselves (`router`, `observability_router`, `hub_router`)
//! live here and mount the handlers from the submodules; the domain facade
//! consumed by the local-plane handlers is
//! `arkflow_core::control_plane::ControlPlane` and contains no Axum types.

mod agent_endpoints;
mod configuration;
mod diagnostics;
mod events;
mod jobs;
mod nodes;
mod operations;
mod rollouts;
mod streams;
#[cfg(test)]
mod tests;

// Crate-root re-exports: `lib.rs` forwards these so `crate::X` paths (used by
// `oidc` tests and `hub::job_orchestration`) keep resolving.
pub(crate) use diagnostics::{hub_oidc_callback, hub_oidc_login, hub_oidc_logout, hub_oidc_status};
pub(crate) use jobs::deep_validate_job;

// Handler imports used by the router builders below.
use agent_endpoints::{
    agent_command_result, agent_commands, agent_heartbeat, agent_job_observation, agent_register,
    agent_report,
};
use configuration::{
    apply_configuration, configuration, configuration_diff, configuration_draft,
    configuration_versions, hub_apply_configuration, hub_configuration, hub_configuration_diff,
    hub_configuration_versions, hub_rollback_configuration, hub_validate_configuration,
    rollback_configuration, save_configuration_draft, validate_configuration,
};
use diagnostics::{
    component, components, health, hub_health, hub_liveness, hub_metrics, hub_operational_status,
    hub_readiness, live, liveness, metrics, node, nodes, readiness, ready, schema, status, system,
};
use events::{events, hub_audit, hub_event_stream, hub_events};
use jobs::{
    hub_create_job, hub_job, hub_job_action, hub_job_checkpoint, hub_job_checkpoints,
    hub_job_desired_state, hub_job_detail, hub_job_plan, hub_job_savepoint, hub_job_upgrade,
    hub_job_upgrade_action, hub_job_upgrade_rollback, hub_job_upgrade_status, hub_job_versions,
    hub_jobs, hub_validate_job,
};
use nodes::{
    hub_drain_node, hub_maintain_node, hub_nodes, hub_resume_node, hub_status, hub_system,
};
use operations::{
    cancel_operation, hub_cancel_operation, hub_operation, hub_operations, operation, operations,
};
use rollouts::{create_rollout, hub_rollout, hub_rollouts, rollout_action};
use streams::{
    hub_desired_state, hub_restart_action, hub_stream, hub_streams, hub_targeted_command,
    restart_stream, start_stream, stop_stream, stream, streams,
};

// Helpers referenced only by the moved test module.
#[cfg(test)]
use crate::storage;
#[cfg(test)]
use agent_endpoints::{agent_session_token, bearer_session_token};
#[cfg(test)]
use axum::extract::Query;
#[cfg(test)]
use diagnostics::{cookie_value, prometheus_label};
#[cfg(test)]
use jobs::reject_if_job_upgrade_active;
#[cfg(test)]
use streams::parse_generation_etag;

use crate::api_contract::{OperatorAction, OperatorPrincipal};
use crate::hub;
use arkflow_core::control::{ApiError, Page};
use arkflow_core::control_plane::ControlPlane;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
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
    /// PEM file path for the control-plane TLS certificate. Both this and
    /// `tls_key` must be set to serve TLS; one without the other fails
    /// startup.
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
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
    /// Permit volatile, unauthenticated Hub operation only for an explicitly
    /// opted-in loopback development server. This is never valid externally.
    #[serde(default)]
    pub insecure_local: bool,
    /// SQLite path used by the standalone Hub. It is intentionally absent by
    /// default; secure startup rejects that absence instead of silently
    /// falling back to volatile state.
    #[serde(default)]
    pub hub_storage: Option<String>,
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_session_ttl_ms")]
    pub session_ttl_ms: u64,
    #[serde(default)]
    pub observability: arkflow_core::config::ObservabilityConfig,
}

impl ServerConfig {
    pub fn from_engine(config: &arkflow_core::config::EngineConfig) -> Self {
        let health = &config.node.health;
        let control_api = &config.node.control_api;
        let agent = &config.node.agent;
        Self {
            enabled: health.enabled,
            address: health.address.clone(),
            api_prefix: control_api.api_prefix.clone(),
            health_path: health.health_path.clone(),
            readiness_path: health.readiness_path.clone(),
            tls_cert: None,
            tls_key: None,
            liveness_path: health.liveness_path.clone(),
            cors_origins: control_api.cors_origins.clone(),
            node_token: agent.node_token.clone(),
            insecure_local: false,
            hub_storage: None,
            lease_ttl_ms: agent.agent_lease_ttl_ms,
            poll_interval_ms: default_poll_interval_ms(),
            session_ttl_ms: agent.agent_session_ttl_ms,
            observability: config.node.observability.clone(),
        }
    }

    /// Validate the deployment boundary before any Hub recovery work or
    /// listener bind.  The returned address is the exact socket address that
    /// the caller may safely bind.
    pub fn validate_hub_startup(&self, hub: &hub::Hub) -> Result<SocketAddr, std::io::Error> {
        let address: SocketAddr = self.address.parse().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid Hub bind address '{}': {error}", self.address),
            )
        })?;
        if self.insecure_local && !address.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "insecure_local is only allowed when the Hub binds a loopback address",
            ));
        }
        if !self.insecure_local {
            if !hub.has_storage() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "secure Hub startup requires durable storage; set ARKFLOW_HUB_STORAGE or enable insecure_local on loopback",
                ));
            }
            if !hub.operator_token_is_set() || !hub.node_token_is_set() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "secure Hub startup requires both operator and node credentials; configure ARKFLOW_OPERATOR_TOKEN and ARKFLOW_NODE_TOKEN",
                ));
            }
            if let Err(message) = hub.validate_operator_credentials() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    message,
                ));
            }
        }
        Ok(address)
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
            tls_cert: None,
            tls_key: None,
            liveness_path: default_liveness_path(),
            cors_origins: Vec::new(),
            node_token: None,
            insecure_local: false,
            hub_storage: None,
            lease_ttl_ms: default_lease_ttl_ms(),
            poll_interval_ms: default_poll_interval_ms(),
            session_ttl_ms: default_session_ttl_ms(),
            observability: arkflow_core::config::ObservabilityConfig::default(),
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
fn default_session_ttl_ms() -> u64 {
    hub::default_session_ttl_ms()
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

#[derive(Debug, Clone, Deserialize)]
struct EventQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    event_type: Option<String>,
    stream_id: Option<String>,
    correlation_id: Option<String>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    resource_id: Option<String>,
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
            // The page number is unvalidated query input: saturating
            // arithmetic keeps a `?page=usize::MAX` request from
            // overflowing (debug panic / wrapped release offsets).
            .skip(page.saturating_sub(1).saturating_mul(page_size))
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
        .route(&config.observability.ready_path, get(ready))
        .route(&config.observability.live_path, get(live))
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

/// The minimal observability router: metrics and health probes only, no
/// resource APIs and no configuration content.
pub fn observability_router(control_plane: ControlPlane, config: &ServerConfig) -> Router {
    let observability = &config.observability;
    Router::new()
        .route(&observability.metrics_path, get(metrics))
        .route(&observability.ready_path, get(ready))
        .route(&observability.live_path, get(live))
        .with_state(control_plane)
}

/// Serve only the process observability endpoints (`/metrics`, `/ready`,
/// `/live`, paths configurable). Used when the control-plane API server is
/// disabled (or absent, e.g. Agent mode) so a pure data-plane process still
/// exposes metrics and health probes. A bind failure logs a warning and
/// returns without error: observability must never block the data plane.
pub async fn serve_observability(
    control_plane: ControlPlane,
    config: ServerConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let observability = &config.observability;
    if !observability.enabled {
        return Ok(());
    }
    let app = observability_router(control_plane, &config);
    let address: SocketAddr = observability.address.parse()?;
    match TcpListener::bind(address).await {
        Ok(listener) => {
            tracing::info!(%address, "observability listener started");
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(cancellation.cancelled_owned())
                .await?;
        }
        Err(error) => {
            tracing::warn!(
                %error,
                address = %observability.address,
                "observability listener failed to bind; continuing without it \
                 (configure health_check.observability.address)"
            );
        }
    }
    Ok(())
}

/// Build the external Hub API. Unlike `router`, this router has no local
/// Engine state; all node-owned resources come from authenticated Agent
/// reports stored in `Hub`.
/// Paths exempt from operator-token auth: health probes, metrics export,
/// the public component catalog, OIDC login flow, and agent routes (agent
/// requests carry a node token verified inside the hub methods, not the
/// operator credential this middleware enforces).
const OPERATOR_AUTH_EXEMPT: &[&str] = &[
    // Health probes, metrics and the public component catalog are mounted
    // on the OUTER router (after nesting) and never reach this middleware;
    // the OIDC login flow and agent routes are inner-router paths exempt
    // from the operator credential (agent requests carry a node token
    // verified inside the hub methods).
    "/auth/oidc/",
    "/agent/",
];

/// Route-level operator authentication: every non-exempt hub route requires
/// a valid operator credential (static token, OIDC session, or scoped
/// credential). A new operator route is protected automatically — the old
/// per-handler boilerplate let a forgotten check become an auth bypass.
async fn operator_auth_middleware(
    State(hub): State<hub::Hub>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    if OPERATOR_AUTH_EXEMPT
        .iter()
        .any(|p| path.starts_with(p) || path.contains(p))
    {
        return next.run(req).await;
    }
    let token = bearer(req.headers());
    if hub.operator_authorized(token.as_deref()).await {
        next.run(req).await
    } else {
        problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        )
    }
}

pub fn hub_router(hub: hub::Hub, config: &ServerConfig) -> Router {
    let auth_hub = hub.clone();
    let prefix = config.api_prefix.trim_end_matches('/');
    // Standby allowlist: probes the load balancer needs to route traffic to
    // the leader, plus the metrics export. Everything else is gated on
    // leadership so a standby never serves its (stale or empty) memory view.
    let standby_allowlist: std::sync::Arc<[String]> = vec![
        config.health_path.clone(),
        config.readiness_path.clone(),
        config.liveness_path.clone(),
        format!("{prefix}/metrics"),
    ]
    .into();
    let api = Router::new()
        .route("/system", get(hub_system))
        .route("/status", get(hub_status))
        .route("/nodes", get(hub_nodes))
        .route("/streams", get(hub_streams))
        .route("/jobs", get(hub_jobs).post(hub_create_job))
        .route("/jobs/validate", post(hub_validate_job))
        .route("/jobs/{id}", get(hub_job))
        .route("/jobs/{id}/detail", get(hub_job_detail))
        .route("/jobs/{id}/versions", get(hub_job_versions))
        .route("/jobs/{id}/upgrades", post(hub_job_upgrade))
        .route(
            "/jobs/{id}/upgrades/{upgrade_id}",
            get(hub_job_upgrade_status),
        )
        .route(
            "/jobs/{id}/upgrades/{upgrade_id}/actions",
            post(hub_job_upgrade_action),
        )
        .route(
            "/jobs/{id}/upgrades/{upgrade_id}/rollback",
            post(hub_job_upgrade_rollback),
        )
        .route("/jobs/{id}/plan", get(hub_job_plan))
        .route("/jobs/{id}/status", get(hub_job))
        .route(
            "/jobs/{id}/checkpoints",
            get(hub_job_checkpoints).post(hub_job_checkpoint),
        )
        .route(
            "/jobs/{id}/savepoints",
            get(hub_job_checkpoints).post(hub_job_savepoint),
        )
        .route("/jobs/{id}/desired-state", put(hub_job_desired_state))
        .route("/jobs/{id}/actions/{action}", post(hub_job_action))
        .route("/nodes/{node_id}/streams/{id}", get(hub_stream))
        .route("/nodes/{node_id}/configuration", get(hub_configuration))
        .route(
            "/nodes/{node_id}/configuration/versions",
            get(hub_configuration_versions),
        )
        .route(
            "/nodes/{node_id}/configuration/validate",
            post(hub_validate_configuration),
        )
        .route(
            "/nodes/{node_id}/configuration/diff",
            get(hub_configuration_diff),
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
        .route(
            "/nodes/{node_id}/streams/{id}/desired-state",
            put(hub_desired_state),
        )
        .route(
            "/nodes/{node_id}/streams/{id}/actions/restart",
            post(hub_restart_action),
        )
        .route("/operations", get(hub_operations))
        .route("/operations/{id}", get(hub_operation))
        .route("/operations/{id}", delete(hub_cancel_operation))
        .route("/events", get(hub_events))
        .route("/events/stream", get(hub_event_stream))
        .route("/components", get(components))
        .route("/components/{kind}/{name}", get(component))
        .route("/schema", get(schema))
        .route("/audit", get(hub_audit))
        .route("/rollouts", get(hub_rollouts).post(create_rollout))
        .route("/rollouts/{id}", get(hub_rollout))
        .route("/rollouts/{id}/actions", post(rollout_action))
        .route("/operations/status", get(hub_operational_status))
        .route("/nodes/{node_id}/drain", post(hub_drain_node))
        .route(
            "/nodes/{node_id}/maintenance",
            post(hub_maintain_node).delete(hub_resume_node),
        )
        .route("/metrics", get(hub_metrics))
        .route("/agent/register", post(agent_register))
        .route("/agent/heartbeat", post(agent_heartbeat))
        .route("/agent/report", post(agent_report))
        .route("/agent/job-observations", post(agent_job_observation))
        .route("/agent/commands", get(agent_commands))
        .route("/agent/commands/{id}/result", post(agent_command_result))
        .route("/auth/oidc/status", get(hub_oidc_status));
    let api = if hub.oidc_login().is_some() {
        api.route("/auth/oidc/login", get(hub_oidc_login))
            .route("/auth/oidc/callback", get(hub_oidc_callback))
            .route("/auth/oidc/logout", get(hub_oidc_logout))
    } else {
        api
    }
    .with_state(hub.clone())
    .layer(axum::middleware::from_fn_with_state(
        auth_hub,
        operator_auth_middleware,
    ));
    let app = Router::new()
        .route(&config.health_path, get(hub_health))
        .route(&config.readiness_path, get(hub_readiness))
        .route(&config.liveness_path, get(hub_liveness))
        .nest(prefix, api)
        .with_state(hub.clone())
        .layer(middleware::from_fn(
            move |request: axum::extract::Request, next: Next| {
                let hub = hub.clone();
                let allowlist = standby_allowlist.clone();
                async move {
                    if hub.is_leader().await {
                        return next.run(request).await;
                    }
                    let path = request.uri().path();
                    if allowlist.iter().any(|allowed| allowed == path) {
                        return next.run(request).await;
                    }
                    // hub-ha stage 3: point Agents straight at the elected
                    // leader when the shared lease row carries its advertised
                    // address. Any read failure or missing advertisement
                    // degrades to the plain standby problem body.
                    let leader_url = async {
                        let storage = hub.storage()?;
                        let snapshot = storage.hub_lease_snapshot().await.ok()??;
                        let advertise = snapshot.advertise_url?;
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|duration| duration.as_millis() as u64)
                            .unwrap_or_default();
                        (snapshot.expires_at_ms > now).then_some(advertise)
                    }
                    .await;
                    let details =
                        leader_url.map(|leader| serde_json::json!({ "leader_url": leader }));
                    problem_with_details(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "hub_standby",
                        "This Hub instance is a standby and does not hold the control-plane \
                         lease; retry against the elected leader"
                            .into(),
                        details,
                    )
                }
            },
        ))
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

/// axum `Listener` adapter wrapping the TCP listener in TLS: every accepted
/// connection completes the TLS handshake before the service sees it. Routes,
/// auth, and readiness semantics are untouched (TLS lives below them).
struct HubTlsListener {
    inner: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

impl HubTlsListener {
    async fn accept_one(
        &mut self,
    ) -> (
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        std::net::SocketAddr,
    ) {
        loop {
            match self.inner.accept().await {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    // Bounded handshake so a stalled peer cannot hold the
                    // accept loop (the handshake runs inline here because
                    // the axum Listener contract returns the IO directly).
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        self.acceptor.accept(stream),
                    )
                    .await
                    {
                        Ok(Ok(tls_stream)) => return (tls_stream, peer),
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "hub TLS handshake failed");
                        }
                        Err(_) => {
                            tracing::warn!("hub TLS handshake timed out");
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "hub TLS accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
}

impl axum::serve::Listener for HubTlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    fn accept(&mut self) -> impl std::future::Future<Output = (Self::Io, Self::Addr)> + Send {
        self.accept_one()
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Maintenance and reconcile side-sweeps must not silently swallow storage
/// errors: they are the only line of defense against unbounded history
/// tables, so a persistently failing sweep has to be visible to operators.
fn sweep_logged<T>(task: &str, result: Result<T, impl std::fmt::Display>) {
    if let Err(error) = result {
        tracing::warn!(task, %error, "periodic sweep failed");
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
    let address = config.validate_hub_startup(&hub)?;
    // HA election (hub-ha stage 2): an HA-enabled Hub starts as standby and
    // earns leadership through the durable lease. Enabling HA without
    // durable storage is rejected before anything binds: a standby has no
    // lease to compete for without the store.
    if hub.ha_config().enabled && !hub.has_storage() {
        return Err(
            "HA election requires durable storage: set ARKFLOW_HUB_STORAGE (PostgreSQL for multi-instance deployments)"
                .into(),
        );
    }
    arkflow_plugin::initialize()?;
    hub.enter_election().await;
    if !hub.is_leader().await {
        tracing::info!(
            "HA standby: durable recovery is deferred until this instance acquires the lease"
        );
    } else {
        hub.recover_persisted_state().await?;
    }
    if !hub.operator_token_is_set() {
        tracing::warn!(
            "Hub is running WITHOUT an operator token: every operator API grants full Admin access. \
             Set operator_token before exposing this Hub beyond localhost."
        );
    }
    if !hub.node_token_is_set() {
        tracing::warn!(
            "Hub is running WITHOUT a node token: any caller can register arbitrary compute nodes \
             (including re-registering an existing node's id). Set node_token before exposing this \
             Hub beyond localhost."
        );
    }
    // Restore persisted operations before binding the listener (and before
    // readiness): the terminal-state dispatch-skip memory and the operations
    // read API must reflect durable history before any reconcile tick or
    // operator request runs. Manual `hub_router` test setups do not restart
    // the Hub and skip this path by construction. A standby defers this: it
    // must not write terminal settlements while another Hub is leader —
    // promotion re-runs the restore after winning the lease.
    if hub.has_storage() && hub.is_leader().await {
        let restored = hub.restore_persisted_operations().await?;
        tracing::info!(
            restored,
            "restored persisted operations into the in-memory registry"
        );
    }
    // Control-plane TLS: both materials or neither — a half-configured
    // listener must fail startup rather than silently serve plaintext.
    let tls_acceptor = match (&config.tls_cert, &config.tls_key) {
        (None, None) => None,
        (Some(cert_path), Some(key_path)) => {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let cert_pem = std::fs::read_to_string(cert_path).map_err(|error| {
                format!("hub TLS certificate '{cert_path}' could not be read: {error}")
            })?;
            let key_pem = std::fs::read_to_string(key_path)
                .map_err(|error| format!("hub TLS key '{key_path}' could not be read: {error}"))?;
            let mut chain = Vec::new();
            for item in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
                chain.push(
                    item.map_err(|error| format!("hub TLS certificate parse failed: {error}"))?,
                );
            }
            if chain.is_empty() {
                return Err("hub TLS certificate contains no PEM certificates".into());
            }
            let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                .map_err(|error| format!("hub TLS key parse failed: {error}"))?
                .ok_or("hub TLS key contains no PEM private key")?;
            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .map_err(|error| format!("hub TLS config rejected: {error}"))?;
            Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
                server_config,
            )))
        }
        _ => {
            return Err("hub TLS requires both ARKFLOW_HUB_TLS_CERT and ARKFLOW_HUB_TLS_KEY".into())
        }
    };
    let listener = TcpListener::bind(address).await?;
    // Lease election loop: leaders renew at ttl/3, standbys probe for
    // takeover. Failover is bounded by the lease TTL plus one probe.
    if hub.ha_config().enabled {
        let election_hub = hub.clone();
        let election_cancel = cancellation.clone();
        let probe_ms = (hub.ha_config().lease_ttl_ms / 3).max(1_000);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(probe_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => { election_hub.run_election_tick().await; }
                    _ = election_cancel.cancelled() => break,
                }
            }
        });
    }
    let sweep_hub = hub.clone();
    let sweep_cancel = cancellation.clone();
    let sweep_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Standbys neither age out node leases nor observe
                    // reports: the leader owns fleet state.
                    if sweep_hub.is_leader().await {
                        sweep_hub.mark_stale().await;
                    }
                }
                _ = sweep_cancel.cancelled() => break,
            }
        }
    });
    let reconcile_hub = hub.clone();
    let reconcile_cancel = cancellation.clone();
    let reconcile_interval = config.poll_interval_ms.max(50);
    let reconcile_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(reconcile_interval));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Reconciliation dispatches commands and mutates durable
                    // desired state: leader-only by definition.
                    if !reconcile_hub.is_leader().await {
                        continue;
                    }
                    sweep_logged("expire_attempts", reconcile_hub.expire_attempts().await);
                    sweep_logged(
                        "schedule_periodic_checkpoints",
                        reconcile_hub.schedule_periodic_checkpoints().await,
                    );
                    let started = crate::hub::now_ms_for_metrics();
                    let result = reconcile_hub.reconcile_once("hub-reconciler").await;
                    reconcile_hub.record_reconcile_result(started, &result).await;
                    // Upgrade orchestration runs before job reconciliation so
                    // a savepoint completing here commits in the same tick and
                    // the (unfenced) job reconciler starts the new generation
                    // immediately — the cutover window costs no extra tick.
                    sweep_logged(
                        "reconcile_job_upgrades",
                        reconcile_hub.reconcile_job_upgrades().await,
                    );
                    sweep_logged("reconcile_jobs", reconcile_hub.reconcile_jobs().await);
                    sweep_logged("reconcile_rollouts", reconcile_hub.reconcile_rollouts().await);
                    sweep_logged(
                        "expire_stale_job_operations",
                        reconcile_hub.expire_stale_job_operations().await,
                    );
                }
                _ = reconcile_cancel.cancelled() => break,
            }
        }
    });
    // Retention sweeps run on their own slow cadence: their cost grows with
    // the retained history and must not steal the single-writer SQLite
    // budget from the per-second reconciliation tick. The first tick fires
    // immediately, so an upgraded Hub reclaims history that accumulated
    // before the bounds existed as soon as it starts serving.
    let maintenance_hub = hub.clone();
    let maintenance_cancel = cancellation.clone();
    let maintenance_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !maintenance_hub.is_leader().await {
                        continue;
                    }
                    sweep_logged("prune_events", maintenance_hub.prune_events(2048).await);
                    sweep_logged(
                        "prune_operation_history",
                        maintenance_hub.prune_operation_history().await,
                    );
                    sweep_logged(
                        "prune_stale_checkpoint_records",
                        maintenance_hub.prune_stale_checkpoint_records().await,
                    );
                    sweep_logged(
                        "prune_audit_history",
                        maintenance_hub.prune_audit_history().await,
                    );
                    sweep_logged(
                        "prune_outbox_history",
                        maintenance_hub.prune_outbox_history().await,
                    );
                    sweep_logged(
                        "prune_attempt_history",
                        maintenance_hub.prune_attempt_history().await,
                    );
                    sweep_logged(
                        "prune_job_upgrade_history",
                        maintenance_hub.prune_job_upgrade_history().await,
                    );
                }
                _ = maintenance_cancel.cancelled() => break,
            }
        }
    });
    let router = hub_router(hub.clone(), &config).into_make_service();
    let result = match tls_acceptor {
        Some(acceptor) => {
            let tls_listener = HubTlsListener {
                inner: listener,
                acceptor,
            };
            axum::serve(tls_listener, router)
                .with_graceful_shutdown(async move {
                    cancellation.cancelled().await;
                    hub.release_leadership().await;
                })
                .await
        }
        None => {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    cancellation.cancelled().await;
                    // Release the lease before the listener drains so a standby can
                    // take over immediately instead of waiting out the TTL.
                    hub.release_leadership().await;
                })
                .await
        }
    };
    sweep_task.abort();
    reconcile_task.abort();
    maintenance_task.abort();
    result?;
    Ok(())
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        return value.strip_prefix("Bearer ").map(str::to_string);
    }
    // Browser sessions authenticate with the OIDC login cookie; expose it
    // through the same credential channel with an explicit prefix so the
    // authorization path can tell the two apart.
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    cookies.split(';').find_map(|pair| {
        let pair = pair.trim();
        let sid = pair.strip_prefix("arkflow_session=")?;
        Some(format!("session:{sid}"))
    })
}

async fn operator_denied(
    hub: &hub::Hub,
    supplied: Option<String>,
    action: OperatorAction,
) -> Response {
    if hub.operator_principal(supplied.as_deref()).await.is_none() {
        problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid operator token is required".into(),
        )
    } else {
        problem(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("Operator is not authorized for {:?}", action),
        )
    }
}

// Err carries a ready-made axum rejection response (middleware idiom); boxing
// it would ripple through every handler call site for no perf win.
#[allow(clippy::result_large_err)]
async fn require_operator_action(
    hub: &hub::Hub,
    headers: &HeaderMap,
    action: OperatorAction,
    resource_type: &str,
    resource_id: Option<String>,
) -> Result<OperatorPrincipal, Response> {
    let supplied = bearer(headers);
    let principal = hub.operator_principal(supplied.as_deref()).await;
    if principal
        .as_ref()
        .is_some_and(|principal| principal.can_scope(action, resource_type, resource_id.as_deref()))
    {
        return Ok(principal.expect("principal checked above"));
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let _ = hub
        .record_audit_event(crate::storage::AuditRecord {
            event_id: 0,
            actor: principal.as_ref().map(|principal| principal.id.clone()),
            action: format!("operator.{action:?}").to_lowercase(),
            resource_type: resource_type.to_owned(),
            resource_id,
            node_id: None,
            stream_id: None,
            correlation_id,
            outcome: "rejected".into(),
            failure_code: Some(
                if principal.is_some() {
                    "forbidden"
                } else {
                    "unauthorized"
                }
                .into(),
            ),
            message: None,
            occurred_at_ms: hub::now_ms_for_metrics(),
        })
        .await;
    Err(operator_denied(hub, supplied, action).await)
}

fn hub_problem(error: hub::HubError) -> Response {
    let status = match error {
        hub::HubError::Unauthorized => StatusCode::UNAUTHORIZED,
        hub::HubError::NodeUnavailable
        | hub::HubError::OrchestrationInProgress
        | hub::HubError::OrchestrationPhaseConflict => StatusCode::CONFLICT,
        hub::HubError::NotFound => StatusCode::NOT_FOUND,
        // A fenced write means this process no longer holds the lease —
        // same family as standby: retry against the elected leader.
        hub::HubError::StaleLeader { .. } => StatusCode::SERVICE_UNAVAILABLE,
        // Storage failures are server-side problems (retryable), not client
        // errors; generation conflicts are semantic conflicts, not bad
        // requests.
        hub::HubError::StorageUnavailable | hub::HubError::Storage(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        hub::HubError::GenerationConflict { .. } => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    };
    let code = match error {
        hub::HubError::OrchestrationInProgress => "orchestration_in_progress",
        hub::HubError::OrchestrationPhaseConflict => "orchestration_conflict",
        hub::HubError::StaleLeader { .. } => "stale_leader",
        hub::HubError::StorageUnavailable | hub::HubError::Storage(_) => "storage_unavailable",
        hub::HubError::GenerationConflict { .. } => "generation_conflict",
        _ => "agent_request_rejected",
    };
    problem(status, code, error.to_string().chars().take(256).collect())
}

fn authorized(cp: &ControlPlane, headers: &HeaderMap) -> bool {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    cp.authorized(token)
}

fn problem(status: StatusCode, code: &str, message: String) -> Response {
    problem_with_details(status, code, message, None)
}

fn problem_with_details(
    status: StatusCode,
    code: &str,
    message: String,
    details: Option<serde_json::Value>,
) -> Response {
    (
        status,
        Json(ApiError {
            code: code.into(),
            message,
            field: None,
            stream_id: None,
            correlation_id: None,
            details,
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
    // Only error responses are buffered so a correlation id can be injected.
    // Success responses (including streaming bodies such as the SSE event
    // stream) must pass through untouched or the stream never delivers.
    if !parts.status.is_client_error() && !parts.status.is_server_error() {
        if let Ok(value) = HeaderValue::from_str(&correlation_id) {
            parts.headers.insert("x-correlation-id", value);
        }
        return Response::from_parts(parts, body);
    }
    let body_bytes = to_bytes(body, 1024 * 1024).await.unwrap_or_default();
    let mut replacement = None;
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
                details: None,
            })
            .ok();
        }
        Err(_) => {}
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
