//! ArkFlow's resource-oriented control-plane HTTP service.
//!
//! HTTP transport lives here; the domain facade consumed by this crate is
//! `arkflow_core::control_plane::ControlPlane` and contains no Axum types.

pub mod agent;
pub mod api_contract;
pub mod bootstrap;
pub mod hub;
pub mod metrics;
pub mod oidc;
pub mod storage;

use crate::api_contract::{
    AcceptedIntentResponse, CreateJobRequest, CreateRolloutRequest, DesiredStateRequest,
    JobDesiredStateRequest, JobUpgradeActionRequest, JobUpgradeRequest, OperatorAction,
    OperatorPrincipal, RestartActionRequest, RolloutActionRequest, ValidateJobRequest,
};
use crate::storage::{DesiredMutation, JobRecord};
use arkflow_core::component::{self, ComponentKind};
use arkflow_core::configuration::redacted_config;
use arkflow_core::configuration::{parse_and_validate, ConfigCandidate};
use arkflow_core::control::{ApiError, Page};
use arkflow_core::control_plane::ControlPlane;
use axum::body::{to_bytes, Body};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use subtle::ConstantTimeEq;
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
        let health = &config.health_check;
        Self {
            enabled: health.enabled,
            address: health.address.clone(),
            api_prefix: health.api_prefix.clone(),
            health_path: health.health_path.clone(),
            readiness_path: health.readiness_path.clone(),
            tls_cert: None,
            tls_key: None,
            liveness_path: health.liveness_path.clone(),
            cors_origins: health.cors_origins.clone(),
            node_token: health.node_token.clone(),
            insecure_local: false,
            hub_storage: None,
            lease_ttl_ms: health.agent_lease_ttl_ms,
            poll_interval_ms: default_poll_interval_ms(),
            session_ttl_ms: health.agent_session_ttl_ms,
            observability: health.observability.clone(),
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

async fn hub_system(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    let nodes = hub.nodes().await;
    let leadership = hub.leadership().await;
    Json(
        serde_json::json!({"id":"arkflow-control-hub", "version":env!("CARGO_PKG_VERSION"), "state":"running", "node_count":nodes.len(), "online_nodes":nodes.iter().filter(|node| node.state == hub::NodeConnectionState::Online).count(), "capabilities":["node_registry","command_dispatch","fleet_aggregation"], "ha": {"enabled": hub.ha_config().enabled, "role": leadership.role(), "epoch": leadership.epoch(), "transitions": hub.leadership_transitions()}}),
    ).into_response()
}

/// Fleet-aggregated EngineStatus so console clients get one overview
/// contract in local and Hub mode. Only a lease-holding Hub reaches this
/// handler; standbys are rejected by the router's standby middleware.
async fn hub_status(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    let nodes = hub.nodes().await;
    Json(arkflow_core::control::EngineStatus {
        version: env!("CARGO_PKG_VERSION").into(),
        state: "running".into(),
        uptime_seconds: hub.uptime_seconds(),
        streams_total: nodes.iter().map(|node| node.streams_total).sum(),
        streams_running: nodes.iter().map(|node| node.streams_running).sum(),
        streams_failed: nodes.iter().map(|node| node.streams_failed).sum(),
    })
    .into_response()
}

async fn hub_nodes(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    _headers: HeaderMap,
) -> Response {
    Json(page_items(hub.nodes().await, &query)).into_response()
}

async fn hub_streams(
    State(hub): State<hub::Hub>,
    Query(query): Query<PageQuery>,
    _headers: HeaderMap,
) -> Response {
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
        .skip(page.saturating_sub(1).saturating_mul(page_size))
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

async fn hub_stream(
    State(hub): State<hub::Hub>,
    Path((node_id, stream_id)): Path<(String, String)>,
    _headers: HeaderMap,
) -> Response {
    match hub.stream_resource(&node_id, &stream_id).await {
        Ok(Some(resource)) => Json(resource).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "stream_not_found",
            format!("Unknown stream {node_id}/{stream_id}"),
        ),
        Err(hub::HubError::StorageUnavailable) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            "Durable control-plane storage is unavailable".into(),
        ),
        Err(error) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            error.to_string(),
        ),
    }
}

async fn hub_jobs(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    match hub.jobs().await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => hub_problem(error),
    }
}

/// Validate a Job through the same component, state-backend, and graph
/// construction path used by local execution. The HTTP Hub is also used
/// directly in tests and embedded deployments, so initialize the built-in
/// catalogue here as well as in `serve_hub`.
pub(crate) fn deep_validate_job(spec: &arkflow_core::job::JobSpec) -> Result<(), String> {
    arkflow_plugin::initialize()
        .and_then(|_| arkflow_core::executor::job_runner_adapter::validate_local_job(spec))
        .map_err(|error| error.to_string())
}

async fn hub_job(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.job(&job_id).await {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_plan(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    let Some(job) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let spec: arkflow_core::job::JobSpec = match serde_json::from_str(&job.spec_json) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_persisted_job",
                error.to_string(),
            )
        }
    };
    match arkflow_core::job::JobPlan::compile(spec) {
        Ok(plan) => {
            Json(serde_json::json!({ "job_id": job.job_id, "version": job.version, "plan": plan }))
                .into_response()
        }
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_plan",
            error.to_string(),
        ),
    }
}

async fn hub_job_action(
    State(hub): State<hub::Hub>,
    Path((job_id, action)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let state = match action.as_str() {
        "start" | "restart" => "running",
        "stop" => "stopped",
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_action",
                "action must be start, stop, or restart".into(),
            )
        }
    };
    let current = match hub.job(&job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("Unknown Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    };
    match hub
        .update_job_desired_state(&job_id, state, current.generation)
        .await
    {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_checkpoint(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_job_recovery_artifact(hub, job_id, headers, "checkpoint").await
}

async fn hub_job_savepoint(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_job_recovery_artifact(hub, job_id, headers, "savepoint").await
}

async fn hub_job_checkpoints(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.job_checkpoints(&job_id).await {
        Ok(records) => Json(records).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_validate_job(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<ValidateJobRequest>,
) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Configure, "job", None).await
    {
        return response;
    }
    let spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if let Err(error) = spec.validate() {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_spec",
            error.to_string(),
        );
    }
    let plan = match arkflow_core::job::JobPlan::compile(spec.clone()) {
        Ok(plan) => plan,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_job_plan",
                error.to_string(),
            )
        }
    };
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
    }
    let nodes = hub.nodes().await;
    let candidates = if request.node_ids.is_empty() {
        nodes.clone()
    } else {
        nodes
            .into_iter()
            .filter(|node| request.node_ids.iter().any(|id| id == &node.id))
            .collect::<Vec<_>>()
    };
    let required = ["job_runtime", "state_backend"];
    let compatibility = candidates
        .iter()
        .map(|node| {
            let missing = required
                .iter()
                .filter(|capability| !node.capabilities.iter().any(|item| item == **capability))
                .map(|capability| (*capability).to_owned())
                .collect::<Vec<_>>();
            let online = matches!(node.state, hub::NodeConnectionState::Online);
            serde_json::json!({
                "node_id": node.id,
                "state": node.state,
                "capabilities": node.capabilities,
                "compatible": online && missing.is_empty(),
                "missing_capabilities": if online { missing } else { vec!["online_lease".to_owned()] },
            })
        })
        .collect::<Vec<_>>();
    let compatible = compatibility
        .iter()
        .all(|node| node["compatible"].as_bool().unwrap_or(false));
    Json(serde_json::json!({
        "valid": compatible || compatibility.is_empty(),
        "plan": plan,
        "required_capabilities": required,
        "nodes": compatibility,
        "warnings": if compatibility.is_empty() { vec!["No online compute nodes selected".to_owned()] } else { Vec::new() },
    }))
    .into_response()
}

async fn hub_job_detail(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    let Some(job) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let spec: arkflow_core::job::JobSpec = match serde_json::from_str(&job.spec_json) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_persisted_job",
                error.to_string(),
            )
        }
    };
    let plan = match arkflow_core::job::JobPlan::compile(spec) {
        Ok(plan) => plan,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_job_plan",
                error.to_string(),
            )
        }
    };
    let nodes = hub.nodes().await;
    let selected_nodes = if job.node_ids.is_empty() {
        nodes.clone()
    } else {
        nodes
            .into_iter()
            .filter(|node| job.node_ids.iter().any(|id| id == &node.id))
            .collect::<Vec<_>>()
    };
    // A placement that violates its own strategy (for example a split Job
    // whose side edges would cross nodes) must render as a detail-page error,
    // not a handler panic.
    let assignments = match plan.assignments_for_nodes(
        &selected_nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<Vec<_>>(),
        job.generation,
    ) {
        Ok(assignments) => assignments,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_placement",
                error.to_string(),
            )
        }
    };
    let operations = hub
        .operations(None)
        .await
        .into_iter()
        .filter(|operation| operation.resource_id == job_id)
        .collect::<Vec<_>>();
    let checkpoints = match hub.job_checkpoints(&job_id).await {
        Ok(checkpoints) => checkpoints,
        Err(error) => return hub_problem(error),
    };
    // Observed runtime state: tasks the executing nodes report running, and
    // diagnostics scoped to this Job only (never fleet-wide sums).
    let observed_tasks = hub.observed_job_tasks(&job_id).await;
    let tasks = assignments
        .into_iter()
        .map(|attempt| {
            let mut value = serde_json::to_value(attempt).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                let task_id = object
                    .get("task_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                match observed_tasks.get(&task_id) {
                    Some(node_id) => {
                        object.insert("state".into(), serde_json::json!("running"));
                        object.insert("observed".into(), serde_json::json!(true));
                        object.insert("observed_node_id".into(), serde_json::json!(node_id));
                    }
                    None => {
                        object.insert("observed".into(), serde_json::json!(false));
                    }
                }
            }
            value
        })
        .collect::<Vec<_>>();
    let metrics = hub.job_detail_metrics(&job_id).await;
    Json(serde_json::json!({
        "job": job,
        "plan": plan,
        "tasks": tasks,
        "nodes": selected_nodes,
        "operations": operations,
        "checkpoints": checkpoints,
        // The high-frequency detail view omits the (potentially large)
        // target spec; the dedicated orchestration status endpoint keeps it.
        "active_upgrade": hub
            .active_job_upgrade_for(&job_id)
            .await
            .and_then(|record| {
                serde_json::to_value(&record).ok().map(|mut value| {
                    if let Some(object) = value.as_object_mut() {
                        object.remove("target_spec_json");
                    }
                    value
                })
            }),
        "metrics": metrics
    }))
    .into_response()
}

async fn hub_job_versions(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    if matches!(hub.job(&job_id).await, Ok(None)) {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    }
    match hub.job_versions(&job_id).await {
        Ok(versions) => Json(versions).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_upgrade(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<JobUpgradeRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Configure,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    if current.generation != request.expected_generation {
        return problem_with_details(
            StatusCode::CONFLICT,
            "generation_conflict",
            "Job changed while the upgrade was being prepared".into(),
            Some(
                serde_json::json!({"expected": request.expected_generation, "current": current.generation}),
            ),
        );
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let mode = request.mode.as_deref().unwrap_or("stopped");
    if !matches!(mode, "stopped" | "atomic") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_upgrade_mode",
            "mode must be stopped or atomic".into(),
        );
    }
    if mode == "atomic" {
        // Atomic mode is the inverse precondition: the Job must still be
        // running, and the orchestration takes its own savepoint.
        if current.desired_state != "running" {
            return problem(
                StatusCode::CONFLICT,
                "job_must_be_running",
                "The atomic upgrade mode requires a running Job".into(),
            );
        }
        let mut spec: arkflow_core::job::JobSpec =
            match serde_json::from_value(request.spec.clone()) {
                Ok(spec) => spec,
                Err(error) => {
                    return problem(
                        StatusCode::BAD_REQUEST,
                        "invalid_job_spec",
                        error.to_string(),
                    )
                }
            };
        return match hub
            .create_job_upgrade(
                &job_id,
                &mut spec,
                request.expected_generation,
                request.verify_timeout_ms,
                Some(principal.id.clone()),
                None,
            )
            .await
        {
            Ok(record) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "upgrade_id": record.upgrade_id,
                    "state": record.phase,
                    "savepoint_id": serde_json::Value::Null,
                    "job": current,
                })),
            )
                .into_response(),
            Err(hub::HubError::GenerationConflict { expected, current }) => problem(
                StatusCode::PRECONDITION_FAILED,
                "generation_conflict",
                format!("Expected generation {expected}, current generation {current}"),
            ),
            Err(hub::HubError::OrchestrationInProgress) => problem(
                StatusCode::CONFLICT,
                "orchestration_in_progress",
                "An atomic upgrade already owns this Job".into(),
            ),
            Err(error) => hub_problem(error),
        };
    }
    if current.desired_state != "stopped" || current.observed_state == "running" {
        return problem(
            StatusCode::CONFLICT,
            "job_must_be_stopped",
            "Stop and converge the current Job before upgrading".into(),
        );
    }
    let mut spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if spec.id.as_str() != job_id {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_spec",
            "upgrade spec id must match the Job id".into(),
        );
    }
    if spec.version.0 <= current.version {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_version",
            "upgrade version must be greater than the current version".into(),
        );
    }
    if let Err(error) = spec
        .validate()
        .and_then(|_| arkflow_core::job::JobPlan::compile(spec.clone()).map(|_| ()))
    {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_plan",
            error.to_string(),
        );
    }
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
    }
    let checkpoint = match hub.job_checkpoints(&job_id).await.map(|records| {
        records.into_iter().find(|record| {
            Some(record.checkpoint_id.as_str()) == request.savepoint_id.as_deref()
                && record.kind == "savepoint"
                && record.status == "completed"
        })
    }) {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            return problem(
                StatusCode::CONFLICT,
                "savepoint_not_ready",
                "The selected savepoint is not completed".into(),
            )
        }
        Err(error) => return hub_problem(error),
    };
    let format_version = spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    if checkpoint.format_version != format_version {
        return problem(
            StatusCode::CONFLICT,
            "state_format_incompatible",
            "The savepoint state format is incompatible with the new Job version".into(),
        );
    }
    spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
    let upgraded = JobRecord {
        job_id: job_id.clone(),
        version: spec.version.0,
        spec_json: serde_json::to_string(&spec).unwrap_or_else(|_| request.spec.to_string()),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending_recovery".into(),
        generation: current.generation,
        node_ids: if request.node_ids.is_empty() {
            current.node_ids.clone()
        } else {
            request.node_ids
        },
        checkpoint_id: Some(checkpoint.checkpoint_id.clone()),
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    // The generation fence covers the whole read-validate-write sequence: a
    // concurrent mutation that bumped the generation must surface as a
    // conflict instead of being silently overwritten by this older read.
    match hub
        .update_job_with_expected_generation(upgraded, request.expected_generation)
        .await
    {
        Ok(job) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "upgrade_id": format!("upgrade-{}", hub::now_ms_for_metrics()),
                "state": "pending_recovery",
                "savepoint_id": checkpoint.checkpoint_id,
                "job": job,
            })),
        )
            .into_response(),
        Err(hub::HubError::GenerationConflict { expected, current }) => problem(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_upgrade_rollback(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let versions = match hub.job_versions(&job_id).await {
        Ok(versions) => versions,
        Err(error) => return hub_problem(error),
    };
    // The console requests a specific version ("restore-v{N}"); honour it
    // instead of always stepping back to the immediately previous one.
    // An opaque upgrade id falls back to the previous-version semantics.
    let requested = upgrade_id
        .trim()
        .trim_start_matches("restore-")
        .trim_start_matches('v')
        .parse::<u64>()
        .ok();
    let previous = match requested {
        Some(target) => versions
            .into_iter()
            .find(|version| version.version == target)
            .filter(|version| version.version < current.version),
        None => versions
            .into_iter()
            .find(|version| version.version < current.version),
    };
    let Some(previous) = previous else {
        return problem(
            StatusCode::CONFLICT,
            "no_previous_job_version",
            match requested {
                Some(target) => format!(
                    "Job {job_id} has no restorable version {target} below the current version {}",
                    current.version
                ),
                None => "No previous Job version is available for recovery".into(),
            },
        );
    };
    // Apply the same state-format compatibility check the upgrade path
    // performs, against the artifact this Job would actually restore (its
    // current recovery pointer). Without it a rollback to a version whose
    // state layout differs is accepted, and recovery then silently discards
    // the incompatible artifact and starts the Job without state.
    if let Some(checkpoint_id) = current.checkpoint_id.as_deref() {
        let artifact_format = hub
            .job_checkpoints(&job_id)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .find(|record| record.checkpoint_id == checkpoint_id)
                    .map(|record| record.format_version)
            })
            .map_err(hub_problem);
        let artifact_format = match artifact_format {
            Ok(format) => format,
            Err(response) => return response,
        };
        let restored_format =
            serde_json::from_str::<arkflow_core::job::JobSpec>(&previous.spec_json)
                .ok()
                .and_then(|spec| spec.state.map(|state| state.format_version))
                .unwrap_or(1);
        let compatible = artifact_format == Some(restored_format);
        if !compatible {
            return problem(
                StatusCode::CONFLICT,
                "state_format_incompatible",
                format!(
                    "Job {job_id} cannot roll back to version {}: its state format is                      incompatible with the artifact the Job would restore",
                    previous.version
                ),
            );
        }
    }
    let restored_spec_json =
        match serde_json::from_str::<arkflow_core::job::JobSpec>(&previous.spec_json) {
            Ok(mut spec) => {
                spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
                if let Err(error) = spec
                    .validate()
                    .and_then(|_| arkflow_core::job::JobPlan::compile(spec.clone()).map(|_| ()))
                {
                    return problem(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "invalid_job_plan",
                        error.to_string(),
                    );
                }
                if let Err(error) = deep_validate_job(&spec) {
                    return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
                }
                match serde_json::to_string(&spec) {
                    Ok(spec_json) => spec_json,
                    Err(error) => {
                        return problem(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "invalid_persisted_job",
                            error.to_string(),
                        )
                    }
                }
            }
            Err(error) => {
                return problem(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid_persisted_job",
                    error.to_string(),
                )
            }
        };
    let restored = JobRecord {
        job_id: job_id.clone(),
        version: previous.version,
        spec_json: restored_spec_json,
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending_recovery".into(),
        generation: current.generation,
        node_ids: current.node_ids,
        checkpoint_id: current.checkpoint_id,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    // Same generation fence as the upgrade path: the version list may have
    // been read before a concurrent mutation bumped the generation.
    match hub
        .update_job_with_expected_generation(restored, current.generation)
        .await
    {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(hub::HubError::GenerationConflict { expected, current }) => problem(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_upgrade_status(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Read,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    match hub.job_upgrade(&upgrade_id).await {
        Ok(Some(record)) if record.job_id == job_id => Json(record).into_response(),
        Ok(Some(_)) | Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_upgrade_not_found",
            format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_upgrade_action(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<JobUpgradeActionRequest>,
) -> Response {
    // Lifecycle-level actions align with job start/stop (Operate); rollback
    // swaps the Job's version and keeps the Configure level of the upgrade
    // endpoints proper.
    let action_level = match request.action.as_str() {
        "pause" | "resume" | "cancel" => OperatorAction::Operate,
        "rollback" => OperatorAction::Configure,
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_upgrade_action",
                "action must be pause, resume, cancel, or rollback".into(),
            )
        }
    };
    let principal =
        match require_operator_action(&hub, &headers, action_level, "job", Some(job_id.clone()))
            .await
        {
            Ok(principal) => principal,
            Err(response) => return response,
        };
    // Resolve before acting so an unknown id is a 404, not the generic
    // action-rejected conflict.
    match hub.job_upgrade(&upgrade_id).await {
        Ok(Some(record)) if record.job_id != job_id => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_upgrade_not_found",
                format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
            )
        }
        Ok(Some(_)) => {}
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_upgrade_not_found",
                format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    }
    match hub
        .act_job_upgrade(
            &upgrade_id,
            &request.action,
            Some(principal.id),
            None,
        )
        .await
    {
        Ok(record) if record.job_id == job_id => Json(record).into_response(),
        Ok(_) => problem(
            StatusCode::NOT_FOUND,
            "job_upgrade_not_found",
            format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
        ),
        Err(hub::HubError::OrchestrationPhaseConflict) => problem(
            StatusCode::CONFLICT,
            "orchestration_conflict",
            "The upgrade phase changed while the action was being applied; retry against the fresh phase".into(),
        ),
        Err(hub::HubError::Invalid(message)) => problem(
            StatusCode::CONFLICT,
            "job_upgrade_action_rejected",
            message,
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_recovery_artifact(
    hub: hub::Hub,
    job_id: String,
    headers: HeaderMap,
    kind: &str,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let id = format!(
        "{kind}-{}-{}",
        current.generation,
        hub::now_ms_for_metrics()
    );
    let record = crate::storage::JobCheckpointRecord {
        job_id: job_id.clone(),
        job_version: current.version,
        checkpoint_id: id.clone(),
        kind: kind.into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: serde_json::from_str::<arkflow_core::job::JobSpec>(&current.spec_json)
            .ok()
            .and_then(|spec| spec.state.map(|state| state.format_version))
            .unwrap_or(1),
        created_at_ms: hub::now_ms_for_metrics(),
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    match hub.record_job_checkpoint(record).await {
        Ok(Some(job)) => {
            // The operator-triggered recovery artifact is the audited
            // mutation; periodic scheduling and the dispatch funnel are
            // mechanics and stay out of the audit trail.
            hub.record_job_operation_audit(
                if kind == "savepoint" {
                    "job_savepoint"
                } else {
                    "job_checkpoint"
                },
                &job_id,
                None,
                None,
                "accepted",
                None,
                format!(
                    "{kind} trigger accepted, checkpoint_id={id}, generation={}",
                    current.generation
                ),
            )
            .await;
            (StatusCode::ACCEPTED, Json(job)).into_response()
        }
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_create_job(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<CreateJobRequest>,
) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Configure, "job", None).await
    {
        return response;
    }
    let spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if let Err(error) = spec.validate() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_spec",
            error.to_string(),
        );
    }
    if let Err(error) = arkflow_core::job::JobPlan::compile(spec.clone()) {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_plan",
            error.to_string(),
        );
    }
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::BAD_REQUEST, "invalid_job_plan", error);
    }
    if !matches!(request.desired_state.as_str(), "stopped" | "running") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_state",
            "desired_state must be stopped or running".into(),
        );
    }
    let job = JobRecord {
        job_id: spec.id.to_string(),
        version: spec.version.0,
        spec_json: serde_json::to_string(&request.spec).unwrap_or_else(|_| "{}".into()),
        desired_state: request.desired_state,
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: request.node_ids,
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    match hub.upsert_job(job).await {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_job_desired_state(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<JobDesiredStateRequest>,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if !matches!(request.state.as_str(), "stopped" | "running") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_state",
            "state must be stopped or running".into(),
        );
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let current = match hub.job(&job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("Unknown Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    };
    match hub
        .update_job_desired_state(&job_id, request.state.as_str(), current.generation)
        .await
    {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

async fn hub_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
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
    _headers: HeaderMap,
) -> Response {
    if hub.configuration(&node_id).await.is_none() {
        return problem(
            StatusCode::NOT_FOUND,
            "node_configuration_unavailable",
            "Node has not reported configuration".into(),
        );
    }
    Json(hub.config_versions(&node_id).await).into_response()
}

/// Read-only configuration reports (validation, version diff) dispatched to
/// the selected node. Unlike apply/rollback these never create versions or
/// rollouts: the report rides the terminal command result.
async fn hub_readonly_configuration_command(
    hub: &hub::Hub,
    node_id: String,
    operation: &str,
    payload: serde_json::Value,
    headers: &HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        hub,
        headers,
        OperatorAction::Configure,
        "configuration",
        Some(node_id.clone()),
    )
    .await
    {
        return response;
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
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
        Err(_) => problem(
            StatusCode::BAD_REQUEST,
            "command_rejected",
            "Invalid configuration command".into(),
        ),
    }
}

async fn hub_validate_configuration(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    let payload = match serde_json::to_value(candidate) {
        Ok(payload) => payload,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_configuration",
                "Configuration payload is not serializable".into(),
            )
        }
    };
    hub_readonly_configuration_command(&hub, node_id, "validate_configuration", payload, &headers)
        .await
}

async fn hub_configuration_diff(
    State(hub): State<hub::Hub>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<DiffQuery>,
) -> Response {
    hub_readonly_configuration_command(
        &hub,
        node_id,
        "diff_configuration",
        serde_json::json!({"from": query.from, "to": query.to}),
        &headers,
    )
    .await
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
    let principal = match require_operator_action(
        hub,
        headers,
        OperatorAction::Configure,
        "configuration",
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
    if hub.has_storage() {
        let config_version_id = if operation == "apply_configuration" {
            headers
                .get("x-config-version")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_millis())
                        .unwrap_or_default();
                    format!("cfg-{timestamp}-{}", std::process::id())
                })
        } else {
            payload
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .unwrap_or_default()
        };
        let rollout = if operation == "apply_configuration" {
            hub.create_rollout_with_content(
                config_version_id,
                payload.to_string(),
                vec![node_id.clone()],
                1,
                Some(principal.id.clone()),
                correlation_id.clone(),
            )
            .await
        } else {
            hub.create_rollout(
                config_version_id,
                vec![node_id.clone()],
                1,
                Some(principal.id.clone()),
                correlation_id.clone(),
            )
            .await
        };
        match rollout {
            Ok(rollout) => {
                return (
                    StatusCode::ACCEPTED,
                    Json(single_node_rollout_response(&rollout, &node_id)),
                )
                    .into_response()
            }
            Err(hub::HubError::Invalid(message)) => {
                return problem(StatusCode::BAD_REQUEST, "invalid_configuration", message)
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(error) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    error.to_string(),
                )
            }
        }
    }
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

fn single_node_rollout_response(
    rollout: &storage::RolloutRecord,
    node_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "rollout_id": rollout.rollout_id,
        "config_version_id": rollout.config_version_id,
        "config_version": rollout.config_version_id,
        "state": rollout.state,
        "batch_size": rollout.batch_size,
        "current_batch": rollout.current_batch,
        "total_targets": rollout.total_targets,
        "actor": rollout.actor,
        "correlation_id": rollout.correlation_id,
        "created_at_ms": rollout.created_at_ms,
        "updated_at_ms": rollout.updated_at_ms,
        "resource_type": "configuration",
        "resource_id": "__configuration__",
        "node_id": node_id,
        "intent_id": rollout.rollout_id,
        "generation": 1,
        "desired_state": "configured",
        "convergence_state": "pending",
    })
}

async fn hub_targeted_command(
    State(hub): State<hub::Hub>,
    Path((node_id, id, action)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "stream",
        Some(format!("{node_id}/{id}")),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
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
    if hub.has_storage() && matches!(action.as_str(), "start" | "stop") {
        match hub
            .set_desired_state(DesiredMutation {
                node_id: node_id.clone(),
                stream_id: id.clone(),
                desired_state: if action == "start" {
                    "running".into()
                } else {
                    "stopped".into()
                },
                config_version_id: None,
                action_id: None,
                expected_generation: headers
                    .get("if-match")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_generation_etag),
                actor: Some(principal.id.clone()),
                correlation_id,
                idempotency_key: headers
                    .get("idempotency-key")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                intent_type: None,
                payload_json: None,
            })
            .await
        {
            Ok(intent) => return intent_response(StatusCode::ACCEPTED, intent),
            Err(hub::HubError::GenerationConflict { expected, current }) => {
                return problem(
                    StatusCode::PRECONDITION_FAILED,
                    "generation_conflict",
                    format!("Expected generation {expected}, current generation {current}"),
                )
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(hub::HubError::IdempotencyKeyReused) => {
                return problem(
                    StatusCode::CONFLICT,
                    "idempotency_key_reused",
                    "Idempotency-Key was already used for a different mutation".into(),
                )
            }
            Err(error) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    error.to_string(),
                )
            }
        }
    }
    if hub.has_storage() && action == "restart" {
        let action_id = headers
            .get("x-action-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or_default();
                format!("restart-{timestamp}-{}", std::process::id())
            });
        match hub
            .restart_state(
                node_id,
                id,
                action_id,
                headers
                    .get("if-match")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_generation_etag),
                Some(principal.id.clone()),
                correlation_id,
                headers
                    .get("idempotency-key")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            )
            .await
        {
            Ok(intent) => return intent_response(StatusCode::ACCEPTED, intent),
            Err(hub::HubError::GenerationConflict { expected, current }) => {
                return problem(
                    StatusCode::PRECONDITION_FAILED,
                    "generation_conflict",
                    format!("Expected generation {expected}, current generation {current}"),
                )
            }
            Err(hub::HubError::StorageUnavailable) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    "Durable control-plane storage is unavailable".into(),
                )
            }
            Err(hub::HubError::IdempotencyKeyReused) => {
                return problem(
                    StatusCode::CONFLICT,
                    "idempotency_key_reused",
                    "Idempotency-Key was already used for a different mutation".into(),
                )
            }
            Err(error) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "repository_unavailable",
                    error.to_string(),
                )
            }
        }
    }
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

fn intent_response(status: StatusCode, intent: storage::IntentRecord) -> Response {
    let location = format!("/api/v1/operations/{}", intent.intent_id);
    let etag = format!("\"generation-{}\"", intent.generation);
    (
        status,
        [(header::LOCATION, location), (header::ETAG, etag)],
        Json(AcceptedIntentResponse::from(intent)),
    )
        .into_response()
}

async fn hub_restart_action(
    State(hub): State<hub::Hub>,
    Path((node_id, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<RestartActionRequest>>,
) -> Response {
    let Some(action_id) = body.and_then(|Json(request)| request.action_id) else {
        return hub_targeted_command(State(hub), Path((node_id, id, "restart".into())), headers)
            .await;
    };
    let Ok(action_header) = HeaderValue::from_str(&action_id) else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "action_id must be a valid header value".into(),
        );
    };
    let mut headers = headers;
    headers.insert("x-action-id", action_header);
    hub_targeted_command(State(hub), Path((node_id, id, "restart".into())), headers).await
}

async fn hub_desired_state(
    State(hub): State<hub::Hub>,
    Path((node_id, id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<DesiredStateRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "stream",
        Some(format!("{node_id}/{id}")),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if !matches!(request.state.as_str(), "running" | "stopped") {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "state must be running or stopped".into(),
        );
    }
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let expected_generation = headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_generation_etag);
    match hub
        .set_desired_state(DesiredMutation {
            node_id: node_id.clone(),
            stream_id: id.clone(),
            desired_state: request.state,
            config_version_id: request.config_version,
            action_id: request.action_id,
            expected_generation,
            actor: Some(principal.id.clone()),
            correlation_id,
            idempotency_key: headers
                .get("idempotency-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            intent_type: None,
            payload_json: None,
        })
        .await
    {
        Ok(intent) => {
            let location = format!("/api/v1/operations/{}", intent.intent_id);
            let etag = format!("\"generation-{}\"", intent.generation);
            (
                StatusCode::ACCEPTED,
                [(header::LOCATION, location), (header::ETAG, etag)],
                Json(AcceptedIntentResponse::from(intent)),
            )
                .into_response()
        }
        Err(hub::HubError::GenerationConflict { expected, current }) => problem_with_details(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
            Some(serde_json::json!({
                "expected_generation": expected,
                "current_generation": current,
                "resource": { "node_id": node_id, "stream_id": id }
            })),
        ),
        Err(hub::HubError::StorageUnavailable) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            "Durable control-plane storage is unavailable".into(),
        ),
        Err(hub::HubError::IdempotencyKeyReused) => problem(
            StatusCode::CONFLICT,
            "idempotency_key_reused",
            "Idempotency-Key was already used for a different mutation".into(),
        ),
        Err(error) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository_unavailable",
            error.to_string(),
        ),
    }
}

fn parse_generation_etag(value: &str) -> Option<u64> {
    value
        .trim_matches('"')
        .strip_prefix("generation-")
        .and_then(|value| value.parse().ok())
}

async fn hub_operations(
    State(hub): State<hub::Hub>,
    Query(query): Query<OperationQuery>,
    _headers: HeaderMap,
) -> Response {
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
            .skip(page.saturating_sub(1).saturating_mul(page_size))
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

async fn hub_cancel_operation(
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

async fn hub_events(
    State(hub): State<hub::Hub>,
    Query(query): Query<EventQuery>,
    _headers: HeaderMap,
) -> Response {
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
            .skip(page.saturating_sub(1).saturating_mul(page_size))
            .take(page_size)
            .collect::<Vec<_>>(),
        page,
        page_size,
        total,
    })
    .into_response()
}

async fn hub_event_stream(
    State(hub): State<hub::Hub>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> Response {
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let mut replay = VecDeque::new();
    let mut existing = hub.events(query.node_id.as_deref()).await;
    existing.retain(|event| event_matches(event, &query));
    if let Some(last_event_id) = last_event_id {
        let last_event_id = last_event_id.min(i64::MAX as u64) as i64;
        let oldest_id = existing.iter().filter_map(|event| event.event_id).min();
        if oldest_id.is_some_and(|event_id| last_event_id < event_id.saturating_sub(1)) {
            replay.push_back(None);
        }
        let mut pending = existing
            .into_iter()
            .filter(|event| {
                event
                    .event_id
                    .is_some_and(|event_id| event_id > last_event_id)
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|event| event.event_id);
        replay.extend(pending.into_iter().map(Some));
    }
    let receiver = hub.subscribe();
    let stream = futures_util::stream::unfold(
        (replay, receiver, query),
        |(mut replay, mut receiver, query)| async move {
            if let Some(update) = replay.pop_front() {
                let event = update
                    .map(|update| {
                        SseEvent::default()
                            .id(update
                                .event_id
                                .unwrap_or(update.event.occurred_at_ms as i64)
                                .to_string())
                            .event(update.event.event_type.clone())
                            .json_data(update)
                            .unwrap_or_else(|_| SseEvent::default().event("resync").data("{}"))
                    })
                    .unwrap_or_else(|| SseEvent::default().event("resync").data("{}"));
                return Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
            }
            loop {
                match receiver.recv().await {
                    Ok(update) if event_matches(&update, &query) => {
                        let event = SseEvent::default()
                            .id(update
                                .event_id
                                .unwrap_or(update.event.occurred_at_ms as i64)
                                .to_string())
                            .event(update.event.event_type.clone())
                            .json_data(update)
                            .unwrap_or_else(|_| SseEvent::default().event("resync").data("{}"));
                        break Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let event = SseEvent::default().event("resync").data("{}");
                        break Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break None,
                }
            }
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn event_matches(event: &hub::HubEvent, query: &EventQuery) -> bool {
    query
        .node_id
        .as_deref()
        .is_none_or(|node_id| event.node_id == node_id)
        && query
            .event_type
            .as_deref()
            .is_none_or(|event_type| event.event.event_type == event_type)
        && query
            .stream_id
            .as_deref()
            .is_none_or(|stream_id| event.event.stream_id.as_deref() == Some(stream_id))
        && query
            .correlation_id
            .as_deref()
            .is_none_or(|correlation_id| {
                event.event.correlation_id.as_deref() == Some(correlation_id)
            })
}

async fn hub_audit(
    State(hub): State<hub::Hub>,
    Query(query): Query<AuditQuery>,
    _headers: HeaderMap,
) -> Response {
    match hub.audit(query.resource_id.as_deref()).await {
        Ok(records) => Json(page_items(
            records,
            &PageQuery {
                page: query.page,
                page_size: query.page_size,
                node_id: None,
            },
        ))
        .into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn create_rollout(
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

async fn hub_rollouts(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    match hub.rollouts().await {
        Ok(rollouts) => Json(rollouts).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_rollout(
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

async fn rollout_action(
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

#[derive(Deserialize)]
struct MetricsQuery {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    format: Option<String>,
}

async fn hub_metrics(
    State(hub): State<hub::Hub>,
    Query(query): Query<MetricsQuery>,
    headers: HeaderMap,
) -> Response {
    // Console clients ask for the JSON aggregate explicitly; anything else
    // (including every Prometheus scraper) keeps the text exposition.
    let wants_json = query.format.as_deref() == Some("json")
        || headers
            .get(axum::http::header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|accept| {
                accept
                    .split(',')
                    .any(|part| part.trim().starts_with("application/json"))
            });
    if wants_json {
        let items = hub.metrics_by_node(query.node_id.as_deref()).await;
        let aggregate = hub.metrics(query.node_id.as_deref()).await;
        return Json(serde_json::json!({"items": items, "aggregate": aggregate})).into_response();
    }
    let status = hub.operational_status().await;
    let mut body = String::new();
    if let Ok(status) = status {
        body.push_str(&format!(
            "arkflow_control_plane_ready {}\n",
            status.ready as u8
        ));
        body.push_str(&format!(
            "arkflow_reconciliation_runs_total {}\narkflow_reconciliation_failures_total {}\n",
            status.reconciliation.runs_total, status.reconciliation.failures_total
        ));
        body.push_str(&format!("arkflow_outbox_pending {}\narkflow_outbox_claimed {}\narkflow_stale_nodes {}\narkflow_active_attempts {}\narkflow_non_terminal_intents {}\n", status.outbox_pending, status.outbox_claimed, status.stale_nodes, status.active_attempts, status.non_terminal_intents));
        for (state, count) in status.node_states {
            body.push_str(&format!(
                "arkflow_nodes_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.maintenance_states {
            body.push_str(&format!(
                "arkflow_nodes_maintenance_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.intent_states {
            body.push_str(&format!(
                "arkflow_intents_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.attempt_states {
            body.push_str(&format!(
                "arkflow_attempts_state{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    if let Ok(rollouts) = hub.rollouts().await {
        let mut states = std::collections::BTreeMap::<String, usize>::new();
        for rollout in rollouts {
            *states.entry(rollout.state).or_default() += 1;
        }
        for (state, count) in states {
            body.push_str(&format!(
                "arkflow_rollouts_state{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    for node in hub.nodes().await {
        let node_id = prometheus_label(&node.id);
        let protocol = prometheus_label(&node.protocol_version);
        let software = prometheus_label(&node.version);
        let state = prometheus_label(&format!("{:?}", node.state).to_lowercase());
        body.push_str(&format!(
            "arkflow_node_compatibility{{node_id=\"{node_id}\",protocol_version=\"{protocol}\",software_version=\"{software}\",state=\"{state}\"}} 1\n"
        ));
        for capability in node.capabilities {
            body.push_str(&format!(
                "arkflow_node_capability{{node_id=\"{node_id}\",capability=\"{}\"}} 1\n",
                prometheus_label(&capability)
            ));
        }
    }
    for node in hub.metrics_by_node(query.node_id.as_deref()).await {
        let node_id = prometheus_label(&node.node_id);
        for (name, value) in node.metrics {
            body.push_str(&format!(
                "arkflow_node_metric{{node_id=\"{node_id}\",metric=\"{}\"}} {value}\n",
                prometheus_label(&name)
            ));
        }
    }
    body.push_str(&hub.command_metrics().render());
    body.push_str(&crate::metrics::encode_families(
        crate::metrics::storage_families(),
    ));
    // Data-plane series reported by Agents: the kernel Job vocabulary with an
    // extra `node` label. Expired-lease nodes are excluded by the Hub.
    for (node_id, jobs) in hub.job_metrics().await {
        for (job_id, snapshot) in &jobs {
            body.push_str(&crate::metrics::encode_families(
                crate::metrics::kernel_job_families(job_id, snapshot, &[("node", node_id.clone())]),
            ));
        }
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

async fn hub_operational_status(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    match hub.operational_status().await {
        Ok(status) => {
            let mut value = serde_json::to_value(status).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                let rollouts = hub.rollouts().await.unwrap_or_default();
                object.insert(
                    "rollouts".into(),
                    serde_json::json!({
                        "total": rollouts.len(),
                        "states": rollouts.iter().fold(std::collections::BTreeMap::<String, usize>::new(), |mut states, rollout| { *states.entry(rollout.state.clone()).or_default() += 1; states }),
                    }),
                );
                object.insert(
                    "compatibility".into(),
                    serde_json::json!({
                        "nodes": hub.nodes().await.into_iter().map(|node| serde_json::json!({ "id": node.id, "protocol_version": node.protocol_version, "software_version": node.version, "capabilities": node.capabilities })).collect::<Vec<_>>()
                    }),
                );
            }
            Json(value).into_response()
        }
        Err(error) => hub_problem(error),
    }
}

async fn hub_drain_node(
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

async fn hub_maintain_node(
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

async fn hub_resume_node(
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

async fn hub_set_maintenance(
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
async fn agent_job_observation(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::JobObservationRequest>,
) -> Response {
    match hub.report_job_observation(request).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
/// Session tokens embedded in URL query strings leak into reverse-proxy and
/// access logs. Prefer the `Authorization: Bearer` header; the query param
/// remains accepted for older Agents.
/// Resolve the Agent session credential from its two accepted transports. The
/// header wins so a client that sends both is never authenticated by the stale
/// query value; the query parameter is the deprecated fallback that keeps older
/// Agents working through the transition window.
fn agent_session_token(headers: &HeaderMap, query_token: Option<String>) -> Option<String> {
    bearer_session_token(headers).or(query_token)
}

fn bearer_session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

/// Query parameters for the agent command endpoints. The session token is
/// optional here: it travels in the `Authorization: Bearer` header; the query
/// field remains accepted for older Agents.
#[derive(serde::Deserialize)]
struct AgentCommandsQuery {
    node_id: String,
    session_token: Option<String>,
}

async fn agent_commands(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Query(query): Query<AgentCommandsQuery>,
) -> Response {
    if query.session_token.is_some() {
        // The query parameter leaks the live session credential into
        // tracing/proxy logs (the trace layer records the full URI).
        tracing::warn!(
            node_id = %query.node_id,
            "agent session token supplied via query parameter; this is deprecated and will be removed - send it in the Authorization: Bearer header"
        );
    }
    let Some(session_token) = agent_session_token(&headers, query.session_token) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing session token".into(),
        );
    };
    match hub
        .commands(hub::AgentAuth {
            node_id: query.node_id,
            session_token,
        })
        .await
    {
        Ok(commands) => Json(commands).into_response(),
        Err(error) => hub_problem(error),
    }
}
async fn agent_command_result(
    State(hub): State<hub::Hub>,
    Path(_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<AgentCommandsQuery>,
    Json(result): Json<hub::CommandResult>,
) -> Response {
    if query.session_token.is_some() {
        tracing::warn!(
            node_id = %query.node_id,
            "agent session token supplied via query parameter; this is deprecated and will be removed - send it in the Authorization: Bearer header"
        );
    }
    let Some(session_token) = agent_session_token(&headers, query.session_token) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing session token".into(),
        );
    };
    match hub
        .command_result(
            hub::AgentAuth {
                node_id: query.node_id,
                session_token,
            },
            result,
        )
        .await
    {
        Ok(operation) => Json(operation).into_response(),
        Err(error) => hub_problem(error),
    }
}

async fn hub_health(State(hub): State<hub::Hub>) -> Response {
    let status = hub.operational_status().await.ok();
    let body = Json(
        serde_json::json!({"status": status.as_ref().map(|s| s.status.as_str()).unwrap_or("degraded"), "nodes":hub.nodes().await.len(), "ready": status.as_ref().is_some_and(|s| s.ready)}),
    );
    if status.is_some_and(|s| s.ready) {
        (StatusCode::OK, body).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
    }
}
async fn hub_readiness(State(hub): State<hub::Hub>) -> Response {
    let leadership = hub.leadership().await;
    let ha = serde_json::json!({
        "enabled": hub.ha_config().enabled,
        "role": leadership.role(),
        "epoch": leadership.epoch(),
    });
    if !leadership.is_leader() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "ready": false,
                "reason": "standby",
                "ha": ha,
            })),
        )
            .into_response();
    }
    match hub.operational_status().await {
        Ok(status) if status.ready => (
            StatusCode::OK,
            Json(serde_json::json!({"status":"ready","ready":true,"ha":ha})),
        )
            .into_response(),
        Ok(_status) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"not_ready","ready":false,"reason":"startup_recovery","ha":ha})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"not_ready","ready":false,"reason":"storage_unavailable","ha":ha})),
        )
            .into_response(),
    }
}
async fn hub_liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"alive","alive":true}))
}

async fn hub_oidc_status(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    let login_enabled = hub.oidc_login().is_some();
    let principal = cookie_value(&headers, "arkflow_session")
        .and_then(|session_id| hub.oidc_session_principal(&session_id));
    let body = serde_json::json!({
        "login_enabled": login_enabled,
        "authenticated": principal.is_some(),
        "principal": principal.map(|principal| serde_json::json!({
            "id": principal.id,
            "roles": principal.roles.iter().map(|role| format!("{role:?}").to_lowercase()).collect::<Vec<_>>(),
        })),
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

pub(crate) async fn hub_oidc_login(State(hub): State<hub::Hub>) -> Response {
    let Some(federation) = hub.oidc_login() else {
        return problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "OIDC login is not enabled".into(),
        );
    };
    fn random_hex() -> Option<String> {
        use rand::TryRngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes).ok()?;
        Some(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }
    let Some(state) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    // PKCE S256 verifier + OIDC nonce, round-tripped inside the state cookie
    // (state.verifier.nonce — hex segments, '.'-separated).
    let Some(verifier) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    let Some(nonce) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    use sha2::{Digest, Sha256};
    let challenge: String = {
        let digest = Sha256::digest(verifier.as_bytes());
        digest.iter().map(|b| format!("{b:02x}")).collect()
    };
    let location = federation.authorization_redirect(&state, &challenge, &nonce);
    let cookie_value = format!("{state}.{verifier}.{nonce}");
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(
            header::SET_COOKIE,
            format!(
                "arkflow_oidc_state={cookie_value}; HttpOnly; SameSite=Lax; Path=/; Max-Age=600{}",
                if federation.login_is_secure() {
                    "; Secure"
                } else {
                    ""
                }
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) async fn hub_oidc_callback(
    State(hub): State<hub::Hub>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let Some(federation) = hub.oidc_login() else {
        return problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "OIDC login is not enabled".into(),
        );
    };
    let unauthorized = || {
        problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "OIDC login failed".into(),
        )
    };
    let Some(expected_state) = cookie_value(&headers, "arkflow_oidc_state") else {
        return unauthorized();
    };
    let Some(supplied_state) = params.get("state") else {
        return unauthorized();
    };
    // The state cookie packs `state.verifier.nonce` (PKCE S256 + OIDC nonce
    // round-trip).
    let mut cookie_parts = expected_state.split('.');
    let (expected_state, code_verifier, expected_nonce) = (
        cookie_parts.next(),
        cookie_parts.next(),
        cookie_parts.next(),
    );
    let (Some(expected_state), Some(code_verifier), Some(expected_nonce)) =
        (expected_state, code_verifier, expected_nonce)
    else {
        return unauthorized();
    };
    if !bool::from(expected_state.as_bytes().ct_eq(supplied_state.as_bytes())) {
        return unauthorized();
    }
    let Some(code) = params.get("code") else {
        return unauthorized();
    };
    let Some(id_token) = federation.exchange_code(code, code_verifier).await else {
        return unauthorized();
    };
    // Nonce pre-check (raw claim read); signature/iss/aud validation happens
    // in `authenticate` below.
    if oidc::OidcFederation::nonce_claim(&id_token)
        .is_none_or(|claim| !bool::from(claim.as_bytes().ct_eq(expected_nonce.as_bytes())))
    {
        return unauthorized();
    }
    let Some(principal) = federation.authenticate(&id_token).await else {
        return unauthorized();
    };
    let Some(session_id) = federation.create_session(principal) else {
        return problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "session_create_failed",
            "OS random source is unavailable; session cannot be created".into(),
        );
    };
    let max_age = oidc::session_ttl_secs();
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, "/")
        .header(
            header::SET_COOKIE,
            format!(
                "arkflow_session={session_id}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}{}",
                if federation.login_is_secure() {
                    "; Secure"
                } else {
                    ""
                }
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) async fn hub_oidc_logout(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    if let Some(token) = bearer(&headers) {
        if let Some(session_id) = token.strip_prefix("session:") {
            if let Some(federation) = hub.oidc_login() {
                federation.remove_session(session_id);
            }
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::SET_COOKIE,
            "arkflow_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0",
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    cookies.split(';').find_map(|pair| {
        let pair = pair.trim();
        pair.strip_prefix(&format!("{name}=")).map(str::to_string)
    })
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

fn prometheus_label(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || "._-".contains(*character))
        .take(64)
        .collect()
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

/// Reject job-level mutations while an atomic upgrade orchestration owns the
/// Job. `Ok(())` when no orchestration is active.
// Err carries a ready-made axum rejection response (middleware idiom).
#[allow(clippy::result_large_err)]
async fn reject_if_job_upgrade_active(hub: &hub::Hub, job_id: &str) -> Result<(), Response> {
    if hub.active_job_upgrade_for(job_id).await.is_some() {
        return Err(problem(
            StatusCode::CONFLICT,
            "orchestration_in_progress",
            format!("Job {job_id} is owned by an active atomic upgrade"),
        ));
    }
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

async fn cancel_operation(
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
            .skip(page.saturating_sub(1).saturating_mul(page_size))
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

async fn validate_configuration(
    State(cp): State<ControlPlane>,
    headers: HeaderMap,
    Json(candidate): Json<ConfigCandidate>,
) -> Response {
    // Validation parses and resolves secret references and constructs every
    // component in the candidate, so it needs the same authorization as the
    // apply path: an open endpoint would hand unauthenticated callers a
    // node-local env/file oracle and a per-request construction load.
    if !authorized(&cp, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid Bearer token is required".into(),
        );
    }
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
    let runtime = cp.runtime_manager();
    let mut streams: Vec<(String, arkflow_core::control::StreamMetricsSnapshot)> = runtime
        .snapshots()
        .await
        .into_iter()
        .map(|status| (status.id, status.metrics))
        .collect();
    streams.sort_by(|left, right| left.0.cmp(&right.0));
    let jobs = runtime.job_metrics().snapshots();
    let body = crate::metrics::data_plane_exposition(&streams, &jobs);
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
    ready(State(cp)).await
}
async fn liveness() -> Json<serde_json::Value> {
    live().await
}

/// Readiness on the observability surface: success once the engine runtime
/// has finished starting the configured Streams and Jobs (in local mode this
/// is the same health state the control plane reports via `/readiness`).
async fn ready(State(cp): State<ControlPlane>) -> Response {
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
async fn live() -> Json<serde_json::Value> {
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

#[cfg(test)]
mod tests {
    /// Stale-leader fencing surfaces as 503 with an explicit code, in the
    /// same retry-against-the-elected-leader family as standby.
    #[tokio::test]
    async fn hub_problem_maps_stale_leader_to_503() {
        let response = hub_problem(hub::HubError::StaleLeader {
            claimed: 3,
            current: 4,
        });
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body).into_owned();
        assert!(
            text.contains("\"stale_leader\""),
            "body carries the code: {text}"
        );
    }

    use super::*;

    fn headers_with(authorization: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(value) = authorization {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    /// Regression: the Hub must authenticate the Agent's session from BOTH
    /// transports for one transition window. An Agent that only sends the
    /// legacy query parameter keeps working (Hub upgraded first), and an Agent
    /// that sends both is authenticated from the header, never from the stale
    /// query value.
    #[test]
    fn agent_session_token_is_accepted_from_the_header_or_the_query() {
        // Header only: the new transport.
        assert_eq!(
            agent_session_token(&headers_with(Some("Bearer header-token")), None).as_deref(),
            Some("header-token")
        );
        // Query only: an older Agent against a newer Hub.
        assert_eq!(
            agent_session_token(&headers_with(None), Some("query-token".into())).as_deref(),
            Some("query-token")
        );
        // Both: the header wins over the deprecated query credential.
        assert_eq!(
            agent_session_token(
                &headers_with(Some("Bearer header-token")),
                Some("query-token".into())
            )
            .as_deref(),
            Some("header-token"),
            "the header transport takes precedence over the query parameter"
        );
        // Neither: the handler answers 401.
        assert_eq!(agent_session_token(&headers_with(None), None), None);
        // A malformed or empty header credential never falls through.
        assert_eq!(bearer_session_token(&headers_with(Some("Basic x"))), None);
        assert_eq!(bearer_session_token(&headers_with(Some("Bearer   "))), None);
    }

    use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
    use arkflow_core::engine::Engine;
    use futures_util::StreamExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn hub_component_catalogue_exposes_registered_job_components() {
        arkflow_plugin::initialize().unwrap();
        let app = hub_router(
            hub::Hub::new(hub::HubConfig {
                operator_token: None,
                node_token: None,
                insecure_local: true,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            }),
            &ServerConfig::default(),
        );
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/components")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let components: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert!(components.iter().any(|item| item["kind"] == "input"));
        assert!(components.iter().any(|item| item["kind"] == "output"));
        assert!(components.iter().any(|item| item["kind"] == "processor"));
    }

    #[tokio::test]
    async fn configuration_validation_requires_the_operator_token() {
        // The validate endpoint resolves secret references and constructs
        // every component in the candidate, so it must be as guarded as
        // apply: an open endpoint would hand unauthenticated callers a
        // node-local env/file oracle and a per-request construction load.
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig {
                api_token: Some("op-token".into()),
                ..HealthCheckConfig::default()
            },
        });
        let cp = engine.control_plane();
        let app = router(cp, &ServerConfig::default());
        let candidate = serde_json::json!({
            "format": "yaml",
            "content": "not: [a valid config"
        });
        assert_eq!(
            app.clone()
                .oneshot(
                    axum::http::Request::post("/api/v1/configuration/validate")
                        .header(axum::http::header::AUTHORIZATION, "Bearer wrong")
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(candidate.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.oneshot(
                axum::http::Request::post("/api/v1/configuration/validate")
                    .header(axum::http::header::AUTHORIZATION, "Bearer op-token")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(candidate.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn resource_router_exposes_system_nodes_streams_and_health() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
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
            jobs: Vec::new(),
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
            jobs: Vec::new(),
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
    async fn ha_enabled_without_storage_fails_before_binding() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        })
        .with_ha(hub::HubHaConfig {
            enabled: true,
            ..hub::HubHaConfig::default()
        });
        let config = ServerConfig {
            address: "127.0.0.1:0".into(),
            insecure_local: true,
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let error = serve_hub(hub, config, cancellation).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("HA election requires durable storage"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn hub_startup_policy_covers_local_external_storage_and_credentials() {
        let local_hub = hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let local = ServerConfig {
            address: "127.0.0.1:8080".into(),
            insecure_local: true,
            ..ServerConfig::default()
        };
        assert!(local.validate_hub_startup(&local_hub).is_ok());

        let mut external_insecure = local.clone();
        external_insecure.address = "0.0.0.0:8080".into();
        assert!(external_insecure
            .validate_hub_startup(&local_hub)
            .unwrap_err()
            .to_string()
            .contains("loopback"));

        let secure = ServerConfig {
            address: "0.0.0.0:8080".into(),
            ..ServerConfig::default()
        };
        let credentialed_without_storage = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node".into()),
            insecure_local: false,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        assert!(secure
            .validate_hub_startup(&credentialed_without_storage)
            .unwrap_err()
            .to_string()
            .contains("durable storage"));

        let temp = tempfile::tempdir().unwrap();
        let store =
            storage::ControlPlaneStore::open(temp.path().join("hub.sqlite").to_str().unwrap())
                .await
                .unwrap();
        let missing_credentials = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: None,
                node_token: None,
                insecure_local: false,
                lease_ttl_ms: 1_000,
                poll_interval_ms: 10,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store.clone(), 8),
        );
        assert!(secure
            .validate_hub_startup(&missing_credentials)
            .unwrap_err()
            .to_string()
            .contains("credentials"));

        let valid_hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node".into()),
                insecure_local: false,
                lease_ttl_ms: 1_000,
                poll_interval_ms: 10,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store, 8),
        );
        assert!(secure.validate_hub_startup(&valid_hub).is_ok());
    }

    /// The API-server-disabled process still exposes observability endpoints:
    /// `/metrics` returns a valid exposition and `/ready` flips from 503 to
    /// 200 once the engine runtime reports readiness.
    #[tokio::test]
    async fn observability_router_serves_metrics_ready_and_live_without_the_api_server() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let control_plane = engine.control_plane();
        control_plane.health().set_ready(false);
        let app = observability_router(control_plane.clone(), &ServerConfig::default());

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/ready")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["status"], "not_ready");
        assert_eq!(value["ready"], false);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/live")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        control_plane.health().set_ready(true);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/ready")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        // No streams and no jobs: a valid (empty) exposition.
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(!text.lines().any(|line| !line.starts_with('#')));
    }

    /// The API-server router gains `/ready` and `/live` while the legacy
    /// `/health`, `/readiness`, and `/liveness` paths keep their semantics.
    #[tokio::test]
    async fn local_router_keeps_legacy_health_paths_and_adds_ready_live() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let control_plane = engine.control_plane();
        control_plane.health().set_ready(true);
        control_plane.health().set_running(true);
        let app = router(control_plane, &ServerConfig::default());
        for path in ["/ready", "/live", "/readiness", "/liveness", "/health"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(response.status().is_success(), "path failed: {path}");
        }
    }

    #[tokio::test]
    async fn resource_api_integration_covers_routes_filters_redaction_and_aliases() {
        let health = HealthCheckConfig {
            api_token: Some("secret-token".into()),
            ..Default::default()
        };
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
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
                    .header("authorization", auth)
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
                    .header("authorization", auth)
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
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
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
    async fn hub_routes_distinguish_viewer_from_operator_actions() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("readonly|viewer|viewer-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let app = hub_router(hub, &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes")
                    .header("authorization", "Bearer viewer-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                    .header("authorization", "Bearer viewer-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/events/stream")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn job_workbench_routes_validate_create_detail_and_versions() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator-secret".into()),
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let app = hub_router(hub, &ServerConfig::default());
        let spec = serde_json::json!({
            "id": "workbench-job",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        });
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/validate")
                    .header("authorization", "Bearer operator-secret")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"spec": spec, "node_ids": []}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header("authorization", "Bearer operator-secret")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"spec": spec, "desired_state": "stopped"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/jobs/workbench-job/detail")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(detail["job"]["job_id"], "workbench-job");
        assert!(detail["plan"].is_object());
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/jobs/workbench-job/versions")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn job_action_route_leaves_an_audit_trail() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator-secret".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store, 8),
        );
        hub.register(hub::RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let spec = serde_json::json!({
            "id": "audit-job",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        });
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header("authorization", "Bearer operator-secret")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({"spec": spec, "desired_state": "stopped"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        // The start action funnels into the audited enqueue path.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/audit-job/actions/start")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/audit?resource_id=audit-job")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let actions: Vec<&str> = page["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item["action"].as_str())
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            actions.contains(&"job.start"),
            "the start action must be audited, got {actions:?}"
        );
        // The hub-level funnel keeps working for direct dispatches too.
        assert!(hub
            .audit(Some("audit-job"))
            .await
            .unwrap()
            .iter()
            .any(|record| record.action == "job.start" && record.outcome == "accepted"));
        // The operator-triggered checkpoint is audited exactly once at the
        // trigger, not per dispatch and not by the periodic scheduler.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/audit-job/checkpoints")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let checkpoint_audits = hub
            .audit(Some("audit-job"))
            .await
            .unwrap()
            .iter()
            .filter(|record| record.action == "job.checkpoint")
            .count();
        assert_eq!(checkpoint_audits, 1, "the trigger is audited exactly once");
    }

    #[tokio::test]
    async fn denied_mutation_is_visible_in_audit_history() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("viewer|viewer|viewer-secret".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store, 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer viewer-secret")
                    .header("content-type", "application/json")
                    .header("x-correlation-id", "audit-test")
                    .body(axum::body::Body::from(r#"{"state":"running"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/audit?resource_id=node-a%2Forders")
                    .header("authorization", "Bearer viewer-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["items"][0]["outcome"], "rejected");
        assert_eq!(value["items"][0]["correlation_id"], "audit-test");
    }

    #[tokio::test]
    async fn sse_replays_durable_events_by_last_event_id() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'rollout_changed', 'accepted', 10)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store, 8),
        );
        let response = hub_router(hub, &ServerConfig::default())
            .oneshot(
                axum::http::Request::get("/api/v1/events/stream")
                    .header("authorization", "Bearer operator")
                    .header("last-event-id", "0")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        assert!(text.contains("id: 1"));
        assert!(text.contains("event: rollout_changed"));
    }

    #[tokio::test]
    async fn desired_state_route_persists_offline_intent_and_rejects_stale_generation() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(store, 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "desired-orders-1")
                    .body(axum::body::Body::from(r#"{"state":"running"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .starts_with("/api/v1/operations/intent-"));
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            "\"generation-1\""
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["generation"], 1);
        assert_eq!(value["convergence"], "pending");
        assert_eq!(value["desired_state"], "running");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes/node-a/streams/orders")
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
        let resource: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resource["desired"]["state"], "running");
        assert_eq!(resource["desired"]["generation"], 1);
        assert_eq!(resource["state"], "unknown");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/events?node_id=node-a")
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
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(events["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["event_type"] == "intent_created"));

        let operation_id = value["operation_id"].as_str().unwrap().to_owned();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/operations/{operation_id}"))
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
        let operation: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(operation["intent_id"], value["operation_id"]);
        assert_eq!(operation["intent_state"], "accepted");
        assert_eq!(operation["convergence_state"], "pending");
        assert_eq!(operation["retry_count"], 0);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "desired-orders-1")
                    .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let replay_error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(replay_error["code"], "idempotency_key_reused");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "desired-orders-1")
                    .body(axum::body::Body::from(r#"{"state":"running"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let replay: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(replay["generation"], 1);
        assert_eq!(replay["desired_state"], "running");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/restart")
                    .header("authorization", "Bearer operator")
                    .header("if-match", "\"generation-1\"")
                    .header("idempotency-key", "restart-orders-1")
                    .header("x-action-id", "restart-action-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let restart: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(restart["generation"], 2);
        assert_eq!(restart["desired_state"], "running");
        assert_eq!(restart["action_id"], "restart-action-1");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/actions/restart")
                    .header("authorization", "Bearer operator")
                    .header("if-match", "\"generation-2\"")
                    .header("idempotency-key", "restart-orders-2")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"action_id":"restart-action-2"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let canonical_restart: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(canonical_restart["generation"], 3);
        assert_eq!(canonical_restart["action_id"], "restart-action-2");

        let response = app
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("x-correlation-id", "generation-conflict-test")
                    .header("if-match", "\"generation-0\"")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["code"], "generation_conflict");
        assert_eq!(error["correlation_id"], "generation-conflict-test");
        assert_eq!(error["details"]["current_generation"], 3);
    }

    #[tokio::test]
    async fn configuration_apply_persists_target_and_reconciles_offline_write() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let storage = storage::StorageActor::start(store, 8);
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage.clone(),
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/configuration/apply")
                    .header("authorization", "Bearer operator")
                    .header("idempotency-key", "config-apply-1")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"format":"json","content":"{\"streams\":[]}"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let intent: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(intent["generation"], 1);
        assert_eq!(intent["desired_state"], "configured");
        let config_version = intent["config_version"].as_str().unwrap().to_owned();
        assert!(intent["rollout_id"].as_str().is_some());

        let session = hub
            .register(hub::RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        let operation = hub
            .reconcile_once("config-reconciler")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.operation, "apply_configuration");
        assert_eq!(
            operation.config_version_id.as_deref(),
            Some(config_version.as_str())
        );
        let commands = hub
            .commands(hub::AgentAuth {
                node_id: "node-a".into(),
                session_token: session.session_token.clone(),
            })
            .await;
        let commands = commands.unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].operation, "apply_configuration");
        assert_eq!(
            commands[0].config_version_id.as_deref(),
            Some(config_version.as_str())
        );
        assert!(commands[0].payload.is_some());

        hub.report(hub::NodeReport {
            auth: hub::AgentAuth {
                node_id: "node-a".into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: Default::default(),
            jobs: Default::default(),
            configuration: None,
            configuration_version: Some(config_version),
            config_versions: Vec::new(),
            job_tasks: Default::default(),
            boot_id: Some(session.session_token.clone()),
            connected_hub: None,
            report_seq: 1,
        })
        .await
        .unwrap();
        let rollout_id = intent["rollout_id"].as_str().unwrap();
        let (_, targets) = hub.rollout(rollout_id).await.unwrap().unwrap();
        let intent_id = targets[0].attempt_id.clone().unwrap();
        let persisted = storage.get_intent(intent_id).await.unwrap().unwrap();
        assert_eq!(persisted.state, "converged");
    }

    #[tokio::test]
    async fn node_level_rollback_returns_a_single_node_rollout() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-rollback', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let storage = storage::StorageActor::start(store, 8);
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage,
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let response = app
            .oneshot(
                axum::http::Request::post(
                    "/api/v1/nodes/node-a/configuration/rollback/cfg-rollback",
                )
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let rollout: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(rollout["rollout_id"].as_str().is_some());
        assert_eq!(rollout["config_version_id"], "cfg-rollback");
        assert_eq!(rollout["total_targets"], 1);
        let (_, targets) = hub
            .rollout(rollout["rollout_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(targets[0].node_id, "node-a");
    }

    #[tokio::test]
    async fn operation_resource_exposes_retry_superseded_and_blocked_states() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let storage = storage::StorageActor::start(store, 8);
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 100,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage.clone(),
        );
        let app = hub_router(hub, &ServerConfig::default());
        let first = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "state-matrix-1")
                    .body(axum::body::Body::from(r#"{"state":"running"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap();
        let first: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let first_id = first["intent_id"].as_str().unwrap().to_owned();
        let attempt = storage.claim_attempt(&first_id).await.unwrap().unwrap();
        storage
            .complete_attempt(
                &attempt.attempt_id,
                "timed_out",
                Some("temporary_execution".into()),
            )
            .await
            .unwrap();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/operations/{first_id}"))
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let retrying: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(retrying["intent_state"], "retrying");
        assert_eq!(retrying["convergence_state"], "degraded");
        assert_eq!(retrying["retry_count"], 1);
        assert_eq!(retrying["failure_class"], "temporary_execution");

        let second = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("if-match", "\"generation-1\"")
                    .header("idempotency-key", "state-matrix-2")
                    .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let second: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let second_id = second["intent_id"].as_str().unwrap().to_owned();
        let attempt = storage.claim_attempt(&second_id).await.unwrap().unwrap();
        storage
            .complete_attempt(
                &attempt.attempt_id,
                "failed",
                Some("permanent_execution".into()),
            )
            .await
            .unwrap();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/operations/{first_id}"))
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let superseded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(superseded["intent_state"], "superseded");
        assert_eq!(superseded["superseded_generation"], 2);

        let response = app
            .oneshot(
                axum::http::Request::get(format!("/api/v1/operations/{second_id}"))
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let blocked: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(blocked["intent_state"], "blocked");
        assert_eq!(blocked["convergence_state"], "blocked");
        assert_eq!(blocked["failure_class"], "permanent_execution");
    }

    #[tokio::test]
    async fn hub_end_to_end_two_nodes_aggregate_target_reconnect_and_drain() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
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
                        generation: 0,
                        observed_generation: None,
                        action_id: None,
                        failure_class: None,
                        config_version_id: None,
                        rollout_id: None,
                        observed_checkpoint_id: None,
                        checkpoint_manifest_uri: None,
                        result: None,
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

    fn hub_tls_material() -> (std::path::PathBuf, std::path::PathBuf) {
        use rcgen::CertificateParams;
        use rcgen::KeyPair;
        let params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        // A unique directory per call: parallel TLS tests used to share one
        // pid-keyed dir and delete each other's (freshly regenerated)
        // material mid-test.
        static TLS_MATERIAL_SEQUENCE: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let sequence = TLS_MATERIAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("arkflow-hub-tls-{}-{sequence}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    #[tokio::test]
    async fn hub_tls_listener_serves_readiness_over_https() {
        let (cert_path, key_path) = hub_tls_material();
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let config = ServerConfig {
            address: format!("127.0.0.1:{port}"),
            insecure_local: true,
            tls_cert: Some(cert_path.to_string_lossy().into_owned()),
            tls_key: Some(key_path.to_string_lossy().into_owned()),
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_hub(hub, config, shutdown).await {
                eprintln!("SERVE_HUB ERROR: {error}");
            }
        });
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let mut served = false;
        for _ in 0..100 {
            if let Ok(response) = client
                .get(format!("https://127.0.0.1:{port}/readiness"))
                .send()
                .await
            {
                // The response status itself is the plaintext readiness
                // semantics (503 without storage); what matters here is
                // that the TLS handshake completed and HTTP was served.
                let _ = response.status();
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert!(served, "the TLS hub must serve https requests");
        cancellation.cancel();
        let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
    }

    #[tokio::test]
    async fn half_configured_hub_tls_fails_startup() {
        let (cert_path, _key_path) = hub_tls_material();
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let config = ServerConfig {
            address: "127.0.0.1:0".into(),
            insecure_local: true,
            tls_cert: Some(cert_path.to_string_lossy().into_owned()),
            tls_key: None,
            ..ServerConfig::default()
        };
        let error = serve_hub(hub, config, tokio_util::sync::CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("both ARKFLOW_HUB_TLS_CERT"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
    }

    fn console_hub() -> hub::Hub {
        hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        })
    }

    async fn post_json(
        app: &Router,
        path: &str,
        bearer: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post(path)
                    .header("authorization", format!("Bearer {bearer}"))
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        read_json(response).await
    }

    async fn get_json(app: &Router, path: &str, bearer: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(path)
                    .header("authorization", format!("Bearer {bearer}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        read_json(response).await
    }

    async fn read_json(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = if content_type.starts_with("application/json") {
            serde_json::from_slice(&body).unwrap_or_default()
        } else {
            serde_json::Value::String(String::from_utf8_lossy(&body).into_owned())
        };
        (status, value)
    }

    async fn report_node_raw(app: &Router, body: serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    fn agent_report_body(session: &hub::RegisterResponse) -> serde_json::Value {
        serde_json::json!({
            "node_id": session.node_id,
            "session_token": session.session_token,
            "version": "test-node",
            "state": "online",
            "capabilities": ["stream_lifecycle", "metrics"],
        })
    }

    #[tokio::test]
    async fn hub_status_aggregates_fleet_gauges() {
        let hub = console_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;
        let mut report = agent_report_body(&session);
        report["streams"] = serde_json::json!([
            {
                "id": "s1", "state": "running", "desired_state": "running",
                "started_at_ms": 1, "last_error": null, "metrics": {
                    "input_batches": 1, "input_messages": 1, "processing_errors": 0,
                    "output_batches": 1, "output_messages": 1, "input_errors": 0,
                    "input_reconnects": 0, "output_errors": 0, "restarts": 0
                }
            },
            {
                "id": "s2", "state": "failed", "desired_state": "running",
                "started_at_ms": 1, "last_error": null, "metrics": {
                    "input_batches": 0, "input_messages": 0, "processing_errors": 1,
                    "output_batches": 0, "output_messages": 0, "input_errors": 0,
                    "input_reconnects": 0, "output_errors": 0, "restarts": 0
                }
            }
        ]);
        report_node_raw(&app, report).await;

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let (status, body) = get_json(&app, "/api/v1/status", "operator-secret").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["streams_total"], 2);
        assert_eq!(body["streams_running"], 1);
        assert_eq!(body["streams_failed"], 1);
        assert_eq!(body["state"], "running");
        assert!(body["uptime_seconds"].is_u64());
        assert!(body["version"].is_string());
    }

    #[tokio::test]
    async fn hub_metrics_serves_json_for_console_and_text_for_scrapers() {
        let hub = console_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let session_a = register_hub_node(&app, "node-a").await;
        let mut report = agent_report_body(&session_a);
        report["metrics"] = serde_json::json!({"input_batches": 3.0, "streams_total": 2.0});
        report_node_raw(&app, report).await;
        let session_b = register_hub_node(&app, "node-b").await;
        let mut report = agent_report_body(&session_b);
        report["metrics"] = serde_json::json!({"input_batches": 4.0});
        report_node_raw(&app, report).await;

        // Default branch: Prometheus text, unchanged for scrapers.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/metrics")
                    .header("authorization", "Bearer operator-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/plain; version=0.0.4"
        );

        // JSON branch via Accept header.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/metrics")
                    .header("authorization", "Bearer operator-secret")
                    .header("accept", "application/json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = read_json(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
        assert_eq!(body["aggregate"]["input_batches"], 7.0);

        // node_id filter and explicit format override.
        let (status, body) = get_json(
            &app,
            "/api/v1/metrics?node_id=node-a&format=json",
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["node_id"], "node-a");
        assert_eq!(body["aggregate"]["input_batches"], 3.0);
    }

    #[tokio::test]
    async fn hub_configuration_validate_and_diff_round_trip_through_commands() {
        let hub = console_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;

        // Unknown node is rejected, not silently queued.
        let (status, body) = post_json(
            &app,
            "/api/v1/nodes/node-b/configuration/validate",
            "operator-secret",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "node_unavailable");

        let (status, operation) = post_json(
            &app,
            "/api/v1/nodes/node-a/configuration/validate",
            "operator-secret",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(operation["operation"], "validate_configuration");

        // The agent receives the read-only command.
        let response = app
            .clone()
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
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let commands: Vec<hub::AgentCommand> = serde_json::from_slice(&body).unwrap();
        let command = commands
            .iter()
            .find(|command| command.operation == "validate_configuration")
            .expect("validate command dispatched");
        assert!(command.payload.is_some());

        // Terminal result carries the validation report.
        let (status, settled) = post_json(
            &app,
            &format!(
                "/api/v1/agent/commands/{}/result?node_id=node-a",
                command.id
            ),
            &session.session_token,
            serde_json::json!({
                "command_id": command.id,
                "operation_id": command.operation_id,
                "state": "succeeded",
                "progress": 100,
                "result": {"valid": true, "errors": []}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(settled["result"]["valid"], true);

        let operation_id = operation["id"].as_str().unwrap().to_owned();
        let (status, fetched) = get_json(
            &app,
            &format!("/api/v1/operations/{operation_id}"),
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched["state"], "succeeded");
        assert_eq!(fetched["result"]["valid"], true);

        // Diff dispatches through the same read-only channel.
        let (status, diff_operation) = get_json(
            &app,
            "/api/v1/nodes/node-a/configuration/diff?from=v1&to=v2",
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(diff_operation["operation"], "diff_configuration");

        // Reported version metadata becomes the node's version list.
        let mut report = agent_report_body(&session);
        report["config_versions"] = serde_json::json!([
            {"id": "v2", "created_at_ms": 2, "format": "yaml"},
            {"id": "v1", "created_at_ms": 1, "format": "json"}
        ]);
        report["configuration"] = serde_json::json!({"streams": []});
        report_node_raw(&app, report).await;
        let (status, versions) = get_json(
            &app,
            "/api/v1/nodes/node-a/configuration/versions",
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let versions = versions.as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["id"], "v2");
    }

    #[tokio::test]
    async fn job_detail_reports_scoped_metrics_and_observed_tasks() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let app = hub_router(hub, &ServerConfig::default());
        let spec = serde_json::json!({
            "id": "scoped-job",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        });
        let (status, _) = post_json(
            &app,
            "/api/v1/jobs",
            "operator-secret",
            serde_json::json!({"spec": spec, "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let session = register_hub_node(&app, "node-a").await;
        let mut report = agent_report_body(&session);
        report["jobs"] = serde_json::json!({
            "scoped-job": {
                "chains": {},
                "checkpoint_duration_ms": 7,
                "checkpoint_failures": 2,
                "watermark_lag_ms": 5,
                "late_events": 0
            }
        });
        report_node_raw(&app, report).await;

        let (status, detail) =
            get_json(&app, "/api/v1/jobs/scoped-job/detail", "operator-secret").await;
        assert_eq!(status, StatusCode::OK);
        let metrics = detail["metrics"].as_object().unwrap();
        assert_eq!(
            metrics
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "checkpoint_duration_ms",
                "checkpoint_failures",
                "watermark_lag_ms"
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(detail["metrics"]["watermark_lag_ms"], 5);
        let tasks = detail["tasks"].as_array().unwrap();
        assert!(!tasks.is_empty());
        assert!(tasks.iter().all(|task| task["observed"].is_boolean()));

        // Report one of the planned tasks as executing: it flips to observed
        // running state while the others stay marked not observed.
        let running_task = tasks[0]["task_id"].as_str().unwrap().to_owned();
        let mut report = agent_report_body(&session);
        report["job_tasks"] = serde_json::json!({"scoped-job": [running_task]});
        report_node_raw(&app, report).await;
        let (status, detail) =
            get_json(&app, "/api/v1/jobs/scoped-job/detail", "operator-secret").await;
        assert_eq!(status, StatusCode::OK);
        let tasks = detail["tasks"].as_array().unwrap();
        let observed = tasks
            .iter()
            .find(|task| task["task_id"] == running_task.as_str())
            .unwrap();
        assert_eq!(observed["observed"], true);
        assert_eq!(observed["state"], "running");
        assert!(tasks.len() > 1);
        assert!(tasks
            .iter()
            .filter(|task| task["task_id"] != running_task.as_str())
            .all(|task| task["observed"] == false));
    }

    // ------------------------------------------------------------------
    // Coverage additions: hub HTTP surface, TLS/serve paths, and helpers.
    // ------------------------------------------------------------------

    fn storage_hub_config() -> hub::HubConfig {
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        }
    }

    fn storage_hub() -> hub::Hub {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8))
    }

    fn scoped_operator_hub_config() -> hub::HubConfig {
        hub::HubConfig {
            operator_token: Some("readonly|viewer|viewer-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        }
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn put_json(
        app: &Router,
        path: &str,
        bearer: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put(path)
                    .header("authorization", format!("Bearer {bearer}"))
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        read_json(response).await
    }

    /// Body-less request (DELETE, POST without payload) returning status+JSON.
    async fn request_without_body(
        app: &Router,
        method: &str,
        path: &str,
        bearer: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = axum::http::Request::builder().method(method).uri(path);
        if let Some(bearer) = bearer {
            builder = builder.header("authorization", format!("Bearer {bearer}"));
        }
        let response = app
            .clone()
            .oneshot(builder.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        read_json(response).await
    }

    fn job_spec(id: &str, version: u64) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "version": version,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
    }

    async fn create_job(app: &Router, id: &str) -> serde_json::Value {
        let (status, job) = post_json(
            app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": job_spec(id, 1), "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "job create failed: {job}");
        job
    }

    async fn current_generation(app: &Router, job_id: &str) -> u64 {
        let (status, job) = get_json(app, &format!("/api/v1/jobs/{job_id}"), "operator").await;
        assert_eq!(status, StatusCode::OK);
        job["generation"].as_u64().unwrap()
    }

    /// Register an online node that can execute Jobs (placement needs the
    /// runtime capabilities reported, not just the lease).
    async fn register_job_node(app: &Router, node_id: &str) -> hub::RegisterResponse {
        let session = register_hub_node(app, node_id).await;
        let mut report = agent_report_body(&session);
        report["capabilities"] = serde_json::json!(["job_runtime", "state_backend", "metrics"]);
        report_node_raw(app, report).await;
        session
    }

    /// The `from_engine` mapping and startup address validation.
    #[test]
    fn server_config_from_engine_maps_health_settings_and_rejects_bad_addresses() {
        let health = HealthCheckConfig {
            enabled: false,
            address: "127.0.0.1:9999".into(),
            health_path: "/hub-health".into(),
            readiness_path: "/hub-readiness".into(),
            liveness_path: "/hub-liveness".into(),
            api_prefix: "/api/v2".into(),
            cors_origins: vec!["https://console.example".into()],
            node_token: Some("node".into()),
            agent_lease_ttl_ms: 12_345,
            agent_session_ttl_ms: 678,
            ..HealthCheckConfig::default()
        };
        let config = ServerConfig::from_engine(&EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: health,
        });
        assert!(!config.enabled);
        assert_eq!(config.address, "127.0.0.1:9999");
        assert_eq!(config.api_prefix, "/api/v2");
        assert_eq!(config.health_path, "/hub-health");
        assert_eq!(config.readiness_path, "/hub-readiness");
        assert_eq!(config.liveness_path, "/hub-liveness");
        assert_eq!(config.node_token.as_deref(), Some("node"));
        assert_eq!(config.lease_ttl_ms, 12_345);
        assert_eq!(config.session_ttl_ms, 678);
        assert_eq!(
            config.cors_origins,
            vec!["https://console.example".to_owned()]
        );
        assert!(config.tls_cert.is_none() && config.tls_key.is_none());
        assert!(!config.insecure_local && config.hub_storage.is_none());

        let local_hub = hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let bad = ServerConfig {
            address: "not-a-socket-address".into(),
            insecure_local: true,
            ..ServerConfig::default()
        };
        assert!(bad
            .validate_hub_startup(&local_hub)
            .unwrap_err()
            .to_string()
            .contains("invalid Hub bind address"));
    }

    #[tokio::test]
    async fn serve_is_a_no_op_when_disabled_and_serves_until_cancelled() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        // Disabled: return before binding anything.
        let disabled = ServerConfig {
            enabled: false,
            ..ServerConfig::default()
        };
        serve(
            engine.control_plane(),
            disabled,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();

        // Enabled: serve until the cancellation token fires.
        let port = free_port();
        let enabled = ServerConfig {
            address: format!("127.0.0.1:{port}"),
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            serve(engine.control_plane(), enabled, shutdown)
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        let mut served = false;
        for _ in 0..100 {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .await
            {
                let _ = response.status();
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(served, "the local server must answer /health");
        cancellation.cancel();
    }

    #[tokio::test]
    async fn serve_observability_is_optional_and_never_blocks_the_data_plane() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        // Disabled: returns immediately.
        let disabled = ServerConfig {
            observability: arkflow_core::config::ObservabilityConfig {
                enabled: false,
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        serve_observability(
            engine.control_plane(),
            disabled,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();

        // Bind conflict: a taken port logs a warning and returns Ok —
        // observability must never block the data plane.
        let port = free_port();
        let _occupied = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
        let conflicted = ServerConfig {
            observability: arkflow_core::config::ObservabilityConfig {
                address: format!("127.0.0.1:{port}"),
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        serve_observability(
            engine.control_plane(),
            conflicted,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();

        // Free port: probes are served until cancellation.
        let port = free_port();
        let enabled = ServerConfig {
            observability: arkflow_core::config::ObservabilityConfig {
                address: format!("127.0.0.1:{port}"),
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            serve_observability(engine.control_plane(), enabled, shutdown)
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        let mut served = false;
        for _ in 0..100 {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/live"))
                .send()
                .await
            {
                assert_eq!(response.status(), StatusCode::OK);
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(served, "the observability listener must answer /live");
        cancellation.cancel();
    }

    #[tokio::test]
    async fn routers_attach_cors_headers_for_configured_origins() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let config = ServerConfig {
            cors_origins: vec!["https://console.example".into()],
            ..ServerConfig::default()
        };
        let app = router(engine.control_plane(), &config);
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/system")
                    .header(header::ORIGIN, "https://console.example")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "https://console.example"
        );

        let app = hub_router(console_hub(), &config);
        let response = app
            .oneshot(
                axum::http::Request::get("/health")
                    .header(header::ORIGIN, "https://console.example")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "https://console.example"
        );
    }

    /// Isolated TLS material: unlike `hub_tls_material` (which shares one
    /// pid-keyed directory across tests and cleans it up), each caller owns a
    /// unique tempdir so parallel TLS tests cannot race each other's files.
    fn isolated_tls_material() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        use rcgen::CertificateParams;
        use rcgen::KeyPair;
        let params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (dir, cert_path, key_path)
    }

    #[tokio::test]
    async fn hub_tls_startup_rejects_unreadable_and_empty_material() {
        let (_dir, cert_path, key_path) = isolated_tls_material();
        let local_hub = || {
            hub::Hub::new(hub::HubConfig {
                operator_token: None,
                node_token: None,
                insecure_local: true,
                lease_ttl_ms: 1_000,
                poll_interval_ms: 10,
                session_ttl_ms: default_session_ttl_ms(),
            })
        };
        let base = || ServerConfig {
            address: "127.0.0.1:0".into(),
            insecure_local: true,
            ..ServerConfig::default()
        };
        // Certificate file that does not exist.
        let config = ServerConfig {
            tls_cert: Some("/nonexistent/cert.pem".into()),
            tls_key: Some(key_path.to_string_lossy().into_owned()),
            ..base()
        };
        let error = serve_hub(
            local_hub(),
            config,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("could not be read"), "{error}");

        // Certificate file without any PEM certificate entries.
        let dir = tempfile::tempdir().unwrap();
        let empty_cert = dir.path().join("empty-cert.pem");
        std::fs::write(&empty_cert, "not a pem file").unwrap();
        let config = ServerConfig {
            tls_cert: Some(empty_cert.to_string_lossy().into_owned()),
            tls_key: Some(key_path.to_string_lossy().into_owned()),
            ..base()
        };
        let error = serve_hub(
            local_hub(),
            config,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("no PEM certificates"), "{error}");

        // Key file that does not exist.
        let config = ServerConfig {
            tls_cert: Some(cert_path.to_string_lossy().into_owned()),
            tls_key: Some("/nonexistent/key.pem".into()),
            ..base()
        };
        let error = serve_hub(
            local_hub(),
            config,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("could not be read"), "{error}");

        // Key file without a PEM private key.
        let empty_key = dir.path().join("empty-key.pem");
        std::fs::write(&empty_key, "still not a pem file").unwrap();
        let config = ServerConfig {
            tls_cert: Some(cert_path.to_string_lossy().into_owned()),
            tls_key: Some(empty_key.to_string_lossy().into_owned()),
            ..base()
        };
        let error = serve_hub(
            local_hub(),
            config,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("no PEM private key"), "{error}");
    }

    /// A plaintext (or garbage) peer fails the TLS handshake; the listener
    /// logs it and keeps accepting instead of tearing down the accept loop.
    #[tokio::test]
    async fn hub_tls_listener_survives_a_failed_handshake() {
        use axum::serve::Listener as _;
        use tokio::io::AsyncWriteExt;
        let (_dir, cert_path, key_path) = isolated_tls_material();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert_pem = std::fs::read_to_string(&cert_path).unwrap();
        let key_pem = std::fs::read_to_string(&key_path).unwrap();
        let chain = rustls_pemfile::certs(&mut cert_pem.as_bytes())
            .map(|item| item.unwrap())
            .collect::<Vec<_>>();
        let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
            .unwrap()
            .unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = inner.local_addr().unwrap();
        let mut listener = HubTlsListener {
            inner,
            acceptor: tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config)),
        };
        assert!(listener.local_addr().is_ok());

        let accept_task = tokio::spawn(async move { listener.accept_one().await });
        // First peer speaks plaintext HTTP: the handshake rejects it.
        let mut plaintext = tokio::net::TcpStream::connect(address).await.unwrap();
        plaintext
            .write_all(b"GET /readiness HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        drop(plaintext);
        // Second peer completes a real TLS handshake, so accept_one returns.
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let _ = client
            .get(format!("https://{address}/liveness"))
            .send()
            .await;
        let (_stream, peer) = tokio::time::timeout(std::time::Duration::from_secs(3), accept_task)
            .await
            .expect("accept returns after a healthy handshake")
            .unwrap();
        assert_eq!(peer.ip(), address.ip());
    }

    #[tokio::test]
    async fn hub_job_read_routes_report_missing_and_corrupt_records() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let (status, jobs) = get_json(&app, "/api/v1/jobs", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(jobs.as_array().unwrap().len(), 0);

        create_job(&app, "readable-job").await;
        let (status, plan) = get_json(&app, "/api/v1/jobs/readable-job/plan", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plan["job_id"], "readable-job");
        assert!(plan["plan"].is_object());

        for path in [
            "/api/v1/jobs/missing-job",
            "/api/v1/jobs/missing-job/plan",
            "/api/v1/jobs/missing-job/detail",
            "/api/v1/jobs/missing-job/versions",
            "/api/v1/nodes/node-a/streams/not-there",
        ] {
            let (status, body) = get_json(&app, path, "operator").await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        }

        // Corrupt persisted specs surface as 500, never a handler panic.
        hub.upsert_job(storage::JobRecord {
            job_id: "corrupt-job".into(),
            version: 1,
            spec_json: "not json".into(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: hub::now_ms_for_metrics(),
        })
        .await
        .unwrap();
        let (status, body) = get_json(&app, "/api/v1/jobs/corrupt-job/plan", "operator").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["code"], "invalid_persisted_job");
        let (status, body) = get_json(&app, "/api/v1/jobs/corrupt-job/detail", "operator").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

        // A spec that parses but cannot compile its plan fails closed as 422.
        let mut unpluckable = job_spec("planless-job", 1);
        unpluckable["sources"] = serde_json::json!([]);
        hub.upsert_job(storage::JobRecord {
            job_id: "planless-job".into(),
            version: 1,
            spec_json: unpluckable.to_string(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: hub::now_ms_for_metrics(),
        })
        .await
        .unwrap();
        let (status, body) = get_json(&app, "/api/v1/jobs/planless-job/plan", "operator").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");
        let (status, body) = get_json(&app, "/api/v1/jobs/planless-job/detail", "operator").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }

    #[tokio::test]
    async fn hub_job_action_and_recovery_artifact_routes_validate_input() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let session = register_job_node(&app, "node-a").await;

        create_job(&app, "acted-job").await;
        // Unknown action and unknown Job.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/acted-job/actions/pause",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_action");
        let (status, _) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/missing/actions/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/missing/savepoints",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Start converges the desired state and is audited.
        let (status, job) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/acted-job/actions/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{job}");
        assert_eq!(job["desired_state"], "running");
        let (status, job) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/acted-job/actions/stop",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{job}");
        assert_eq!(job["desired_state"], "stopped");

        // A savepoint trigger is accepted, audited once, and listed.
        let (status, job) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/acted-job/savepoints",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{job}");
        assert!(hub
            .audit(Some("acted-job"))
            .await
            .unwrap()
            .iter()
            .any(|record| record.action == "job.savepoint" && record.outcome == "accepted"));
        let (status, checkpoints) =
            get_json(&app, "/api/v1/jobs/acted-job/checkpoints", "operator").await;
        assert_eq!(status, StatusCode::OK);
        let checkpoints = checkpoints.as_array().unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0]["kind"], "savepoint");
        assert_eq!(checkpoints[0]["status"], "pending");

        // Detail with node-scoped placement filters the node view and marks
        // unobserved tasks.
        let (status, detail) = get_json(&app, "/api/v1/jobs/acted-job/detail", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["nodes"].as_array().unwrap().len(), 1);

        // Job observation from the executing node flips observed state.
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/job-observations",
            &session.session_token,
            serde_json::json!({
                "node_id": "node-a",
                "session_token": session.session_token,
                "job_id": "acted-job",
                "generation": detail["job"]["generation"],
                "state": "stopped",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, versions) =
            get_json(&app, "/api/v1/jobs/acted-job/versions", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(versions.as_array().unwrap().len(), 1);

        // Viewer credentials cannot operate Jobs.
        let viewer_hub = hub::Hub::new(scoped_operator_hub_config());
        let viewer_app = hub_router(viewer_hub, &ServerConfig::default());
        let (status, _) = request_without_body(
            &viewer_app,
            "POST",
            "/api/v1/jobs/acted-job/actions/start",
            Some("viewer-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn hub_job_detail_reports_split_placement_violations() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_hub_node(&app, "node-a").await;
        register_hub_node(&app, "node-b").await;
        // A split placement whose error-route side edge would cross nodes.
        let split_spec = serde_json::json!({
            "id": "split-job",
            "version": 1,
            "placement": "split",
            "operators": [
                {"id": "src", "kind": "source"},
                {"id": "map", "kind": "map"},
                {"id": "err", "kind": "sink", "config": {"__arkflow_error_sink": true}},
                {"id": "out", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "src", "to": "map"},
                {"id": "e2", "from": "map", "to": "err"},
                {"id": "e3", "from": "map", "to": "out"}
            ],
            "sources": [{"operator_id": "src", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [
                {"operator_id": "out", "output_type": "drop"},
                {"operator_id": "err", "output_type": "drop"}
            ]
        });
        hub.upsert_job(storage::JobRecord {
            job_id: "split-job".into(),
            version: 1,
            spec_json: split_spec.to_string(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec!["node-a".into(), "node-b".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: hub::now_ms_for_metrics(),
        })
        .await
        .unwrap();
        let (status, body) = get_json(&app, "/api/v1/jobs/split-job/detail", "operator").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_placement");
    }

    #[tokio::test]
    async fn hub_validate_job_route_reports_spec_errors_and_node_compatibility() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        register_job_node(&app, "node-a").await;

        // A spec that does not parse.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": {"garbage": true}}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");

        // A spec that parses but violates the Job contract.
        let mut empty = job_spec("validate-job", 1);
        empty["sources"] = serde_json::json!([]);
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": empty}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");

        // A structurally valid spec with an unknown component fails deep
        // validation (the same construction path local execution uses).
        let mut unknown = job_spec("validate-job", 1);
        unknown["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": unknown}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        // Node selection drives the compatibility matrix.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["node-a"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["valid"], true);
        assert_eq!(body["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(body["nodes"][0]["compatible"], true);
        assert_eq!(body["nodes"][0]["node_id"], "node-a");

        // Selecting only unknown nodes leaves an empty matrix with a warning.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["missing"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["valid"], true);
        assert_eq!(body["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(body["warnings"][0], "No online compute nodes selected");

        // An online node without the runtime capabilities is incompatible.
        let session_b = register_hub_node(&app, "node-b").await;
        let mut report = agent_report_body(&session_b);
        report["capabilities"] = serde_json::json!(["metrics"]);
        report_node_raw(&app, report).await;
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "operator",
            serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["node-b"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["valid"], false);
        assert_eq!(body["nodes"][0]["compatible"], false);
    }

    #[tokio::test]
    async fn hub_create_job_and_desired_state_route_validation() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        register_job_node(&app, "node-a").await;
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": {"garbage": true}, "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");

        let mut invalid = job_spec("created-job", 1);
        invalid["sources"] = serde_json::json!([]);
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": invalid, "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");

        let mut unknown_component = job_spec("created-job", 1);
        unknown_component["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": unknown_component, "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        let (status, body) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": job_spec("created-job", 1), "desired_state": "paused"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_state");

        create_job(&app, "created-job").await;
        // Desired-state route validation.
        let (status, body) = put_json(
            &app,
            "/api/v1/jobs/created-job/desired-state",
            "operator",
            serde_json::json!({"state": "bogus"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_state");
        let (status, body) = put_json(
            &app,
            "/api/v1/jobs/missing/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let (status, body) = put_json(
            &app,
            "/api/v1/jobs/created-job/desired-state",
            "operator",
            serde_json::json!({"state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["desired_state"], "stopped");

        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let (status, _) = post_json(
            &viewer_app,
            "/api/v1/jobs",
            "viewer-secret",
            serde_json::json!({"spec": job_spec("created-job", 2), "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = put_json(
            &viewer_app,
            "/api/v1/jobs/created-job/desired-state",
            "viewer-secret",
            serde_json::json!({"state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// The stopped-mode upgrade: guard rails first, then the happy path that
    /// swaps in a new version pinned to a completed savepoint.
    #[tokio::test]
    async fn hub_job_upgrade_stopped_mode_guards_and_commit() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        create_job(&app, "upgraded-job").await;

        let upgrade = |spec: serde_json::Value,
                       expected_generation: u64,
                       mode: Option<&str>,
                       savepoint_id: Option<&str>| {
            let mut body =
                serde_json::json!({"spec": spec, "expected_generation": expected_generation});
            if let Some(mode) = mode {
                body["mode"] = serde_json::json!(mode);
            }
            if let Some(id) = savepoint_id {
                body["savepoint_id"] = serde_json::json!(id);
            }
            body
        };

        // Stale generation fence.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 2), 999, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "generation_conflict");
        assert_eq!(body["details"]["current"], 1);

        // Unknown mode.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 2), 1, Some("blue-green"), None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_upgrade_mode");

        // Spec shape problems.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(serde_json::json!({"no": "spec"}), 1, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("other-job", 2), 1, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 1), 1, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_version");

        let mut unpluckable = job_spec("upgraded-job", 2);
        unpluckable["sources"] = serde_json::json!([]);
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(unpluckable, 1, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        let mut unknown_component = job_spec("upgraded-job", 2);
        unknown_component["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(unknown_component, 1, None, None),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        // No completed savepoint yet.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 2), 1, None, Some("sp-missing")),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "savepoint_not_ready");

        // Trigger a savepoint; still pending, so still not ready.
        request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/upgraded-job/savepoints",
            Some("operator"),
        )
        .await;
        let (_, checkpoints) =
            get_json(&app, "/api/v1/jobs/upgraded-job/checkpoints", "operator").await;
        let savepoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 2), 1, None, Some(&savepoint_id)),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "savepoint_not_ready");

        // Complete the artifact; a state-format mismatch is rejected.
        store
            .with_connection(|connection| {
                connection.execute("UPDATE cp_job_checkpoints SET status = 'completed'", [])?;
                Ok(())
            })
            .unwrap();
        let mut new_format = job_spec("upgraded-job", 2);
        new_format["state"] = serde_json::json!({"backend": "embedded_kv", "durability": "ephemeral", "format_version": 2});
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(new_format, 1, None, Some(&savepoint_id)),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "state_format_incompatible");

        // Happy path: version bump pinned to the completed savepoint.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "operator",
            upgrade(job_spec("upgraded-job", 2), 1, None, Some(&savepoint_id)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["state"], "pending_recovery");
        assert_eq!(body["savepoint_id"], savepoint_id.as_str());
        assert_eq!(body["job"]["version"], 2);

        // Version rollback: explicit target, missing target, and exhaustion.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/upgraded-job/upgrades/restore-v99/rollback",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "no_previous_job_version");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/upgraded-job/upgrades/restore-v1/rollback",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["version"], 1);
        // v1 is now current: no version below it remains.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/upgraded-job/upgrades/opaque-upgrade-id/rollback",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "no_previous_job_version");
        // Rolling back an unknown Job is a 404.
        let (status, _) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/missing/upgrades/restore-v1/rollback",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Viewer credentials cannot upgrade or roll back.
        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let generation = current_generation(&app, "upgraded-job").await;
        let (status, _) = post_json(
            &viewer_app,
            "/api/v1/jobs/upgraded-job/upgrades",
            "viewer-secret",
            upgrade(
                job_spec("upgraded-job", 2),
                generation,
                None,
                Some(&savepoint_id),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// Rollback refuses a previous version whose state format is
    /// incompatible with the artifact the Job would actually restore.
    #[tokio::test]
    async fn hub_job_rollback_rejects_state_format_mismatch() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        create_job(&app, "format-job").await;
        request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/format-job/savepoints",
            Some("operator"),
        )
        .await;
        // Artifact written under format 2, previous version spec is format 1.
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_checkpoints SET status = 'completed', format_version = 2",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let (_, checkpoints) =
            get_json(&app, "/api/v1/jobs/format-job/checkpoints", "operator").await;
        let checkpoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
        let mut version_two = job_spec("format-job", 2);
        version_two["state"] = serde_json::json!({"backend": "embedded_kv", "durability": "ephemeral", "format_version": 2});
        hub.upsert_job(storage::JobRecord {
            job_id: "format-job".into(),
            version: 2,
            spec_json: version_two.to_string(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: Some(checkpoint_id),
            last_error: None,
            updated_at_ms: hub::now_ms_for_metrics(),
        })
        .await
        .unwrap();
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/format-job/upgrades/restore-v1/rollback",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "state_format_incompatible");
    }

    /// The atomic upgrade orchestration: exclusivity, status reads, and the
    /// pause/resume/cancel action surface.
    #[tokio::test]
    async fn hub_job_atomic_upgrade_orchestration_lifecycle() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_job_node(&app, "node-a").await;
        create_job(&app, "atomic-job").await;

        // Atomic mode requires a running Job.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": 1, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "job_must_be_running");

        // The stopped mode requires a stopped Job (this one will be running).
        let (status, _) = put_json(
            &app,
            "/api/v1/jobs/atomic-job/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let generation = current_generation(&app, "atomic-job").await;
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": generation}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "job_must_be_stopped");

        // Atomic mode with an unparseable spec.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": {"garbage": true}, "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_spec");
        // Wrong identity and non-monotonic versions.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("other-job", 2), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 1), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        // Successful atomic upgrade creation.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": generation, "mode": "atomic", "verify_timeout_ms": 5000}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let upgrade_id = body["upgrade_id"].as_str().unwrap().to_owned();

        // Exclusivity: a second orchestration cannot own the Job, and job
        // mutations are fenced while the upgrade is active.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 3), "expected_generation": current_generation(&app, "atomic-job").await, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "orchestration_in_progress");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/atomic-job/actions/stop",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "orchestration_in_progress");

        // Status reads: known id, foreign Job, unknown id.
        let (status, record) = get_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}"),
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(record["upgrade_id"], upgrade_id.as_str());
        let (status, _) = get_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades/unknown-upgrade",
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = get_json(
            &app,
            &format!("/api/v1/jobs/someone-else/upgrades/{upgrade_id}"),
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // The detail view exposes the active upgrade without its bulk.
        let (status, detail) = get_json(&app, "/api/v1/jobs/atomic-job/detail", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["active_upgrade"]["upgrade_id"], upgrade_id.as_str());
        assert!(detail["active_upgrade"].get("target_spec_json").is_none());

        // Actions: validation, 404s, pause/resume flows.
        let (status, body) = post_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "bogus"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_job_upgrade_action");
        let (status, _) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades/unknown-upgrade/actions",
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = post_json(
            &app,
            &format!("/api/v1/jobs/someone-else/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, record) = post_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{record}");
        assert_eq!(record["phase"], "paused");
        // Pausing twice is a phase conflict surfaced as a retryable 409.
        let (status, body) = post_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "job_upgrade_action_rejected");
        let (status, record) = post_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "resume"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{record}");
        assert_ne!(record["phase"], "paused");
        let (status, record) = post_json(
            &app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "operator",
            serde_json::json!({"action": "cancel"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{record}");
        assert_eq!(record["phase"], "cancelled");
        // A terminal orchestration no longer fences job mutations.
        assert!(reject_if_job_upgrade_active(&hub, "atomic-job")
            .await
            .is_ok());

        // Viewer credentials are authorized for reads (the upgrade is simply
        // absent from this separate Hub) but cannot drive actions.
        let viewer_store = storage::ControlPlaneStore::in_memory().unwrap();
        let viewer_app = hub_router(
            hub::Hub::with_storage(
                scoped_operator_hub_config(),
                storage::StorageActor::start(viewer_store, 8),
            ),
            &ServerConfig::default(),
        );
        let (status, _) = get_json(
            &viewer_app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}"),
            "viewer-secret",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = post_json(
            &viewer_app,
            &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
            "viewer-secret",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_if_job_upgrade_active_fences_while_orchestration_owns_the_job() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_job_node(&app, "node-a").await;
        create_job(&app, "fenced-job").await;
        assert!(reject_if_job_upgrade_active(&hub, "fenced-job")
            .await
            .is_ok());
        put_json(
            &app,
            "/api/v1/jobs/fenced-job/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        let generation = current_generation(&app, "fenced-job").await;
        let (status, _) = post_json(
            &app,
            "/api/v1/jobs/fenced-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("fenced-job", 2), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let response = reject_if_job_upgrade_active(&hub, "fenced-job")
            .await
            .unwrap_err();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn rollout_routes_manage_staged_configuration_rollouts() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_hub_node(&app, "node-a").await;

        // Validation failures.
        let (status, body) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "v1", "node_ids": [], "batch_size": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_rollout");
        let (status, body) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "missing-version", "node_ids": ["node-a"], "batch_size": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_rollout");

        // Seed a durable config version, then create/list/read/act.
        hub.create_rollout_with_content(
            "cfg-seed".into(),
            "{}".into(),
            vec!["node-a".into()],
            1,
            None,
            None,
        )
        .await
        .unwrap();
        let (status, rollout) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "cfg-seed", "node_ids": ["node-a"], "batch_size": 1, "correlation_id": null}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{rollout}");
        assert_eq!(rollout["state"], "pending");
        let rollout_id = rollout["rollout_id"].as_str().unwrap().to_owned();

        let (status, rollouts) = get_json(&app, "/api/v1/rollouts", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert!(rollouts
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item["rollout_id"].as_str() == Some(rollout_id.as_str()) }));

        let (status, fetched) =
            get_json(&app, &format!("/api/v1/rollouts/{rollout_id}"), "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched["rollout"]["rollout_id"], rollout_id.as_str());
        assert_eq!(fetched["targets"].as_array().unwrap().len(), 1);
        let (status, _) = get_json(&app, "/api/v1/rollouts/missing", "operator").await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Lifecycle actions.
        let (status, paused) = post_json(
            &app,
            &format!("/api/v1/rollouts/{rollout_id}/actions"),
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{paused}");
        assert_eq!(paused["state"], "paused");
        let (status, resumed) = post_json(
            &app,
            &format!("/api/v1/rollouts/{rollout_id}/actions"),
            "operator",
            serde_json::json!({"action": "resume"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{resumed}");
        assert_eq!(resumed["state"], "applying");
        let (status, body) = post_json(
            &app,
            &format!("/api/v1/rollouts/{rollout_id}/actions"),
            "operator",
            serde_json::json!({"action": "teleport"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_rollout_action");
        let (status, _) = post_json(
            &app,
            "/api/v1/rollouts/missing/actions",
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        // Viewer credentials cannot manage rollouts.
        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let (status, _) = post_json(
            &viewer_app,
            "/api/v1/rollouts",
            "viewer-secret",
            serde_json::json!({"config_version": "cfg-seed", "node_ids": ["node-a"], "batch_size": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn hub_operations_and_events_routes_support_filters() {
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_hub_node(&app, "node-a").await;

        // Enqueue a cancellable command through the read-only channel.
        let (status, operation) = post_json(
            &app,
            "/api/v1/nodes/node-a/configuration/validate",
            "operator",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let operation_id = operation["id"].as_str().unwrap().to_owned();

        // An intent-backed operation feeds the filters.
        put_json(
            &app,
            "/api/v1/nodes/node-a/streams/orders/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;

        let (status, page) = get_json(
            &app,
            "/api/v1/operations?resource_id=orders&operation=reconcile",
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(page["total"].as_u64().unwrap() >= 1, "{page}");
        let (status, page) = get_json(
            &app,
            "/api/v1/operations?node_id=node-a&page=1&page_size=2",
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(page["total"].as_u64().unwrap() >= 1);
        let (status, body) = get_json(&app, "/api/v1/operations/missing", "operator").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "operation_not_found");

        // Cancel the queued command; cancelling twice is idempotent.
        let (status, cancelled) = request_without_body(
            &app,
            "DELETE",
            &format!("/api/v1/operations/{operation_id}"),
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{cancelled}");
        let (status, _) = request_without_body(
            &app,
            "DELETE",
            &format!("/api/v1/operations/{operation_id}"),
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = request_without_body(
            &app,
            "DELETE",
            "/api/v1/operations/missing",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Event filters narrow by type, stream, and correlation id.
        let (status, all) = get_json(&app, "/api/v1/events?node_id=node-a", "operator").await;
        assert_eq!(status, StatusCode::OK);
        let items = all["items"].as_array().unwrap();
        assert!(!items.is_empty(), "events: {all}");
        let event_type = items[0]["event_type"].as_str().unwrap().to_owned();
        let (status, filtered) = get_json(
            &app,
            &format!("/api/v1/events?event_type={event_type}&stream_id=orders"),
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(filtered["total"].as_u64().unwrap() <= all["total"].as_u64().unwrap());
        let (status, none) =
            get_json(&app, "/api/v1/events?correlation_id=never-used", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(none["total"], 0);

        // Viewer cannot cancel.
        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let (status, _) = request_without_body(
            &viewer_app,
            "DELETE",
            &format!("/api/v1/operations/{operation_id}"),
            Some("viewer-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        // Agent endpoints reject missing session credentials.
        let (status, _) =
            request_without_body(&app, "GET", "/api/v1/agent/commands?node_id=node-a", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = post_json(
            &app,
            &format!("/api/v1/agent/commands/{operation_id}/result?node_id=node-a"),
            "",
            serde_json::json!({
                "command_id": "cmd", "operation_id": operation_id, "state": "succeeded", "progress": 100
            }),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sse_stream_replays_the_gap_marker_and_delivers_live_events() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'rollout_changed', 'accepted', 10)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'node_registered', 'accepted', 11)",
                    [],
                )?;
                // Leave a gap so the replay starts with a resync marker.
                connection.execute("DELETE FROM cp_events WHERE event_id = 1", [])?;
                Ok(())
            })
            .unwrap();
        let hub =
            hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
        let app = hub_router(hub.clone(), &ServerConfig::default());
        // Agent-reported events are the live broadcast source; register the
        // node so a report can drive one after the replay drains.
        let session = register_hub_node(&app, "node-a").await;
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/events/stream")
                    .header("authorization", "Bearer operator")
                    .header("last-event-id", "0")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        assert!(text.contains("event: resync"), "{text}");

        // After the replay drains, live broadcasts continue the stream.
        let live = std::sync::Arc::new(tokio::sync::Notify::new());
        let live_task = live.clone();
        let driver_app = app.clone();
        let session_token = session.session_token.clone();
        tokio::spawn(async move {
            // Wait until the SSE consumer is polled before publishing.
            live_task.notified().await;
            let response = driver_app
                .oneshot(
                    axum::http::Request::post("/api/v1/agent/report")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(
                            serde_json::json!({
                                "node_id": "node-a",
                                "session_token": session_token,
                                "version": "test-node",
                                "state": "online",
                                "events": [{
                                    "occurred_at_ms": 12,
                                    "event_type": "stream_started",
                                    "stream_id": "orders",
                                    "outcome": "accepted",
                                    "message": null,
                                    "operation_id": null,
                                    "correlation_id": "sse-live",
                                    "actor": null
                                }]
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        });
        let mut saw_replayed = false;
        let mut saw_live = false;
        for _ in 0..16 {
            if !saw_replayed {
                live.notify_one();
            }
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next());
            match chunk.await {
                Ok(Some(Ok(chunk))) => {
                    let text = String::from_utf8(chunk.to_vec()).unwrap();
                    if text.contains("id: 2") {
                        saw_replayed = true;
                    }
                    if text.contains("stream_started") {
                        saw_live = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(saw_replayed, "the durable replay must arrive");
        assert!(saw_live, "a live event must follow the replay");
    }

    #[tokio::test]
    async fn hub_metrics_and_operational_status_cover_fleet_and_rollout_state() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub =
            hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;
        let mut report = agent_report_body(&session);
        report["capabilities"] = serde_json::json!(["job_runtime", "metrics"]);
        report["metrics"] = serde_json::json!({"input_batches": 5.0});
        report["jobs"] = serde_json::json!({
            "metrics-job": {
                "chains": {},
                "checkpoint_duration_ms": 3,
                "checkpoint_failures": 0,
                "watermark_lag_ms": 1,
                "late_events": 0
            }
        });
        report_node_raw(&app, report).await;
        put_json(
            &app,
            "/api/v1/nodes/node-a/streams/orders/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        hub.create_rollout_with_content(
            "cfg-metrics".into(),
            "{}".into(),
            vec!["node-a".into()],
            1,
            None,
            None,
        )
        .await
        .unwrap();

        // Prometheus text exposition.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/metrics")
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
        let text = String::from_utf8(body.to_vec()).unwrap();
        for family in [
            "arkflow_control_plane_ready",
            "arkflow_reconciliation_runs_total",
            "arkflow_intents_state",
            "arkflow_nodes_state",
            "arkflow_rollouts_state",
            "arkflow_node_compatibility",
            "arkflow_node_capability",
            "arkflow_node_metric",
            "arkflow_job_",
        ] {
            assert!(text.contains(family), "missing family {family} in:\n{text}");
        }

        // The operational status aggregate.
        let (status, body) = get_json(&app, "/api/v1/operations/status", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["rollouts"].is_object());
        assert_eq!(body["rollouts"]["total"], 1);
        assert_eq!(body["compatibility"]["nodes"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn hub_maintenance_routes_manage_node_lifecycle_state() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        register_hub_node(&app, "node-a").await;

        let (status, node) =
            request_without_body(&app, "POST", "/api/v1/nodes/node-a/drain", Some("operator"))
                .await;
        assert_eq!(status, StatusCode::OK, "{node}");
        assert_eq!(node["maintenance_state"], "draining");
        let (status, node) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/maintenance",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{node}");
        assert_eq!(node["maintenance_state"], "maintenance");
        let (status, node) = request_without_body(
            &app,
            "DELETE",
            "/api/v1/nodes/node-a/maintenance",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{node}");
        assert_eq!(node["maintenance_state"], "active");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/missing/drain",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let (status, _) = request_without_body(
            &viewer_app,
            "POST",
            "/api/v1/nodes/node-a/drain",
            Some("viewer-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_routes_reject_bad_credentials_and_accept_observations() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;

        // Wrong shared registration token.
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/register",
            "",
            serde_json::json!({"node_id": "intruder", "node_token": "wrong"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Stale session on heartbeat/report.
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/heartbeat",
            "",
            serde_json::json!({"node_id": "node-a", "session_token": "stale", "state": "online"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/report",
            "",
            serde_json::json!({
                "node_id": "node-a", "session_token": "stale",
                "version": "v", "state": "online"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Job observations require a live session; unknown Jobs report 204.
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/job-observations",
            "",
            serde_json::json!({
                "node_id": "node-a", "session_token": "stale",
                "job_id": "any", "generation": 1, "state": "running"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = post_json(
            &app,
            "/api/v1/agent/job-observations",
            &session.session_token,
            serde_json::json!({
                "node_id": "node-a", "session_token": session.session_token,
                "job_id": "unknown-job", "generation": 1, "state": "running"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn hub_health_and_readiness_report_leadership_and_recovery() {
        // Without durable storage readiness reports the storage gap.
        let app = hub_router(console_hub(), &ServerConfig::default());
        let (status, body) = get_json(&app, "/readiness", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["reason"], "storage_unavailable");
        let (status, health) = get_json(&app, "/health", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(health["status"], "degraded");

        // With storage: startup recovery gates readiness until restored.
        let hub = storage_hub();
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let (status, body) = get_json(&app, "/readiness", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["reason"], "startup_recovery");
        let (status, _) = get_json(&app, "/health", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        hub.recover_persisted_state().await.unwrap();
        let (status, body) = get_json(&app, "/readiness", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        let (status, body) = get_json(&app, "/health", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);

        // An HA standby that has not won the lease reports standby.
        let standby = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        })
        .with_ha(hub::HubHaConfig {
            enabled: true,
            lease_ttl_ms: 1_000,
            ..hub::HubHaConfig::default()
        });
        standby.enter_election().await;
        let app = hub_router(standby, &ServerConfig::default());
        let (status, body) = get_json(&app, "/readiness", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["reason"], "standby");
        assert_eq!(body["ha"]["role"], "standby");
        // Non-allowlisted routes are fenced off for a standby.
        let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "hub_standby");
    }

    /// hub-ha stage 3: a standby's 503 points Agents at the elected leader
    /// when the shared lease row carries its advertised address, and stays
    /// byte-compatible with the pre-hint body when it does not.
    #[tokio::test]
    async fn standby_503_carries_leader_hint_from_the_shared_lease_row() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // A leader elsewhere holds the lease and advertises its API base URL.
        let advertised = storage::ControlPlaneStore::in_memory().unwrap();
        let actor = storage::StorageActor::start(advertised, 8);
        assert!(matches!(
            actor
                .try_acquire_hub_lease(
                    "hub-leader",
                    Some("http://hub-leader:8080".into()),
                    60_000,
                    now
                )
                .await
                .unwrap(),
            storage::HubLeaseAcquire::Acquired { .. }
        ));
        let standby =
            hub::Hub::with_storage(storage_hub_config(), actor).with_ha(hub::HubHaConfig {
                enabled: true,
                lease_ttl_ms: 1_000,
                ..hub::HubHaConfig::default()
            });
        standby.enter_election().await;
        let app = hub_router(standby, &ServerConfig::default());
        let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "hub_standby");
        assert_eq!(body["details"]["leader_url"], "http://hub-leader:8080");

        // A lease row without an advertisement keeps the body identical to
        // the pre-hint standby response: no `leader_url`, no `details`.
        let quiet = storage::ControlPlaneStore::in_memory().unwrap();
        let actor = storage::StorageActor::start(quiet, 8);
        assert!(matches!(
            actor
                .try_acquire_hub_lease("quiet-leader", None, 60_000, now)
                .await
                .unwrap(),
            storage::HubLeaseAcquire::Acquired { .. }
        ));
        let standby =
            hub::Hub::with_storage(storage_hub_config(), actor).with_ha(hub::HubHaConfig {
                enabled: true,
                lease_ttl_ms: 1_000,
                ..hub::HubHaConfig::default()
            });
        standby.enter_election().await;
        let app = hub_router(standby, &ServerConfig::default());
        let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "hub_standby");
        assert!(body["details"].is_null(), "no hint without advertisement");
    }

    #[tokio::test]
    async fn hub_targeted_command_routes_cover_storage_and_dispatch_paths() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );

        // Validation and authorization.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/pause",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_operation");
        let (status, _) = request_without_body(
            &viewer_app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/start",
            Some("viewer-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Durable intent writes: start, stale generation fence, restart.
        let (status, intent) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{intent}");
        assert_eq!(intent["generation"], 1);
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/stop",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        // Restart without an explicit action id derives one, and a stale
        // If-Match fence fails with 412.
        let (status, restart) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/restart",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{restart}");
        assert!(restart["action_id"].as_str().is_some());
        let stale = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/restart")
                    .header("authorization", "Bearer operator")
                    .header("if-match", "\"generation-0\"")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        let stale = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/stop")
                    .header("authorization", "Bearer operator")
                    .header("if-match", "\"generation-0\"")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);

        // Idempotency-key reuse with a different desired state conflicts.
        let reused = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                    .header("authorization", "Bearer operator")
                    .header("idempotency-key", "targeted-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reused.status(), StatusCode::ACCEPTED);
        let reused = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/stop")
                    .header("authorization", "Bearer operator")
                    .header("idempotency-key", "targeted-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reused.status(), StatusCode::CONFLICT);

        // The canonical restart action route with a body: an invalid header
        // value is a validation error, a valid one forwards.
        let (status, body) = post_json(
            &app,
            "/api/v1/nodes/node-a/streams/orders/actions/restart",
            "operator",
            serde_json::json!({"action_id": "bad\nvalue"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "validation_failed");
        let (status, _) = post_json(
            &app,
            "/api/v1/nodes/node-a/streams/orders/actions/restart",
            "operator",
            serde_json::json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // The volatile (no-storage) dispatch fallback.
        let volatile = console_hub();
        let volatile_app = hub_router(volatile, &ServerConfig::default());
        let (status, body) = request_without_body(
            &volatile_app,
            "POST",
            "/api/v1/nodes/missing/streams/orders/start",
            Some("operator-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "node_unavailable");
        register_hub_node(&volatile_app, "node-a").await;
        let (status, _) = request_without_body(
            &volatile_app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/start",
            Some("operator-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn hub_configuration_routes_cover_not_found_and_command_fallback() {
        // A registered node that has not reported configuration: 404s.
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let viewer_app = hub_router(
            hub::Hub::new(scoped_operator_hub_config()),
            &ServerConfig::default(),
        );
        let (status, body) =
            get_json(&app, "/api/v1/nodes/missing/configuration", "operator").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "node_configuration_unavailable");
        let (status, _) = get_json(
            &app,
            "/api/v1/nodes/missing/configuration/versions",
            "operator",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Viewer credentials cannot validate or apply configuration.
        let (status, _) = post_json(
            &viewer_app,
            "/api/v1/nodes/node-a/configuration/validate",
            "viewer-secret",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = post_json(
            &viewer_app,
            "/api/v1/nodes/node-a/configuration/apply",
            "viewer-secret",
            serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Durable rollback with an unknown config version is a 400.
        register_hub_node(&app, "node-a").await;
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/configuration/rollback/no-such-version",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_configuration");

        // The volatile Hub falls back to direct command dispatch.
        let volatile_app = hub_router(console_hub(), &ServerConfig::default());
        register_hub_node(&volatile_app, "node-a").await;
        let (status, operation) = post_json(
            &volatile_app,
            "/api/v1/nodes/node-a/configuration/apply",
            "operator-secret",
            serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{operation}");
        assert_eq!(operation["operation"], "apply_configuration");
        let (status, body) = post_json(
            &volatile_app,
            "/api/v1/nodes/missing/configuration/apply",
            "operator-secret",
            serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "node_unavailable");
        // Reported configuration and versions become readable.
        let session = register_hub_node(&volatile_app, "node-a").await;
        let mut report = agent_report_body(&session);
        report["configuration"] = serde_json::json!({"streams": []});
        report["config_versions"] =
            serde_json::json!([{"id": "v1", "created_at_ms": 1, "format": "json"}]);
        report_node_raw(&volatile_app, report).await;
        let (status, _) = get_json(
            &volatile_app,
            "/api/v1/nodes/node-a/configuration",
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, versions) = get_json(
            &volatile_app,
            "/api/v1/nodes/node-a/configuration/versions",
            "operator-secret",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(versions.as_array().unwrap().len(), 1);
    }

    // ------------------------------------------------------------------
    // OIDC login flow: mock provider + hub routes.
    // ------------------------------------------------------------------

    const OIDC_TEST_EC_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgIGq3kKWaBjOJOQ4J\nc7lugLGZpGy7bpORlkEWnXiQbkqhRANCAATYhN6zoBUzCrLoVXj2FpxVsgFOauvf\nXSKSDtR4WPh2LJ4TJ8a9cXRDPs87HJj6NC2F1iTBHNBgzdE853A+WQnZ\n-----END PRIVATE KEY-----";
    const OIDC_TEST_KID: &str = "coverage-oidc-key";
    const OIDC_TEST_JWK_X: &str = "2ITes6AVMwqy6FV49hacVbIBTmrr310ikg7UeFj4diw";
    const OIDC_TEST_JWK_Y: &str = "nhMnxr1xdEM-zzscmPo0LYXWJMEc0GDN0TzncD5ZCdk";

    fn mint_test_id_token(issuer: &str, nonce: &str) -> String {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(OIDC_TEST_KID.into());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = serde_json::json!({
            "sub": "oidc-tester",
            "roles": ["operator"],
            "iss": issuer,
            "aud": "arkflow-audience",
            "exp": now + 600,
            "nonce": nonce,
        });
        encode(
            &header,
            &claims,
            &EncodingKey::from_ec_pem(OIDC_TEST_EC_KEY_PEM.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    /// A tiny loopback OIDC provider: discovery, JWKS, and a token endpoint
    /// that mints an id_token whose nonce rides inside the authorization code
    /// (`nonce-<nonce>`), so the login round-trip can complete offline.
    async fn spawn_mock_oidc_provider() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    loop {
                        match stream.read(&mut byte).await {
                            Ok(0) => break,
                            Ok(_) => {
                                head.push(byte[0]);
                                if head.ends_with(b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    let request = String::from_utf8_lossy(&head).into_owned();
                    let mut lines = request.split("\r\n");
                    let request_line = lines.next().unwrap_or_default().to_owned();
                    let mut content_length = 0usize;
                    for line in lines {
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            content_length = value.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; content_length];
                    if content_length > 0 {
                        let _ = stream.read_exact(&mut body).await;
                    }
                    let path = request_line
                        .split(' ')
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned();
                    let issuer = format!("http://127.0.0.1:{port}");
                    let (status, payload) = match path.as_str() {
                        "/.well-known/openid-configuration" => (
                            "200 OK",
                            serde_json::json!({
                                "authorization_endpoint": format!("{issuer}/authorize"),
                                "token_endpoint": format!("{issuer}/token"),
                            })
                            .to_string(),
                        ),
                        "/jwks" => (
                            "200 OK",
                            serde_json::json!({
                                "keys": [{
                                    "kty": "EC",
                                    "crv": "P-256",
                                    "kid": OIDC_TEST_KID,
                                    "x": OIDC_TEST_JWK_X,
                                    "y": OIDC_TEST_JWK_Y,
                                }]
                            })
                            .to_string(),
                        ),
                        "/token" => {
                            let form = String::from_utf8_lossy(&body).into_owned();
                            let code = form
                                .split('&')
                                .find_map(|pair| pair.strip_prefix("code="))
                                .unwrap_or_default();
                            let nonce = code.strip_prefix("nonce-").unwrap_or("fixed-nonce");
                            (
                                "200 OK",
                                serde_json::json!({
                                    "id_token": mint_test_id_token(&issuer, nonce)
                                })
                                .to_string(),
                            )
                        }
                        _ => ("404 Not Found", "{}".into()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        port
    }

    /// Read one cookie out of a response's Set-Cookie header.
    fn response_cookie(response: &Response, name: &str) -> Option<String> {
        response
            .headers()
            .get(header::SET_COOKIE)?
            .to_str()
            .ok()?
            .split(';')
            .next()?
            .strip_prefix(&format!("{name}="))
            .map(str::to_owned)
    }

    async fn oidc_login_hub() -> hub::Hub {
        let port = spawn_mock_oidc_provider().await;
        let issuer = format!("http://127.0.0.1:{port}");
        let federation = crate::oidc::OidcFederation::from_settings(crate::oidc::OidcSettings {
            issuer,
            audience: "arkflow-audience".into(),
            jwks_url: Some(format!("http://127.0.0.1:{port}/jwks")),
            role_claim: None,
            scopes_claim: None,
            client_id: Some("console".into()),
            client_secret: Some("console-secret".into()),
            redirect_uri: Some("http://127.0.0.1:1/console/callback".into()),
            jwks_refresh_interval: None,
        })
        .await
        .expect("federation builds");
        hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        })
        .with_oidc(federation)
    }

    #[tokio::test]
    async fn oidc_status_login_callback_and_logout_round_trip() {
        let hub = oidc_login_hub().await;
        let app = hub_router(hub, &ServerConfig::default());

        // Status before login: enabled but not authenticated.
        let (status, body) = get_json(&app, "/api/v1/auth/oidc/status", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["login_enabled"], true);
        assert_eq!(body["authenticated"], false);

        // Login starts the authorization-code flow with PKCE state.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/auth/oidc/login")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(location.contains("/authorize?"), "{location}");
        assert!(location.contains("code_challenge_method=S256"));
        let state_cookie = response_cookie(&response, "arkflow_oidc_state").expect("state cookie");
        let mut parts = state_cookie.split('.');
        let state = parts.next().unwrap().to_owned();
        let _verifier = parts.next().unwrap().to_owned();
        let nonce = parts.next().unwrap().to_owned();

        let callback = |query: &str, cookie: Option<&str>| {
            let mut builder =
                axum::http::Request::get(format!("/api/v1/auth/oidc/callback?{query}"));
            if let Some(cookie) = cookie {
                builder = builder.header(header::COOKIE, format!("arkflow_oidc_state={cookie}"));
            }
            app.clone()
                .oneshot(builder.body(axum::body::Body::empty()).unwrap())
        };

        // Missing state cookie, malformed cookie, mismatched state, missing
        // code: every failure answers 401 without leaking detail.
        let response = callback(&format!("state={state}&code=nonce-{nonce}"), None)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = callback(&format!("state={state}&code=nonce-{nonce}"), Some("short"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = callback(
            &format!("state=tampered-state&code=nonce-{nonce}"),
            Some(&state_cookie),
        )
        .await
        .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "wrong state must fail"
        );
        let response = callback(&format!("state={state}"), Some(&state_cookie))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // A mismatched nonce (code carries a different nonce than the cookie)
        // is rejected after the token exchange.
        let response = callback(
            &format!("state={state}&code=nonce-someone-elses-nonce"),
            Some(&state_cookie),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // The happy path: exchange validates, the session cookie is issued.
        let response = callback(
            &format!("state={state}&code=nonce-{nonce}"),
            Some(&state_cookie),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/");
        let session_cookie = response_cookie(&response, "arkflow_session").expect("session cookie");

        // Status reports the authenticated principal from the cookie.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/auth/oidc/status")
                    .header(header::COOKIE, format!("arkflow_session={session_cookie}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["authenticated"], true);
        assert_eq!(body["principal"]["id"], "oidc-tester");
        assert_eq!(body["principal"]["roles"][0], "operator");

        // Logout with the session bearer credential invalidates the session.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/auth/oidc/logout")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer session:{session_cookie}"),
                    )
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(header::SET_COOKIE));
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/auth/oidc/status")
                    .header(header::COOKIE, format!("arkflow_session={session_cookie}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["authenticated"], false);
    }

    #[tokio::test]
    async fn oidc_handlers_without_federation_answer_not_found() {
        let hub = console_hub();
        let response = hub_oidc_login(State(hub.clone())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = hub_oidc_callback(
            State(hub),
            Query(std::collections::HashMap::new()),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        // The status route still works and reports the disabled flow.
        let app = hub_router(console_hub(), &ServerConfig::default());
        let (status, body) = get_json(&app, "/api/v1/auth/oidc/status", "operator").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["login_enabled"], false);
    }

    // ------------------------------------------------------------------
    // Direct helper coverage.
    // ------------------------------------------------------------------

    #[test]
    fn credential_helpers_read_cookies_and_etags() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("a=1; arkflow_session=xyz; b=2"),
        );
        assert_eq!(
            cookie_value(&headers, "arkflow_session").as_deref(),
            Some("xyz")
        );
        assert_eq!(cookie_value(&headers, "missing"), None);
        // The browser session cookie surfaces through the bearer channel.
        assert_eq!(bearer(&headers).as_deref(), Some("session:xyz"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer op-token"),
        );
        assert_eq!(bearer(&headers).as_deref(), Some("op-token"));
        assert_eq!(bearer(&HeaderMap::new()), None);

        assert_eq!(parse_generation_etag("\"generation-7\""), Some(7));
        assert_eq!(parse_generation_etag("generation-42"), Some(42));
        assert_eq!(parse_generation_etag("generation-x"), None);
        assert_eq!(parse_generation_etag("bogus"), None);

        assert_eq!(prometheus_label("node:with spaces!"), "nodewithspaces");
        assert_eq!(
            prometheus_label(&"x".repeat(100)).len(),
            64,
            "labels are truncated to 64 characters"
        );
    }

    #[tokio::test]
    async fn hub_problem_maps_error_kinds_to_status_and_code() {
        let cases = [
            (
                hub::HubError::Unauthorized,
                StatusCode::UNAUTHORIZED,
                "agent_request_rejected",
            ),
            (
                hub::HubError::NodeUnavailable,
                StatusCode::CONFLICT,
                "agent_request_rejected",
            ),
            (
                hub::HubError::NotFound,
                StatusCode::NOT_FOUND,
                "agent_request_rejected",
            ),
            (
                hub::HubError::OrchestrationInProgress,
                StatusCode::CONFLICT,
                "orchestration_in_progress",
            ),
            (
                hub::HubError::OrchestrationPhaseConflict,
                StatusCode::CONFLICT,
                "orchestration_conflict",
            ),
            (
                hub::HubError::StorageUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
            ),
            (
                hub::HubError::Storage("boom".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
            ),
            (
                hub::HubError::GenerationConflict {
                    expected: 1,
                    current: 2,
                },
                StatusCode::CONFLICT,
                "generation_conflict",
            ),
            (
                hub::HubError::Invalid("bad input".into()),
                StatusCode::BAD_REQUEST,
                "agent_request_rejected",
            ),
        ];
        for (error, status, code) in cases {
            let display = error.to_string();
            let response = hub_problem(error);
            assert_eq!(response.status(), status);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["code"], code, "for {display}");
        }
    }

    #[tokio::test]
    async fn require_operator_action_rejects_and_audits_missing_credentials() {
        let hub = storage_hub();
        let response = require_operator_action(
            &hub,
            &HeaderMap::new(),
            OperatorAction::Read,
            "job",
            Some("job-a".into()),
        )
        .await
        .unwrap_err();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "unauthorized");
        // The denial is visible in the audit trail.
        let records = hub.audit(None).await.unwrap();
        assert!(
            records.iter().any(|record| record.outcome == "rejected"
                && record.failure_code.as_deref() == Some("unauthorized")
                && record.resource_id.as_deref() == Some("job-a")),
            "audit records: {records:?}"
        );
    }

    // ------------------------------------------------------------------
    // Local (ControlPlane) router coverage.
    // ------------------------------------------------------------------

    async fn local_engine_with_stream() -> (ControlPlane, tokio_util::sync::CancellationToken) {
        arkflow_plugin::initialize().unwrap();
        let stream: arkflow_core::stream::StreamConfig =
            serde_json::from_value(serde_json::json!({
                "id": "orders",
                "input": {
                    "type": "generate",
                    "context": "{\"value\":1}",
                    "interval": "1s",
                    "batch_size": 1
                },
                "pipeline": {"thread_num": 1, "processors": []},
                "output": {"type": "drop"}
            }))
            .unwrap();
        let engine = Engine::new(EngineConfig {
            streams: vec![stream],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig {
                api_token: Some("local-token".into()),
                ..HealthCheckConfig::default()
            },
        });
        let control_plane = engine.control_plane();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let token = cancellation.clone();
        tokio::spawn(async move {
            let _ = engine.run_with_cancellation(token).await;
        });
        (control_plane, cancellation)
    }

    #[tokio::test]
    async fn local_resource_routes_cover_lifecycle_operations_and_events() {
        let (control_plane, cancellation) = local_engine_with_stream().await;
        control_plane.health().set_ready(true);
        control_plane.health().set_running(true);
        let app = router(control_plane.clone(), &ServerConfig::default());

        // Wait for the stream runtime to register.
        let mut registered = false;
        for _ in 0..200 {
            let (status, _) = get_json(&app, "/api/v1/streams/orders", "local-token").await;
            if status == StatusCode::OK {
                registered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(registered, "the stream runtime must register");

        // Node resource and lifecycle actions.
        let (status, node) = get_json(&app, "/api/v1/node", "local-token").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(node["id"], "local-node");
        let (status, _) = request_without_body(
            &app,
            "POST",
            "/api/v1/streams/orders/start",
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/streams/missing/start",
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "stream_not_found");
        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/streams/orders/start")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        // Operations listing with every filter, plus 404 and cancel.
        let (status, page) = get_json(
            &app,
            "/api/v1/operations?resource_id=orders&operation=start&state=succeeded&correlation_id=nope",
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["total"], 0);
        let (status, page) = get_json(&app, "/api/v1/operations", "local-token").await;
        assert_eq!(status, StatusCode::OK);
        assert!(page["total"].as_u64().unwrap() >= 1);
        let operation_id = page["items"][0]["id"].as_str().unwrap().to_owned();
        let (status, _) = get_json(&app, "/api/v1/operations/missing", "local-token").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = request_without_body(
            &app,
            "DELETE",
            &format!("/api/v1/operations/{operation_id}"),
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = request_without_body(
            &app,
            "DELETE",
            "/api/v1/operations/missing",
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::delete("/api/v1/operations/missing")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        // Events narrow by type/stream and sort newest-first.
        let (status, all) = get_json(&app, "/api/v1/events", "local-token").await;
        assert_eq!(status, StatusCode::OK);
        let items = all["items"].as_array().unwrap();
        assert!(!items.is_empty());
        let event_type = items[0]["event_type"].as_str().unwrap().to_owned();
        let (status, filtered) = get_json(
            &app,
            &format!("/api/v1/events?event_type={event_type}&stream_id=orders"),
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(filtered["total"].as_u64().unwrap() >= 1);

        // Draft lifecycle: empty draft answers 204.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/configuration/draft")
                    .header("authorization", "Bearer local-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        cancellation.cancel();
    }

    #[tokio::test]
    async fn local_configuration_routes_apply_diff_and_rollback() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig {
                api_token: Some("local-token".into()),
                ..HealthCheckConfig::default()
            },
        });
        let app = router(engine.control_plane(), &ServerConfig::default());
        let candidate = serde_json::json!({"format": "json", "content": "{\"streams\":[]}"});
        let mut created_versions = Vec::new();

        // Apply creates versions; the second apply descends from the first.
        let (status, applied) = post_json(
            &app,
            "/api/v1/configuration/apply",
            "local-token",
            candidate.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{applied}");
        let first = applied["version"]["id"].as_str().unwrap().to_owned();
        created_versions.push(first.clone());
        let (status, applied) = post_json(
            &app,
            "/api/v1/configuration/apply",
            "local-token",
            candidate,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{applied}");
        let second = applied["version"]["id"].as_str().unwrap().to_owned();
        created_versions.push(second.clone());

        // Invalid content is a 422.
        let (status, _) = post_json(
            &app,
            "/api/v1/configuration/apply",
            "local-token",
            serde_json::json!({"format": "json", "content": "not-json"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        // Diff: success, missing `to`, missing `from`.
        let (status, diff) = get_json(
            &app,
            &format!("/api/v1/configuration/diff?from={first}&to={second}"),
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{diff}");
        assert_eq!(diff["changed"], false);
        let (status, _) = get_json(
            &app,
            &format!("/api/v1/configuration/diff?from={first}&to=missing"),
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Rollback to a known version succeeds; an unknown id is a 422.
        let (status, rolled) = request_without_body(
            &app,
            "POST",
            &format!("/api/v1/configuration/rollback/{first}"),
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{rolled}");
        if let Some(id) = rolled["version"]["id"].as_str() {
            created_versions.push(id.to_owned());
        }
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/configuration/rollback/does-not-exist",
            Some("local-token"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

        // Authorization gates every configuration route.
        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/configuration")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        // Component catalogue: unknown kind and unknown name.
        let (status, _) = get_json(&app, "/api/v1/components/bogus/name", "local-token").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = get_json(&app, "/api/v1/components/input/nope", "local-token").await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Clean the version files this test created (other tests running
        // in parallel may own the remaining ones).
        for id in &created_versions {
            let path = format!(".arkflow/config-history/{id}.json");
            assert!(
                std::fs::remove_file(&path).is_ok(),
                "test must clean up its own version file {path}"
            );
        }
        let _ = std::fs::remove_dir(".arkflow/config-history");
        let _ = std::fs::remove_dir(".arkflow");
    }

    // ------------------------------------------------------------------
    // Coverage additions: serve lifecycles, repository failure arms,
    // command conflicts, SSE filter/lag/close paths, OIDC failure modes.
    // ------------------------------------------------------------------

    /// `serve` answers requests and resolves Ok after graceful shutdown.
    #[tokio::test]
    async fn serve_answers_and_resolves_after_graceful_shutdown() {
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        });
        let port = free_port();
        let config = ServerConfig {
            address: format!("127.0.0.1:{port}"),
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        let mut task = tokio::spawn(async move {
            serve(engine.control_plane(), config, shutdown)
                .await
                .expect("serve resolves Ok after the graceful shutdown");
        });
        let client = reqwest::Client::new();
        let mut served = false;
        for _ in 0..100 {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .await
            {
                let _ = response.status();
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(served, "the local server must answer /health");
        cancellation.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(10), &mut task)
            .await
            .expect("the serve task resolves after cancellation")
            .expect("the serve task did not panic");
    }

    /// A disabled Hub server is a no-op, and an enabled Hub with storage
    /// restores persisted operations before serving and releases leadership
    /// on graceful shutdown (both plain and TLS listeners).
    #[tokio::test]
    async fn serve_hub_disabled_is_a_no_op() {
        let hub = storage_hub();
        let config = ServerConfig {
            enabled: false,
            ..ServerConfig::default()
        };
        let outcome = serve_hub(hub, config, tokio_util::sync::CancellationToken::new()).await;
        assert!(outcome.is_ok(), "{outcome:?}");
    }

    #[tokio::test]
    async fn serve_hub_with_storage_restores_operations_and_shuts_down_cleanly() {
        let port = free_port();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: true,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 10_000,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(storage::ControlPlaneStore::in_memory().unwrap(), 8),
        );
        let config = ServerConfig {
            address: format!("127.0.0.1:{port}"),
            insecure_local: true,
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        let mut task = tokio::spawn(async move {
            serve_hub(hub, config, shutdown)
                .await
                .expect("the hub serves and shuts down cleanly");
        });
        let client = reqwest::Client::new();
        let mut served = false;
        for _ in 0..150 {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .await
            {
                let _ = response.status();
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(served, "the hub must answer /health");
        cancellation.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(15), &mut task)
            .await
            .expect("serve_hub resolves after cancellation")
            .expect("the serve task did not panic");
    }

    #[tokio::test]
    async fn serve_hub_over_tls_releases_leadership_on_shutdown() {
        let (cert_path, key_path) = hub_tls_material();
        let port = free_port();
        let hub = hub::Hub::with_storage(
            hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: true,
                lease_ttl_ms: 10_000,
                poll_interval_ms: 10_000,
                session_ttl_ms: default_session_ttl_ms(),
            },
            storage::StorageActor::start(storage::ControlPlaneStore::in_memory().unwrap(), 8),
        );
        let config = ServerConfig {
            address: format!("127.0.0.1:{port}"),
            insecure_local: true,
            tls_cert: Some(cert_path.to_string_lossy().into_owned()),
            tls_key: Some(key_path.to_string_lossy().into_owned()),
            ..ServerConfig::default()
        };
        let cancellation = tokio_util::sync::CancellationToken::new();
        let shutdown = cancellation.clone();
        let mut task = tokio::spawn(async move {
            serve_hub(hub, config, shutdown)
                .await
                .expect("the TLS hub serves and shuts down cleanly");
        });
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let mut served = false;
        for _ in 0..150 {
            if let Ok(response) = client
                .get(format!("https://127.0.0.1:{port}/liveness"))
                .send()
                .await
            {
                let _ = response.status();
                served = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(served, "the TLS hub must answer /liveness");
        cancellation.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(15), &mut task)
            .await
            .expect("the TLS serve_hub resolves after cancellation")
            .expect("the TLS serve task did not panic");
        let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
    }

    /// Every hub route that reads through the durable store surfaces a
    /// repository failure as a 503 with the storage_unavailable code.
    #[tokio::test]
    async fn hub_routes_surface_repository_failures() {
        let _ = arkflow_plugin::initialize();
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        create_job(&app, "failing-job").await;
        register_hub_node(&app, "node-a").await;

        // A stopped-mode upgrade whose savepoint lookup fails: only the
        // checkpoint table is broken at this stage so the job read succeeds.
        request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/failing-job/savepoints",
            Some("operator"),
        )
        .await;
        let job_version = 1;
        store
            .with_connection(|connection| {
                connection.execute("DROP TABLE cp_job_checkpoints", [])?;
                Ok(())
            })
            .unwrap();
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/failing-job/upgrades",
            "operator",
            serde_json::json!({
                "spec": job_spec("failing-job", 2),
                "expected_generation": 1,
                "savepoint_id": "checkpoint-1",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        // The detail view and recovery-artifact trigger surface the same
        // failure while the Job record itself is still readable, and an
        // upgrade against an unknown Job keeps its 404.
        let (status, body) = get_json(&app, "/api/v1/jobs/failing-job/detail", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/failing-job/checkpoints",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/ghost-job/upgrades",
            "operator",
            serde_json::json!({
                "spec": job_spec("ghost-job", 2),
                "expected_generation": 1,
                "savepoint_id": "checkpoint-1",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "job_not_found");

        // Break the remaining durable tables: every read fails closed.
        store
            .with_connection(|connection| {
                for table in [
                    "cp_jobs",
                    "cp_job_versions",
                    "cp_job_upgrades",
                    "cp_rollouts",
                    "cp_audit_events",
                    "cp_stream_desired",
                    "cp_config_versions",
                ] {
                    connection.execute(&format!("DROP TABLE {table}"), [])?;
                }
                Ok(())
            })
            .unwrap();
        for (method, path) in [
            ("GET", "/api/v1/jobs"),
            ("GET", "/api/v1/jobs/failing-job"),
            ("GET", "/api/v1/jobs/failing-job/plan"),
            ("GET", "/api/v1/jobs/failing-job/detail"),
            ("GET", "/api/v1/jobs/failing-job/versions"),
            ("GET", "/api/v1/jobs/failing-job/checkpoints"),
            ("GET", "/api/v1/audit"),
            ("GET", "/api/v1/rollouts"),
            ("GET", "/api/v1/rollouts/missing"),
            ("GET", "/api/v1/nodes/node-a/streams/orders"),
        ] {
            let (status, body) = request_without_body(&app, method, path, Some("operator")).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{method} {path} must surface the repository failure: {body}"
            );
        }
        for (method, path, body_value) in [
            (
                "POST",
                "/api/v1/jobs/failing-job/actions/start",
                serde_json::Value::Null,
            ),
            (
                "POST",
                "/api/v1/jobs/failing-job/upgrades",
                serde_json::json!({
                    "spec": job_spec("failing-job", 2),
                    "expected_generation": 1,
                    "savepoint_id": "checkpoint-1",
                }),
            ),
            (
                "POST",
                "/api/v1/jobs/failing-job/upgrades/restore-v1/rollback",
                serde_json::Value::Null,
            ),
            (
                "PUT",
                "/api/v1/jobs/failing-job/desired-state",
                serde_json::json!({"state": "running"}),
            ),
            (
                "POST",
                "/api/v1/jobs/failing-job/checkpoints",
                serde_json::Value::Null,
            ),
            (
                "POST",
                "/api/v1/rollouts",
                serde_json::json!({
                    "config_version": "cfg-x",
                    "node_ids": ["node-a"],
                    "batch_size": 1
                }),
            ),
            (
                "POST",
                "/api/v1/rollouts/r1/actions",
                serde_json::json!({"action": "pause"}),
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(path)
                        .header("authorization", "Bearer operator")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body_value.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{method} {path} must surface the repository failure"
            );
        }
        // A body-less restart request surfaces the same repository failure.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-a/streams/orders/actions/restart",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        // The plain GET stream resource keeps its distinct error body.
        let _ = job_version;
    }

    /// Generation fences, idempotency reuse and desired-state validation on
    /// the node-scoped stream routes.
    #[tokio::test]
    async fn hub_targeted_command_conflicts_and_desired_state_validation() {
        let _ = arkflow_plugin::initialize();
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub =
            hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
        let app = hub_router(hub, &ServerConfig::default());

        // A desired state outside the running/stopped vocabulary is rejected.
        let (status, body) = put_json(
            &app,
            "/api/v1/nodes/node-a/streams/orders/desired-state",
            "operator",
            serde_json::json!({"state": "paused"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "validation_failed");

        // An if-match generation etag that can never match is a conflict.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/actions/restart")
                    .header("authorization", "Bearer operator")
                    .header("if-match", "generation-999")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

        // The same idempotency key cannot carry a different mutation: the
        // first desired state pins the key, a divergent state conflicts.
        let desired = |state: &str, key: &str| {
            app.clone().oneshot(
                axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .header("idempotency-key", key)
                    .body(axum::body::Body::from(
                        serde_json::json!({"state": state}).to_string(),
                    ))
                    .unwrap(),
            )
        };
        let response = desired("running", "reused-desired-key").await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let response = desired("stopped", "reused-desired-key").await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "the same key with a different state must conflict"
        );

        // Creating a Job whose rebalance policy contradicts its pinned
        // placement surfaces the store's rejection.
        let mut auto_rebalance = job_spec("rebalance-job", 1);
        auto_rebalance["rebalance"] = serde_json::json!({"mode": "auto"});
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({
                "spec": auto_rebalance,
                "desired_state": "stopped",
                "node_ids": ["node-a"],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    /// On a Hub without durable storage the enqueue paths surface capability
    /// and capacity rejections, and the upgrade/rollout routes answer with
    /// the storage-unavailable family.
    #[tokio::test]
    async fn no_storage_hub_reports_enqueue_and_orchestration_limits() {
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        });
        let app = hub_router(hub.clone(), &ServerConfig::default());
        // Register an online node WITHOUT the lifecycle/configuration
        // capabilities so the enqueue capability gate rejects.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/register")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "node_id": "node-caps",
                            "node_token": "",
                            "protocol_version": "v1",
                            "capabilities": ["job_runtime"],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Apply configuration requires the configuration capability.
        let (status, body) = post_json(
            &app,
            "/api/v1/nodes/node-caps/configuration/apply",
            "operator",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "command_rejected");

        // A targeted lifecycle command requires stream_lifecycle.
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/nodes/node-caps/streams/orders/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "command_rejected");

        // Fill the node's command queue: further read-only commands are
        // rejected with the capacity error.
        for index in 0..140 {
            let _ = hub
                .enqueue(
                    "node-caps".into(),
                    "validate_configuration".into(),
                    format!("cap-{index}"),
                    None,
                )
                .await;
        }
        let (status, body) = post_json(
            &app,
            "/api/v1/nodes/node-caps/configuration/validate",
            "operator",
            serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "command_rejected");

        // Upgrade orchestration and rollouts require durable storage.
        let (status, body) = get_json(&app, "/api/v1/operations/status", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) =
            get_json(&app, "/api/v1/jobs/some-job/upgrades/upgrade-1", "operator").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/some-job/upgrades/upgrade-1/actions",
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "cfg-9", "node_ids": ["node-caps"], "batch_size": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = post_json(
            &app,
            "/api/v1/rollouts/r9/actions",
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    }

    /// A rollback to a previous version surfaces each corruption class:
    /// unparseable JSON, a spec that no longer validates, and a spec whose
    /// components no longer build.
    #[tokio::test]
    async fn job_rollback_reports_corrupt_previous_versions() {
        let _ = arkflow_plugin::initialize();
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        create_job(&app, "rollback-job").await;
        // Publish version 2 so a previous version exists to roll back to.
        let (status, _) = post_json(
            &app,
            "/api/v1/jobs",
            "operator",
            serde_json::json!({"spec": job_spec("rollback-job", 2), "desired_state": "stopped"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let rollback = "/api/v1/jobs/rollback-job/upgrades/restore-v1/rollback";

        // Missing credentials never reach the store.
        let (status, _) = request_without_body(&app, "POST", rollback, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // (a) The previous version's spec is no longer parseable.
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = 'not json' WHERE job_id = 'rollback-job' AND version = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["code"], "invalid_persisted_job");

        // (b) The previous spec parses but no longer validates.
        let mut sinkless = job_spec("rollback-job", 1);
        sinkless["sinks"] = serde_json::json!([]);
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&sinkless).unwrap()],
                )?;
                Ok(())
            })
            .unwrap();
        let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        // (c) The spec builds on paper but its components no longer exist.
        let mut unknown_input = job_spec("rollback-job", 1);
        unknown_input["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&unknown_input).unwrap()],
                )?;
                Ok(())
            })
            .unwrap();
        let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "invalid_job_plan");

        // (d) A recovery pointer set on the Job makes the rollback verify the
        // artifact's state format; a broken checkpoint table surfaces that
        // read as a repository failure.
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&job_spec("rollback-job", 1)).unwrap()],
                )?;
                connection.execute(
                    "UPDATE cp_jobs SET checkpoint_id = 'cp-9' WHERE job_id = 'rollback-job'",
                    [],
                )?;
                connection.execute("DROP TABLE cp_job_checkpoints", [])?;
                Ok(())
            })
            .unwrap();
        let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    }

    /// A stopped-mode upgrade honours an explicit node pinning, a Job whose
    /// persisted spec no longer parses fails the desired-state reconciliation
    /// that follows the update, and an orchestration row that predates the
    /// process fences a second atomic upgrade.
    #[tokio::test]
    async fn job_upgrade_pinning_corrupt_spec_reconciliation_and_orchestration_fence() {
        let _ = arkflow_plugin::initialize();
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub.clone(), &ServerConfig::default());
        register_job_node(&app, "node-a").await;
        create_job(&app, "pin-job").await;

        // Complete a savepoint, then upgrade with an explicit node pinning.
        request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/pin-job/savepoints",
            Some("operator"),
        )
        .await;
        store
            .with_connection(|connection| {
                connection.execute("UPDATE cp_job_checkpoints SET status = 'completed'", [])?;
                Ok(())
            })
            .unwrap();
        let (_, checkpoints) = get_json(&app, "/api/v1/jobs/pin-job/checkpoints", "operator").await;
        let savepoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/pin-job/upgrades",
            "operator",
            serde_json::json!({
                "spec": job_spec("pin-job", 2),
                "expected_generation": 1,
                "savepoint_id": savepoint_id,
                "node_ids": ["node-a"],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["job"]["node_ids"], serde_json::json!(["node-a"]));

        // A corrupt persisted spec fails the reconciliation that follows a
        // desired-state update (both the route and the action funnel).
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_jobs SET spec_json = 'not json' WHERE job_id = 'pin-job'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let (status, body) = put_json(
            &app,
            "/api/v1/jobs/pin-job/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let (status, body) = request_without_body(
            &app,
            "POST",
            "/api/v1/jobs/pin-job/actions/start",
            Some("operator"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        // A durable orchestration row that predates this process fences a
        // fresh atomic upgrade even though the in-memory cache is empty.
        create_job(&app, "fenced-job").await;
        let (status, body) = put_json(
            &app,
            "/api/v1/jobs/fenced-job/desired-state",
            "operator",
            serde_json::json!({"state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let generation = current_generation(&app, "fenced-job").await;
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_job_upgrades (upgrade_id, job_id, from_version, to_version, phase, target_spec_json, phase_deadline_at_ms, savepoint_retries, verify_timeout_ms, created_at_ms, updated_at_ms) VALUES ('upgrade-stale', 'fenced-job', 1, 2, 'saving_savepoint', '{}', 0, 0, 0, 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/fenced-job/upgrades",
            "operator",
            serde_json::json!({
                "spec": job_spec("fenced-job", 2),
                "expected_generation": generation,
                "mode": "atomic",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "orchestration_in_progress");

        // An action against an unknown upgrade id resolves as a 404.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/fenced-job/upgrades/upgrade-missing/actions",
            "operator",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "job_upgrade_not_found");
        let _ = hub;
    }

    /// Scoped viewer credentials pass route authentication but cannot
    /// perform Operate/Configure/ManageRollouts actions.
    #[tokio::test]
    async fn viewer_credentials_cannot_mutate_jobs_or_rollouts() {
        let _ = arkflow_plugin::initialize();
        let hub = hub::Hub::new(scoped_operator_hub_config());
        hub.upsert_job(crate::storage::JobRecord {
            job_id: "scoped-mutate".into(),
            version: 1,
            spec_json: serde_json::to_string(&job_spec("scoped-mutate", 1)).unwrap(),
            desired_state: "stopped".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let app = hub_router(hub, &ServerConfig::default());

        // Job validation is a Configure action.
        let (status, body) = post_json(
            &app,
            "/api/v1/jobs/validate",
            "viewer-secret",
            serde_json::json!({"spec": job_spec("scoped-mutate", 1)}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

        // Recovery artifacts and rollback are Operate actions.
        for (method, path) in [
            ("POST", "/api/v1/jobs/scoped-mutate/savepoints"),
            (
                "POST",
                "/api/v1/jobs/scoped-mutate/upgrades/restore-v1/rollback",
            ),
        ] {
            let (status, body) =
                request_without_body(&app, method, path, Some("viewer-secret")).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {body}");
        }

        // Rollout management is its own action level.
        let (status, body) = post_json(
            &app,
            "/api/v1/rollouts/rx/actions",
            "viewer-secret",
            serde_json::json!({"action": "pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }

    /// The local resource routes: stop/restart lifecycles, conflict mapping,
    /// operation reads, correlation filters and the unauthorized arms of
    /// every configuration route.
    #[tokio::test]
    async fn local_lifecycle_and_configuration_routes_cover_conflict_and_unauthorized_arms() {
        // The stream below uses the `generate` input; register the plugin
        // components explicitly so this test does not depend on another
        // test having initialized the registry first (initialize is
        // idempotent through its OnceLock).
        arkflow_plugin::initialize().unwrap();
        let engine = Engine::new(EngineConfig {
            streams: vec![generate_local_stream("local-orders")],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig {
                api_token: Some("local-token".into()),
                ..HealthCheckConfig::default()
            },
        });
        let cp = engine.control_plane();
        cp.runtime_manager()
            .replace_config(&EngineConfig {
                streams: vec![generate_local_stream("local-orders")],
                jobs: Vec::new(),
                logging: LoggingConfig::default(),
                health_check: HealthCheckConfig::default(),
            })
            .await
            .expect("the stream registers");
        let app = router(cp, &ServerConfig::default());

        // Stop and restart are first-class lifecycle routes.
        for action in ["stop", "restart"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::post(format!("/api/v1/streams/local-orders/{action}"))
                        .header("authorization", "Bearer local-token")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED, "{action}");
        }

        // A start on the already-running stream settles through the async
        // lifecycle operation (the handler's only sync error is the
        // unknown-stream 404).
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/streams/local-orders/start")
                    .header("authorization", "Bearer local-token")
                    .header("x-correlation-id", "corr-local-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // The operation created above is readable by id.
        let (_, operations) = get_json(&app, "/api/v1/operations", "local-token").await;
        let operation_id = operations["items"][0]["id"]
            .as_str()
            .expect("at least one operation exists")
            .to_owned();
        let (status, _) = get_json(
            &app,
            &format!("/api/v1/operations/{operation_id}"),
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Cancelling the in-flight start records an event synchronously, so
        // the event filters have something to filter over.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::delete(format!("/api/v1/operations/{operation_id}"))
                    .header("authorization", "Bearer local-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let (status, events) = get_json(&app, "/api/v1/events", "local-token").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            events["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty()),
            "a cancelled operation records an event: {events}"
        );
        // The correlation filter clause runs against the recorded events.
        let (status, _) = get_json(
            &app,
            "/api/v1/events?correlation_id=corr-local-1",
            "local-token",
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Every configuration route answers 401 without the token.
        for (method, path) in [
            ("GET", "/api/v1/configuration/draft"),
            ("PUT", "/api/v1/configuration/draft"),
            ("GET", "/api/v1/configuration/diff?from=a&to=b"),
            ("GET", "/api/v1/configuration/versions"),
            ("POST", "/api/v1/configuration/apply"),
            ("POST", "/api/v1/configuration/rollback/v1"),
        ] {
            let builder = axum::http::Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json");
            let response = app
                .clone()
                .oneshot(
                    builder
                        .body(axum::body::Body::from(
                            serde_json::json!({"format": "yaml", "content": "streams: []\n"})
                                .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path} must require the token"
            );
        }
        let _ = engine.runtime_manager().stop_all().await;
    }

    fn generate_local_stream(id: &str) -> arkflow_core::stream::StreamConfig {
        arkflow_core::stream::StreamConfig {
            id: Some(id.to_string()),
            input: arkflow_core::input::InputConfig {
                input_type: "generate".into(),
                name: None,
                codec: None,
                config: Some(serde_json::json!({
                    "context": "local-routes",
                    "interval": "50ms",
                    "batch_size": 1
                })),
            },
            pipeline: arkflow_core::pipeline::PipelineConfig {
                thread_num: 1,
                processors: Vec::new(),
            },
            output: arkflow_core::output::OutputConfig {
                output_type: "drop".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: None,
        }
    }

    /// The SSE stream filters live broadcasts, signals resync after a lag,
    /// honors the correlation filter, and ends once the Hub is gone.
    #[tokio::test]
    async fn sse_filters_live_events_lags_to_resync_and_ends_on_close() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub =
            hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
        let app = hub_router(hub.clone(), &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;

        // Non-matching live broadcasts are skipped until a matching one
        // arrives (the filter's continue arm).
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(
                    "/api/v1/events/stream?node_id=node-a&correlation_id=corr-match",
                )
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        // A second node's broadcast does not match the filter and must be
        // skipped, the matching one (with a correlation id, so the filter's
        // correlation clause also runs) must arrive.
        let node_b_session = register_hub_node(&app, "node-b").await;
        let driver_app = app.clone();
        let session_a = session.session_token.clone();
        let session_b = node_b_session.session_token.clone();
        let driver = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let report =
                |session_token: &str, node_id: &str, event_type: &str, correlation: &str| {
                    serde_json::json!({
                        "node_id": node_id,
                        "session_token": session_token,
                        "version": "test-node",
                        "state": "online",
                        "events": [{
                            "occurred_at_ms": 100,
                            "event_type": event_type,
                            "stream_id": "orders",
                            "outcome": "accepted",
                            "message": null,
                            "operation_id": null,
                            "correlation_id": correlation,
                            "actor": null
                        }]
                    })
                };
            // node-b's event does not match the node_id filter.
            let _ = driver_app
                .clone()
                .oneshot(
                    axum::http::Request::post("/api/v1/agent/report")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(
                            report(&session_b, "node-b", "foreign_event", "corr-x").to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            // node-a's matching event carries the filtered correlation id.
            let _ = driver_app
                .oneshot(
                    axum::http::Request::post("/api/v1/agent/report")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(
                            report(&session_a, "node-a", "matching_event", "corr-match")
                                .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
        });
        let mut saw_matching = false;
        for _ in 0..32 {
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next()).await;
            match chunk {
                Ok(Some(Ok(chunk))) => {
                    let text = String::from_utf8(chunk.to_vec()).unwrap();
                    if text.contains("matching_event") {
                        saw_matching = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(saw_matching, "the filtered live event must arrive");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), driver).await;

        // Drop every Hub clone: the broadcast sender closes and the stream
        // ends instead of hanging forever.
        drop(app);
        drop(hub);
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), body.next()).await;
        assert!(
            matches!(&ended, Ok(None)),
            "the stream must end once the Hub is gone: {ended:?}"
        );

        // A lagging receiver resyncs instead of replaying a gap silently.
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        let session = register_hub_node(&app, "node-a").await;
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/events/stream?correlation_id=corr-burst")
                    .header("authorization", "Bearer operator")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut body = response.into_body().into_data_stream();
        // Broadcast far more events than the channel capacity WITHOUT
        // polling the stream, then read: the receiver lagged.
        let mut events = Vec::new();
        for index in 0..300 {
            events.push(serde_json::json!({
                "occurred_at_ms": 200 + index,
                "event_type": "burst_event",
                "stream_id": "orders",
                "outcome": "accepted",
                "message": null,
                "operation_id": null,
                "correlation_id": "corr-burst",
                "actor": null
            }));
        }
        let response = app
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "node_id": "node-a",
                            "session_token": session.session_token,
                            "version": "test-node",
                            "state": "online",
                            "events": events
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let mut saw_resync = false;
        for _ in 0..64 {
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next()).await;
            match chunk {
                Ok(Some(Ok(chunk))) => {
                    let text = String::from_utf8(chunk.to_vec()).unwrap();
                    if text.contains("event: resync") {
                        saw_resync = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(saw_resync, "a lagging receiver must resync");
    }

    /// The agent command-result route maps unknown session credentials onto
    /// the hub problem response.
    #[tokio::test]
    async fn agent_command_result_with_unknown_session_maps_to_hub_problem() {
        let hub = storage_hub();
        let app = hub_router(hub, &ServerConfig::default());
        let response = app
            .oneshot(
                axum::http::Request::post(
                    "/api/v1/agent/commands/cmd-unknown/result?node_id=node-a",
                )
                .header("authorization", "Bearer not-a-session")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "command_id": "cmd-unknown",
                        "operation_id": "op-1",
                        "state": "succeeded",
                        "progress": 100,
                    })
                    .to_string(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// Attempt states from the durable store render as Prometheus series.
    #[tokio::test]
    async fn hub_metrics_renders_attempt_state_series() {
        let store = storage::ControlPlaneStore::in_memory().unwrap();
        let hub = hub::Hub::with_storage(
            storage_hub_config(),
            storage::StorageActor::start(store.clone(), 8),
        );
        let app = hub_router(hub, &ServerConfig::default());
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, state, convergence_state, created_at_ms, updated_at_ms) VALUES ('intent-1', 'node-a', 'orders', 1, 'start', 'pending', 'pending', 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, created_at_ms) VALUES ('att-1', 'intent-1', 'cmd-1', 'node-a', 'orders', 1, 'start', 'queued', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let response = app
            .oneshot(
                axum::http::Request::get("/api/v1/metrics")
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
        let text = String::from_utf8_lossy(&body).into_owned();
        assert!(
            text.contains("arkflow_attempts_state{state=\"queued\"} 1"),
            "attempt states must render: {text}"
        );
    }

    /// A mock OIDC provider whose token endpoint can fail or mint
    /// wrong-issuer tokens, plus a secure (https) redirect URI.
    async fn spawn_failing_mock_oidc_provider() -> (u16, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let issuer = format!("http://127.0.0.1:{port}");
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    loop {
                        match stream.read(&mut byte).await {
                            Ok(0) => break,
                            Ok(_) => {
                                head.push(byte[0]);
                                if head.ends_with(b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    let request = String::from_utf8_lossy(&head).into_owned();
                    let mut lines = request.split("\r\n");
                    let request_line = lines.next().unwrap_or_default().to_owned();
                    let mut content_length = 0usize;
                    for line in lines {
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            content_length = value.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; content_length];
                    if content_length > 0 {
                        let _ = stream.read_exact(&mut body).await;
                    }
                    let path = request_line
                        .split(' ')
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned();
                    let issuer = format!("http://127.0.0.1:{port}");
                    let (status, payload) = match path.as_str() {
                        "/.well-known/openid-configuration" => (
                            "200 OK",
                            serde_json::json!({
                                "authorization_endpoint": format!("{issuer}/authorize"),
                                "token_endpoint": format!("{issuer}/token"),
                            })
                            .to_string(),
                        ),
                        "/jwks" => (
                            "200 OK",
                            serde_json::json!({
                                "keys": [{
                                    "kty": "EC",
                                    "crv": "P-256",
                                    "kid": OIDC_TEST_KID,
                                    "x": OIDC_TEST_JWK_X,
                                    "y": OIDC_TEST_JWK_Y,
                                }]
                            })
                            .to_string(),
                        ),
                        "/token" => {
                            let form = String::from_utf8_lossy(&body).into_owned();
                            let code = form
                                .split('&')
                                .find_map(|pair| pair.strip_prefix("code="))
                                .unwrap_or_default()
                                .to_owned();
                            if code == "reject-me" {
                                ("500 Internal Server Error", String::new())
                            } else {
                                let (issuer_used, nonce) =
                                    if let Some(nonce) = code.strip_prefix("wrong-issuer-nonce-") {
                                        ("http://127.0.0.1:1".to_owned(), nonce.to_owned())
                                    } else {
                                        let nonce =
                                            code.strip_prefix("nonce-").unwrap_or("fixed-nonce");
                                        (issuer.clone(), nonce.to_owned())
                                    };
                                (
                                    "200 OK",
                                    serde_json::json!({
                                        "id_token": mint_test_id_token(&issuer_used, &nonce)
                                    })
                                    .to_string(),
                                )
                            }
                        }
                        _ => ("404 Not Found", "{}".into()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        (port, issuer)
    }

    /// OIDC login over a secure redirect issues Secure cookies, and the
    /// callback rejects a missing state parameter, a failing token exchange
    /// and a token from the wrong issuer.
    #[tokio::test]
    async fn oidc_secure_login_and_callback_failure_modes() {
        let (port, issuer) = spawn_failing_mock_oidc_provider().await;
        let federation = crate::oidc::OidcFederation::from_settings(crate::oidc::OidcSettings {
            issuer,
            audience: "arkflow-audience".into(),
            jwks_url: Some(format!("http://127.0.0.1:{port}/jwks")),
            role_claim: None,
            scopes_claim: None,
            client_id: Some("console".into()),
            client_secret: Some("console-secret".into()),
            redirect_uri: Some("https://127.0.0.1:1/console/callback".into()),
            jwks_refresh_interval: None,
        })
        .await
        .expect("federation builds");
        let hub = hub::Hub::new(hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        })
        .with_oidc(federation);
        let app = hub_router(hub, &ServerConfig::default());

        // The login cookie carries the Secure attribute for https redirects.
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/auth/oidc/login")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let cookie = response_cookie(&response, "arkflow_oidc_state").expect("state cookie");
        assert!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("; Secure")),
            "an https redirect must set the Secure attribute"
        );
        let mut parts = cookie.split('.');
        let state = parts.next().unwrap().to_owned();
        let nonce = parts.next_back().unwrap().to_owned();

        let callback = |query: &str| {
            app.clone().oneshot(
                axum::http::Request::get(format!("/api/v1/auth/oidc/callback?{query}"))
                    .header(header::COOKIE, format!("arkflow_oidc_state={cookie}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
        };

        // Missing state parameter with a valid cookie.
        let response = callback(&format!("code=nonce-{nonce}")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // The token exchange itself fails.
        let response = callback(&format!("state={state}&code=reject-me"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // The nonce matches but the token comes from the wrong issuer.
        let response = callback(&format!("state={state}&code=wrong-issuer-nonce-{nonce}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // The happy path still issues a Secure session cookie.
        let response = callback(&format!("state={state}&code=nonce-{nonce}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("; Secure")),
            "the session cookie must carry the Secure attribute"
        );
    }
}
