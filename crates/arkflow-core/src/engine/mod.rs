/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

use crate::component::{self, ComponentKind};
use crate::config::EngineConfig;
use crate::configuration::{
    parse_and_validate, redacted_config, validate_config, ConfigCandidate, ConfigVersionStore,
};
use crate::control::{ApiError, EngineStatus, OperationResult, StreamState};
use crate::runtime::RuntimeManager;
use axum::extract::{Json as ExtractJson, Path};
use axum::response::Response;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use subtle::ConstantTimeEq;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::response::Json;
// Import axum related dependencies
use axum::{
    routing::{get, post},
    Router,
};
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Health check status
struct HealthState {
    /// Whether the engine has been initialized
    is_ready: AtomicBool,
    /// Whether the engine is currently running
    is_running: AtomicBool,
}

#[derive(Clone)]
struct ControlState {
    health: Arc<HealthState>,
    runtime_manager: RuntimeManager,
    started_at: Instant,
    configuration: Arc<RwLock<EngineConfig>>,
    version_store: ConfigVersionStore,
    current_version: Arc<RwLock<Option<String>>>,
    api_token: Option<String>,
}

/// Readiness response structure for JSON serialization
#[derive(Serialize)]
struct ReadinessResponse {
    status: String,
    ready: bool,
}

/// Health response structure for JSON serialization
#[derive(Serialize)]
struct HealthResponse {
    status: String,
    running: bool,
}

/// Liveness response structure for JSON serialization
#[derive(Serialize)]
struct LivenessResponse {
    status: String,
    alive: bool,
}

/// The main engine that manages stream processing flows and health checks
///
/// The Engine is responsible for:
/// - Starting and managing the health check server
/// - Initializing and running all configured streams
/// - Handling graceful shutdown on signals
pub struct Engine {
    /// Engine configuration containing stream definitions and health check settings
    config: EngineConfig,
    /// Health check status shared between the engine and health check endpoints
    health_state: Arc<HealthState>,
    runtime_manager: RuntimeManager,
    started_at: Instant,
    configuration: Arc<RwLock<EngineConfig>>,
    version_store: ConfigVersionStore,
    current_version: Arc<RwLock<Option<String>>>,
}
impl Engine {
    /// Create a new engine with the provided configuration
    ///
    /// Initializes a new Engine instance with the given configuration and
    /// sets up the health state with default values (not ready, not running).
    ///
    /// # Arguments
    /// * `config` - The engine configuration containing stream definitions and settings
    pub fn new(config: EngineConfig) -> Self {
        let configuration = Arc::new(RwLock::new(config.clone()));
        Self {
            config,
            health_state: Arc::new(HealthState {
                is_ready: AtomicBool::new(false),
                is_running: AtomicBool::new(false),
            }),
            runtime_manager: RuntimeManager::new(),
            started_at: Instant::now(),
            configuration,
            version_store: ConfigVersionStore::new(".arkflow/config-history"),
            current_version: Arc::new(RwLock::new(None)),
        }
    }

    /// Access the process-local Stream registry used by control-plane routes.
    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }

    fn build_router(&self) -> Router {
        let health_check = &self.config.health_check;
        let state = Arc::new(ControlState {
            health: self.health_state.clone(),
            runtime_manager: self.runtime_manager.clone(),
            started_at: self.started_at,
            configuration: self.configuration.clone(),
            version_store: self.version_store.clone(),
            current_version: self.current_version.clone(),
            api_token: self.config.health_check.api_token.clone(),
        });
        let api_prefix = health_check.api_prefix.trim_end_matches('/').to_string();
        let api = Router::new()
            .route("/system", get(Self::handle_system))
            .route("/status", get(Self::handle_system))
            .route("/streams", get(Self::handle_streams))
            .route("/streams/{id}", get(Self::handle_stream))
            .route("/streams/{id}/start", post(Self::handle_stream_start))
            .route("/streams/{id}/stop", post(Self::handle_stream_stop))
            .route("/streams/{id}/restart", post(Self::handle_stream_restart))
            .route("/components", get(Self::handle_components))
            .route("/components/{kind}/{name}", get(Self::handle_component))
            .route("/schema", get(Self::handle_schema))
            .route("/events", get(Self::handle_events))
            .route("/config", get(Self::handle_config))
            .route("/config/validate", post(Self::handle_config_validate))
            .route("/config/apply", post(Self::handle_config_apply))
            .route("/config/versions", get(Self::handle_config_versions))
            .route("/config/rollback/{id}", post(Self::handle_config_rollback))
            .with_state(state.clone());

        let mut router = Router::new()
            .route(&health_check.health_path, get(Self::handle_health))
            .route(&health_check.readiness_path, get(Self::handle_readiness))
            .route(&health_check.liveness_path, get(Self::handle_liveness))
            .route("/metrics", get(Self::handle_metrics))
            .nest(&api_prefix, api)
            .with_state(state)
            .layer(TraceLayer::new_for_http());
        if !health_check.cors_origins.is_empty() {
            let mut cors = CorsLayer::new();
            for origin in &health_check.cors_origins {
                if let Ok(value) = origin.parse::<HeaderValue>() {
                    cors = cors.allow_origin(value);
                }
            }
            router = router.layer(cors.allow_methods(Any));
        }
        router
    }

    /// Start the health check server if enabled in configuration
    ///
    /// Sets up HTTP endpoints for health, readiness, and liveness checks.
    /// The server runs in a separate task and doesn't block the main execution.
    ///
    /// # Returns
    /// * `Ok(())` if the server started successfully or if health checks are disabled
    /// * `Err` if there was an error parsing the address or starting the server
    async fn start_health_check_server(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let health_check = &self.config.health_check;

        if !health_check.enabled {
            return Ok(());
        }

        let app = self.build_router();

        let addr = &health_check.address;
        let addr = addr.clone();
        info!("Starting health check server on {}", &addr);

        let listener = TcpListener::bind(&addr).await?;
        info!("Control HTTP server bound on {}", &addr);

        tokio::spawn(async move {
            let server = axum::serve(listener, app.into_make_service());

            // Run the server with graceful shutdown
            let graceful = server.with_graceful_shutdown(Self::shutdown_signal(cancellation_token));
            if let Err(e) = graceful.await {
                error!("Health check server error: {}", e);
            } else {
                info!("Health check server stopped");
            }
        });

        Ok(())
    }

    async fn shutdown_signal(cancellation_token: CancellationToken) {
        cancellation_token.cancelled().await;
    }

    /// Health check handler function that returns the overall health status
    ///
    /// Returns OK (200) with JSON body if the engine is running,
    /// otherwise SERVICE_UNAVAILABLE (503) with JSON body
    ///
    /// # Arguments
    /// * `state` - The shared health state containing running status
    async fn handle_health(State(state): State<Arc<ControlState>>) -> impl IntoResponse {
        let is_running = state.health.is_running.load(Ordering::SeqCst);
        let status = if is_running { "healthy" } else { "unhealthy" };

        let response = HealthResponse {
            status: status.to_string(),
            running: is_running,
        };

        let status_code = if is_running {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        (status_code, Json(response))
    }

    /// Readiness check handler function that indicates if the engine is ready to process requests
    ///
    /// Returns OK (200) with JSON body if the engine is initialized and ready,
    /// otherwise SERVICE_UNAVAILABLE (503) with JSON body
    ///
    /// # Arguments
    /// * `state` - The shared health state containing readiness status
    async fn handle_readiness(State(state): State<Arc<ControlState>>) -> impl IntoResponse {
        let is_ready = state.health.is_ready.load(Ordering::SeqCst);
        let status = if is_ready { "ready" } else { "not ready" };

        let response = ReadinessResponse {
            status: status.to_string(),
            ready: is_ready,
        };

        let status_code = if is_ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        (status_code, Json(response))
    }

    /// Liveness check handler function that indicates if the engine process is alive
    ///
    /// Always returns OK (200) with JSON body as long as the server can respond to the request
    ///
    /// # Arguments
    /// * `_` - Unused health state parameter
    async fn handle_liveness(_: State<Arc<ControlState>>) -> impl IntoResponse {
        // As long as the server can respond, it is considered alive
        let response = LivenessResponse {
            status: "alive".to_string(),
            alive: true,
        };

        (StatusCode::OK, Json(response))
    }

    async fn handle_system(State(state): State<Arc<ControlState>>) -> impl IntoResponse {
        let streams = state.runtime_manager.snapshots().await;
        let streams_running = streams
            .iter()
            .filter(|stream| stream.state == StreamState::Running)
            .count();
        let streams_failed = streams
            .iter()
            .filter(|stream| stream.state == StreamState::Failed)
            .count();
        Json(EngineStatus {
            version: env!("CARGO_PKG_VERSION").to_string(),
            state: if state.health.is_running.load(Ordering::SeqCst) {
                "running".to_string()
            } else {
                "starting".to_string()
            },
            uptime_seconds: state.started_at.elapsed().as_secs(),
            streams_total: streams.len(),
            streams_running,
            streams_failed,
        })
    }

    async fn handle_streams(State(state): State<Arc<ControlState>>) -> impl IntoResponse {
        Json(state.runtime_manager.snapshots().await)
    }

    async fn handle_stream(
        State(state): State<Arc<ControlState>>,
        Path(id): Path<String>,
    ) -> Response {
        match state.runtime_manager.get(&id).await {
            Some(entry) => Json(entry.lock().await.snapshot()).into_response(),
            None => api_error(
                StatusCode::NOT_FOUND,
                ApiError {
                    code: "not_found".to_string(),
                    message: format!("Unknown stream: {id}"),
                    field: None,
                    stream_id: Some(id),
                },
            ),
        }
    }

    async fn handle_stream_start(
        State(state): State<Arc<ControlState>>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        Self::handle_stream_operation(state, id, "start", &headers).await
    }

    async fn handle_stream_stop(
        State(state): State<Arc<ControlState>>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        Self::handle_stream_operation(state, id, "stop", &headers).await
    }

    async fn handle_stream_restart(
        State(state): State<Arc<ControlState>>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        Self::handle_stream_operation(state, id, "restart", &headers).await
    }

    async fn handle_stream_operation(
        state: Arc<ControlState>,
        id: String,
        operation: &str,
        headers: &HeaderMap,
    ) -> Response {
        if !is_authorized(&state, headers) {
            return unauthorized();
        }
        if state.runtime_manager.get(&id).await.is_none() {
            return api_error(
                StatusCode::NOT_FOUND,
                ApiError {
                    code: "not_found".to_string(),
                    message: format!("Unknown stream: {id}"),
                    field: None,
                    stream_id: Some(id),
                },
            );
        }

        let result = match operation {
            "start" => state.runtime_manager.start(&id).await,
            "stop" => state.runtime_manager.stop(&id).await,
            "restart" => state.runtime_manager.restart(&id).await,
            _ => unreachable!("only known lifecycle operations are routed"),
        };
        match result {
            Ok(()) => match state.runtime_manager.get(&id).await {
                Some(entry) => {
                    let status = entry.lock().await.snapshot();
                    Json(OperationResult {
                        operation: operation.to_string(),
                        stream_id: id,
                        state: status.state,
                        message: None,
                    })
                    .into_response()
                }
                None => api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiError {
                        code: "runtime_disappeared".to_string(),
                        message: "Stream runtime disappeared after operation".to_string(),
                        field: None,
                        stream_id: None,
                    },
                ),
            },
            Err(error) => api_error(
                StatusCode::CONFLICT,
                ApiError {
                    code: "operation_failed".to_string(),
                    message: error.to_string(),
                    field: None,
                    stream_id: Some(id),
                },
            ),
        }
    }

    async fn handle_components() -> impl IntoResponse {
        let components: Vec<Value> = component::list_components()
            .into_iter()
            .map(|(kind, metadata)| {
                serde_json::json!({
                    "kind": kind,
                    "name": metadata.name,
                    "description": metadata.description,
                    "config_optional": metadata.config_optional,
                    "config_schema": metadata.config_schema,
                    "config_example": metadata.config_example,
                })
            })
            .collect();
        Json(components)
    }

    async fn handle_component(Path((kind, name)): Path<(String, String)>) -> Response {
        let kind = match kind.parse::<ComponentKind>() {
            Ok(kind) => kind,
            Err(error) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    ApiError {
                        code: "invalid_component_kind".to_string(),
                        message: error.to_string(),
                        field: Some("kind".to_string()),
                        stream_id: None,
                    },
                )
            }
        };
        match component::get_component_metadata(kind, &name) {
            Some(metadata) => Json(serde_json::json!({
                "kind": kind,
                "name": metadata.name,
                "description": metadata.description,
                "config_optional": metadata.config_optional,
                "config_schema": metadata.config_schema,
                "config_example": metadata.config_example,
            }))
            .into_response(),
            None => api_error(
                StatusCode::NOT_FOUND,
                ApiError {
                    code: "not_found".to_string(),
                    message: format!("Unknown component: {kind}/{name}"),
                    field: None,
                    stream_id: None,
                },
            ),
        }
    }

    async fn handle_schema() -> impl IntoResponse {
        Json(component::build_config_schema())
    }

    async fn handle_events(State(state): State<Arc<ControlState>>) -> impl IntoResponse {
        Json(state.runtime_manager.event_store().snapshot().await)
    }

    async fn handle_config(State(state): State<Arc<ControlState>>, headers: HeaderMap) -> Response {
        if !is_authorized(&state, &headers) {
            return unauthorized();
        }
        let config = state.configuration.read().await;
        match redacted_config(&config) {
            Ok(config) => Json(config).into_response(),
            Err(error) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiError {
                    code: "config_serialization_failed".to_string(),
                    message: error.to_string(),
                    field: None,
                    stream_id: None,
                },
            ),
        }
    }

    async fn handle_config_validate(
        State(state): State<Arc<ControlState>>,
        headers: HeaderMap,
        ExtractJson(candidate): ExtractJson<ConfigCandidate>,
    ) -> Response {
        if !is_authorized(&state, &headers) {
            return unauthorized();
        }
        match parse_and_validate(&candidate) {
            Ok(report) => Json(report).into_response(),
            Err(issue) => Json(crate::configuration::ConfigValidationReport {
                valid: false,
                errors: vec![issue],
            })
            .into_response(),
        }
    }

    async fn handle_config_versions(
        State(state): State<Arc<ControlState>>,
        headers: HeaderMap,
    ) -> Response {
        if !is_authorized(&state, &headers) {
            return unauthorized();
        }
        match state.version_store.list() {
            Ok(versions) => Json(versions).into_response(),
            Err(error) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiError {
                    code: "config_version_list_failed".to_string(),
                    message: error.to_string(),
                    field: None,
                    stream_id: None,
                },
            ),
        }
    }

    async fn handle_config_apply(
        State(state): State<Arc<ControlState>>,
        headers: HeaderMap,
        ExtractJson(candidate): ExtractJson<ConfigCandidate>,
    ) -> Response {
        if !is_authorized(&state, &headers) {
            return unauthorized();
        }
        let config = match candidate.parse() {
            Ok(config) => config,
            Err(issue) => {
                return Json(serde_json::json!({
                    "valid": false,
                    "errors": [issue]
                }))
                .into_response()
            }
        };
        let report = validate_config(&config);
        if !report.valid {
            return Json(report).into_response();
        }

        let parent_version = state.current_version.read().await.clone();
        let version = match state
            .version_store
            .save_with_parent(&candidate, parent_version)
        {
            Ok(version) => version,
            Err(error) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiError {
                        code: "config_version_save_failed".to_string(),
                        message: error.to_string(),
                        field: None,
                        stream_id: None,
                    },
                )
            }
        };
        let affected_streams = match state.runtime_manager.replace_config(&config).await {
            Ok(affected_streams) => affected_streams,
            Err(error) => {
                return api_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    ApiError {
                        code: "config_apply_failed".to_string(),
                        message: error.to_string(),
                        field: None,
                        stream_id: None,
                    },
                )
            }
        };
        *state.configuration.write().await = config;
        *state.current_version.write().await = Some(version.id.clone());
        Json(serde_json::json!({
            "version": version,
            "affected_streams": affected_streams
        }))
        .into_response()
    }

    async fn handle_config_rollback(
        State(state): State<Arc<ControlState>>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        if !is_authorized(&state, &headers) {
            return unauthorized();
        }
        let candidate = match state.version_store.load(&id) {
            Ok(candidate) => candidate,
            Err(error) => {
                return api_error(
                    StatusCode::NOT_FOUND,
                    ApiError {
                        code: "config_version_not_found".to_string(),
                        message: error.to_string(),
                        field: Some("id".to_string()),
                        stream_id: None,
                    },
                )
            }
        };
        let config = match candidate.parse() {
            Ok(config) => config,
            Err(error) => {
                return api_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    ApiError {
                        code: "config_version_invalid".to_string(),
                        message: error.message,
                        field: Some(error.path),
                        stream_id: None,
                    },
                )
            }
        };
        let report = validate_config(&config);
        if !report.valid {
            return Json(report).into_response();
        }
        let version = match state
            .version_store
            .save_with_parent(&candidate, Some(id.clone()))
        {
            Ok(version) => version,
            Err(error) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiError {
                        code: "config_version_save_failed".to_string(),
                        message: error.to_string(),
                        field: None,
                        stream_id: None,
                    },
                )
            }
        };
        let affected_streams = match state.runtime_manager.replace_config(&config).await {
            Ok(affected_streams) => affected_streams,
            Err(error) => {
                return api_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    ApiError {
                        code: "config_rollback_failed".to_string(),
                        message: error.to_string(),
                        field: None,
                        stream_id: None,
                    },
                )
            }
        };
        *state.configuration.write().await = config;
        *state.current_version.write().await = Some(version.id.clone());
        Json(serde_json::json!({
            "rollback_from": id,
            "version": version,
            "affected_streams": affected_streams
        }))
        .into_response()
    }

    async fn handle_metrics(State(state): State<Arc<ControlState>>) -> Response {
        let mut body = String::new();
        for stream in state.runtime_manager.snapshots().await {
            let labels = format!("stream_id=\"{}\"", escape_metric_label(&stream.id));
            body.push_str(&format!(
                "arkflow_stream_input_batches{{{labels}}} {}\n",
                stream.metrics.input_batches
            ));
            body.push_str(&format!(
                "arkflow_stream_input_messages{{{labels}}} {}\n",
                stream.metrics.input_messages
            ));
            body.push_str(&format!(
                "arkflow_stream_input_errors{{{labels}}} {}\n",
                stream.metrics.input_errors
            ));
            body.push_str(&format!(
                "arkflow_stream_input_reconnects{{{labels}}} {}\n",
                stream.metrics.input_reconnects
            ));
            body.push_str(&format!(
                "arkflow_stream_processing_errors{{{labels}}} {}\n",
                stream.metrics.processing_errors
            ));
            body.push_str(&format!(
                "arkflow_stream_output_batches{{{labels}}} {}\n",
                stream.metrics.output_batches
            ));
            body.push_str(&format!(
                "arkflow_stream_output_messages{{{labels}}} {}\n",
                stream.metrics.output_messages
            ));
            body.push_str(&format!(
                "arkflow_stream_output_errors{{{labels}}} {}\n",
                stream.metrics.output_errors
            ));
            body.push_str(&format!(
                "arkflow_stream_restarts{{{labels}}} {}\n",
                stream.metrics.restarts
            ));
            body.push_str(&format!(
                "arkflow_stream_state{{{labels},state=\"{:?}\"}} 1\n",
                stream.state
            ));
        }

        let mut response = body.into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4"),
        );
        response
    }
    /// Run the engine and all configured streams
    ///
    /// This method:
    /// 1. Starts the health check server if enabled
    /// 2. Initializes all configured streams
    /// 3. Sets up signal handlers for graceful shutdown
    /// 4. Runs all streams concurrently
    /// 5. Waits for all streams to complete
    ///
    /// Returns an error if any part of the initialization or execution fails
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();

        // Start the health check server
        self.start_health_check_server(token.clone()).await?;

        let ids = self.config.stream_ids()?;
        for (index, stream_config) in self.config.streams.iter().enumerate() {
            let id = ids[index].clone();
            if stream_config.id.is_none() {
                tracing::warn!(
                    stream_id = %id,
                    "Stream has no explicit id; assign one to keep control-plane identity stable"
                );
            }
            self.runtime_manager
                .register(id, stream_config.clone())
                .await?;
        }

        if let Err(error) = self.runtime_manager.start_all().await {
            let _ = self.runtime_manager.stop_all().await;
            return Err(Box::new(error));
        }

        // Set the readiness status
        self.health_state.is_ready.store(true, Ordering::SeqCst);
        // Set up signal handlers
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token_clone = token.clone();
        let runtime_manager = self.runtime_manager.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("Received SIGINT, exiting...");

                },
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, exiting...");
                }
            }

            token_clone.cancel();
            if let Err(error) = runtime_manager.stop_all().await {
                error!("Failed to stop all Stream runtimes: {}", error);
            }
        });

        // Set the running status
        self.health_state.is_running.store(true, Ordering::SeqCst);

        // Wait for all supervised runtimes to complete.
        self.runtime_manager.wait_all().await?;
        self.health_state.is_running.store(false, Ordering::SeqCst);

        info!("All flow tasks have been complete");
        Ok(())
    }
}

fn api_error(status: StatusCode, error: ApiError) -> Response {
    (status, Json(error)).into_response()
}

fn is_authorized(state: &ControlState, headers: &HeaderMap) -> bool {
    let Some(expected) = &state.api_token else {
        return true;
    };
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    value
        .strip_prefix("Bearer ")
        .is_some_and(|received| received.as_bytes().ct_eq(expected.as_bytes()).into())
}

fn unauthorized() -> Response {
    api_error(
        StatusCode::UNAUTHORIZED,
        ApiError {
            code: "unauthorized".to_string(),
            message: "A valid Bearer token is required".to_string(),
            field: None,
            stream_id: None,
        },
    )
}

fn escape_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
    use crate::input::InputConfig;
    use crate::output::OutputConfig;
    use crate::pipeline::PipelineConfig;
    use crate::stream::StreamConfig;
    use axum::body::Body;
    use axum::http::Request;
    use tokio_util::sync::CancellationToken;
    use tower::util::ServiceExt;

    fn engine() -> Engine {
        Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        })
    }

    #[tokio::test]
    async fn unified_router_preserves_health_and_exposes_read_only_api() {
        let response = engine()
            .build_router()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let response = engine()
            .build_router()
            .oneshot(Request::get("/api/v1/streams").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = engine()
            .build_router()
            .oneshot(
                Request::get("/api/v1/streams/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = engine()
            .build_router()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
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

        let response = engine()
            .build_router()
            .oneshot(Request::get("/api/v1/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = serde_json::json!({
            "format": "json",
            "content": "{\"streams\":[]}"
        });
        let response = engine()
            .build_router()
            .oneshot(
                Request::post("/api/v1/config/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn server_bind_failure_is_returned_to_startup() {
        let mut health = HealthCheckConfig::default();
        health.address = "not-an-address".to_string();
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });

        assert!(engine
            .start_health_check_server(CancellationToken::new())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn configured_bearer_token_protects_control_operations() {
        let mut health = HealthCheckConfig::default();
        health.api_token = Some("secret".to_string());
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });
        let response = engine
            .build_router()
            .oneshot(Request::get("/api/v1/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = engine
            .build_router()
            .oneshot(
                Request::get("/api/v1/config")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn configured_bearer_token_rejects_lifecycle_writes() {
        let mut health = HealthCheckConfig::default();
        health.api_token = Some("secret".to_string());
        let engine = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });
        let response = engine
            .build_router()
            .oneshot(
                Request::post("/api/v1/streams/missing/restart")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cors_is_denied_by_default_and_explicitly_allowlisted() {
        let response = engine()
            .build_router()
            .oneshot(
                Request::get("/api/v1/system")
                    .header("origin", "https://console.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_none());

        let mut health = HealthCheckConfig::default();
        health.cors_origins = vec!["https://console.example".to_string()];
        let configured = Engine::new(EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        });
        let response = configured
            .build_router()
            .oneshot(
                Request::get("/api/v1/system")
                    .header("origin", "https://console.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "https://console.example"
        );
    }

    #[tokio::test]
    async fn lifecycle_api_observes_stop_state_transition() {
        let engine = engine();
        let manager = engine.runtime_manager();
        manager
            .register(
                "orders".into(),
                StreamConfig {
                    id: Some("orders".into()),
                    input: InputConfig {
                        input_type: "missing".into(),
                        name: None,
                        codec: None,
                        config: None,
                    },
                    pipeline: PipelineConfig {
                        thread_num: 1,
                        processors: vec![],
                    },
                    output: OutputConfig {
                        output_type: "missing".into(),
                        name: None,
                        codec: None,
                        config: None,
                    },
                    error_output: None,
                    buffer: None,
                    durability: None,
                    temporary: None,
                },
            )
            .await
            .unwrap();
        let entry = manager.get("orders").await.unwrap();
        let cancellation = entry.lock().await.cancellation.clone();
        entry.lock().await.state = StreamState::Running;
        entry.lock().await.handle = Some(tokio::spawn(async move {
            cancellation.cancelled().await;
            Ok(())
        }));

        let response = engine
            .build_router()
            .oneshot(
                Request::post("/api/v1/streams/orders/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(manager.snapshots().await[0].state, StreamState::Stopped);
    }
}
