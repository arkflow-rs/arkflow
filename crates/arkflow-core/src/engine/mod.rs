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

use crate::config::EngineConfig;
use crate::input::Input;
use crate::output::Output;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Json;
// Import axum related dependencies
use axum::{routing::get, Router};
use serde::Serialize;
use tokio::net::TcpListener;

/// Holds references to stream components for health checking
struct StreamComponents {
    input: Arc<dyn Input>,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
}

struct EngineApiState {
    health_state: Arc<HealthState>,
    components: Vec<StreamComponents>,
}

/// Health check status
struct HealthState {
    /// Whether the engine is currently running
    is_running: AtomicBool,
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
        Self {
            config,
            health_state: Arc::new(HealthState {
                is_running: AtomicBool::new(false),
            }),
        }
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
        components: Vec<StreamComponents>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let health_check = &self.config.health_check;

        if !health_check.enabled {
            return Ok(());
        }

        let health_state = self.health_state.clone();

        // Create routes
        let app = Router::new()
            .route(&*health_check.health_path, get(Self::handle_health))
            .route(&*health_check.readiness_path, get(Self::handle_readiness))
            .route(&*health_check.liveness_path, get(Self::handle_liveness))
            .with_state(Arc::new(EngineApiState {
                health_state,
                components,
            }));

        let addr = &health_check.address;
        let addr = addr.clone();
        info!("Starting health check server on {}", &addr);

        // Start the server
        tokio::spawn(async move {
            let server = axum::serve(
                TcpListener::bind(addr).await.expect("bind error"),
                app.into_make_service(),
            );

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
    async fn handle_health(State(state): State<Arc<EngineApiState>>) -> impl IntoResponse {
        let is_running = state.health_state.is_running.load(Ordering::SeqCst);
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
    async fn handle_readiness(State(state): State<Arc<EngineApiState>>) -> impl IntoResponse {
        let mut is_ready = true;

        for component in state.components.iter() {
            if !component.input.check_ready().await.is_ok()
                || !component.output.check_ready().await.is_ok()
            {
                is_ready = false;
            }
            if let Some(error_output) = component.error_output.as_ref() {
                if !error_output.check_ready().await.is_ok() {
                    is_ready = false;
                }
            }
            if !is_ready {
                break;
            }
        }

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
    async fn handle_liveness(_: State<Arc<EngineApiState>>) -> impl IntoResponse {
        // As long as the server can respond, it is considered alive
        let response = LivenessResponse {
            status: "alive".to_string(),
            alive: true,
        };

        (StatusCode::OK, Json(response))
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

        // Create and run all flows
        let mut streams = Vec::new();
        let mut handles = Vec::new();

        for (i, stream_config) in self.config.streams.iter().enumerate() {
            info!("Initializing flow #{}", i + 1);

            match stream_config.build() {
                Ok(stream) => {
                    streams.push(stream);
                }
                Err(e) => {
                    error!("Initializing flow #{} error: {}", i + 1, e);
                    process::exit(1);
                }
            }
        }

        let components = streams
            .iter()
            .map(|stream| StreamComponents {
                input: stream.get_input(),
                output: stream.get_output(),
                error_output: stream.get_error_output(),
            })
            .collect();

        // Start the health check server
        self.start_health_check_server(token.clone(), components)
            .await?;

        // Set up signal handlers
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token_clone = token.clone();
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
        });

        for (i, mut stream) in streams.into_iter().enumerate() {
            info!("Starting flow #{}", i + 1);
            let cancellation_token = token.clone();
            let handle = tokio::spawn(async move {
                match stream.run(cancellation_token).await {
                    Ok(_) => info!("Flow #{} completed successfully", i + 1),
                    Err(e) => {
                        error!("Flow #{} ran with error: {}", i + 1, e)
                    }
                }
            });

            handles.push(handle);
        }

        // Set the running status
        self.health_state.is_running.store(true, Ordering::SeqCst);

        // Wait for all flows to complete
        for handle in handles {
            handle.await?;
        }

        info!("All flow tasks have been complete");
        Ok(())
    }
}
