use crate::config::EngineConfig;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use axum::extract::State;
use axum::http::StatusCode;
// Import axum related dependencies
use axum::{routing::get, Router};

/// Health check status
struct HealthState {
    /// Whether the engine has been initialized
    is_ready: AtomicBool,
    /// Whether the engine is currently running
    is_running: AtomicBool,
}

pub struct Engine {
    config: EngineConfig,
    /// Health check status
    health_state: Arc<HealthState>,
}
impl Engine {
    /// Create a new engine
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            health_state: Arc::new(HealthState {
                is_ready: AtomicBool::new(false),
                is_running: AtomicBool::new(false),
            }),
        }
    }

    /// Start the health check server
    async fn start_health_check_server(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.health_check.enabled {
            return Ok(());
        }

        let health_state = self.health_state.clone();

        // Create routes
        let app = Router::new()
            .route(&self.config.health_check.path, get(Self::handle_health))
            .route(
                &self.config.health_check.readiness_path,
                get(Self::handle_readiness),
            )
            .route(
                &self.config.health_check.liveness_path,
                get(Self::handle_liveness),
            )
            .with_state(health_state);

        let addr = self
            .config
            .health_check
            .address
            .parse()
            .map_err(|e| format!("Invalid health check address: {}", e))?;

        info!("Starting health check server on {}", addr);

        // Start the server
        tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(|e| error!("Health check server error: {}", e))
        });

        Ok(())
    }

    /// Health check handler function
    async fn handle_health(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_running.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// Readiness check handler function
    async fn handle_readiness(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_ready.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// Liveness check handler function
    async fn handle_liveness(_: State<Arc<HealthState>>) -> StatusCode {
        // As long as the server can respond, it is considered alive
        StatusCode::OK
    }
    /// Run the engine
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start the health check server
        self.start_health_check_server().await?;

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

        // Set the readiness status
        self.health_state.is_ready.store(true, Ordering::SeqCst);
        // Set up signal handlers
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token = CancellationToken::new();
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
