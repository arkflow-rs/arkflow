use crate::config::EngineConfig;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use axum::extract::State;
use axum::http::StatusCode;
// 引入axum相关依赖
use axum::{routing::get, Router};

/// 健康检查状态
struct HealthState {
    /// 引擎是否已经初始化完成
    is_ready: AtomicBool,
    /// 引擎是否正在运行
    is_running: AtomicBool,
}

pub struct Engine {
    config: EngineConfig,
    /// 健康检查状态
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

    /// 启动健康检查服务器
    async fn start_health_check_server(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.health_check.enabled {
            return Ok(());
        }

        let health_state = self.health_state.clone();

        // 创建路由
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

        // 启动服务器
        tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(|e| error!("Health check server error: {}", e))
        });

        Ok(())
    }

    /// 健康检查处理函数
    async fn handle_health(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_running.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// 就绪检查处理函数
    async fn handle_readiness(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_ready.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// 存活检查处理函数
    async fn handle_liveness(_: State<Arc<HealthState>>) -> StatusCode {
        // 只要服务器能响应，就认为是存活的
        StatusCode::OK
    }
    /// Run the engine
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // 启动健康检查服务器
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

        // 设置就绪状态
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

        // 设置运行状态
        self.health_state.is_running.store(true, Ordering::SeqCst);

        // Wait for all flows to complete
        for handle in handles {
            handle.await?;
        }

        info!("All flow tasks have been complete");
        Ok(())
    }
}
