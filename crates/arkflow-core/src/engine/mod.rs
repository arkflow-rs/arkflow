/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 */

use crate::config::EngineConfig;
use crate::control_plane::ControlPlane;
use crate::runtime::RuntimeManager;
use std::error::Error;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// The stream-processing engine. HTTP transport is intentionally owned by
/// `arkflow-server`; this type only manages the runtime domain.
pub struct Engine {
    config: EngineConfig,
    runtime_manager: RuntimeManager,
    control_plane: ControlPlane,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let runtime_manager = RuntimeManager::new();
        let control_plane = ControlPlane::new(config.clone(), runtime_manager.clone());
        Self {
            config,
            runtime_manager,
            control_plane,
        }
    }

    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }

    pub fn control_plane(&self) -> ControlPlane {
        self.control_plane.clone()
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        self.run_with_cancellation(CancellationToken::new()).await
    }

    /// Run the engine domain without starting an HTTP server.
    pub async fn run_with_cancellation(
        &self,
        token: CancellationToken,
    ) -> Result<(), Box<dyn Error>> {
        let ids = self.config.stream_ids()?;
        for (index, stream_config) in self.config.streams.iter().enumerate() {
            let id = ids[index].clone();
            if stream_config.id.is_none() {
                tracing::warn!(stream_id = %id, "Stream has no explicit id; assign one to keep control-plane identity stable");
            }
            self.runtime_manager
                .register(id, stream_config.clone())
                .await?;
        }

        if let Err(error) = self.runtime_manager.start_all().await {
            let _ = self.runtime_manager.stop_all().await;
            return Err(Box::new(error));
        }

        self.control_plane.health().set_ready(true);
        self.control_plane.health().set_running(true);

        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token_clone = token.clone();
        let runtime_manager = self.runtime_manager.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => info!("Received SIGINT, exiting..."),
                _ = sigterm.recv() => info!("Received SIGTERM, exiting..."),
                _ = token_clone.cancelled() => info!("Cancellation requested, exiting..."),
            }
            token_clone.cancel();
            if let Err(error) = runtime_manager.stop_all().await {
                error!("Failed to stop all Stream runtimes: {}", error);
            }
        });

        self.runtime_manager.wait_all().await?;
        self.control_plane.health().set_running(false);
        info!("All flow tasks have been complete");
        Ok(())
    }
}
