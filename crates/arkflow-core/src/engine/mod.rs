/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 */

use crate::config::EngineConfig;
use crate::control_plane::ControlPlane;
use crate::executor::stream_adapter::StreamJobAdapter;
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
    ///
    /// YAML-declared `jobs` execute on the unified kernel (local mode,
    /// single process). Registered streams are compiled to JobSpecs by the
    /// RuntimeManager and use the same kernel path.
    pub async fn run_with_cancellation(
        &self,
        token: CancellationToken,
    ) -> Result<(), Box<dyn Error>> {
        let ids = self.config.stream_ids()?;
        self.config.job_specs()?; // validate declared jobs up front
        let validation = crate::configuration::validate_config(&self.config);
        if !validation.valid {
            let details = validation
                .errors
                .iter()
                .map(|issue| format!("{}: {}", issue.path, issue.message))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Box::new(crate::Error::Config(format!(
                "Configuration validation failed: {details}"
            ))));
        }
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

        // Local Jobs declared in YAML run on the unified kernel alongside the
        // compiled streams. They are bounded by the engine cancellation token.
        // `Resource` holds a `RefCell` (built per job inside the spawned
        // task). The deep build above has already validated every declared
        // Job before any stream starts, so startup failures here are limited
        // to the real resource connection path.
        let mut job_handles = Vec::new();
        let local_jobs_token = token.child_token();
        let (local_job_failure_tx, mut local_job_failure_rx) =
            tokio::sync::mpsc::unbounded_channel::<String>();
        for job in &self.config.jobs {
            let spec = job.clone();
            let token = local_jobs_token.clone();
            let failure_tx = local_job_failure_tx.clone();
            let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
            info!(job_id = %spec.id, "starting local Job on the unified kernel");
            let handle = tokio::spawn(async move {
                let adapter = StreamJobAdapter::new(None)?;
                let mut resource = crate::Resource {
                    temporary: std::collections::HashMap::new(),
                    input_names: std::cell::RefCell::new(Vec::new()),
                };
                let result = crate::executor::job_runner_adapter::run_job_with_checkpoints_started(
                    &spec,
                    &adapter,
                    &mut resource,
                    token,
                    Some(startup_tx),
                )
                .await;
                if let Err(error) = &result {
                    let _ = failure_tx.send(format!("local Job '{}' failed: {error}", spec.id));
                }
                result
            });
            job_handles.push(handle);
            match startup_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(message)) => {
                    local_jobs_token.cancel();
                    let _ = self.runtime_manager.stop_all().await;
                    for handle in job_handles {
                        let _ = handle.await;
                    }
                    return Err(Box::new(crate::Error::Process(message)));
                }
                Err(_) => {
                    local_jobs_token.cancel();
                    let _ = self.runtime_manager.stop_all().await;
                    for handle in job_handles {
                        let _ = handle.await;
                    }
                    return Err(Box::new(crate::Error::Process(
                        "local Job exited before startup completed".into(),
                    )));
                }
            }
        }

        self.control_plane.health().set_ready(true);
        self.control_plane.health().set_running(true);

        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token_clone = token.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => info!("Received SIGINT, exiting..."),
                _ = sigterm.recv() => info!("Received SIGTERM, exiting..."),
                _ = token_clone.cancelled() => info!("Cancellation requested, exiting..."),
            }
            token_clone.cancel();
        });

        tokio::select! {
            _ = token.cancelled() => {}
            failure = local_job_failure_rx.recv() => {
                local_jobs_token.cancel();
                let _ = self.runtime_manager.stop_all().await;
                self.runtime_manager.wait_all().await?;
                for handle in job_handles {
                    let _ = handle.await;
                }
                self.control_plane.health().set_ready(false);
                self.control_plane.health().set_running(false);
                let message = failure.unwrap_or_else(|| "local Job failed".into());
                return Err(Box::new(crate::Error::Process(message)));
            }
        }
        if let Err(error) = self.runtime_manager.stop_all().await {
            error!("Failed to stop all Stream runtimes: {}", error);
            return Err(Box::new(error));
        }
        self.runtime_manager.wait_all().await?;
        // Local kernel Jobs stop via the shared token; surface their results.
        for handle in job_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => error!("Local Job failed: {}", error),
                Err(error) => error!("Local Job task panicked: {}", error),
            }
        }
        self.control_plane.health().set_running(false);
        info!("All flow tasks have been complete");
        Ok(())
    }
}
