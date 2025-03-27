use crate::config::EngineConfig;
use std::process;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct Engine {
    config: EngineConfig,
}
impl Engine {
    /// Create a new engine
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }
    /// Run the engine
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
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

        // Wait for all flows to complete
        for handle in handles {
            handle.await?;
        }

        info!("All flow tasks have been complete");
        Ok(())
    }
}
