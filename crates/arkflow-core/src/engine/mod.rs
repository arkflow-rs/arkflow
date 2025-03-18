use crate::config::EngineConfig;
use std::process;
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

        for (i, mut stream) in streams.into_iter().enumerate() {
            info!("Starting flow #{}", i + 1);

            let handle = tokio::spawn(async move {
                match stream.run().await {
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
