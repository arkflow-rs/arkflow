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

//! Example of running stateful streams with configuration

use arkflow::config::EngineConfig;
use arkflow::engine_builder::EngineBuilder;
use arkflow::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration from file
    let config_path = "examples/stateful_example.yaml";
    info!("Loading configuration from: {}", config_path);

    let engine_config = EngineConfig::from_file(config_path)?;

    // Create engine builder
    let mut engine_builder = EngineBuilder::new(engine_config);

    // Build all streams with state management
    info!("Building streams with state management...");
    let mut streams = engine_builder.build_streams().await?;

    // Get state managers for monitoring
    let state_managers = engine_builder.get_state_managers();
    info!("Created {} state managers", state_managers.len());

    // Create cancellation token for graceful shutdown
    let cancellation_token = CancellationToken::new();

    // Spawn tasks for each stream
    let mut handles = Vec::new();
    for (i, mut stream) in streams.into_iter().enumerate() {
        let token = cancellation_token.clone();

        let handle = tokio::spawn(async move {
            info!("Starting stream {}", i);
            if let Err(e) = stream.run(token).await {
                error!("Stream {} failed: {}", i, e);
            }
            info!("Stream {} stopped", i);
        });

        handles.push(handle);
    }

    // Spawn monitoring task
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            // Print state statistics
            for (operator_id, state_manager) in state_managers {
                let stats = {
                    let manager = state_manager.read().await;
                    manager.get_state_stats().await
                };

                info!(
                    "Operator '{}' - Active transactions: {}, States: {}, Checkpoint ID: {}",
                    operator_id,
                    stats.active_transactions,
                    stats.local_states_count,
                    stats.current_checkpoint_id
                );
            }
        }
    });

    // Wait for Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    // Cancel all streams
    cancellation_token.cancel();

    // Abort monitoring task
    monitor_handle.abort();

    // Wait for all streams to finish
    for handle in handles {
        handle.await?;
    }

    // Shutdown state managers
    info!("Shutting down state managers...");
    engine_builder.shutdown().await?;

    info!("All streams stopped successfully");
    Ok(())
}

// Example of programmatic configuration
pub fn create_example_config() -> EngineConfig {
    use arkflow::config::{
        EngineConfig, LoggingConfig, S3StateBackendConfig, StateBackendType, StateManagementConfig,
        StreamStateConfig,
    };
    use arkflow::input::InputConfig;
    use arkflow::output::OutputConfig;
    use arkflow::pipeline::PipelineConfig;
    use arkflow::stream::StreamConfig;

    EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                // ... input configuration
                r#type: "file".to_string(),
                ..Default::default()
            },
            pipeline: PipelineConfig {
                // ... pipeline configuration
                thread_num: 2,
                processors: vec![],
            },
            output: OutputConfig {
                // ... output configuration
                r#type: "stdout".to_string(),
                ..Default::default()
            },
            error_output: None,
            buffer: None,
            temporary: None,
            state: Some(StreamStateConfig {
                operator_id: "example-processor".to_string(),
                enabled: true,
                state_timeout_ms: Some(3600000),
                custom_keys: Some(vec!["counter".to_string()]),
            }),
        }],
        logging: LoggingConfig {
            level: "info".to_string(),
            file_path: None,
            format: arkflow::config::LogFormat::PLAIN,
        },
        health_check: Default::default(),
        state_management: StateManagementConfig {
            enabled: true,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 30000,
            retained_checkpoints: 5,
            exactly_once: true,
            state_timeout_ms: 86400000,
        },
    }
}
