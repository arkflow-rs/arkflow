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

//! Simple test to verify state management integration

use arkflow_core::config::{
    EngineConfig, LoggingConfig, StateBackendType, StateManagementConfig, StreamStateConfig,
};
use arkflow_core::engine_builder::EngineBuilder;
use arkflow_core::input::InputConfig;
use arkflow_core::output::OutputConfig;
use arkflow_core::pipeline::PipelineConfig;
use arkflow_core::stream::StreamConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing state management integration...");

    // Create configuration with state management enabled
    let config = EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                input_type: "file".to_string(),
                name: None,
                config: Some(serde_json::json!({
                    "path": "./examples/data/test.txt",
                    "format": "text"
                })),
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "stdout".to_string(),
                name: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            temporary: None,
            state: Some(StreamStateConfig {
                operator_id: "test-operator".to_string(),
                enabled: true,
                state_timeout_ms: Some(60000),
                custom_keys: Some(vec!["message_count".to_string()]),
            }),
        }],
        logging: LoggingConfig {
            level: "info".to_string(),
            file_path: None,
            format: arkflow_core::config::LogFormat::PLAIN,
        },
        health_check: Default::default(),
        state_management: StateManagementConfig {
            enabled: true,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 10000,
            retained_checkpoints: 3,
            exactly_once: true,
            state_timeout_ms: 3600000,
        },
    };

    // Create engine builder
    let mut engine_builder = EngineBuilder::new(config);

    // Build streams
    println!("Building streams with state management...");
    let streams = engine_builder.build_streams().await?;

    // Check if state managers were created
    let state_managers = engine_builder.get_state_managers();
    println!("Created {} state managers", state_managers.len());

    for (operator_id, _) in state_managers {
        println!("State manager created for operator: {}", operator_id);
    }

    // Shutdown
    engine_builder.shutdown().await?;

    println!("State management integration test completed successfully!");
    Ok(())
}
