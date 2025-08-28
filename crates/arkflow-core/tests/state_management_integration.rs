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

//! Test state management integration

use arkflow_core::config::{
    EngineConfig, LoggingConfig, StateBackendType, StateManagementConfig, StreamStateConfig,
};
use arkflow_core::engine::Engine;
use arkflow_core::input::InputConfig;
use arkflow_core::output::OutputConfig;
use arkflow_core::pipeline::PipelineConfig;
use arkflow_core::stream::StreamConfig;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_state_management_integration() {
    // Create a temporary input file
    let temp_dir = "/tmp/arkflow_test";
    fs::create_dir_all(temp_dir).unwrap();
    let input_path = format!("{}/test_input.txt", temp_dir);

    fs::write(
        &input_path,
        "Hello World\nThis is a test\nState management test\n",
    )
    .unwrap();

    // Create engine configuration with state management
    let config = EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                input_type: "file".to_string(),
                name: None,
                config: Some(serde_json::json!({
                    "path": input_path,
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
            level: "warn".to_string(),
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

    // Create and run engine
    let engine = Engine::new(config);

    // Run the engine
    let result = engine.run().await;

    // Clean up
    fs::remove_dir_all(temp_dir).ok();

    // The test passes if it runs without error
    assert!(result.is_ok());
}
