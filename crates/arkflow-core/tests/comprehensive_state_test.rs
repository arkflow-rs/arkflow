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

//! Comprehensive test for state management integration

use arkflow_core::config::{
    EngineConfig, LoggingConfig, StateBackendType, StateManagementConfig, StreamStateConfig,
};
use arkflow_core::engine_builder::EngineBuilder;
use arkflow_core::input::{Input, InputBuilder, InputConfig, NoopAck};
use arkflow_core::output::{Output, OutputBuilder, OutputConfig};
use arkflow_core::pipeline::PipelineConfig;
use arkflow_core::stream::StreamConfig;
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Mock input for testing
struct MockInputBuilder;
struct MockInput {
    message_count: u32,
}

#[async_trait]
impl Input for MockInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn arkflow_core::input::Ack>), Error> {
        let batch = MessageBatch::from_string(&format!("test message {}", self.message_count))?;
        Ok((batch, Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl InputBuilder for MockInputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        Ok(Arc::new(MockInput { message_count: 0 }))
    }
}

// Mock output for testing
struct MockOutputBuilder;
struct MockOutput;

#[async_trait]
impl Output for MockOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, _batch: MessageBatch) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl OutputBuilder for MockOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(MockOutput))
    }
}

#[tokio::test]
async fn test_complete_state_management_integration() {
    println!("Testing complete state management integration...");

    // Register mock components
    let _ = arkflow_core::input::register_input_builder("mock", Arc::new(MockInputBuilder));
    let _ = arkflow_core::output::register_output_builder("mock", Arc::new(MockOutputBuilder));

    // Create configuration with state management enabled
    let config = EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                input_type: "mock".to_string(),
                name: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "mock".to_string(),
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
            checkpoint_interval_ms: 1000, // Short interval for testing
            retained_checkpoints: 3,
            exactly_once: true,
            state_timeout_ms: 3600000,
        },
    };

    // Create engine builder
    let mut engine_builder = EngineBuilder::new(config);

    // Build streams
    println!("Building streams with state management...");
    let streams = engine_builder
        .build_streams()
        .await
        .expect("Failed to build streams");

    // Verify streams were created
    assert_eq!(streams.len(), 1, "Expected 1 stream");

    // Check if state managers were created
    let state_managers = engine_builder.get_state_managers();
    assert_eq!(state_managers.len(), 1, "Expected 1 state manager");

    // Get the state manager
    let (operator_id, state_manager) = state_managers.iter().next().unwrap();
    assert_eq!(operator_id, "test-operator");

    // Test state operations
    {
        let mut manager = state_manager.write().await;

        // Test setting and getting state
        manager
            .set_state_value(operator_id, &"test_key", "test_value")
            .await
            .unwrap();
        let value: Option<String> = manager
            .get_state_value(operator_id, &"test_key")
            .await
            .unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        // Test state stats
        let stats = manager.get_state_stats().await;
        assert!(stats.enabled);
        assert_eq!(stats.local_states_count, 1);

        // Test checkpoint creation
        let checkpoint_id = manager.create_checkpoint().await.unwrap();
        assert!(checkpoint_id > 0);
    }

    // Shutdown
    engine_builder
        .shutdown()
        .await
        .expect("Failed to shutdown state managers");

    println!("State management integration test completed successfully!");
}

#[tokio::test]
async fn test_state_management_disabled() {
    println!("Testing state management when disabled...");

    // Register mock components
    let _ = arkflow_core::input::register_input_builder("mock", Arc::new(MockInputBuilder));
    let _ = arkflow_core::output::register_output_builder("mock", Arc::new(MockOutputBuilder));

    // Create configuration with state management disabled
    let config = EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                input_type: "mock".to_string(),
                name: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "mock".to_string(),
                name: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            temporary: None,
            state: None, // No state configuration
        }],
        logging: LoggingConfig {
            level: "warn".to_string(),
            file_path: None,
            format: arkflow_core::config::LogFormat::PLAIN,
        },
        health_check: Default::default(),
        state_management: StateManagementConfig {
            enabled: false, // Disabled globally
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 1000,
            retained_checkpoints: 3,
            exactly_once: true,
            state_timeout_ms: 3600000,
        },
    };

    // Create engine builder
    let mut engine_builder = EngineBuilder::new(config);

    // Build streams
    let streams = engine_builder
        .build_streams()
        .await
        .expect("Failed to build streams");

    // Check if no state managers were created
    let state_managers = engine_builder.get_state_managers();
    assert_eq!(state_managers.len(), 0, "Expected no state managers");

    // Shutdown should still work
    engine_builder.shutdown().await.expect("Failed to shutdown");

    println!("State management disabled test completed successfully!");
}
