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

//! Distributed Acknowledgment Usage Example
//!
//! This example demonstrates how to use the distributed acknowledgment system
//! in various ways within Arkflow.

use arkflow_core::config::EngineConfig;
use arkflow_core::distributed_ack_config::DistributedAckConfig;
use arkflow_core::distributed_ack_init::init_distributed_ack_components;
use arkflow_core::distributed_ack_integration::DistributedAckBuilder;
use arkflow_core::input::InputConfig;
use arkflow_core::output::OutputConfig;
use arkflow_core::pipeline::PipelineConfig;
use arkflow_core::processor::ProcessorConfig;
use arkflow_core::stream::StreamConfig;
use arkflow_core::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize distributed acknowledgment components
    init_distributed_ack_components()?;
    println!("✓ Distributed acknowledgment components initialized");

    // Example 1: Using distributed acknowledgment at the stream level
    println!("\n=== Example 1: Stream-level distributed acknowledgment ===");
    example_stream_level_distributed_ack().await?;

    // Example 2: Using distributed acknowledgment as an input wrapper
    println!("\n=== Example 2: Input-level distributed acknowledgment ===");
    example_input_level_distributed_ack().await?;

    // Example 3: Using distributed acknowledgment as a processor wrapper
    println!("\n=== Example 3: Processor-level distributed acknowledgment ===");
    example_processor_level_distributed_ack().await?;

    // Example 4: Using configuration file
    println!("\n=== Example 4: Configuration-based setup ===");
    example_configuration_based_setup().await?;

    Ok(())
}

/// Example 1: Stream-level distributed acknowledgment
async fn example_stream_level_distributed_ack() -> Result<(), Error> {
    // Create a basic stream configuration
    let stream_config = StreamConfig {
        input: InputConfig {
            input_type: "memory".to_string(),
            name: Some("memory_input".to_string()),
            config: Some(serde_json::json!({
                "data": ["message1", "message2", "message3"]
            })),
        },
        pipeline: PipelineConfig {
            thread_num: 2,
            processors: vec![],
        },
        output: OutputConfig {
            output_type: "memory".to_string(),
            name: Some("memory_output".to_string()),
            config: None,
        },
        error_output: None,
        buffer: None,
        temporary: None,
        reliable_ack: None,
        distributed_ack: Some(DistributedAckConfig {
            enabled: true,
            node_id: "node-1".to_string(),
            cluster_nodes: vec!["node-1:8080".to_string()],
            object_storage: Some(arkflow_core::object_storage::ObjectStorageConfig {
                storage_type: "local".to_string(),
                endpoint: None,
                region: None,
                access_key_id: None,
                secret_access_key: None,
                bucket: None,
                path: Some("./distributed_ack_storage".to_string()),
            }),
            wal: Some(arkflow_core::distributed_wal::DistributedWalConfig {
                wal_type: "rocksdb".to_string(),
                path: "./distributed_ack_wal".to_string(),
                max_size: Some(1024 * 1024 * 1024),
                sync_interval_ms: Some(1000),
            }),
            performance: Some(arkflow_core::distributed_ack_config::PerformanceConfig {
                max_pending_acks: 5000,
                batch_size: 100,
                flush_interval_ms: 1000,
                retry_config: arkflow_core::distributed_ack_config::RetryConfig {
                    max_retries: 5,
                    initial_delay_ms: 1000,
                    max_delay_ms: 30000,
                    backoff_multiplier: 2.0,
                },
            }),
            recovery: Some(arkflow_core::distributed_ack_config::RecoveryConfig {
                enable_recovery: true,
                recovery_interval_ms: 30000,
                checkpoint_interval_ms: 60000,
            }),
        }),
    };

    // Build and run the stream
    let mut stream = stream_config.build()?;
    let cancellation_token = tokio_util::sync::CancellationToken::new();

    println!("✓ Stream with distributed acknowledgment created and configured");

    // In a real application, you would run the stream
    // stream.run(cancellation_token).await?;

    Ok(())
}

/// Example 2: Input-level distributed acknowledgment
async fn example_input_level_distributed_ack() -> Result<(), Error> {
    // Create distributed acknowledgment configuration
    let distributed_ack_config = DistributedAckConfig {
        enabled: true,
        node_id: "node-1".to_string(),
        cluster_nodes: vec!["node-1:8080".to_string()],
        object_storage: Some(arkflow_core::object_storage::ObjectStorageConfig {
            storage_type: "local".to_string(),
            endpoint: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            bucket: None,
            path: Some("./distributed_ack_storage".to_string()),
        }),
        wal: Some(arkflow_core::distributed_wal::DistributedWalConfig {
            wal_type: "rocksdb".to_string(),
            path: "./distributed_ack_wal".to_string(),
            max_size: Some(1024 * 1024 * 1024),
            sync_interval_ms: Some(1000),
        }),
        performance: Some(arkflow_core::distributed_ack_config::PerformanceConfig {
            max_pending_acks: 5000,
            batch_size: 100,
            flush_interval_ms: 1000,
            retry_config: arkflow_core::distributed_ack_config::RetryConfig {
                max_retries: 5,
                initial_delay_ms: 1000,
                max_delay_ms: 30000,
                backoff_multiplier: 2.0,
            },
        }),
        recovery: Some(arkflow_core::distributed_ack_config::RecoveryConfig {
            enable_recovery: true,
            recovery_interval_ms: 30000,
            checkpoint_interval_ms: 60000,
        }),
    };

    // Create input
    let input_config = InputConfig {
        input_type: "memory".to_string(),
        name: Some("memory_input".to_string()),
        config: Some(serde_json::json!({
            "data": ["message1", "message2", "message3"]
        })),
    };

    let resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    };

    let input = input_config.build(&resource)?;

    // Create distributed acknowledgment processor
    let tracker = tokio_util::task::TaskTracker::new();
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let distributed_processor =
        arkflow_core::distributed_ack_processor::DistributedAckProcessor::new(
            tracker.clone(),
            cancellation_token.clone(),
            &distributed_ack_config,
        )
        .await?;

    // Wrap input with distributed acknowledgment
    let builder = DistributedAckBuilder::new(distributed_ack_config);
    let wrapped_input = builder.wrap_input(input, Arc::new(distributed_processor));

    println!("✓ Input with distributed acknowledgment created");

    Ok(())
}

/// Example 3: Processor-level distributed acknowledgment
async fn example_processor_level_distributed_ack() -> Result<(), Error> {
    // Create distributed acknowledgment configuration
    let distributed_ack_config = DistributedAckConfig {
        enabled: true,
        node_id: "node-1".to_string(),
        cluster_nodes: vec!["node-1:8080".to_string()],
        object_storage: Some(arkflow_core::object_storage::ObjectStorageConfig {
            storage_type: "local".to_string(),
            endpoint: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            bucket: None,
            path: Some("./distributed_ack_storage".to_string()),
        }),
        wal: Some(arkflow_core::distributed_wal::DistributedWalConfig {
            wal_type: "rocksdb".to_string(),
            path: "./distributed_ack_wal".to_string(),
            max_size: Some(1024 * 1024 * 1024),
            sync_interval_ms: Some(1000),
        }),
        performance: Some(arkflow_core::distributed_ack_config::PerformanceConfig {
            max_pending_acks: 5000,
            batch_size: 100,
            flush_interval_ms: 1000,
            retry_config: arkflow_core::distributed_ack_config::RetryConfig {
                max_retries: 5,
                initial_delay_ms: 1000,
                max_delay_ms: 30000,
                backoff_multiplier: 2.0,
            },
        }),
        recovery: Some(arkflow_core::distributed_ack_config::RecoveryConfig {
            enable_recovery: true,
            recovery_interval_ms: 30000,
            checkpoint_interval_ms: 60000,
        }),
    };

    // Create processor
    let processor_config = ProcessorConfig {
        processor_type: "noop".to_string(),
        name: Some("noop_processor".to_string()),
        config: None,
    };

    let resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    };

    let processor = processor_config.build(&resource)?;

    // Create distributed acknowledgment processor
    let tracker = tokio_util::task::TaskTracker::new();
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let distributed_processor =
        arkflow_core::distributed_ack_processor::DistributedAckProcessor::new(
            tracker.clone(),
            cancellation_token.clone(),
            &distributed_ack_config,
        )
        .await?;

    // Wrap processor with distributed acknowledgment
    let builder = DistributedAckBuilder::new(distributed_ack_config);
    let wrapped_processor = builder.wrap_processor(processor, Arc::new(distributed_processor));

    println!("✓ Processor with distributed acknowledgment created");

    Ok(())
}

/// Example 4: Configuration-based setup
async fn example_configuration_based_setup() -> Result<(), Error> {
    // This would typically load from a file
    let config_str = r#"
    [[streams]]
    name = "distributed_ack_stream"

    [streams.input]
    type = "memory"
    config = { data = ["message1", "message2", "message3"] }

    [streams.pipeline]
    thread_num = 2
    processors = []

    [streams.output]
    type = "memory"

    [streams.distributed_ack]
    enabled = true
    node_id = "node-1"
    cluster_nodes = ["node-1:8080"]

    [streams.distributed_ack.object_storage]
    type = "local"
    path = "./distributed_ack_storage"

    [streams.distributed_ack.wal]
    type = "rocksdb"
    path = "./distributed_ack_wal"

    [streams.distributed_ack.performance]
    max_pending_acks = 5000
    batch_size = 100
    flush_interval_ms = 1000

    [streams.distributed_ack.performance.retry_config]
    max_retries = 5
    initial_delay_ms = 1000
    max_delay_ms = 30000
    backoff_multiplier = 2.0

    [streams.distributed_ack.recovery]
    enable_recovery = true
    recovery_interval_ms = 30000
    checkpoint_interval_ms = 60000
    "#;

    // Parse configuration
    let engine_config: EngineConfig = toml::from_str(config_str)
        .map_err(|e| Error::Config(format!("Failed to parse config: {}", e)))?;

    // Run engine
    let engine = arkflow_core::engine::Engine::new(engine_config);
    println!("✓ Engine with distributed acknowledgment configured");

    // In a real application, you would run the engine
    // engine.run().await?;

    Ok(())
}
