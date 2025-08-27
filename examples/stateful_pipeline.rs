//! Stateful pipeline example using ArkFlow with state management
//!
//! This example demonstrates how to integrate state management with existing
//! ArkFlow components including inputs, processors, and outputs.

use arkflow_core::state::{
    EnhancedStateConfig, EnhancedStateManager, ExactlyOnceProcessor, MonitoredStateManager,
    OperationTimer, StateBackendType, StateMonitor, TwoPhaseCommitOutput,
};
use arkflow_core::{
    config::Config, input::Input, output::Output, processor::Processor, stream::Stream, Error,
    MessageBatch,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Configuration for the stateful pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatefulPipelineConfig {
    /// Input configuration
    pub input: InputConfig,
    /// Output configuration
    pub output: OutputConfig,
    /// State management configuration
    pub state: StateConfig,
    /// Processor configuration
    pub processor: ProcessorConfig,
}

/// Input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    pub r#type: String,
    pub topic: String,
    pub brokers: Vec<String>,
    pub consumer_group: String,
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub r#type: String,
    pub topic: String,
    pub brokers: Vec<String>,
}

/// State configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub enabled: bool,
    pub backend_type: String,
    pub checkpoint_interval_ms: u64,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub enable_monitoring: bool,
}

/// Processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    pub r#type: String,
    pub window_size_ms: u64,
    pub aggregation_key: String,
}

/// Aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResult {
    pub key: String,
    pub window_start: u64,
    pub window_end: u64,
    pub count: u64,
    pub sum: f64,
    pub avg: f64,
}

/// Stateful aggregation processor
pub struct AggregationProcessor {
    window_size_ms: u64,
    aggregation_key: String,
    state_manager: Arc<RwLock<EnhancedStateManager>>,
    operator_id: String,
}

impl AggregationProcessor {
    pub fn new(
        window_size_ms: u64,
        aggregation_key: String,
        state_manager: Arc<RwLock<EnhancedStateManager>>,
        operator_id: String,
    ) -> Self {
        Self {
            window_size_ms,
            aggregation_key,
            state_manager,
            operator_id,
        }
    }

    /// Get window for timestamp
    fn get_window(&self, timestamp: u64) -> (u64, u64) {
        let window_start = (timestamp / self.window_size_ms) * self.window_size_ms;
        let window_end = window_start + self.window_size_ms;
        (window_start, window_end)
    }

    /// Get state key for window
    fn get_state_key(&self, key: &str, window_start: u64) -> String {
        format!("agg_{}_{}", key, window_start)
    }

    /// Get aggregated results for a window
    pub async fn get_window_results(
        &self,
        key: &str,
        window_start: u64,
    ) -> Result<Option<AggregationResult>, Error> {
        let state_manager = self.state_manager.read().await;
        let state_key = self.get_state_key(key, window_start);

        if let Some(result) = state_manager
            .get_state_value::<AggregationResult>(&self.operator_id, &state_key)
            .await?
        {
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl Processor for AggregationProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut results = Vec::new();

        // Extract messages
        if let Ok(messages) = batch.to_binary("__value__") {
            for message_data in messages {
                // Parse message
                let message: serde_json::Value =
                    serde_json::from_slice(&message_data).map_err(|e| Error::Serialization(e))?;

                // Extract timestamp and key
                let timestamp = message
                    .get("timestamp")
                    .and_then(|v| v.as_u64())
                    .unwrap_or_else(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                    });

                let key = message
                    .get(&self.aggregation_key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");

                let value = message.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);

                // Get window
                let (window_start, window_end) = self.get_window(timestamp);

                // Update state
                let mut state_manager = self.state_manager.write().await;
                let state_key = self.get_state_key(key, window_start);

                // Get or create aggregation result
                let mut agg_result = state_manager
                    .get_state_value::<AggregationResult>(&self.operator_id, &state_key)
                    .await?
                    .unwrap_or_else(|| AggregationResult {
                        key: key.to_string(),
                        window_start,
                        window_end,
                        count: 0,
                        sum: 0.0,
                        avg: 0.0,
                    });

                // Update aggregation
                agg_result.count += 1;
                agg_result.sum += value;
                agg_result.avg = agg_result.sum / agg_result.count as f64;

                // Save back to state
                state_manager
                    .set_state_value(&self.operator_id, &state_key, agg_result.clone())
                    .await?;

                // Create result message
                let result_data =
                    serde_json::to_vec(&agg_result).map_err(|e| Error::Serialization(e))?;

                let result_batch = MessageBatch::new_binary(vec![result_data])?;
                results.push(result_batch);
            }
        }

        Ok(results)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Build stateful pipeline
pub async fn build_stateful_pipeline(
    config: StatefulPipelineConfig,
) -> Result<(Stream, Arc<RwLock<EnhancedStateManager>>), Error> {
    // Parse state backend type
    let backend_type = match config.state.backend_type.as_str() {
        "memory" => StateBackendType::Memory,
        "s3" => StateBackendType::S3,
        _ => return Err(Error::Config("Invalid state backend type".to_string())),
    };

    // Create state configuration
    let state_config = EnhancedStateConfig {
        enabled: config.state.enabled,
        backend_type,
        checkpoint_interval_ms: config.state.checkpoint_interval_ms,
        exactly_once: true,
        s3_config: if backend_type == StateBackendType::S3 {
            Some(arkflow_core::state::S3StateBackendConfig {
                bucket: config.state.s3_bucket.unwrap_or_default(),
                region: config
                    .state
                    .s3_region
                    .unwrap_or_else(|| "us-east-1".to_string()),
                prefix: Some("pipeline/checkpoints".to_string()),
                ..Default::default()
            })
        } else {
            None
        },
        ..Default::default()
    };

    // Create state manager
    let state_manager = if config.state.enable_monitoring {
        // Create monitor
        let monitor = Arc::new(StateMonitor::new()?);
        let monitored = MonitoredStateManager::new(state_config, monitor).await?;
        Arc::new(RwLock::new(monitored.inner))
    } else {
        Arc::new(RwLock::new(EnhancedStateManager::new(state_config).await?))
    };

    // Create stream configuration
    let stream_config = Config {
        logging: arkflow_core::config::LoggingConfig {
            level: "info".to_string(),
        },
        streams: vec![arkflow_core::config::StreamConfig {
            name: "stateful_pipeline".to_string(),
            input: arkflow_core::config::InputConfig {
                r#type: config.input.r#type,
                config: serde_json::json!({
                    "topic": config.input.topic,
                    "brokers": config.input.brokers,
                    "consumer_group": config.input.consumer_group,
                }),
            },
            pipeline: arkflow_core::config::PipelineConfig {
                thread_num: 4,
                processors: vec![],
            },
            output: arkflow_core::config::OutputConfig {
                r#type: config.output.r#type,
                config: serde_json::json!({
                    "topic": config.output.topic,
                    "brokers": config.output.brokers,
                }),
            },
            error_output: None,
        }],
    };

    // Create stream
    let stream = Stream::new(stream_config).await?;

    Ok((stream, state_manager))
}

/// Sample configuration for the stateful pipeline
pub fn sample_config() -> StatefulPipelineConfig {
    StatefulPipelineConfig {
        input: InputConfig {
            r#type: "kafka".to_string(),
            topic: "input_events".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            consumer_group: "stateful_pipeline_group".to_string(),
        },
        output: OutputConfig {
            r#type: "kafka".to_string(),
            topic: "aggregated_results".to_string(),
            brokers: vec!["localhost:9092".to_string()],
        },
        state: StateConfig {
            enabled: true,
            backend_type: "memory".to_string(),
            checkpoint_interval_ms: 30000,
            s3_bucket: None,
            s3_region: None,
            enable_monitoring: true,
        },
        processor: ProcessorConfig {
            r#type: "aggregation".to_string(),
            window_size_ms: 60000, // 1 minute windows
            aggregation_key: "user_id".to_string(),
        },
    }
}

/// Run the stateful pipeline
pub async fn run_stateful_pipeline() -> Result<(), Error> {
    // Initialize logging
    env_logger::init();

    // Get configuration
    let config = sample_config();

    // Build pipeline
    let (mut stream, state_manager) = build_stateful_pipeline(config).await?;

    // Create aggregation processor
    let processor = AggregationProcessor::new(
        config.processor.window_size_ms,
        config.processor.aggregation_key,
        state_manager.clone(),
        "aggregation_operator".to_string(),
    );

    // Wrap with exactly-once semantics
    let exactly_once_processor = ExactlyOnceProcessor::new(
        processor,
        state_manager.clone(),
        "aggregation_pipeline".to_string(),
    );

    println!("Starting stateful pipeline...");
    println!("Window size: {}ms", config.processor.window_size_ms);
    println!("Aggregation key: {}", config.processor.aggregation_key);

    // In a real application, you would run the stream continuously
    // For this example, we'll process some sample data

    // Generate sample events
    let sample_events = vec![
        serde_json::json!({
            "user_id": "user1",
            "value": 10.5,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }),
        serde_json::json!({
            "user_id": "user2",
            "value": 20.0,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }),
        serde_json::json!({
            "user_id": "user1",
            "value": 15.5,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64 + 1000
        }),
    ];

    // Process events
    for event in sample_events {
        let event_data = serde_json::to_vec(&event).map_err(|e| Error::Serialization(e))?;

        let batch = MessageBatch::new_binary(vec![event_data])?;
        let results = exactly_once_processor.process(batch).await?;

        // Print results
        for result in results {
            if let Ok(results_data) = result.to_binary("__value__") {
                for result_data in results_data {
                    let agg_result: AggregationResult = serde_json::from_slice(&result_data)?;
                    println!(
                        "Aggregation: key={}, count={}, sum={}, avg={:.2}",
                        agg_result.key, agg_result.count, agg_result.sum, agg_result.avg
                    );
                }
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Create checkpoint
    {
        let mut state_manager = state_manager.write().await;
        let checkpoint_id = state_manager.create_checkpoint().await?;
        println!("Created checkpoint: {}", checkpoint_id);
    }

    // Print final statistics
    let state_manager = state_manager.read().await;
    let stats = state_manager.get_state_stats().await;
    println!("\nPipeline Statistics:");
    println!("  Active transactions: {}", stats.active_transactions);
    println!("  Local states: {}", stats.local_states_count);
    println!("  Current checkpoint: {}", stats.current_checkpoint_id);
    println!("  Backend type: {:?}", stats.backend_type);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run_stateful_pipeline().await
}
