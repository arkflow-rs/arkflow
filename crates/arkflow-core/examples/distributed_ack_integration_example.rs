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

//! Integration example for distributed acknowledgment system
//!
//! This example demonstrates how to use the enhanced distributed acknowledgment
//! system with optimized error handling, retry mechanisms, and metrics collection.

use arkflow_core::{
    distributed_ack_config::DistributedAckConfig, enhanced_ack_task::AckTaskPool,
    enhanced_config::EnhancedConfig, enhanced_metrics::EnhancedMetrics, MessageBatch,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Distributed Acknowledgment Integration Example ===");

    // Create enhanced configuration
    let config = EnhancedConfig::production();
    config.validate()?;

    // Initialize metrics collection
    let metrics = Arc::new(EnhancedMetrics::new());

    // Create distributed acknowledgment configuration
    let distributed_ack_config = DistributedAckConfig {
        cluster_id: "example-cluster".to_string(),
        node_id: "node-1".to_string(),
        wal_enabled: true,
        checkpoint_enabled: true,
        retry_max_attempts: config.retry.max_retries,
        retry_base_delay_ms: config.retry.base_delay_ms,
        // ... other configuration fields
    };

    // Create task pool for enhanced acknowledgment processing
    let task_pool = AckTaskPool::new(config.retry.clone());

    // Simulate message processing with distributed acknowledgments
    println!("Processing messages with distributed acknowledgments...");

    for i in 0..10 {
        // Create a test message
        let message = MessageBatch::from_string(&format!("Test message {}", i))?;

        // Simulate processing with acknowledgment
        process_with_ack(&message, &task_pool, &metrics).await?;

        // Small delay between messages
        sleep(Duration::from_millis(100)).await;
    }

    // Print final metrics
    println!("\n=== Final Metrics ===");
    print_metrics(&metrics);

    println!("Example completed successfully!");
    Ok(())
}

async fn process_with_ack(
    message: &MessageBatch,
    task_pool: &AckTaskPool,
    metrics: &EnhancedMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();

    // Increment message counter
    metrics.counter("messages_received").unwrap().increment();

    // Simulate message processing
    println!("Processing message: {:?}", message.get_input_name());

    // Simulate some processing work
    sleep(Duration::from_millis(50)).await;

    // Record processing time
    let processing_time = start_time.elapsed().as_millis() as f64;
    metrics
        .histogram("message_processing_time_ms")
        .unwrap()
        .observe(processing_time);

    // Simulate acknowledgment task
    let ack_task = arkflow_core::enhanced_ack_task::EnhancedAckTask::new(
        Arc::new(TestAck),
        format!("ack-{}", message.len()),
        config.retry.clone(),
    );

    // Add task to pool
    task_pool.add_task(ack_task).await?;

    // Update active connections gauge
    metrics.gauge("active_connections").unwrap().set(1.0);

    println!("Message processed and acknowledgment queued");

    Ok(())
}

fn print_metrics(metrics: &EnhancedMetrics) {
    // Print counter metrics
    if let Some(count) = metrics.get_counter_value("messages_received") {
        println!("Messages received: {}", count);
    }

    // Print gauge metrics
    if let Some(gauge_value) = metrics.get_gauge_value("active_connections") {
        println!("Active connections: {}", gauge_value);
    }

    // Print histogram metrics
    if let Some(percentiles) = metrics.get_histogram_percentiles("message_processing_time_ms") {
        println!("Processing time percentiles:");
        println!("  P50: {:.2}ms", percentiles.p50);
        println!("  P90: {:.2}ms", percentiles.p90);
        println!("  P95: {:.2}ms", percentiles.p95);
        println!("  P99: {:.2}ms", percentiles.p99);
        println!("  Count: {}", percentiles.count);
    }
}

// Test acknowledgment implementation
struct TestAck;

#[async_trait::async_trait]
impl arkflow_core::enhanced_ack_task::Ack for TestAck {
    async fn ack(&self) -> Result<(), String> {
        // Simulate acknowledgment processing
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    async fn retry(&self, _attempt: u32) -> Result<(), String> {
        // Simulate retry logic
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}
