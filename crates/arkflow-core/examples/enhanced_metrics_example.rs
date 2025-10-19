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

//! Enhanced metrics collection example
//!
//! This example demonstrates how to use the enhanced metrics system
//! with counters, gauges, and histograms for monitoring system performance.

use arkflow_core::enhanced_metrics::{EnhancedMetrics, Histogram};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Enhanced Metrics Collection Example ===");

    // Create enhanced metrics instance
    let metrics = Arc::new(EnhancedMetrics::new());

    // Simulate various operations and collect metrics
    simulate_message_processing(&metrics).await?;
    simulate_database_operations(&metrics).await?;
    simulate_network_requests(&metrics).await?;

    // Print comprehensive metrics report
    println!("\n=== Comprehensive Metrics Report ===");
    print_comprehensive_metrics(&metrics);

    // Demonstrate metrics export
    println!("\n=== Exported Metrics ===");
    let exported = metrics.export_metrics();
    println!("{}", exported);

    println!("Example completed successfully!");
    Ok(())
}

async fn simulate_message_processing(
    metrics: &EnhancedMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nSimulating message processing...");

    for i in 0..100 {
        let start_time = std::time::Instant::now();

        // Simulate message processing with varying complexity
        let processing_time = if i % 10 == 0 {
            // Simulate occasional slow messages
            sleep(Duration::from_millis(200)).await;
            200.0
        } else {
            // Normal processing time
            let delay = 10 + (i % 20);
            sleep(Duration::from_millis(delay)).await;
            delay as f64
        };

        // Record metrics
        metrics.counter("messages_processed").unwrap().increment();
        metrics
            .histogram("message_processing_time_ms")
            .unwrap()
            .observe(processing_time);

        // Simulate errors occasionally
        if i % 20 == 0 {
            metrics.counter("message_errors").unwrap().increment();
        }

        // Update queue size gauge
        let queue_size = 50 + (i % 100);
        metrics
            .gauge("message_queue_size")
            .unwrap()
            .set(queue_size as f64);
    }

    Ok(())
}

async fn simulate_database_operations(
    metrics: &EnhancedMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Simulating database operations...");

    for i in 0..50 {
        let start_time = std::time::Instant::now();

        // Simulate database query
        let query_time = match i % 7 {
            0 => 150.0, // Slow query
            1 => 5.0,   // Fast query
            _ => 25.0 + (i % 30) as f64,
        };

        sleep(Duration::from_millis(query_time as u64)).await;

        // Record database metrics
        metrics.counter("database_queries").unwrap().increment();
        metrics
            .histogram("database_query_time_ms")
            .unwrap()
            .observe(query_time);

        // Update connection pool gauge
        let active_connections = 5 + (i % 15);
        metrics
            .gauge("db_active_connections")
            .unwrap()
            .set(active_connections as f64);

        // Simulate connection failures
        if i % 25 == 0 {
            metrics
                .counter("db_connection_failures")
                .unwrap()
                .increment();
        }
    }

    Ok(())
}

async fn simulate_network_requests(
    metrics: &EnhancedMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Simulating network requests...");

    for i in 0..75 {
        let start_time = std::time::Instant::now();

        // Simulate network request with varying latency
        let network_latency = match i % 10 {
            0 => 500.0, // High latency
            1 => 10.0,  // Low latency
            _ => 50.0 + (i % 100) as f64,
        };

        sleep(Duration::from_millis(network_latency as u64)).await;

        // Record network metrics
        metrics.counter("network_requests").unwrap().increment();
        metrics
            .histogram("network_latency_ms")
            .unwrap()
            .observe(network_latency);

        // Update bandwidth gauge
        let bandwidth_mbps = 100.0 + (i % 900) as f64;
        metrics
            .gauge("network_bandwidth_mbps")
            .unwrap()
            .set(bandwidth_mbps);

        // Simulate timeouts
        if i % 30 == 0 {
            metrics.counter("network_timeouts").unwrap().increment();
        }
    }

    Ok(())
}

fn print_comprehensive_metrics(metrics: &EnhancedMetrics) {
    println!("Message Processing Metrics:");
    if let Some(count) = metrics.get_counter_value("messages_processed") {
        println!("  Messages processed: {}", count);
    }
    if let Some(errors) = metrics.get_counter_value("message_errors") {
        println!("  Message errors: {}", errors);
        if let Some(total) = metrics.get_counter_value("messages_processed") {
            let error_rate = (errors as f64 / total as f64) * 100.0;
            println!("  Error rate: {:.2}%", error_rate);
        }
    }
    if let Some(percentiles) = metrics.get_histogram_percentiles("message_processing_time_ms") {
        println!(
            "  Processing time - P50: {:.2}ms, P90: {:.2}ms, P95: {:.2}ms",
            percentiles.p50, percentiles.p90, percentiles.p95
        );
    }

    println!("\nDatabase Metrics:");
    if let Some(queries) = metrics.get_counter_value("database_queries") {
        println!("  Database queries: {}", queries);
    }
    if let Some(failures) = metrics.get_counter_value("db_connection_failures") {
        println!("  Connection failures: {}", failures);
    }
    if let Some(percentiles) = metrics.get_histogram_percentiles("database_query_time_ms") {
        println!(
            "  Query time - P50: {:.2}ms, P90: {:.2}ms",
            percentiles.p50, percentiles.p90
        );
    }

    println!("\nNetwork Metrics:");
    if let Some(requests) = metrics.get_counter_value("network_requests") {
        println!("  Network requests: {}", requests);
    }
    if let Some(timeouts) = metrics.get_counter_value("network_timeouts") {
        println!("  Timeouts: {}", timeouts);
    }
    if let Some(percentiles) = metrics.get_histogram_percentiles("network_latency_ms") {
        println!(
            "  Latency - P50: {:.2}ms, P90: {:.2}ms, P99: {:.2}ms",
            percentiles.p50, percentiles.p90, percentiles.p99
        );
    }

    println!("\nResource Gauges:");
    if let Some(queue_size) = metrics.get_gauge_value("message_queue_size") {
        println!("  Message queue size: {:.0}", queue_size);
    }
    if let Some(connections) = metrics.get_gauge_value("db_active_connections") {
        println!("  DB active connections: {:.0}", connections);
    }
    if let Some(bandwidth) = metrics.get_gauge_value("network_bandwidth_mbps") {
        println!("  Network bandwidth: {:.1} Mbps", bandwidth);
    }
}
