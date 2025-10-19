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

//! Integration tests for distributed acknowledgment processing

use arkflow_core::distributed_ack_config::DistributedAckConfig;
use arkflow_core::distributed_ack_processor::DistributedAckProcessor;
use arkflow_core::input::NoopAck;
use arkflow_core::object_storage::StorageType;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[tokio::test]
async fn test_distributed_ack_processor_lifecycle() {
    let temp_dir = TempDir::new().unwrap();

    let config = DistributedAckConfig::for_local_testing("test-cluster".to_string())
        .with_local_wal_path(
            temp_dir
                .path()
                .join("local_wal")
                .to_string_lossy()
                .to_string(),
        );

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    // Create processor
    let processor = DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config)
        .await
        .expect("Failed to create distributed ack processor");

    // Test ack processing
    let ack = Arc::new(NoopAck);
    let result = processor
        .ack(ack, "test_ack".to_string(), b"test_payload".to_vec())
        .await;

    assert!(result.is_ok());

    // Test metrics
    let metrics = processor.get_metrics();
    assert_eq!(
        metrics
            .total_acks
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // Test cluster status
    let status = processor
        .get_cluster_status()
        .await
        .expect("Failed to get cluster status");
    assert_eq!(status.cluster_id, "test-cluster");
    assert!(status.distributed_mode);

    // Test checkpoint creation
    let checkpoint_id = processor.create_checkpoint().await;
    assert!(checkpoint_id.is_ok());

    // Cleanup
    cancellation_token.cancel();
    processor
        .shutdown()
        .await
        .expect("Failed to shutdown processor");
}

#[tokio::test]
async fn test_multiple_nodes_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_string_lossy().to_string();

    // Create two processors simulating two nodes
    let storage_type = StorageType::Local(crate::object_storage::LocalConfig {
        base_path: format!("{}/shared_storage", base_path),
    });

    let config1 = DistributedAckConfig::for_production(
        "test-cluster".to_string(),
        storage_type.clone(),
        base_path.clone(),
    )
    .with_node_id("node-1".to_string())
    .with_heartbeat_interval_ms(1000)
    .with_node_timeout_ms(5000);

    let config2 = DistributedAckConfig::for_production(
        "test-cluster".to_string(),
        storage_type,
        base_path.clone(),
    )
    .with_node_id("node-2".to_string())
    .with_heartbeat_interval_ms(1000)
    .with_node_timeout_ms(5000);

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    // Create first processor
    let processor1 = DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config1)
        .await
        .expect("Failed to create processor 1");

    // Create second processor
    let processor2 = DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config2)
        .await
        .expect("Failed to create processor 2");

    // Wait for nodes to discover each other
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Test that both nodes see each other
    let status1 = processor1
        .get_cluster_status()
        .await
        .expect("Failed to get status 1");
    let status2 = processor2
        .get_cluster_status()
        .await
        .expect("Failed to get status 2");

    assert_eq!(status1.total_nodes, 2);
    assert_eq!(status2.total_nodes, 2);
    assert!(status1.active_nodes >= 1);
    assert!(status2.active_nodes >= 1);

    // Process acks on both nodes
    let ack1 = Arc::new(NoopAck);
    let ack2 = Arc::new(NoopAck);

    processor1
        .ack(
            ack1.clone(),
            "test_ack_node1".to_string(),
            b"payload1".to_vec(),
        )
        .await
        .expect("Failed to process ack on node 1");

    processor2
        .ack(
            ack2.clone(),
            "test_ack_node2".to_string(),
            b"payload2".to_vec(),
        )
        .await
        .expect("Failed to process ack on node 2");

    // Create checkpoints
    let checkpoint1 = processor1.create_checkpoint().await;
    let checkpoint2 = processor2.create_checkpoint().await;

    assert!(checkpoint1.is_ok());
    assert!(checkpoint2.is_ok());

    // Test consistency check
    let consistency_report = processor1.perform_consistency_check().await;
    assert!(consistency_report.is_ok());

    // Cleanup
    cancellation_token.cancel();
    processor1
        .shutdown()
        .await
        .expect("Failed to shutdown processor 1");
    processor2
        .shutdown()
        .await
        .expect("Failed to shutdown processor 2");
}

#[tokio::test]
async fn test_recovery_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_string_lossy().to_string();

    let config = DistributedAckConfig::for_local_testing("recovery-test-cluster".to_string())
        .with_local_wal_path(
            temp_dir
                .path()
                .join("local_wal")
                .to_string_lossy()
                .to_string(),
        )
        .with_base_path(format!("{}/shared_storage", base_path));

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    // Create initial processor
    let processor1 =
        DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config.clone())
            .await
            .expect("Failed to create initial processor");

    // Process some acks
    for i in 0..10 {
        let ack = Arc::new(NoopAck);
        processor1
            .ack(
                ack,
                format!("test_ack_{}", i),
                format!("payload_{}", i).into_bytes(),
            )
            .await
            .expect("Failed to process ack");
    }

    // Create checkpoint
    let checkpoint_id = processor1
        .create_checkpoint()
        .await
        .expect("Failed to create checkpoint");
    println!("Created checkpoint: {}", checkpoint_id);

    // Wait for uploads to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Shutdown first processor
    processor1
        .shutdown()
        .await
        .expect("Failed to shutdown first processor");

    // Create second processor with same config (simulating recovery)
    let processor2 = DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config)
        .await
        .expect("Failed to create recovery processor");

    // Test recovery
    let recovery_info = processor2
        .trigger_recovery()
        .await
        .expect("Failed to trigger recovery");
    println!("Recovery completed: {:?}", recovery_info.status);

    // Check cluster status
    let status = processor2
        .get_cluster_status()
        .await
        .expect("Failed to get cluster status");
    println!("Cluster status after recovery: {:?}", status);

    // Cleanup
    cancellation_token.cancel();
    processor2
        .shutdown()
        .await
        .expect("Failed to shutdown recovery processor");
}

#[tokio::test]
async fn test_fallback_mode() {
    let temp_dir = TempDir::new().unwrap();

    // Create config with distributed mode disabled
    let mut config = DistributedAckConfig::for_local_testing("fallback-test-cluster".to_string());
    config.enabled = false;
    config.local_wal_path = temp_dir
        .path()
        .join("local_wal")
        .to_string_lossy()
        .to_string();

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    // Create processor in fallback mode
    let processor = DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config)
        .await
        .expect("Failed to create fallback processor");

    // Test that it works in fallback mode
    let ack = Arc::new(NoopAck);
    let result = processor
        .ack(ack, "test_ack".to_string(), b"test_payload".to_vec())
        .await;

    assert!(result.is_ok());

    // Check cluster status - should show non-distributed mode
    let status = processor
        .get_cluster_status()
        .await
        .expect("Failed to get cluster status");
    assert!(!status.distributed_mode);
    assert_eq!(status.total_nodes, 1);

    // Test that distributed operations fail
    let checkpoint_result = processor.create_checkpoint().await;
    assert!(checkpoint_result.is_err());

    let recovery_result = processor.trigger_recovery().await;
    assert!(recovery_result.is_err());

    // Cleanup
    cancellation_token.cancel();
    processor
        .shutdown()
        .await
        .expect("Failed to shutdown fallback processor");
}

#[tokio::test]
async fn test_configuration_validation() {
    // Test valid configuration
    let config = DistributedAckConfig::for_local_testing("validation-test-cluster".to_string());
    assert!(config.validate().is_ok());

    // Test invalid configuration (empty cluster ID)
    let mut invalid_config = DistributedAckConfig::for_local_testing("".to_string());
    assert!(invalid_config.validate().is_err());

    // Test invalid configuration (zero batch size)
    let mut invalid_config = DistributedAckConfig::for_local_testing("test-cluster".to_string());
    invalid_config.wal.upload_batch_size = 0;
    assert!(invalid_config.validate().is_err());

    // Test invalid configuration (zero max checkpoints)
    let mut invalid_config = DistributedAckConfig::for_local_testing("test-cluster".to_string());
    invalid_config.checkpoint.max_checkpoints = 0;
    assert!(invalid_config.validate().is_err());
}

#[tokio::test]
async fn test_high_load_scenario() {
    let temp_dir = TempDir::new().unwrap();

    let config = DistributedAckConfig::for_local_testing("load-test-cluster".to_string())
        .with_local_wal_path(
            temp_dir
                .path()
                .join("local_wal")
                .to_string_lossy()
                .to_string(),
        )
        .with_upload_batch_size(5) // Smaller batch size for testing
        .with_checkpoint_interval_ms(1000); // Faster checkpoints

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let processor = Arc::new(
        DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config)
            .await
            .expect("Failed to create processor for load test"),
    );

    // Process large number of acks concurrently
    let mut handles = Vec::new();
    let ack_count = 100;

    for i in 0..ack_count {
        let processor = processor.clone();
        let handle = tokio::spawn(async move {
            let ack = Arc::new(NoopAck);
            processor
                .ack(
                    ack,
                    format!("load_test_ack_{}", i),
                    format!("payload_{}", i).into_bytes(),
                )
                .await
        });
        handles.push(handle);
    }

    // Wait for all acks to complete
    let mut successful_acks = 0;
    for handle in handles {
        let result = handle.await.expect("Task failed");
        match result {
            Ok(_) => successful_acks += 1,
            Err(_) => println!("Ack failed"),
        }
    }

    println!(
        "Successfully processed {}/{} acks",
        successful_acks, ack_count
    );

    // Check metrics
    let metrics = processor.get_metrics();
    let total_acks = metrics
        .total_acks
        .load(std::sync::atomic::Ordering::Relaxed);
    let successful_acks_metric = metrics
        .successful_acks
        .load(std::sync::atomic::Ordering::Relaxed);

    println!(
        "Metrics - Total: {}, Successful: {}",
        total_acks, successful_acks_metric
    );

    // Most acks should succeed
    assert!(successful_acks >= ack_count * 90 / 100); // Allow 10% failure rate

    // Wait for processing to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Create final checkpoint
    let checkpoint_result = processor.create_checkpoint().await;
    if let Ok(checkpoint_id) = checkpoint_result {
        println!("Final checkpoint created: {}", checkpoint_id);
    }

    // Cleanup
    cancellation_token.cancel();
    Arc::try_unwrap(processor)
        .unwrap()
        .shutdown()
        .await
        .expect("Failed to shutdown processor");
}
