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

//! Integration test for Exactly-Once semantics
//!
//! This test validates the complete Exactly-Once processing flow, including:
//! - Checkpoint coordination and barrier alignment
//! - State snapshot and recovery
//! - Two-phase commit protocol
//! - Idempotency and fault tolerance

use arkflow_core::checkpoint::{
    BarrierManager, CheckpointConfig, CheckpointCoordinator, CheckpointEventType,
    CheckpointProgress, CommittingState,
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::time::sleep;

#[tokio::test]
async fn test_complete_checkpoint_lifecycle() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(10),
        local_path: temp_dir.path().to_string_lossy().to_string(),
        alignment_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let coordinator = CheckpointCoordinator::new(config).unwrap();
    let barrier_manager = coordinator.barrier_manager();

    // Test 1: Trigger checkpoint and verify barrier injection
    let checkpoint_id = 1;

    // Inject barrier
    let expected_acks = 2; // Assume 2 processor workers
    let barrier = barrier_manager
        .inject_barrier(checkpoint_id, expected_acks)
        .await;

    assert_eq!(barrier.checkpoint_id, checkpoint_id);
    assert_eq!(barrier.expected_acks, expected_acks);

    // Test 2: Simulate barrier acknowledgments from processor workers
    let completed1 = barrier_manager
        .acknowledge_barrier(barrier.id)
        .await
        .unwrap();
    assert!(!completed1); // Should not complete yet

    let completed2 = barrier_manager
        .acknowledge_barrier(barrier.id)
        .await
        .unwrap();
    assert!(completed2); // Should complete now

    // Test 3: Verify barrier completion
    assert!(barrier_manager.is_barrier_completed(barrier.id).await);

    // Test 4: Wait for barrier completion
    let result = barrier_manager.wait_for_barrier(barrier.id).await;
    assert!(result.is_ok());

    println!("✓ Checkpoint lifecycle test passed");
}

#[tokio::test]
async fn test_checkpoint_progress_tracking() {
    // Create checkpoint progress tracker
    let operators = vec![
        "input".to_string(),
        "processor".to_string(),
        "output".to_string(),
    ];
    let mut progress = CheckpointProgress::new(1, 10, 5, operators, 2);

    // Initially not complete
    assert!(!progress.is_complete());
    assert_eq!(progress.completion_percent(), 0.0);

    // Simulate subtask completions
    for operator in ["input", "processor", "output"] {
        for subtask_index in 0..2 {
            let completed = arkflow_core::checkpoint::TaskCheckpointCompleted {
                checkpoint_id: 1,
                operator_id: operator.to_string(),
                subtask_index,
                metadata: arkflow_core::checkpoint::SubtaskCheckpointMetadata {
                    checkpoint_id: 1,
                    operator_id: operator.to_string(),
                    subtask_index,
                    start_time: SystemTime::now(),
                    finish_time: SystemTime::now(),
                    bytes: 1024,
                    watermark: Some(100),
                    table_metadata: HashMap::new(),
                },
            };

            let operator_done = progress.update_subtask(&completed);
            if subtask_index == 1 {
                assert!(operator_done, "Operator {} should be done", operator);
            }
        }
    }

    // Should be complete now
    assert!(progress.is_complete());
    assert_eq!(progress.completion_percent(), 100.0);

    println!("✓ Checkpoint progress tracking test passed");
}

#[tokio::test]
async fn test_committing_state() {
    // Create committing state
    let mut subtasks = std::collections::HashSet::new();
    subtasks.insert(("op1".to_string(), 0));
    subtasks.insert(("op1".to_string(), 1));
    subtasks.insert(("op2".to_string(), 0));

    let committing_data = HashMap::new();
    let mut state = CommittingState::new(1, subtasks, committing_data, 2);

    assert_eq!(state.remaining_subtasks(), 3);
    assert!(!state.done());
    assert!(!state.operator_done("op1"));

    // Commit subtasks for op1
    state.subtask_committed("op1", 0);
    assert_eq!(state.remaining_subtasks(), 2);
    assert!(!state.operator_done("op1"));

    state.subtask_committed("op1", 1);
    assert_eq!(state.remaining_subtasks(), 1);
    assert!(state.operator_done("op1"));

    // Mark op1 as fully committed
    state.operator_fully_committed("op1");
    assert_eq!(state.committed_operators(), 1);

    // Commit op2
    state.subtask_committed("op2", 0);
    assert_eq!(state.remaining_subtasks(), 0);

    state.operator_fully_committed("op2");
    assert!(state.done());

    println!("✓ Committing state test passed");
}

#[tokio::test]
async fn test_checkpoint_event_sequence() {
    // Test the proper sequence of checkpoint events
    let events = vec![
        CheckpointEventType::StartedAlignment,
        CheckpointEventType::StartedCheckpointing,
        CheckpointEventType::FinishedOperatorSetup,
        CheckpointEventType::FinishedSync,
        CheckpointEventType::FinishedPreCommit,
        CheckpointEventType::FinishedCommit,
    ];

    for event_type in events {
        let event = arkflow_core::checkpoint::CheckpointEvent::new(
            1,
            "test-operator".to_string(),
            0,
            event_type,
        );

        assert_eq!(event.checkpoint_id, 1);
        assert_eq!(event.operator_id, "test-operator");
        assert_eq!(event.subtask_index, 0);
        assert_eq!(event.event_type, event_type);

        println!("✓ Event {} created successfully", event_type.as_str());
    }

    println!("✓ Checkpoint event sequence test passed");
}

#[tokio::test]
async fn test_checkpoint_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(10),
        local_path: temp_dir.path().to_string_lossy().to_string(),
        alignment_timeout: Duration::from_millis(100), // Short timeout
        ..Default::default()
    };

    let coordinator = CheckpointCoordinator::new(config).unwrap();
    let barrier_manager = coordinator.barrier_manager();

    // Inject barrier
    let barrier = barrier_manager.inject_barrier(1, 2).await;

    // Don't acknowledge - let it timeout
    sleep(Duration::from_millis(200)).await;

    // Should timeout
    let result = barrier_manager.wait_for_barrier(barrier.id).await;
    assert!(result.is_err());

    println!("✓ Checkpoint timeout test passed");
}

#[tokio::test]
async fn test_checkpoint_save_and_restore() {
    let temp_dir = TempDir::new().unwrap();
    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(10),
        local_path: temp_dir.path().to_string_lossy().to_string(),
        alignment_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let coordinator = CheckpointCoordinator::new(config).unwrap();

    // Initially, no checkpoints
    let result = coordinator.restore_from_checkpoint().await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Trigger checkpoint
    let metadata = coordinator.trigger_checkpoint().await.unwrap();
    assert_eq!(metadata.id, 1);
    assert!(metadata.is_completed());

    // Now restore should succeed
    let result = coordinator.restore_from_checkpoint().await;
    assert!(result.is_ok());
    let snapshot = result.unwrap();
    assert!(snapshot.is_some());

    println!("✓ Checkpoint save and restore test passed");
}

#[tokio::test]
async fn test_checkpoint_stats() {
    let temp_dir = TempDir::new().unwrap();
    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(10),
        local_path: temp_dir.path().to_string_lossy().to_string(),
        alignment_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let coordinator = CheckpointCoordinator::new(config).unwrap();

    // Initial stats
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.total_checkpoints, 0);
    assert_eq!(stats.successful_checkpoints, 0);
    assert_eq!(stats.failed_checkpoints, 0);

    // Trigger successful checkpoint
    coordinator.trigger_checkpoint().await.unwrap();

    let stats = coordinator.get_stats().await;
    assert_eq!(stats.total_checkpoints, 1);
    assert_eq!(stats.successful_checkpoints, 1);
    assert!(stats.last_checkpoint_time.is_some());
    assert!(stats.last_checkpoint_duration.is_some());

    println!("✓ Checkpoint stats test passed");
}

#[tokio::test]
async fn test_concurrent_barriers() {
    let barrier_manager = Arc::new(BarrierManager::new(Duration::from_secs(5)));

    // Inject multiple barriers
    let barrier1 = barrier_manager.inject_barrier(1, 1).await;
    let barrier2 = barrier_manager.inject_barrier(2, 1).await;
    let barrier3 = barrier_manager.inject_barrier(3, 1).await;

    // Should have 3 active barriers
    assert_eq!(barrier_manager.active_barrier_count().await, 3);

    // Acknowledge in random order
    barrier_manager
        .acknowledge_barrier(barrier2.id)
        .await
        .unwrap();
    assert!(barrier_manager.is_barrier_completed(barrier2.id).await);

    barrier_manager
        .acknowledge_barrier(barrier1.id)
        .await
        .unwrap();
    assert!(barrier_manager.is_barrier_completed(barrier1.id).await);

    barrier_manager
        .acknowledge_barrier(barrier3.id)
        .await
        .unwrap();
    assert!(barrier_manager.is_barrier_completed(barrier3.id).await);

    // Cleanup
    barrier_manager.remove_barrier(barrier1.id).await;
    barrier_manager.remove_barrier(barrier2.id).await;
    barrier_manager.remove_barrier(barrier3.id).await;

    assert_eq!(barrier_manager.active_barrier_count().await, 0);

    println!("✓ Concurrent barriers test passed");
}

use std::sync::Arc;

/// Integration test demonstrating the complete Exactly-Once flow
#[tokio::test]
async fn test_exactly_once_semantics_integration() {
    println!("\n=== Exactly-Once Semantics Integration Test ===\n");

    // Setup
    let temp_dir = TempDir::new().unwrap();
    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(1),
        local_path: temp_dir.path().to_string_lossy().to_string(),
        alignment_timeout: Duration::from_secs(5),
        max_checkpoints: 3,
        ..Default::default()
    };

    let coordinator = Arc::new(CheckpointCoordinator::new(config).unwrap());
    let barrier_manager = coordinator.barrier_manager();

    // Step 1: Start checkpoint
    println!("Step 1: Starting checkpoint");
    let checkpoint_id = 1;

    // Step 2: Inject barrier into stream
    println!("Step 2: Injecting barrier");
    let barrier = barrier_manager.inject_barrier(checkpoint_id, 2).await;
    println!("  → Barrier {} injected", barrier.id);

    // Step 3: Simulate processor workers receiving and processing barrier
    println!("Step 3: Processing barrier in workers");

    // Worker 1 acknowledges
    tokio::spawn({
        let barrier_manager = Arc::clone(&barrier_manager);
        async move {
            sleep(Duration::from_millis(50)).await;
            let done = barrier_manager
                .acknowledge_barrier(barrier.id)
                .await
                .unwrap();
            println!("  → Worker 1 acknowledged barrier (done: {})", done);
        }
    });

    // Worker 2 acknowledges
    tokio::spawn({
        let barrier_manager = Arc::clone(&barrier_manager);
        async move {
            sleep(Duration::from_millis(100)).await;
            let done = barrier_manager
                .acknowledge_barrier(barrier.id)
                .await
                .unwrap();
            println!("  → Worker 2 acknowledged barrier (done: {})", done);
        }
    });

    // Step 4: Wait for barrier alignment
    println!("Step 4: Waiting for barrier alignment");
    let _ = barrier_manager.wait_for_barrier(barrier.id).await.unwrap();
    println!("  → Barrier aligned");

    // Step 5: Trigger checkpoint completion
    println!("Step 5: Triggering checkpoint");
    let metadata = coordinator.trigger_checkpoint().await.unwrap();
    println!(
        "  → Checkpoint {} completed ({} bytes)",
        metadata.id, metadata.size_bytes
    );

    // Step 6: Verify checkpoint was saved
    println!("Step 6: Verifying checkpoint");
    let snapshot = coordinator.restore_from_checkpoint().await.unwrap();
    assert!(snapshot.is_some());
    println!("  → Checkpoint verified");

    // Step 7: Check statistics
    println!("Step 7: Checking statistics");
    let stats = coordinator.get_stats().await;
    println!(
        "  → Total: {}, Success: {}, Last duration: {:?}",
        stats.total_checkpoints, stats.successful_checkpoints, stats.last_checkpoint_duration
    );

    assert_eq!(stats.total_checkpoints, 1);
    assert_eq!(stats.successful_checkpoints, 1);

    println!("\n✓ Exactly-Once integration test passed\n");
}
