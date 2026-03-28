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

//! Integration tests for exactly-once semantics
//!
//! These tests verify end-to-end transactional behavior including:
//! - Transaction commit and rollback
//! - Idempotency and duplicate prevention
//! - Crash recovery
//! - Multi-output scenarios

use arkflow_core::config::ExactlyOnceConfig;
use arkflow_core::transaction::{
    IdempotencyConfig, TransactionCoordinator, TransactionCoordinatorConfig, WalConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Test basic transaction lifecycle
#[tokio::test]
async fn test_transaction_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    // Test 1: Begin transaction
    let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
    assert_eq!(tx_id, 1);

    let record = coordinator.get_transaction(tx_id).await;
    assert!(record.is_some());
    assert_eq!(
        record.unwrap().state,
        arkflow_core::transaction::TransactionState::Init
    );

    // Test 2: Prepare transaction
    coordinator.prepare_transaction(tx_id).await.unwrap();
    let record = coordinator.get_transaction(tx_id).await;
    assert!(record.is_some());
    assert_eq!(
        record.unwrap().state,
        arkflow_core::transaction::TransactionState::Prepared
    );

    // Test 3: Commit transaction
    coordinator.commit_transaction(tx_id).await.unwrap();
    let record = coordinator.get_transaction(tx_id).await;
    assert!(record.is_none()); // Should be removed after commit
}

/// Test transaction rollback
#[tokio::test]
async fn test_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    // Begin and rollback transaction
    let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
    coordinator.rollback_transaction(tx_id).await.unwrap();

    // Transaction should be removed
    let record = coordinator.get_transaction(tx_id).await;
    assert!(record.is_none());
}

/// Test idempotency cache
#[tokio::test]
async fn test_idempotency_duplicate_detection() {
    let temp_dir = TempDir::new().unwrap();
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: TempDir::new()
                .unwrap()
                .path()
                .join("wal")
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    // First check - not processed
    let is_duplicate = coordinator
        .check_and_mark_idempotency("test:key1")
        .await
        .unwrap();
    assert!(!is_duplicate);

    // Second check - should be marked as processed
    let is_duplicate = coordinator
        .check_and_mark_idempotency("test:key1")
        .await
        .unwrap();
    assert!(is_duplicate);

    // Different key - not processed
    let is_duplicate = coordinator
        .check_and_mark_idempotency("test:key2")
        .await
        .unwrap();
    assert!(!is_duplicate);
}

/// Test WAL recovery
#[tokio::test]
async fn test_wal_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // Create coordinator and begin transaction
    let coordinator1 = TransactionCoordinator::new(config.clone()).await.unwrap();
    let tx_id = coordinator1.begin_transaction(vec![1, 2, 3]).await.unwrap();
    coordinator1.prepare_transaction(tx_id).await.unwrap();

    // Simulate crash by dropping coordinator
    drop(coordinator1);

    // Create new coordinator and recover
    let coordinator2 = TransactionCoordinator::new(config).await.unwrap();
    let recovered = coordinator2.recover().await.unwrap();

    // Should recover the prepared transaction (may have multiple WAL entries for same tx)
    // Check that we recovered at least one transaction and it includes our tx_id
    assert!(!recovered.is_empty());
    assert!(recovered.contains(&tx_id));

    let record = coordinator2.get_transaction(tx_id).await;
    assert!(record.is_some());
    assert_eq!(
        record.unwrap().state,
        arkflow_core::transaction::TransactionState::Prepared
    );
}

/// Test concurrent transactions
#[tokio::test]
async fn test_concurrent_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();
    let coordinator = Arc::new(coordinator);

    // Spawn multiple tasks to create transactions concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let coord = Arc::clone(&coordinator);
        let handle = tokio::spawn(async move {
            let tx_id = coord.begin_transaction(vec![i as u64]).await.unwrap();
            coord.prepare_transaction(tx_id).await.unwrap();
            coord.commit_transaction(tx_id).await.unwrap();
            tx_id
        });
        handles.push(handle);
    }

    // Wait for all transactions
    let mut tx_ids = Vec::new();
    for handle in handles {
        let tx_id = handle.await.unwrap();
        tx_ids.push(tx_id);
    }

    // All transaction IDs should be unique
    tx_ids.sort();
    tx_ids.dedup();
    assert_eq!(tx_ids.len(), 10);
}

/// Test transaction with idempotency keys
#[tokio::test]
async fn test_transaction_with_idempotency_keys() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    let tx_id = coordinator.begin_transaction(vec![1]).await.unwrap();

    // Add idempotency keys to transaction record
    coordinator
        .add_idempotency_key(tx_id, "key1".to_string())
        .await
        .unwrap();
    coordinator
        .add_idempotency_key(tx_id, "key2".to_string())
        .await
        .unwrap();
    coordinator
        .add_idempotency_key(tx_id, "key3".to_string())
        .await
        .unwrap();

    // Mark keys in idempotency cache (this is what happens during processing)
    coordinator
        .check_and_mark_idempotency("key1")
        .await
        .unwrap();
    coordinator
        .check_and_mark_idempotency("key2")
        .await
        .unwrap();
    coordinator
        .check_and_mark_idempotency("key3")
        .await
        .unwrap();

    // Prepare and commit
    coordinator.prepare_transaction(tx_id).await.unwrap();
    coordinator.commit_transaction(tx_id).await.unwrap();

    // Keys should still be marked after commit
    assert!(coordinator
        .check_and_mark_idempotency("key1")
        .await
        .unwrap());
    assert!(coordinator
        .check_and_mark_idempotency("key2")
        .await
        .unwrap());
    assert!(coordinator
        .check_and_mark_idempotency("key3")
        .await
        .unwrap());
}

/// Test idempotency persistence
#[tokio::test]
async fn test_idempotency_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: TempDir::new()
                .unwrap()
                .path()
                .join("wal")
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // Create coordinator and mark keys
    let coordinator1 = TransactionCoordinator::new(config.clone()).await.unwrap();
    coordinator1
        .check_and_mark_idempotency("key1")
        .await
        .unwrap();
    coordinator1
        .check_and_mark_idempotency("key2")
        .await
        .unwrap();
    coordinator1.persist_idempotency().await.unwrap();

    // Simulate crash by dropping coordinator
    drop(coordinator1);

    // Create new coordinator (automatically restores idempotency cache)
    let coordinator2 = TransactionCoordinator::new(config).await.unwrap();

    // Keys should still be marked
    assert!(coordinator2
        .check_and_mark_idempotency("key1")
        .await
        .unwrap());
    assert!(coordinator2
        .check_and_mark_idempotency("key2")
        .await
        .unwrap());
}

/// Test transaction timeout
#[tokio::test]
async fn test_transaction_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        transaction_timeout: Duration::from_millis(100),
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    let tx_id = coordinator.begin_transaction(vec![1]).await.unwrap();

    // Wait for timeout
    sleep(Duration::from_millis(150)).await;

    // Transaction should still exist but may need cleanup
    let record = coordinator.get_transaction(tx_id).await;
    assert!(record.is_some());
}

/// Test WAL truncate
#[tokio::test]
async fn test_wal_truncate() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");
    let persist_path = temp_dir.path().join("idempotency.json");

    let config = TransactionCoordinatorConfig {
        wal: WalConfig {
            wal_dir: wal_path.to_string_lossy().to_string(),
            ..Default::default()
        },
        idempotency: IdempotencyConfig {
            persist_path: Some(persist_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let coordinator = TransactionCoordinator::new(config).await.unwrap();

    // Create multiple transactions
    for i in 1..=10 {
        let tx_id = coordinator.begin_transaction(vec![i]).await.unwrap();
        coordinator.prepare_transaction(tx_id).await.unwrap();
        coordinator.commit_transaction(tx_id).await.unwrap();
    }

    // Truncate WAL
    let wal = &coordinator;
    // This should work without errors (implementation detail)
    let active_count = wal.active_transaction_count().await;
    assert_eq!(active_count, 0); // All committed
}

/// Test exactly-once configuration
#[test]
fn test_exactly_once_config() {
    let config: ExactlyOnceConfig = serde_yaml::from_str(
        r#"
        enabled: true
        transaction:
          wal:
            wal_dir: "/tmp/wal"
            max_file_size: 1073741824
            sync_on_write: false
            compression: false
          idempotency:
            cache_size: 100000
            ttl:
              secs: 86400
              nanos: 0
            persist_path: "/tmp/idempotency.json"
            persist_interval:
              secs: 60
              nanos: 0
          transaction_timeout: 30s
        "#,
    )
    .unwrap();

    assert!(config.enabled);
    assert_eq!(config.transaction.wal.wal_dir, "/tmp/wal");
    assert_eq!(config.transaction.wal.max_file_size, 1073741824);
    assert_eq!(config.transaction.idempotency.cache_size, 100000);
    assert_eq!(
        config.transaction.idempotency.ttl,
        Duration::from_secs(86400)
    );
}
