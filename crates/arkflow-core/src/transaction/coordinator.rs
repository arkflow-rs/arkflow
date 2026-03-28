/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file file except in compliance with the License.
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

//! Transaction coordinator for exactly-once semantics
//!
//! The transaction coordinator manages two-phase commit (2PC) protocol
//! across outputs, ensuring atomic writes and fault tolerance.

use super::{
    idempotency::IdempotencyCache, types::TransactionRecord, wal::WriteAheadLog, TransactionId,
    TransactionState,
};
use crate::Error;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Transaction coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCoordinatorConfig {
    /// WAL configuration
    pub wal: super::wal::WalConfig,

    /// Idempotency cache configuration
    pub idempotency: super::idempotency::IdempotencyConfig,

    /// Transaction timeout
    #[serde(default = "default_transaction_timeout")]
    #[serde(with = "humantime_serde")]
    pub transaction_timeout: Duration,
}

fn default_transaction_timeout() -> Duration {
    Duration::from_secs(30)
}

impl Default for TransactionCoordinatorConfig {
    fn default() -> Self {
        Self {
            wal: super::wal::WalConfig::default(),
            idempotency: super::idempotency::IdempotencyConfig::default(),
            transaction_timeout: default_transaction_timeout(),
        }
    }
}

/// Transaction coordinator
pub struct TransactionCoordinator {
    /// WAL for transaction durability
    wal: Arc<dyn WriteAheadLog>,

    /// Idempotency cache for duplicate detection
    idempotency_cache: Arc<IdempotencyCache>,

    /// Active transactions
    active_transactions: Arc<Mutex<std::collections::HashMap<TransactionId, TransactionRecord>>>,

    /// Next transaction ID
    next_transaction_id: Arc<Mutex<TransactionId>>,

    /// Configuration
    config: TransactionCoordinatorConfig,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator
    pub async fn new(config: TransactionCoordinatorConfig) -> Result<Self, Error> {
        // Create WAL
        let wal = Arc::new(super::FileWal::new(config.wal.clone())?);

        // Create idempotency cache
        let idempotency_cache = Arc::new(IdempotencyCache::new(config.idempotency.clone()));

        // Try to restore idempotency cache
        let _ = idempotency_cache.restore().await;

        Ok(Self {
            wal,
            idempotency_cache,
            active_transactions: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_transaction_id: Arc::new(Mutex::new(1)),
            config,
        })
    }

    /// Begin a new transaction
    pub async fn begin_transaction(
        &self,
        sequence_numbers: Vec<u64>,
    ) -> Result<TransactionId, Error> {
        let mut tx_id_guard = self.next_transaction_id.lock().await;
        let tx_id = *tx_id_guard;
        *tx_id_guard += 1;
        drop(tx_id_guard);

        // Create transaction record
        let record = TransactionRecord::new(tx_id, sequence_numbers);

        // Log to WAL
        self.wal.append(&record).await?;

        // Store in active transactions
        let mut active = self.active_transactions.lock().await;
        active.insert(tx_id, record.clone());

        tracing::debug!("Transaction {} started", tx_id);
        Ok(tx_id)
    }

    /// Prepare transaction (2PC phase 1)
    pub async fn prepare_transaction(&self, tx_id: TransactionId) -> Result<(), Error> {
        let mut active = self.active_transactions.lock().await;

        let record = active
            .get_mut(&tx_id)
            .ok_or_else(|| Error::Process(format!("Transaction {} not found", tx_id)))?;

        // Transition to Preparing
        record.transition_to(TransactionState::Preparing);

        // Log to WAL
        self.wal.append(record).await?;

        // Transition to Prepared
        record.transition_to(TransactionState::Prepared);

        // Log to WAL
        self.wal.append(record).await?;

        tracing::debug!("Transaction {} prepared", tx_id);
        Ok(())
    }

    /// Commit transaction (2PC phase 2)
    pub async fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), Error> {
        let mut active = self.active_transactions.lock().await;

        let record = active
            .get_mut(&tx_id)
            .ok_or_else(|| Error::Process(format!("Transaction {} not found", tx_id)))?;

        // Transition to Committing
        record.transition_to(TransactionState::Committing);

        // Log to WAL
        self.wal.append(record).await?;

        // Transition to Committed
        record.transition_to(TransactionState::Committed);

        // Log to WAL
        self.wal.append(record).await?;

        // Remove from active transactions
        active.remove(&tx_id);

        tracing::debug!("Transaction {} committed", tx_id);
        Ok(())
    }

    /// Rollback transaction
    pub async fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), Error> {
        let mut active = self.active_transactions.lock().await;

        let record = active
            .get_mut(&tx_id)
            .ok_or_else(|| Error::Process(format!("Transaction {} not found", tx_id)))?;

        // Transition to RollingBack
        record.transition_to(TransactionState::RollingBack);

        // Log to WAL
        self.wal.append(record).await?;

        // Transition to RolledBack
        record.transition_to(TransactionState::RolledBack);

        // Log to WAL
        self.wal.append(record).await?;

        // Remove from active transactions
        active.remove(&tx_id);

        tracing::debug!("Transaction {} rolled back", tx_id);
        Ok(())
    }

    /// Check if an idempotency key has been processed and mark it
    pub async fn check_and_mark_idempotency(&self, key: &str) -> Result<bool, Error> {
        self.idempotency_cache.check_and_mark(key).await
    }

    /// Add idempotency key to transaction record
    pub async fn add_idempotency_key(
        &self,
        tx_id: TransactionId,
        key: String,
    ) -> Result<(), Error> {
        let mut active = self.active_transactions.lock().await;

        let record = active
            .get_mut(&tx_id)
            .ok_or_else(|| Error::Process(format!("Transaction {} not found", tx_id)))?;

        record.add_idempotency_key(key);

        // Log to WAL
        self.wal.append(record).await?;

        Ok(())
    }

    /// Recover from WAL
    pub async fn recover(&self) -> Result<Vec<TransactionId>, Error> {
        // Read WAL to recover incomplete transactions
        let records = self.wal.recover().await?;

        let mut recovered = Vec::new();
        let mut active = self.active_transactions.lock().await;

        for record in records {
            // Only recover non-terminal transactions
            if !record.is_terminal() {
                tracing::info!(
                    "Recovering transaction {} in state {:?}",
                    record.id,
                    record.state
                );

                // For transactions in Prepared state, they may need to be committed or rolled back
                // depending on the output state. For now, just mark them as active.
                active.insert(record.id, record.clone());
                recovered.push(record.id);
            }
        }

        Ok(recovered)
    }

    /// Get transaction record
    pub async fn get_transaction(&self, tx_id: TransactionId) -> Option<TransactionRecord> {
        let active = self.active_transactions.lock().await;
        active.get(&tx_id).cloned()
    }

    /// Cleanup expired idempotency entries
    pub async fn cleanup_idempotency(&self) {
        self.idempotency_cache.cleanup_expired().await;
    }

    /// Persist idempotency cache
    pub async fn persist_idempotency(&self) -> Result<(), Error> {
        self.idempotency_cache.persist().await
    }

    /// Get the number of active transactions
    pub async fn active_transaction_count(&self) -> usize {
        self.active_transactions.lock().await.len()
    }

    /// Get the number of idempotency entries
    pub async fn idempotency_cache_size(&self) -> usize {
        self.idempotency_cache.len().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{IdempotencyConfig, WalConfig};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_coordinator_creation() {
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

        let coordinator = TransactionCoordinator::new(config).await;
        assert!(coordinator.is_ok());
    }

    #[tokio::test]
    async fn test_begin_transaction() {
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

        // Begin a transaction
        let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
        assert_eq!(tx_id, 1);

        // Check that transaction is active
        let record = coordinator.get_transaction(tx_id).await;
        assert!(record.is_some());
        assert_eq!(record.unwrap().state, TransactionState::Init);
    }

    #[tokio::test]
    async fn test_prepare_transaction() {
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

        // Begin and prepare a transaction
        let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
        coordinator.prepare_transaction(tx_id).await.unwrap();

        // Check state
        let record = coordinator.get_transaction(tx_id).await;
        assert!(record.is_some());
        assert_eq!(record.unwrap().state, TransactionState::Prepared);
    }

    #[tokio::test]
    async fn test_commit_transaction() {
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

        // Begin, prepare and commit a transaction
        let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
        coordinator.prepare_transaction(tx_id).await.unwrap();
        coordinator.commit_transaction(tx_id).await.unwrap();

        // Transaction should no longer be active
        let record = coordinator.get_transaction(tx_id).await;
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_rollback_transaction() {
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

        // Begin and rollback a transaction
        let tx_id = coordinator.begin_transaction(vec![1, 2, 3]).await.unwrap();
        coordinator.rollback_transaction(tx_id).await.unwrap();

        // Transaction should no longer be active
        let record = coordinator.get_transaction(tx_id).await;
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_idempotency_check_and_mark() {
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

        // First check - not processed
        let is_duplicate = coordinator
            .check_and_mark_idempotency("key1")
            .await
            .unwrap();
        assert!(!is_duplicate);

        // Second check - should be marked as processed
        let is_duplicate = coordinator
            .check_and_mark_idempotency("key1")
            .await
            .unwrap();
        assert!(is_duplicate);
    }
}
