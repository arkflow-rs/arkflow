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

use super::enhanced::TransactionInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// Transaction context for exactly-once processing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionContext {
    /// Unique transaction identifier
    pub transaction_id: String,
    /// Checkpoint identifier
    pub checkpoint_id: u64,
    /// Barrier type
    pub barrier_type: BarrierType,
    /// Timestamp when the transaction was created
    pub timestamp: u64,
}

/// Barrier type for checkpoint alignment
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BarrierType {
    /// Normal processing
    None,
    /// Checkpoint barrier - triggers state snapshot
    Checkpoint,
    /// Savepoint barrier - manual triggered checkpoint
    Savepoint,
    /// Aligned barrier - waits for all messages to be processed
    AlignedCheckpoint,
}

impl TransactionContext {
    /// Create new transaction context
    pub fn new(checkpoint_id: u64, barrier_type: BarrierType) -> Self {
        Self {
            transaction_id: Uuid::new_v4().to_string(),
            checkpoint_id,
            barrier_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Create checkpoint barrier
    pub fn checkpoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::Checkpoint)
    }

    /// Create aligned checkpoint barrier
    pub fn aligned_checkpoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::AlignedCheckpoint)
    }

    /// Create savepoint barrier
    pub fn savepoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::Savepoint)
    }

    /// Check if this is a checkpoint barrier
    pub fn is_checkpoint(&self) -> bool {
        matches!(
            self.barrier_type,
            BarrierType::Checkpoint | BarrierType::AlignedCheckpoint
        )
    }

    /// Check if alignment is required
    pub fn requires_alignment(&self) -> bool {
        self.barrier_type == BarrierType::AlignedCheckpoint
    }
}

/// Transaction coordinator for managing two-phase commit
pub struct TransactionCoordinator {
    /// Next checkpoint ID
    next_checkpoint_id: AtomicU64,
    /// Active transactions
    active_transactions: Arc<tokio::sync::RwLock<HashMap<String, TransactionInfo>>>,
    /// Checkpoint interval in milliseconds
    checkpoint_interval: u64,
}

/// Transaction participant
#[derive(Debug, Clone)]
pub struct TransactionParticipant {
    pub id: String,
    pub state: ParticipantState,
}

/// Participant state in two-phase commit
#[derive(Debug, Clone, PartialEq)]
pub enum ParticipantState {
    Prepared,
    Committed,
    Aborted,
}

impl TransactionCoordinator {
    /// Create new transaction coordinator
    pub fn new(checkpoint_interval_ms: u64) -> Self {
        Self {
            next_checkpoint_id: AtomicU64::new(1),
            active_transactions: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            checkpoint_interval: checkpoint_interval_ms,
        }
    }

    /// Get next checkpoint ID
    pub fn next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Start a new transaction
    pub async fn begin_transaction(&self, barrier_type: BarrierType) -> TransactionContext {
        let checkpoint_id = self.next_checkpoint_id();
        let tx_ctx = TransactionContext::new(checkpoint_id, barrier_type);

        let mut transactions = self.active_transactions.write().await;
        transactions.insert(
            tx_ctx.transaction_id.clone(),
            TransactionInfo {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id,
                participants: Vec::new(),
                created_at: std::time::SystemTime::now(),
            },
        );

        tx_ctx
    }

    /// Register a participant for the transaction
    pub async fn register_participant(
        &self,
        transaction_id: &str,
        participant_id: String,
    ) -> Result<(), crate::Error> {
        let mut transactions = self.active_transactions.write().await;
        if let Some(tx_info) = transactions.get_mut(transaction_id) {
            tx_info.participants.push(participant_id);
            Ok(())
        } else {
            Err(crate::Error::Process(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Complete the transaction (commit or abort)
    pub async fn complete_transaction(
        &self,
        transaction_id: &str,
        _success: bool,
    ) -> Result<(), crate::Error> {
        let mut transactions = self.active_transactions.write().await;
        if let Some(_tx_info) = transactions.remove(transaction_id) {
            // Transaction completed - all participants are just strings now
            // In a real implementation, you would notify each participant
            Ok(())
        } else {
            Err(crate::Error::Process(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Get checkpoint interval
    pub fn checkpoint_interval(&self) -> u64 {
        self.checkpoint_interval
    }
}

/// Barrier injector for inserting barriers into the stream
pub struct BarrierInjector {
    coordinator: Arc<TransactionCoordinator>,
    last_checkpoint_time: Arc<tokio::sync::RwLock<std::time::Instant>>,
}

impl BarrierInjector {
    /// Create new barrier injector
    pub fn new(coordinator: Arc<TransactionCoordinator>) -> Self {
        Self {
            coordinator,
            last_checkpoint_time: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
        }
    }

    /// Check if a barrier should be injected
    pub async fn should_inject_barrier(&self) -> Option<TransactionContext> {
        let last_time = *self.last_checkpoint_time.read().await;
        let elapsed = last_time.elapsed();

        if elapsed.as_millis() as u64 >= self.coordinator.checkpoint_interval() {
            let tx_ctx = self
                .coordinator
                .begin_transaction(BarrierType::AlignedCheckpoint)
                .await;
            *self.last_checkpoint_time.write().await = std::time::Instant::now();
            Some(tx_ctx)
        } else {
            None
        }
    }

    /// Inject barrier into message batch metadata
    pub async fn inject_into_batch(
        &self,
        batch: &crate::MessageBatch,
    ) -> Option<(crate::MessageBatch, TransactionContext)> {
        if let Some(tx_ctx) = self.should_inject_barrier().await {
            let mut metadata =
                crate::state::Metadata::extract_from_batch(batch).unwrap_or_default();
            metadata.transaction = Some(tx_ctx.clone());

            match metadata.embed_to_batch(batch.clone()) {
                Ok(new_batch) => Some((new_batch, tx_ctx)),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}
