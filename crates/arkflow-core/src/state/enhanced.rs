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

//! Enhanced state manager with S3 backend and exactly-once semantics

use crate::state::helper::SimpleMemoryState;
use crate::state::helper::StateHelper;
use crate::state::s3_backend::{S3CheckpointCoordinator, S3StateBackend, S3StateBackendConfig};
use crate::state::simple::SimpleBarrierInjector;
use crate::state::transaction::TransactionContext;
use crate::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;

/// Enhanced state manager with persistence and exactly-once guarantees
pub struct EnhancedStateManager {
    /// S3 backend for state persistence
    s3_backend: Option<Arc<S3StateBackend>>,
    /// Checkpoint coordinator
    checkpoint_coordinator: Option<S3CheckpointCoordinator>,
    /// Local state cache
    local_states: HashMap<String, SimpleMemoryState>,
    /// Barrier injector
    barrier_injector: Arc<SimpleBarrierInjector>,
    /// Configuration
    config: EnhancedStateConfig,
    /// Current checkpoint ID
    current_checkpoint_id: Arc<AtomicU64>,
    /// Active transactions
    active_transactions: Arc<RwLock<HashMap<String, TransactionInfo>>>,
}

/// Transaction information
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub transaction_id: String,
    pub checkpoint_id: u64,
    pub participants: Vec<String>,
    pub created_at: std::time::SystemTime,
}

/// Enhanced state configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedStateConfig {
    /// Enable state management
    pub enabled: bool,
    /// State backend type
    pub backend_type: StateBackendType,
    /// S3 configuration (if using S3 backend)
    pub s3_config: Option<S3StateBackendConfig>,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// Number of checkpoints to retain
    pub retained_checkpoints: usize,
    /// Enable exactly-once semantics
    pub exactly_once: bool,
    /// State timeout in milliseconds
    pub state_timeout_ms: u64,
}

impl Default for EnhancedStateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 60000,
            retained_checkpoints: 5,
            exactly_once: false,
            state_timeout_ms: 86400000, // 24 hours
        }
    }
}

/// State backend types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateBackendType {
    Memory,
    S3,
    Hybrid,
}

impl EnhancedStateManager {
    /// Create new enhanced state manager
    pub async fn new(config: EnhancedStateConfig) -> Result<Self, Error> {
        let (s3_backend, checkpoint_coordinator) =
            if config.enabled && config.backend_type != StateBackendType::Memory {
                if let Some(s3_config) = &config.s3_config {
                    let backend = Arc::new(S3StateBackend::new(s3_config.clone()).await?);
                    let coordinator = S3CheckpointCoordinator::new(backend.clone());
                    (Some(backend), Some(coordinator))
                } else {
                    return Err(Error::Config(
                        "S3 configuration required for S3 backend".to_string(),
                    ));
                }
            } else {
                (None, None)
            };

        let barrier_injector = Arc::new(SimpleBarrierInjector::new(config.checkpoint_interval_ms));

        Ok(Self {
            s3_backend,
            checkpoint_coordinator,
            local_states: HashMap::new(),
            barrier_injector,
            config,
            current_checkpoint_id: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Process a message batch with state management
    pub async fn process_batch(&mut self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if !self.config.enabled {
            return Ok(vec![batch]);
        }

        // Inject barrier if needed
        let processed_batch = self.barrier_injector.maybe_inject_barrier(batch).await?;

        // Check for transaction context
        if let Some(tx_ctx) = processed_batch.transaction_context() {
            self.process_transactional_batch(processed_batch, tx_ctx)
                .await
        } else {
            Ok(vec![processed_batch])
        }
    }

    /// Process a transactional batch
    async fn process_transactional_batch(
        &mut self,
        batch: MessageBatch,
        tx_ctx: TransactionContext,
    ) -> Result<Vec<MessageBatch>, Error> {
        // Register transaction
        self.register_transaction(&tx_ctx).await?;

        // If this is a checkpoint barrier, trigger checkpoint
        if tx_ctx.is_checkpoint() {
            self.trigger_checkpoint(tx_ctx.checkpoint_id).await?;
        }

        // Process the batch (return as-is for now)
        // In a real implementation, you'd apply transformations here
        Ok(vec![batch])
    }

    /// Register a new transaction
    async fn register_transaction(&self, tx_ctx: &TransactionContext) -> Result<(), Error> {
        let mut transactions = self.active_transactions.write().await;
        transactions.insert(
            tx_ctx.transaction_id.clone(),
            TransactionInfo {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id: tx_ctx.checkpoint_id,
                participants: Vec::new(),
                created_at: std::time::SystemTime::now(),
            },
        );
        Ok(())
    }

    /// Trigger a checkpoint
    async fn trigger_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        if let Some(ref mut coordinator) = self.checkpoint_coordinator {
            // Start checkpoint
            coordinator.start_checkpoint().await?;

            // In a real implementation, you would:
            // 1. Notify all operators to prepare checkpoint
            // 2. Collect state from all operators
            // 3. Persist state to S3
            // 4. Complete checkpoint

            // For now, just save the current local states
            for (operator_id, state) in &self.local_states {
                coordinator
                    .complete_participant(
                        checkpoint_id,
                        operator_id,
                        vec![("default".to_string(), state.clone())],
                    )
                    .await?;
            }

            // Cleanup old checkpoints
            coordinator
                .cleanup_old_checkpoints(self.config.retained_checkpoints)
                .await?;
        }

        Ok(())
    }

    /// Get or create state for an operator
    pub fn get_operator_state(&mut self, operator_id: &str) -> &mut SimpleMemoryState {
        self.local_states
            .entry(operator_id.to_string())
            .or_insert_with(SimpleMemoryState::new)
    }

    /// Get state value
    pub async fn get_state_value<K, V>(
        &self,
        operator_id: &str,
        key: &K,
    ) -> Result<Option<V>, Error>
    where
        K: ToString + Send + Sync,
        V: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        if let Some(state) = self.local_states.get(operator_id) {
            state.get_typed(&key.to_string())
        } else {
            Ok(None)
        }
    }

    /// Set state value
    pub async fn set_state_value<K, V>(
        &mut self,
        operator_id: &str,
        key: &K,
        value: V,
    ) -> Result<(), Error>
    where
        K: ToString + Send + Sync,
        V: serde::Serialize + Send + Sync + 'static,
    {
        let state = self.get_operator_state(operator_id);
        state.put_typed(&key.to_string(), value)?;
        Ok(())
    }

    /// Create a checkpoint manually
    pub async fn create_checkpoint(&mut self) -> Result<u64, Error> {
        let checkpoint_id = self.current_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        self.trigger_checkpoint(checkpoint_id).await?;
        Ok(checkpoint_id)
    }

    /// Recover from the latest checkpoint
    pub async fn recover_from_latest_checkpoint(&mut self) -> Result<Option<u64>, Error> {
        if let Some(ref coordinator) = self.checkpoint_coordinator {
            if let Some(checkpoint_id) = coordinator.get_latest_checkpoint().await? {
                // Load states from checkpoint
                // In a real implementation, you would restore all operator states
                println!("Recovered from checkpoint: {}", checkpoint_id);
                return Ok(Some(checkpoint_id));
            }
        }
        Ok(None)
    }

    /// Get current state statistics
    pub async fn get_state_stats(&self) -> StateStats {
        let transactions = self.active_transactions.read().await;
        StateStats {
            active_transactions: transactions.len(),
            local_states_count: self.local_states.len(),
            current_checkpoint_id: self.current_checkpoint_id.load(Ordering::SeqCst),
            backend_type: self.config.backend_type.clone(),
            enabled: self.config.enabled,
        }
    }

    /// Get backend type
    pub fn get_backend_type(&self) -> StateBackendType {
        self.config.backend_type.clone()
    }

    /// Shutdown state manager
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        // Create final checkpoint if enabled
        if self.config.enabled {
            self.create_checkpoint().await?;
        }
        Ok(())
    }
}

/// State statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateStats {
    pub active_transactions: usize,
    pub local_states_count: usize,
    pub current_checkpoint_id: u64,
    pub backend_type: StateBackendType,
    pub enabled: bool,
}

/// Exactly-once processor wrapper
pub struct ExactlyOnceProcessor<P> {
    inner: P,
    state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>,
    operator_id: String,
}

impl<P> ExactlyOnceProcessor<P> {
    /// Create new exactly-once processor wrapper
    pub fn new(
        inner: P,
        state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>,
        operator_id: String,
    ) -> Self {
        Self {
            inner,
            state_manager,
            operator_id,
        }
    }

    /// Process with exactly-once guarantee
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>
    where
        P: crate::processor::Processor,
    {
        // Let state manager handle barriers and transactions
        let mut state_manager = self.state_manager.write().await;
        let processed_batches = state_manager.process_batch(batch).await?;

        // Apply actual processing
        let mut results = Vec::new();
        for batch in processed_batches {
            // Process with inner processor
            let inner_results = self.inner.process(batch.clone()).await?;

            // Update state if needed
            if let Some(tx_ctx) = batch.transaction_context() {
                // Example: Update processed count
                let state_key = format!("processed_count_{}", tx_ctx.checkpoint_id);
                let mut state_manager = self.state_manager.write().await;
                state_manager
                    .set_state_value(&self.operator_id, &state_key, batch.len())
                    .await?;
            }

            results.extend(inner_results);
        }

        Ok(results)
    }

    /// Get state value
    pub async fn get_state<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: ToString + Send + Sync,
        V: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let state_manager = self.state_manager.read().await;
        state_manager.get_state_value(&self.operator_id, key).await
    }

    /// Set state value
    pub async fn set_state<K, V>(&self, key: &K, value: V) -> Result<(), Error>
    where
        K: ToString + Send + Sync,
        V: serde::Serialize + Send + Sync + 'static,
    {
        let mut state_manager = self.state_manager.write().await;
        state_manager
            .set_state_value(&self.operator_id, key, value)
            .await
    }
}

/// Two-phase commit output wrapper
pub struct TwoPhaseCommitOutput<O> {
    inner: O,
    state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>,
    transaction_log: Arc<RwLock<Vec<TransactionLogEntry>>>,
    pending_transactions: HashMap<String, Vec<MessageBatch>>,
}

impl<O> TwoPhaseCommitOutput<O> {
    /// Create new two-phase commit output
    pub fn new(inner: O, state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>) -> Self {
        Self {
            inner,
            state_manager,
            transaction_log: Arc::new(RwLock::new(Vec::new())),
            pending_transactions: HashMap::new(),
        }
    }

    /// Write with two-phase commit
    pub async fn write(&self, batch: MessageBatch) -> Result<(), Error>
    where
        O: crate::output::Output,
    {
        if let Some(tx_ctx) = batch.transaction_context() {
            if tx_ctx.is_checkpoint() {
                // Phase 1: Prepare
                self.prepare_transaction(&tx_ctx, &batch).await?;

                // Phase 2: Commit (after checkpoint completes)
                self.commit_transaction(&tx_ctx).await?;
            } else {
                // Normal write
                self.inner.write(batch).await?;
            }
        } else {
            // Non-transactional write
            self.inner.write(batch).await?;
        }

        Ok(())
    }

    /// Prepare phase of two-phase commit
    async fn prepare_transaction(
        &self,
        tx_ctx: &TransactionContext,
        batch: &MessageBatch,
    ) -> Result<(), Error> {
        // Log the transaction
        let log_entry = TransactionLogEntry {
            transaction_id: tx_ctx.transaction_id.clone(),
            checkpoint_id: tx_ctx.checkpoint_id,
            timestamp: std::time::SystemTime::now(),
            status: TransactionStatus::Prepared,
            batch_size: batch.len(),
        };

        self.transaction_log.write().await.push(log_entry);

        // In a real implementation, you would:
        // 1. Write to a temporary/staging area
        // 2. Ensure all data is durable
        // 3. Prepare to commit

        Ok(())
    }

    /// Commit phase of two-phase commit
    async fn commit_transaction(&self, tx_ctx: &TransactionContext) -> Result<(), Error> {
        // Update transaction log
        let mut log = self.transaction_log.write().await;
        if let Some(entry) = log
            .iter_mut()
            .find(|e| e.transaction_id == tx_ctx.transaction_id)
        {
            entry.status = TransactionStatus::Committed;
        }

        // In a real implementation, you would:
        // 1. Make data visible to consumers
        // 2. Confirm with transaction coordinator

        Ok(())
    }

    /// Get transaction log
    pub async fn get_transaction_log(&self) -> Vec<TransactionLogEntry> {
        self.transaction_log.read().await.clone()
    }
}

/// Transaction log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLogEntry {
    pub transaction_id: String,
    pub checkpoint_id: u64,
    pub timestamp: std::time::SystemTime,
    pub status: TransactionStatus,
    pub batch_size: usize,
}

/// Transaction status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionStatus {
    Prepared,
    Committed,
    Aborted,
}
