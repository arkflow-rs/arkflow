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

//! Basic state management example without complex trait modifications

use super::enhanced::{TransactionLogEntry, TransactionStatus};
use crate::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Example processor that maintains state without modifying trait signatures
pub struct StatefulExampleProcessor {
    /// Internal state storage
    state: Arc<tokio::sync::RwLock<HashMap<String, serde_json::Value>>>,
    /// Processor name
    name: String,
}

impl StatefulExampleProcessor {
    /// Create new stateful processor
    pub fn new(name: String) -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            name,
        }
    }

    /// Process messages with state access
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // Check for transaction context in metadata
        if let Some(tx_ctx) = batch.transaction_context() {
            println!(
                "Processing batch with transaction: checkpoint_id={}",
                tx_ctx.checkpoint_id
            );
        }

        // Example: Count messages by input source
        if let Some(input_name) = batch.get_input_name() {
            let count_key = format!("count_{}", input_name);

            let mut state = self.state.write().await;
            let current_count = state.get(&count_key).and_then(|v| v.as_u64()).unwrap_or(0);
            let new_count = current_count + batch.len() as u64;

            state.insert(
                count_key,
                serde_json::Value::Number(serde_json::Number::from(new_count)),
            );

            println!(
                "Processor {}: Updated count for {} to {}",
                self.name, input_name, new_count
            );
        }

        // Process the batch (normally you'd do actual transformation here)
        Ok(vec![batch])
    }

    /// Get current count for an input
    pub async fn get_count(&self, input_name: &str) -> Result<u64, Error> {
        let state = self.state.read().await;
        let count_key = format!("count_{}", input_name);
        Ok(state.get(&count_key).and_then(|v| v.as_u64()).unwrap_or(0))
    }

    /// Get all current state (for debugging/monitoring)
    pub async fn get_state_snapshot(&self) -> HashMap<String, serde_json::Value> {
        self.state.read().await.clone()
    }
}

/// Transaction-aware wrapper for existing Output implementations
pub struct TransactionalOutputWrapper<O> {
    inner: O,
    transaction_log: Arc<tokio::sync::RwLock<Vec<TransactionLogEntry>>>,
}

impl<O> TransactionalOutputWrapper<O> {
    /// Create new transactional output wrapper
    pub fn new(inner: O) -> Self {
        Self {
            inner,
            transaction_log: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    /// Write with transaction support
    pub async fn write(&self, msg: MessageBatch) -> Result<(), Error>
    where
        O: crate::output::Output,
    {
        // Check if this is a transactional batch
        if let Some(tx_ctx) = msg.transaction_context() {
            // Log the transaction
            let log_entry = TransactionLogEntry {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id: tx_ctx.checkpoint_id,
                timestamp: std::time::SystemTime::now(),
                status: TransactionStatus::Prepared,
                batch_size: msg.len(),
            };

            self.transaction_log.write().await.push(log_entry);

            // Write to the actual output
            self.inner.write(msg).await?;

            // Mark as committed
            if let Some(entry) = self.transaction_log.write().await.last_mut() {
                entry.status = TransactionStatus::Committed;
            }
        } else {
            // Non-transactional write
            self.inner.write(msg).await?;
        }

        Ok(())
    }

    /// Get transaction log
    pub async fn get_transaction_log(&self) -> Vec<TransactionLogEntry> {
        self.transaction_log.read().await.clone()
    }
}

/// Barrier injector for inserting checkpoints into the stream
pub struct SimpleBarrierInjector {
    interval: std::time::Duration,
    last_injection: Arc<tokio::sync::RwLock<std::time::Instant>>,
    next_checkpoint_id: Arc<AtomicU64>,
}

impl SimpleBarrierInjector {
    /// Create new barrier injector
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval: std::time::Duration::from_millis(interval_ms),
            last_injection: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
            next_checkpoint_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Check if barrier should be injected
    pub async fn should_inject(&self) -> bool {
        let last = *self.last_injection.read().await;
        last.elapsed() >= self.interval
    }

    /// Inject barrier into a batch if needed
    pub async fn maybe_inject_barrier(&self, batch: MessageBatch) -> Result<MessageBatch, Error> {
        if self.should_inject().await {
            let checkpoint_id = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);

            // Create transaction context
            let tx_ctx =
                crate::state::transaction::TransactionContext::aligned_checkpoint(checkpoint_id);

            // Create metadata with transaction
            let mut metadata = crate::state::Metadata::new();
            metadata.transaction = Some(tx_ctx);

            // Embed metadata into batch
            *self.last_injection.write().await = std::time::Instant::now();
            batch.with_metadata(metadata)
        } else {
            Ok(batch)
        }
    }
}

/// Configuration for state management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleStateConfig {
    /// Enable state management features
    pub enabled: bool,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// Enable transactional outputs
    pub transactional_outputs: bool,
}

impl Default for SimpleStateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            checkpoint_interval_ms: 60000, // 1 minute
            transactional_outputs: false,
        }
    }
}

/// Example usage
pub async fn example_usage() -> Result<(), Error> {
    // Create configuration
    let config = SimpleStateConfig {
        enabled: true,
        checkpoint_interval_ms: 5000, // 5 seconds for demo
        transactional_outputs: true,
    };

    if config.enabled {
        // Create stateful processor
        let processor = StatefulExampleProcessor::new("example_processor".to_string());

        // Create barrier injector
        let barrier_injector = SimpleBarrierInjector::new(config.checkpoint_interval_ms);

        // Process some messages
        let batch1 = MessageBatch::from_string("hello")?;
        let batch1_with_barrier = barrier_injector.maybe_inject_barrier(batch1).await?;
        let _result = processor.process(batch1_with_barrier).await?;

        let batch2 = MessageBatch::from_string("world")?;
        let batch2_with_barrier = barrier_injector.maybe_inject_barrier(batch2).await?;
        let _result = processor.process(batch2_with_barrier).await?;

        // Check counts
        let count = processor.get_count("unknown").await?;
        println!("Total messages processed: {}", count);

        // Show state snapshot
        let snapshot = processor.get_state_snapshot().await;
        println!("Current state: {:?}", snapshot);
    }

    Ok(())
}
