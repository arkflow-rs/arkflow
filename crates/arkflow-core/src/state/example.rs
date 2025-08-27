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

//! Simplified state management example

use crate::state::SimpleMemoryState;
use crate::state::StateHelper;
use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Example of using state management in a processor
pub struct CountingProcessor {
    /// State for counting events per key
    count_state: Arc<tokio::sync::RwLock<SimpleMemoryState>>,
    /// Operator ID for this processor
    operator_id: String,
}

impl CountingProcessor {
    /// Create new counting processor
    pub fn new(operator_id: String) -> Self {
        Self {
            count_state: Arc::new(tokio::sync::RwLock::new(SimpleMemoryState::new())),
            operator_id,
        }
    }

    /// Process a batch and count events
    pub async fn process_batch(&self, batch: &crate::MessageBatch) -> Result<(), Error> {
        // Extract transaction context if present
        if let Some(tx_ctx) = batch.transaction_context() {
            println!(
                "Processing batch in transaction: checkpoint_id={}",
                tx_ctx.checkpoint_id
            );
        }

        // Example: Count messages by input source
        if let Some(input_name) = batch.get_input_name() {
            let key = format!("count_{}", input_name);

            let mut state = self.count_state.write().await;
            let current_count: Option<u64> = state.get_typed(&key)?;
            let new_count = current_count.unwrap_or(0) + batch.len() as u64;
            state.put_typed(&key, new_count)?;

            println!("Updated count for {}: {}", input_name, new_count);
        }

        Ok(())
    }

    /// Get current count for an input source
    pub async fn get_count(&self, input_name: &str) -> Result<u64, Error> {
        let key = format!("count_{}", input_name);
        let state = self.count_state.read().await;
        state.get_typed(&key)?.unwrap_or(0)
    }

    /// Example of keyed state processing
    pub async fn process_keyed_batch<K, V>(
        &self,
        batch: &crate::MessageBatch,
        key_column: &str,
    ) -> Result<(), Error>
    where
        K: for<'de> Deserialize<'de> + Send + Sync + 'static + ToString,
        V: for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // This is a simplified example - in practice you'd extract keys from the batch
        // For now, we'll just demonstrate the pattern

        // Simulate extracting keys and values from the batch
        let mut state = self.count_state.write().await;

        // Example: Process each row in the batch
        for _ in 0..batch.len() {
            // In a real implementation, you'd extract actual key-value pairs from the batch
            let dummy_key = "example_key".to_string();
            let dummy_value: u64 = 42;

            let state_key = format!("keyed_{}_{}", self.operator_id, dummy_key);
            let current: Option<u64> = state.get_typed(&state_key)?;
            let updated = current.unwrap_or(0) + dummy_value;
            state.put_typed(&state_key, updated)?;
        }

        Ok(())
    }
}

/// Example configuration for state management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Whether state management is enabled
    pub enabled: bool,
    /// State backend type
    pub backend: StateBackendType,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// State TTL in milliseconds (0 = no expiration)
    pub state_ttl_ms: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: StateBackendType::Memory,
            checkpoint_interval_ms: 60000, // 1 minute
            state_ttl_ms: 0,               // No expiration
        }
    }
}

/// State backend types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateBackendType {
    /// In-memory state backend
    Memory,
    /// File system state backend
    FileSystem,
    /// S3 state backend
    S3,
}

/// Example of using state management with configuration
pub struct StatefulProcessor<T> {
    inner: T,
    state: Arc<tokio::sync::RwLock<SimpleMemoryState>>,
    config: StateConfig,
    operator_id: String,
}

impl<T> StatefulProcessor<T> {
    /// Create new stateful processor wrapper
    pub fn new(inner: T, config: StateConfig, operator_id: String) -> Self {
        Self {
            inner,
            state: Arc::new(tokio::sync::RwLock::new(SimpleMemoryState::new())),
            config,
            operator_id,
        }
    }

    /// Get access to the state
    pub fn state(&self) -> Arc<tokio::sync::RwLock<SimpleMemoryState>> {
        self.state.clone()
    }

    /// Get configuration
    pub fn config(&self) -> &StateConfig {
        &self.config
    }
}

/// Example usage
#[tokio::main]
async fn example_usage() -> Result<(), Error> {
    // Create a counting processor
    let processor = CountingProcessor::new("counter_1".to_string());

    // Create a sample message batch
    let batch = crate::MessageBatch::from_string("hello world")?;

    // Process the batch
    processor.process_batch(&batch).await?;

    // Get the count
    let count = processor.get_count("unknown").await?;
    println!("Total count: {}", count);

    Ok(())
}
