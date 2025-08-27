//! Word count example using ArkFlow state management
//!
//! This example demonstrates how to build a stateful word counting application
//! with exactly-once processing semantics and persistent state.

use arkflow_core::state::{
    EnhancedStateConfig, EnhancedStateManager, ExactlyOnceProcessor, StateBackendType,
};
use arkflow_core::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Word count processor that maintains running counts
pub struct WordCountProcessor {
    // In this example, state is managed externally by the state manager
}

#[async_trait::async_trait]
impl arkflow_core::processor::Processor for WordCountProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut results = Vec::new();

        // Extract text from messages
        if let Ok(texts) = batch.to_binary("__value__") {
            for text in texts {
                let text = String::from_utf8_lossy(&text);
                let words: Vec<&str> = text.split_whitespace().collect();

                // Count word frequencies
                let mut word_counts = HashMap::new();
                for word in words {
                    *word_counts.entry(word.to_lowercase()).or_insert(0) += 1;
                }

                // Create result message with counts
                let result =
                    serde_json::to_vec(&word_counts).map_err(|e| Error::Serialization(e))?;

                let result_batch = MessageBatch::new_binary(vec![result])?;
                results.push(result_batch);
            }
        }

        Ok(results)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Enhanced word count processor with state management
pub struct StatefulWordCountProcessor {
    state_manager: Arc<RwLock<EnhancedStateManager>>,
    operator_id: String,
}

impl StatefulWordCountProcessor {
    pub fn new(state_manager: Arc<RwLock<EnhancedStateManager>>, operator_id: String) -> Self {
        Self {
            state_manager,
            operator_id,
        }
    }

    /// Get total word count from state
    pub async fn get_total_words(&self) -> Result<u64, Error> {
        let state_manager = self.state_manager.read().await;
        state_manager
            .get_state_value(&self.operator_id, &"total_words")
            .await
    }

    /// Get count for specific word
    pub async fn get_word_count(&self, word: &str) -> Result<u64, Error> {
        let state_manager = self.state_manager.read().await;
        state_manager
            .get_state_value(&self.operator_id, &format!("word_{}", word))
            .await
    }

    /// Get top N words by count
    pub async fn get_top_words(&self, n: usize) -> Result<Vec<(String, u64)>, Error> {
        let state_manager = self.state_manager.read().await;

        // This is a simplified example - in production, you'd maintain
        // a sorted data structure for better performance
        let mut word_counts = Vec::new();

        // Since we can't iterate over all keys directly, this example
        // assumes you track top words separately
        if let Some(top_words) = state_manager
            .get_state_value::<Vec<(String, u64)>>(&self.operator_id, &"top_words")
            .await?
        {
            word_counts = top_words;
        }

        word_counts.truncate(n);
        Ok(word_counts)
    }
}

#[async_trait::async_trait]
impl arkflow_core::processor::Processor for StatefulWordCountProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut results = Vec::new();

        // Extract text from messages
        if let Ok(texts) = batch.to_binary("__value__") {
            for text in texts {
                let text = String::from_utf8_lossy(&text);
                let words: Vec<&str> = text.split_whitespace().collect();

                // Update state
                let mut state_manager = self.state_manager.write().await;

                // Update total word count
                let total_words: u64 = state_manager
                    .get_state_value(&self.operator_id, &"total_words")
                    .await?
                    .unwrap_or(0);
                let new_total = total_words + words.len() as u64;
                state_manager
                    .set_state_value(&self.operator_id, &"total_words", new_total)
                    .await?;

                // Update individual word counts
                let mut word_counts = HashMap::new();
                for word in words {
                    let word_lower = word.to_lowercase();
                    let count: u64 = state_manager
                        .get_state_value(&self.operator_id, &format!("word_{}", word_lower))
                        .await?
                        .unwrap_or(0);
                    let new_count = count + 1;
                    state_manager
                        .set_state_value(
                            &self.operator_id,
                            &format!("word_{}", word_lower),
                            new_count,
                        )
                        .await?;
                    word_counts.insert(word_lower, new_count);
                }

                // Create result message
                let result =
                    serde_json::to_vec(&word_counts).map_err(|e| Error::Serialization(e))?;

                let result_batch = MessageBatch::new_binary(vec![result])?;
                results.push(result_batch);
            }
        }

        Ok(results)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Configuration for word count application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WordCountConfig {
    pub checkpoint_interval_ms: u64,
    pub state_backend: StateBackendType,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
}

impl Default for WordCountConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval_ms: 60000,
            state_backend: StateBackendType::Memory,
            s3_bucket: None,
            s3_region: None,
        }
    }
}

/// Build word count pipeline with exactly-once guarantees
pub async fn build_word_count_pipeline(
    config: WordCountConfig,
) -> Result<Arc<RwLock<EnhancedStateManager>>, Error> {
    // Configure state manager
    let state_config = EnhancedStateConfig {
        enabled: true,
        backend_type: config.state_backend.clone(),
        checkpoint_interval_ms: config.checkpoint_interval_ms,
        exactly_once: true,
        s3_config: if config.state_backend == StateBackendType::S3 {
            Some(arkflow_core::state::S3StateBackendConfig {
                bucket: config
                    .s3_bucket
                    .unwrap_or_else(|| "wordcount-state".to_string()),
                region: config.s3_region.unwrap_or_else(|| "us-east-1".to_string()),
                prefix: Some("wordcount/checkpoints".to_string()),
                ..Default::default()
            })
        } else {
            None
        },
        ..Default::default()
    };

    // Create state manager
    let state_manager = Arc::new(RwLock::new(EnhancedStateManager::new(state_config).await?));

    Ok(state_manager)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize logging
    env_logger::init();

    // Create configuration
    let config = WordCountConfig {
        checkpoint_interval_ms: 30000, // 30 seconds for demo
        state_backend: StateBackendType::Memory,
        ..Default::default()
    };

    // Build pipeline
    let state_manager = build_word_count_pipeline(config).await?;

    // Create processor
    let processor =
        StatefulWordCountProcessor::new(state_manager.clone(), "word_count_operator".to_string());

    // Wrap with exactly-once semantics
    let exactly_once_processor =
        ExactlyOnceProcessor::new(processor, state_manager.clone(), "word_count".to_string());

    // Process sample data
    let sample_texts = vec![
        "hello world",
        "hello rust",
        "stream processing with rust",
        "hello arkflow",
    ];

    for text in sample_texts {
        let batch = MessageBatch::from_string(text)?;
        let results = exactly_once_processor.process(batch).await?;

        // Print results
        for result in results {
            if let Ok(texts) = result.to_binary("__value__") {
                for text in texts {
                    let counts: HashMap<String, u64> = serde_json::from_slice(&text)?;
                    println!("Word counts: {:?}", counts);
                }
            }
        }
    }

    // Print statistics
    let processor_inner = exactly_once_processor;
    let total_words = processor_inner.get_state(&"total_words").await?;
    let hello_count = processor_inner.get_state(&"word_hello").await?;

    println!("\nFinal Statistics:");
    println!("Total words processed: {:?}", total_words);
    println!("'hello' count: {:?}", hello_count);

    // Create checkpoint
    let mut state_manager_write = state_manager.write().await;
    let checkpoint_id = state_manager_write.create_checkpoint().await?;
    println!("Created checkpoint: {}", checkpoint_id);

    Ok(())
}
