//! Batch Processor Components
//!
//! Batch multiple messages into one or more messages

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Batch processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessorConfig {
    /// Batch size
    pub count: usize,
    /// Batch timeout (ms)
    pub timeout_ms: u64,
    /// Batch data type
    pub data_type: String,
}

/// Batch Processor Components
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    batch: Arc<RwLock<Vec<MessageBatch>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

impl BatchProcessor {
    /// Create a new batch processor component
    pub fn new(config: BatchProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            batch: Arc::new(RwLock::new(Vec::with_capacity(config.count))),
            last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
        })
    }

    /// Check if the batch should be refreshed
    async fn should_flush(&self) -> bool {
        // 如果批处理已满，则刷新
        let batch = self.batch.read().await;
        if batch.len() >= self.config.count {
            return true;
        }
        let last_batch_time = self.last_batch_time.lock().await;
        // 如果超过超时时间且批处理不为空，则刷新
        if !batch.is_empty()
            && last_batch_time.elapsed().as_millis() >= self.config.timeout_ms as u128
        {
            return true;
        }

        false
    }

    /// Refresh the batch
    async fn flush(&self) -> Result<Vec<MessageBatch>, Error> {
        let mut batch = self.batch.write().await;

        if batch.is_empty() {
            return Ok(vec![]);
        }

        // Create a new batch message
        let new_batch = match self.config.data_type.as_str() {
            "arrow" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Arrow(v) = &msg.content {
                        combined_content.push(v.clone());
                    }
                }
                let schema = combined_content[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &combined_content)
                    .map_err(|e| Error::Processing(format!("Merge batches failed: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
            "binary" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Binary(v) = &msg.content {
                        combined_content.extend(v.clone());
                    }
                }
                Ok(vec![MessageBatch::new_binary(combined_content)])
            }
            _ => Err(Error::Processing("Invalid data type".to_string())),
        };

        batch.clear();
        let mut last_batch_time = self.last_batch_time.lock().await;

        *last_batch_time = std::time::Instant::now();

        new_batch
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match &msg.content {
            Content::Arrow(_) => {
                if self.config.data_type != "arrow" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
            Content::Binary(_) => {
                if self.config.data_type != "binary" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
        }

        {
            let mut batch = self.batch.write().await;

            // Add messages to a batch
            batch.push(msg);
        }

        // Check if the batch should be refreshed
        if self.should_flush().await {
            self.flush().await
        } else {
            // If it is not refreshed, an empty result is returned
            Ok(vec![])
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut batch = self.batch.write().await;

        batch.clear();
        Ok(())
    }
}

pub(crate) struct BatchProcessorBuilder;
impl ProcessorBuilder for BatchProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: BatchProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(BatchProcessor::new(config)?))
    }
}

pub fn init() {
    register_processor_builder("batch", Arc::new(BatchProcessorBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    // Helper function to create test configuration
    fn create_test_config(count: usize, timeout_ms: u64, data_type: &str) -> BatchProcessorConfig {
        BatchProcessorConfig {
            count,
            timeout_ms,
            data_type: data_type.to_string(),
        }
    }

    #[tokio::test]
    async fn test_batch_size_control() -> Result<(), Error> {
        // Test batch size control with binary data
        let config = create_test_config(2, 1000, "binary");
        let processor = BatchProcessor::new(config)?;

        // Process first message
        let msg1 = MessageBatch::new_binary(vec![vec![1u8, 2u8, 3u8]]); // 使用u8类型
        let result1 = processor.process(msg1).await?;
        assert!(result1.is_empty(), "First message should not trigger flush");

        // Process second message - should trigger flush due to batch size
        let msg2 = MessageBatch::new_binary(vec![vec![4u8, 5u8, 6u8]]); // 使用u8类型
        let result2 = processor.process(msg2).await?;

        assert_eq!(result2.len(), 1, "Should return one combined batch");
        if let Content::Binary(data) = &result2[0].content {
            assert_eq!(
                data,
                &vec![vec![1u8, 2u8, 3u8], vec![4u8, 5u8, 6u8]],
                "Combined binary data should match"
            );
        } else {
            panic!("Expected binary content");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout_flush() -> Result<(), Error> {
        // Test timeout-based flush with short timeout
        let config = create_test_config(5, 100, "binary");
        let processor = BatchProcessor::new(config)?;

        // Process one message and wait for timeout
        let msg = MessageBatch::new_binary(vec![vec![1u8, 2u8, 3u8]]); // 使用u8类型
        let result1 = processor.process(msg).await?;
        assert!(result1.is_empty(), "First message should not trigger flush");

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Process another message - should trigger flush due to timeout
        let msg2 = MessageBatch::new_binary(vec![vec![4u8, 5u8, 6u8]]); // 使用u8类型
        let result2 = processor.process(msg2).await?;

        assert_eq!(result2.len(), 1, "Should return one combined batch");
        if let Content::Binary(data) = &result2[0].content {
            assert_eq!(
                data,
                &vec![vec![1u8, 2u8, 3u8], vec![4u8, 5u8, 6u8]],
                "Timeout flush should contain both messages"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_data_type() -> Result<(), Error> {
        // Test error handling for mismatched data types
        let config = create_test_config(2, 1000, "arrow");
        let processor = BatchProcessor::new(config)?;

        // Try to process binary message with arrow configuration
        let msg = MessageBatch::new_binary(vec![vec![1u8, 2u8, 3u8]]); // 使用u8类型
        let result = processor.process(msg).await;

        assert!(result.is_err(), "Should return error for invalid data type");
        assert!(
            matches!(result, Err(Error::Processing(_))),
            "Should be processing error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_close() -> Result<(), Error> {
        // Test processor cleanup
        let config = create_test_config(2, 1000, "binary");
        let processor = BatchProcessor::new(config)?;

        // Add a message to the batch
        let msg = MessageBatch::new_binary(vec![vec![1u8, 2u8, 3u8]]); // 使用u8类型
        processor.process(msg).await?;

        // Close the processor
        processor.close().await?;

        // Verify batch is cleared
        let batch = processor.batch.read().await;
        assert!(batch.is_empty(), "Batch should be empty after close");

        Ok(())
    }
}
