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

//! Batch Processor Components
//!
//! Batch multiple messages into one or more messages

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Batch processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchProcessorConfig {
    /// Batch size
    count: usize,
    /// Batch timeout (ms)
    timeout_ms: u64,
}

/// Batch Processor Components
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    batch: Arc<RwLock<Vec<MessageBatch>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

impl BatchProcessor {
    /// Create a new batch processor component
    fn new(config: BatchProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            batch: Arc::new(RwLock::new(Vec::with_capacity(config.count))),
            last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
        })
    }

    /// Check if the batch should be refreshed
    async fn should_flush(&self) -> bool {
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

        let schema = batch[0].schema();
        let x: Vec<RecordBatch> = batch.iter().map(|batch| batch.clone().into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
        let new_batch = Ok(vec![MessageBatch::new_arrow(new_batch)]);

        batch.clear();
        let mut last_batch_time = self.last_batch_time.lock().await;

        *last_batch_time = std::time::Instant::now();

        new_batch
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
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

struct BatchProcessorBuilder;
impl ProcessorBuilder for BatchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: BatchProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(BatchProcessor::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder("batch", Arc::new(BatchProcessorBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_batch_processor_size() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 1000,
        })
        .unwrap();

        // First message should not trigger flush
        let result = processor
            .process(MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap())
            .await
            .unwrap();
        assert!(result.is_empty());

        // Second message should trigger flush due to batch size
        let result = processor
            .process(MessageBatch::new_binary(vec!["test2".as_bytes().to_vec()]).unwrap())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_processor_timeout() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 100,
        })
        .unwrap();

        // Add one message
        let result = processor
            .process(MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap())
            .await
            .unwrap();
        assert!(result.is_empty());

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Next message should trigger flush due to timeout
        let result = processor
            .process(MessageBatch::new_binary(vec!["test2".as_bytes().to_vec()]).unwrap())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_processor_empty() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 1000,
        })
        .unwrap();

        let result = processor.flush().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_close() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 1000,
        })
        .unwrap();

        // Add a message to the batch
        processor
            .process(MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap())
            .await
            .unwrap();

        // Close the processor
        processor.close().await.unwrap();

        // Verify the batch is empty by checking that flush returns empty
        let result = processor.flush().await.unwrap();
        assert!(result.is_empty());
    }
}
