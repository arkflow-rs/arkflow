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

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
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
    batch: Arc<RwLock<Vec<MessageBatchRef>>>,
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
    async fn flush(&self) -> Result<Vec<MessageBatchRef>, Error> {
        let mut batch = self.batch.write().await;

        if batch.is_empty() {
            return Ok(vec![]);
        }

        let schema = batch[0].schema();
        let x: Vec<RecordBatch> = batch.iter().map(|b| (**b).clone().into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
        let result = vec![Arc::new(MessageBatch::new_arrow(new_batch))];

        batch.clear();
        let mut last_batch_time = self.last_batch_time.lock().await;

        *last_batch_time = std::time::Instant::now();

        Ok(result)
    }

    fn as_process_result(batches: Vec<MessageBatchRef>) -> ProcessResult {
        match batches.len() {
            0 => ProcessResult::None,
            1 => ProcessResult::Single(batches.into_iter().next().unwrap()),
            _ => ProcessResult::Multiple(batches),
        }
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatchRef) -> Result<ProcessResult, Error> {
        {
            let mut batch = self.batch.write().await;
            // Add messages to a batch
            batch.push(msg);
        }

        // Check if the batch should be refreshed
        if self.should_flush().await {
            let batches = self.flush().await?;
            Ok(Self::as_process_result(batches))
        } else {
            // If it is not refreshed, return None (filtered)
            Ok(ProcessResult::None)
        }
    }

    async fn finish(&self) -> Result<ProcessResult, Error> {
        // EOS: emit the partial batch so its acknowledgements settle through
        // the normal output path instead of dying in `close`.
        let batches = self.flush().await?;
        Ok(Self::as_process_result(batches))
    }

    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        // Idle input: fire the timeout trigger without waiting for the next
        // arrival to run the flush check.
        if !self.should_flush().await {
            return Ok(ProcessResult::None);
        }
        let batches = self.flush().await?;
        Ok(Self::as_process_result(batches))
    }

    async fn close(&self) -> Result<(), Error> {
        let mut batch = self.batch.write().await;
        if !batch.is_empty() {
            // Only reachable when the chain exited without an orderly EOS
            // drain (`finish` already emitted on normal shutdown paths).
            let rows: usize = batch.iter().map(|b| b.len()).sum();
            tracing::warn!(
                batches = batch.len(),
                rows,
                "batch processor closed with retained messages; dropping them"
            );
            batch.clear();
        }
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
    register_processor_builder("batch", Arc::new(BatchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "batch",
        "Batches messages by count with an idle timeout before forwarding.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "count": {"type": "integer", "minimum": 1, "description": "Number of messages per batch."},
                "timeout_ms": {"type": "integer", "minimum": 1, "description": "Idle timeout that flushes a partial batch (milliseconds)."}
            },
            "required": ["count", "timeout_ms"]
        }),
    ).with_optional().with_example(serde_json::json!({"count": 100, "timeout_ms": 5000})))
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
            .process(Arc::new(
                MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();
        assert!(result.is_empty());

        // Second message should trigger flush due to batch size
        let result = processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["test2".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 2); // 2 messages combined
            }
            _ => panic!("Expected single result"),
        }
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
            .process(Arc::new(
                MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();
        assert!(result.is_empty());

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Next message should trigger flush due to timeout
        let result = processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["test2".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 2); // 2 messages combined
            }
            _ => panic!("Expected single result"),
        }
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
            .process(Arc::new(
                MessageBatch::new_binary(vec!["test1".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        // Orderly shutdown drains the partial batch through `finish`
        // before `close` releases the processor.
        let drained = processor.finish().await.unwrap();
        assert!(matches!(drained, ProcessResult::Single(ref b) if b.len() == 1));

        // Close the processor
        processor.close().await.unwrap();

        // Verify the batch is empty by checking that flush returns empty
        let result = processor.flush().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_finish_drains_partial() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 60_000,
        })
        .unwrap();

        // Two messages below the count threshold: no flush on process
        processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["a".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();
        processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["b".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        // EOS drains the partial batch instead of dropping it in close
        match processor.finish().await.unwrap() {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 2),
            other => panic!(
                "expected ProcessResult::Single, got empty: {}",
                other.is_empty()
            ),
        }

        // A second finish has nothing left to emit
        assert!(processor.finish().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_on_tick_flushes_timeout() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 100,
        })
        .unwrap();

        processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["late".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        // Before the timeout elapses the tick is a no-op
        assert!(processor.on_tick().await.unwrap().is_empty());

        sleep(Duration::from_millis(150)).await;

        // The idle tick fires the timeout flush without a new arrival
        match processor.on_tick().await.unwrap() {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 1),
            other => panic!(
                "expected ProcessResult::Single, got empty: {}",
                other.is_empty()
            ),
        }
    }

    #[tokio::test]
    async fn test_batch_processor_flush_failure_retains_buffer() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        // Two batches with incompatible schemas make the merge fail
        processor
            .process(Arc::new(
                MessageBatch::new_binary(vec!["a".as_bytes().to_vec()]).unwrap(),
            ))
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arrow_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(arrow_batch)))
            .await;

        // The merge failure propagates...
        assert!(result.is_err());

        // ...and the buffer retained both messages: retrying the merge
        // fails again instead of reporting an empty buffer.
        assert!(processor.finish().await.is_err());

        // After a failed flush the buffer is still occupied
        let batch = processor.batch.read().await;
        assert_eq!(batch.len(), 2);
    }
}
