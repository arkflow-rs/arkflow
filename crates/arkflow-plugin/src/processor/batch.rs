//! Batch Processor Components
//!
//! Batch multiple messages into one or more messages

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
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
