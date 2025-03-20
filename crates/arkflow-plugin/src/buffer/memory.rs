use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {
    max_cap: u32,
}

pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<Mutex<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            queue: Arc::new(Default::default()),
        })
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error> {
        let queue_arc = self.queue.clone();
        let mut queue_lock = queue_arc.lock().await;
        if queue_lock.len() >= self.config.max_cap as usize {
            queue_lock.pop_back();
        }
        queue_lock.push_front((msg, arc));

        Ok(())
    }

    async fn reade(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        todo!()
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}

struct MemoryBufferBuilder;

impl BufferBuilder for MemoryBufferBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Memory buffer configuration is missing".to_string(),
            ));
        }

        let config: MemoryBufferConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MemoryBuffer::new(config)?))
    }
}

pub fn init() {
    register_buffer_builder("memory", Arc::new(MemoryBufferBuilder))
}
