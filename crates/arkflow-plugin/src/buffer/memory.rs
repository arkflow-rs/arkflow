use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time;

use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {
    max_cap: u32,
    duration: time::Duration,
}

pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<Mutex<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    pub rx: Arc<Receiver<()>>,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        let duration = config.duration.clone();
        let (tx, rx) = tokio::sync::broadcast::<()>::channel(1);
        tokio::spawn(async {
            loop {
                sleep(duration).await;
                match tx.send(()) {
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        });
        Ok(Self {
            config,
            queue: Arc::new(Default::default()),
            rx: Arc::new(rx),
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

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut rx_arc = self.rx.clone();
        let result = rx_arc.recv().await;
        if result.is_err() {
            return Ok(None);
        }

        let queue_arc = self.queue.clone();
        let mut queue_lock = queue_arc.lock().await;
        let mut v = vec![];
        let mut acks = vec![];
        while let Some((msg, ack)) = queue_lock.pop_front() {
            v.push(msg);
            acks.push(ack);
        }
        Ok(queue_lock.pop_back())
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}
struct ArrayAck(Vec<Arc<dyn Ack>>);
impl Ack for ArrayAck {
    async fn ack(&self) {
        let x = &self.0;
        for ack in &self.0.iter() {
            ack.ack().await;
        }
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
