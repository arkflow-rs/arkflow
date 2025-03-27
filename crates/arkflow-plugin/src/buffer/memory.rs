use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar};
use std::time;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {
    capacity: u32,
    #[serde(deserialize_with = "deserialize_duration")]
    timeout: time::Duration,
}

pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    pub cond_rx: Arc<Receiver<()>>,
    pub cond_tx: Arc<Sender<()>>,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        let duration = config.timeout.clone();
        let (cond_tx, cond_rx) = tokio::sync::broadcast::channel(1);
        let cond_rx = Arc::new(cond_rx);
        let cond_tx = Arc::new(cond_tx);
        let cond_tx_clone = Arc::clone(&cond_tx);
        tokio::spawn(async move {
            loop {
                tokio::select! {

                    _ = sleep(duration) => {
                        match cond_tx_clone.send(()) {
                            Ok(_) => {}
                            _ => {
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            config,
            queue: Arc::new(Default::default()),
            cond_rx,
            cond_tx,
        })
    }

    // 处理队列中的所有消息
    async fn process_messages(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let queue_arc = self.queue.clone();
        let mut queue_lock = queue_arc.write().await;

        // 如果队列为空，返回 None
        if queue_lock.is_empty() {
            return Ok(None);
        }

        // 收集所有消息和确认
        let mut messages = Vec::new();
        let mut acks = Vec::new();

        while let Some((msg, ack)) = queue_lock.pop_back() {
            messages.push(msg);
            acks.push(ack);
        }

        // 如果没有消息，返回 None
        if messages.is_empty() {
            return Ok(None);
        }
        let schema = messages[0].schema();
        let x: Vec<RecordBatch> = messages.iter().map(|batch| batch.clone().into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(ArrayAck(acks));
        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error> {
        let queue_arc = self.queue.clone();
        loop {
            {
                let queue_lock = queue_arc.read().await;
                if queue_lock.len() >= self.config.capacity as usize {
                    let _ = self.cond_tx.clone().send(());
                } else {
                    break;
                }
            }
        }

        let mut queue_lock = queue_arc.write().await;

        queue_lock.push_front((msg, arc));

        Ok(())
    }

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut rx = self.cond_rx.resubscribe();
        if let Err(e) = rx.recv().await {
            error!("receiver error:{}", e);
            return Err(Error::EOF);
        }

        self.process_messages().await
    }

    async fn flush(&self) -> Result<(), Error> {
        let _ = self.cond_tx.clone().send(());
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
struct ArrayAck(Vec<Arc<dyn Ack>>);
#[async_trait]
impl Ack for ArrayAck {
    async fn ack(&self) {
        for ack in self.0.iter() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;

    #[tokio::test]
    async fn test_memory_buffer_new() {
        let p = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();

        let x = p
            .write(
                MessageBatch::new_binary(vec!["test".as_bytes().to_vec()]).unwrap(),
                Arc::new(NoopAck),
            )
            .await;
    }
}
