use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time;

use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {
    max_cap: u32,
    duration: time::Duration,
}

pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    pub cond_rx: Arc<Receiver<()>>,
    pub write_tx: Arc<Sender<()>>,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        let duration = config.duration.clone();
        let (cond_tx, cond_rx) = tokio::sync::broadcast::channel(1);
        let (write_tx, mut write_rx) = tokio::sync::broadcast::channel(1);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = write_rx.recv() => {
                        match cond_tx.send(()) {
                            Ok(_) => {}
                            _ => {
                                break;
                            }
                        }
                    },
                    _ = sleep(duration) => {
                        match cond_tx.send(()) {
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
            cond_rx: Arc::new(cond_rx),
            write_tx: Arc::new(write_tx),
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

        // 合并所有消息
        let mut combined_message = messages.remove(0);
        for msg in messages {
            match (&mut combined_message.content, &msg.content) {
                (
                    arkflow_core::Content::Binary(combined),
                    arkflow_core::Content::Binary(to_add),
                ) => {
                    combined.extend(to_add);
                }
                (arkflow_core::Content::Arrow(combined), arkflow_core::Content::Arrow(to_add)) => {
                    let schema = combined.schema();
                    if let Ok(merged) =
                        datafusion::arrow::compute::concat_batches(&schema, &[combined, to_add])
                    {
                        *combined = merged;
                    }
                }
                _ => {}
            }
        }

        let combined_ack = Arc::new(ArrayAck(acks));

        Ok(Some((combined_message, combined_ack)))
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error> {
        let queue_arc = self.queue.clone();
        loop {
            {
                let queue_lock = queue_arc.read().await;
                if queue_lock.len() >= self.config.max_cap as usize {
                    let _ = self.write_tx.send(());
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
        let mut cond_rx_arc = self.cond_rx.clone();
        match &cond_rx_arc.recv().await {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::EOF);
            }
        };

        self.process_messages().await
    }

    async fn close(&self) -> Result<(), Error> {
        // 清空队列
        let queue_arc = self.queue.clone();
        let mut queue_lock = queue_arc.write().await;
        queue_lock.clear();
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
