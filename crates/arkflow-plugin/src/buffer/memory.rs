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

//! Memory Buffer Implementation
//!
//! This module implements a memory-based buffer that accumulates messages until
//! either a capacity threshold is reached or a timeout occurs. When either condition
//! is met, the buffer releases all accumulated messages as a single batch.

use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Configuration for the memory buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemoryBufferConfig {
    /// Maximum number of messages to accumulate before releasing
    capacity: u32,
    /// Maximum time to wait before releasing accumulated messages
    #[serde(deserialize_with = "deserialize_duration")]
    timeout: time::Duration,
}

/// Memory buffer implementation
/// Accumulates messages in memory until capacity or timeout conditions are met
struct MemoryBuffer {
    /// Configuration parameters for the memory buffer
    config: MemoryBufferConfig,
    /// Thread-safe queue to store message batches and their acknowledgments
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    /// Notification mechanism for signaling between threads
    notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    close: CancellationToken,
}

impl MemoryBuffer {
    /// Creates a new memory buffer with the given configuration
    ///
    /// # Arguments
    /// * `config` - Configuration parameters for the memory buffer
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new memory buffer instance or an error
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let duration = config.timeout.clone();
        let close = CancellationToken::new();
        let close_clone = close.clone();

        tokio::spawn(async move {
            loop {
                let timer = sleep(duration);
                tokio::select! {
                    _ = timer => {
                        // notify read
                        notify_clone.notify_waiters();
                    }
                    _ = close_clone.cancelled() => {
                         // notify read
                        notify_clone.notify_waiters();
                        break;
                    }
                    _ = notify_clone.notified() => {
                    }
                }
            }
        });
        Ok(Self {
            close,
            notify,
            config,
            queue: Arc::new(Default::default()),
        })
    }

    /// Processes accumulated messages by merging them into a single batch
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the queue is empty
    async fn process_messages(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let queue_arc = Arc::clone(&self.queue);
        let mut queue_lock = queue_arc.write().await;

        if queue_lock.is_empty() {
            return Ok(None);
        }

        let mut messages = Vec::new();
        let mut acks = Vec::new();

        while let Some((msg, ack)) = queue_lock.pop_back() {
            messages.push(msg);
            acks.push(ack);
        }

        if messages.is_empty() {
            return Ok(None);
        }
        let schema = messages[0].schema();
        let x: Vec<RecordBatch> = messages.into_iter().map(|batch| batch.into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(ArrayAck(acks));
        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    /// Writes a message batch to the memory buffer
    ///
    /// # Arguments
    /// * `msg` - The message batch to write
    /// * `arc` - The acknowledgment for the message batch
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error> {
        let queue_arc = Arc::clone(&self.queue);

        let mut queue_lock = queue_arc.write().await;
        queue_lock.push_front((msg, arc));

        // Calculate the total number of messages in the buffer
        let cnt = queue_lock.iter().map(|x| x.0.len()).reduce(|acc, x| {
            return acc + x;
        });
        let cnt = cnt.unwrap_or(0);

        // If capacity threshold is reached, notify readers to process the batch
        if cnt >= self.config.capacity as usize {
            let notify = self.notify.clone();
            notify.notify_waiters();
        }
        Ok(())
    }

    /// Reads a message batch from the memory buffer
    /// Waits until either messages are available or the buffer is closed
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the buffer is closed and empty
    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        loop {
            {
                let queue_arc = Arc::clone(&self.queue);
                let queue_lock = queue_arc.read().await;
                // If there are messages available, break the loop and process them
                if !queue_lock.is_empty() {
                    break;
                }
                // If the buffer is closed, return None
                if self.close.is_cancelled() {
                    return Ok(None);
                }
            }
            // Wait for notification from timer, write operation, or close
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        // Process and return the accumulated messages
        self.process_messages().await
    }

    /// Flushes the buffer by cancelling the background task and notifying waiters
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn flush(&self) -> Result<(), Error> {
        self.close.cancel();

        let queue_arc = Arc::clone(&self.queue);
        let queue_lock = queue_arc.read().await;
        if !queue_lock.is_empty() {
            // Notify any waiting readers to process remaining messages
            let notify = Arc::clone(&self.notify);
            notify.notify_waiters();
        }
        Ok(())
    }

    /// Closes the buffer by cancelling the background task
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        Ok(())
    }
}
/// Acknowledgment implementation that combines multiple acknowledgments
/// When acknowledged, it acknowledges all contained acknowledgments
struct ArrayAck(Vec<Arc<dyn Ack>>);

#[async_trait]
impl Ack for ArrayAck {
    /// Acknowledges all contained acknowledgments
    async fn ack(&self) {
        for ack in self.0.iter() {
            ack.ack().await;
        }
    }
}

struct MemoryBufferBuilder;

impl BufferBuilder for MemoryBufferBuilder {
    /// Builds a memory buffer from the provided configuration
    ///
    /// # Arguments
    /// * `config` - JSON configuration for the memory buffer
    ///
    /// # Returns
    /// * `Result<Arc<dyn Buffer>, Error>` - A new memory buffer instance or an error
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Memory buffer configuration is missing".to_string(),
            ));
        }

        let config: MemoryBufferConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MemoryBuffer::new(config)?))
    }
}

/// Initializes the memory buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("memory", Arc::new(MemoryBufferBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;

    #[tokio::test]
    async fn test_memory_buffer_capacity_limit() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 2,
            timeout: time::Duration::from_millis(100),
        })
        .unwrap();
        let msg1 = MessageBatch::new_binary(vec![b"a".to_vec()]).unwrap();
        let msg2 = MessageBatch::new_binary(vec![b"b".to_vec()]).unwrap();
        let msg3 = MessageBatch::new_binary(vec![b"c".to_vec()]).unwrap();
        buf.write(msg1, Arc::new(NoopAck)).await.unwrap();
        buf.write(msg2, Arc::new(NoopAck)).await.unwrap();
        buf.write(msg3, Arc::new(NoopAck)).await.unwrap();
        let r = tokio::time::timeout(time::Duration::from_millis(200), buf.read()).await;
        assert!(r.is_ok());
        let batch = r.unwrap().unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_memory_buffer_timeout_notify() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_millis(100),
        })
        .unwrap();
        let msg = MessageBatch::new_binary(vec![b"x".to_vec()]).unwrap();
        buf.write(msg, Arc::new(NoopAck)).await.unwrap();
        let r = tokio::time::timeout(time::Duration::from_millis(200), buf.read()).await;
        assert!(r.is_ok());
        let batch = r.unwrap().unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_memory_buffer_flush() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();
        let msg = MessageBatch::new_binary(vec![b"flush".to_vec()]).unwrap();
        buf.write(msg, Arc::new(NoopAck)).await.unwrap();
        let _ = buf.flush().await;
        let r = tokio::time::timeout(time::Duration::from_millis(100), buf.read()).await;
        assert!(r.is_ok());
        let batch = r.unwrap().unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_memory_buffer_close() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();
        let msg = MessageBatch::new_binary(vec![b"close".to_vec()]).unwrap();
        buf.write(msg, Arc::new(NoopAck)).await.unwrap();
        let _ = buf.close().await;
        let r = tokio::time::timeout(time::Duration::from_millis(100), buf.read()).await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_memory_buffer_concurrent_write_read() {
        let buf = Arc::new(
            MemoryBuffer::new(MemoryBufferConfig {
                capacity: 100,
                timeout: time::Duration::from_millis(100),
            })
            .unwrap(),
        );
        let buf2 = buf.clone();
        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let msg = MessageBatch::new_binary(vec![format!("msg{}", i).into_bytes()]).unwrap();
                buf2.write(msg, Arc::new(NoopAck)).await.unwrap();
            }
        });
        let mut total = 0;
        let mut tries = 0;
        while total < 10 && tries < 10 {
            let r = tokio::time::timeout(time::Duration::from_millis(200), buf.read()).await;
            if let Ok(Ok(Some((batch, _)))) = r {
                total += batch.len();
            }
            tries += 1;
        }
        handle.await.unwrap();
        assert_eq!(total, 10);
    }
}
