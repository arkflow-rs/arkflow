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
use arkflow_core::component::{register_buffer_metadata, ComponentMetadata};
use arkflow_core::input::{Ack, VecAck};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
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
    #[allow(clippy::type_complexity)]
    queue: Arc<RwLock<VecDeque<(MessageBatchRef, Arc<dyn Ack>)>>>,
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
        let duration = config.timeout;
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
    /// The merge runs on clones while the queue is still intact; the queue is
    /// only cleared after the merge succeeded, so a merge failure leaves every
    /// retained message and acknowledgement available for a later retry.
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the queue is empty
    async fn process_messages(&self) -> Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error> {
        let mut queue_lock = self.queue.write().await;

        if queue_lock.is_empty() {
            return Ok(None);
        }

        // Writes push to the front, so the back of the deque is the oldest
        // delivery; merge oldest-first to preserve arrival order.
        let schema = queue_lock.back().map(|(msg, _)| msg.schema()).unwrap();
        let x: Vec<RecordBatch> = queue_lock
            .iter()
            .rev()
            .map(|(msg, _)| (**msg).clone().into())
            .collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
        let acks: Vec<Arc<dyn Ack>> = queue_lock
            .iter()
            .rev()
            .map(|(_, ack)| Arc::clone(ack))
            .collect();

        queue_lock.clear();
        // Capacity released: wake writers that are waiting for room.
        self.notify.notify_waiters();
        drop(queue_lock);

        let new_ack: Arc<dyn Ack> = Arc::new(VecAck(acks));
        Ok(Some((
            Arc::new(MessageBatch::new_arrow(new_batch)),
            new_ack,
        )))
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    /// Writes a message batch to the memory buffer
    ///
    /// Once the retained message count reaches `capacity`, the write awaits a
    /// drain instead of accumulating further, so backpressure propagates
    /// upstream. An awaiting write is released when the buffer closes.
    ///
    /// # Arguments
    /// * `msg` - The message batch to write
    /// * `arc` - The acknowledgment for the message batch
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn write(&self, msg: MessageBatchRef, arc: Arc<dyn Ack>) -> Result<(), Error> {
        loop {
            // Register for a capacity notification before checking the fill
            // level: a drain between the check and the wait must not be lost
            // (the periodic timer would only paper over it one timeout
            // later).
            let mut notified = std::pin::pin!(self.notify.notified());
            notified.as_mut().enable();
            {
                let mut queue_lock = self.queue.write().await;
                // Calculate the total number of messages in the buffer
                let cnt: usize = queue_lock.iter().map(|x| x.0.len()).sum();

                if cnt < self.config.capacity as usize {
                    queue_lock.push_front((Arc::clone(&msg), Arc::clone(&arc)));
                    // If capacity threshold is reached, notify readers to process the batch
                    if cnt + msg.len() >= self.config.capacity as usize {
                        self.notify.notify_waiters();
                    }
                    return Ok(());
                }
                // At capacity: wait for a drain (or close) outside the lock.
            }
            tokio::select! {
                _ = notified => {}
                _ = self.close.cancelled() => {
                    return Err(Error::Process(
                        "memory buffer closed while awaiting capacity".to_string(),
                    ));
                }
            }
        }
    }

    /// Reads a message batch from the memory buffer
    /// Waits until either messages are available or the buffer is closed
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the buffer is closed and empty
    async fn read(&self) -> Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error> {
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

    /// Flushes the buffer by waking waiting readers
    ///
    /// Non-terminal: the accumulated messages are drained by the readers and
    /// the timeout-release task keeps running. Only `close` terminates it.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn flush(&self) -> Result<(), Error> {
        self.notify.notify_waiters();
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
        // The capacity bound is what makes `write` backpressure instead of
        // accumulating without limit; a zero capacity would block every
        // write forever, so reject it here rather than at the schema layer
        // only (serde does not enforce the documented `minimum: 1`).
        if config.capacity == 0 {
            return Err(Error::Config(
                "memory buffer 'capacity' must be at least 1".to_string(),
            ));
        }
        Ok(Arc::new(MemoryBuffer::new(config)?))
    }
}

/// Initializes the memory buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("memory", Arc::new(MemoryBufferBuilder))?;
    register_buffer_metadata(ComponentMetadata::with_schema(
        "memory",
        "In-memory buffer that releases a batch when it reaches capacity or after a timeout.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "capacity": {"type": "integer", "minimum": 1, "description": "Maximum number of messages to accumulate before releasing."},
                "timeout": {"type": "string", "description": "Maximum time to wait before releasing a partial batch (humantime)."}
            },
            "required": ["capacity", "timeout"]
        }),
    ).with_example(serde_json::json!({"capacity": 1000, "timeout": "5s"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Acknowledgement double that can inject a failure and counts undos.
    struct RecordingAck {
        fail: bool,
        undos: AtomicUsize,
    }

    impl RecordingAck {
        fn new(fail: bool) -> Arc<Self> {
            Arc::new(Self {
                fail,
                undos: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail {
                Err(Error::Process(
                    "injected acknowledgement failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }

        async fn undo(&self) -> Result<(), Error> {
            self.undos.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn binary_msg(body: &str) -> MessageBatchRef {
        Arc::new(MessageBatch::new_binary(vec![body.as_bytes().to_vec()]).unwrap())
    }

    #[tokio::test]
    async fn test_memory_buffer_capacity_limit() {
        let buf = Arc::new(
            MemoryBuffer::new(MemoryBufferConfig {
                capacity: 2,
                timeout: time::Duration::from_millis(100),
            })
            .unwrap(),
        );
        let reader_buf = Arc::clone(&buf);
        let reader = tokio::spawn(async move {
            let mut total = 0usize;
            while total < 3 {
                match reader_buf.read().await {
                    Ok(Some((batch, _))) => total += batch.len(),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            total
        });

        // The third write blocks at capacity until the reader drains.
        for body in ["a", "b", "c"] {
            buf.write(binary_msg(body), Arc::new(NoopAck))
                .await
                .unwrap();
        }

        let total = tokio::time::timeout(time::Duration::from_secs(2), reader)
            .await
            .expect("reads should complete as the writer is released")
            .unwrap();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn test_memory_buffer_timeout_notify() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_millis(100),
        })
        .unwrap();
        buf.write(binary_msg("x"), Arc::new(NoopAck)).await.unwrap();
        let r = tokio::time::timeout(time::Duration::from_millis(200), buf.read()).await;
        assert!(r.is_ok());
        let batch = r.unwrap().unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_memory_buffer_flush_keeps_timeout_release() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_millis(100),
        })
        .unwrap();
        buf.write(binary_msg("first"), Arc::new(NoopAck))
            .await
            .unwrap();
        buf.flush().await.unwrap();

        // The pending message is readable right after the flush.
        let first = tokio::time::timeout(time::Duration::from_secs(1), buf.read())
            .await
            .expect("flush should make pending messages readable")
            .unwrap()
            .expect("pending message expected");
        assert_eq!(first.0.len(), 1);

        // A later write below capacity must still be released by the
        // timeout task: flush must not have terminated it.
        buf.write(binary_msg("second"), Arc::new(NoopAck))
            .await
            .unwrap();
        let second = tokio::time::timeout(time::Duration::from_millis(400), buf.read())
            .await
            .expect("timeout release must still fire after a flush")
            .unwrap()
            .expect("message expected");
        assert_eq!(second.0.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_buffer_close() {
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();
        buf.write(binary_msg("close"), Arc::new(NoopAck))
            .await
            .unwrap();
        buf.close().await.unwrap();

        // Pending messages are drained after close...
        let pending = tokio::time::timeout(time::Duration::from_millis(100), buf.read())
            .await
            .expect("close must not hang a pending read")
            .unwrap()
            .expect("pending message expected");
        assert_eq!(pending.0.len(), 1);

        // ...then the reader observes the end of the stream.
        let end = tokio::time::timeout(time::Duration::from_millis(100), buf.read())
            .await
            .expect("closed empty buffer must return")
            .unwrap();
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_memory_buffer_close_unblocks_awaiting_write() {
        let buf = Arc::new(
            MemoryBuffer::new(MemoryBufferConfig {
                capacity: 1,
                timeout: time::Duration::from_secs(10),
            })
            .unwrap(),
        );
        buf.write(binary_msg("full"), Arc::new(NoopAck))
            .await
            .unwrap();

        let writer_buf = Arc::clone(&buf);
        let mut writer = tokio::spawn(async move {
            writer_buf
                .write(binary_msg("overflow"), Arc::new(NoopAck))
                .await
        });

        // The overflowing write sits at capacity and must not complete
        // while nothing drains.
        sleep(time::Duration::from_millis(50)).await;
        assert!(
            tokio::time::timeout(time::Duration::from_millis(100), &mut writer)
                .await
                .is_err(),
            "write should await capacity release"
        );

        // Closing releases the awaiting write instead of hanging shutdown.
        buf.close().await.unwrap();
        let result = tokio::time::timeout(time::Duration::from_secs(1), writer)
            .await
            .expect("close must release the awaiting write")
            .unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_buffer_zero_capacity_rejected() {
        use arkflow_core::Resource;
        use std::cell::RefCell;

        let resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: RefCell::new(Vec::new()),
        };
        let error = MemoryBufferBuilder
            .build(
                None,
                &Some(serde_json::json!({"capacity": 0, "timeout": "1s"})),
                &resource,
            )
            .err()
            .expect("zero capacity must be rejected at build time");
        assert!(error.to_string().contains("capacity"));
    }

    #[tokio::test]
    async fn test_memory_buffer_merge_failure_retains_queue() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();

        buf.write(binary_msg("a"), Arc::new(NoopAck)).await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arrow_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        buf.write(
            Arc::new(MessageBatch::new_arrow(arrow_batch)),
            Arc::new(NoopAck),
        )
        .await
        .unwrap();

        // The merge fails on the incompatible schemas...
        assert!(buf.read().await.is_err());

        // ...but the queue kept both deliveries: a retry sees the same
        // failure instead of an emptied buffer.
        {
            let queue = buf.queue.read().await;
            assert_eq!(queue.len(), 2);
        }
        assert!(buf.read().await.is_err());
    }

    #[tokio::test]
    async fn test_memory_buffer_merged_ack_compensates_sibling_failure() {
        let ok_ack = RecordingAck::new(false);
        let fail_ack = RecordingAck::new(true);
        let buf = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();

        buf.write(binary_msg("first"), ok_ack.clone())
            .await
            .unwrap();
        buf.write(binary_msg("second"), fail_ack.clone())
            .await
            .unwrap();

        let (merged, ack) = buf.read().await.unwrap().expect("merged batch expected");
        assert_eq!(merged.len(), 2);

        // The composite acknowledgement fails on the second constituent and
        // compensates the already-successful first one (VecAck semantics).
        assert!(ack.ack().await.is_err());
        assert_eq!(ok_ack.undos.load(Ordering::Relaxed), 1);
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
                buf2.write(binary_msg(&format!("msg{}", i)), Arc::new(NoopAck))
                    .await
                    .unwrap();
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
