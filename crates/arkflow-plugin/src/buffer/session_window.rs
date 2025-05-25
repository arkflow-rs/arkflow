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

//! Session Window Buffer Implementation
//!
//! This module implements a session window buffer that groups messages into sessions
//! based on a configurable gap duration. Messages are considered part of the same session
//! if they arrive within the gap duration of each other. When the gap duration elapses
//! without new messages, the session is closed and all accumulated messages are emitted.

use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::{Ack, VecAck};
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
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

/// Configuration for the session window buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionWindowConfig {
    /// The maximum time gap between messages in a session
    /// If no new messages arrive within this duration, the session is considered complete
    #[serde(deserialize_with = "deserialize_duration")]
    gap: time::Duration,
}

/// Session window buffer implementation
/// Groups messages into sessions based on timing gaps between messages
struct SessionWindow {
    /// Configuration parameters for the session window
    config: SessionWindowConfig,
    /// Thread-safe queue to store message batches and their acknowledgments
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    /// Notification mechanism for signaling between threads
    notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    close: CancellationToken,
    /// Timestamp of the last received message, used to determine session boundaries
    last_message_time: Arc<RwLock<Instant>>,
}

impl SessionWindow {
    /// Creates a new session window buffer with the given configuration
    ///
    /// # Arguments
    /// * `config` - Configuration parameters for the session window
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new session window instance or an error
    fn new(config: SessionWindowConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let gap = config.gap;
        let close = CancellationToken::new();
        let close_clone = close.clone();
        let last_message_time = Arc::new(RwLock::new(Instant::now()));

        tokio::spawn(async move {
            loop {
                let timer = sleep(gap);
                tokio::select! {
                    _ = timer => {
                        notify_clone.notify_waiters();
                    }
                    _ = close_clone.cancelled() => {
                        notify_clone.notify_waiters();
                        break;
                    }
                    _ = notify_clone.notified() => {
                        if close_clone.is_cancelled(){
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            close,
            notify,
            config,
            queue: Arc::new(Default::default()),
            last_message_time,
        })
    }

    /// Processes the current session by merging all accumulated messages
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the queue is empty
    async fn process_session(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut queue_lock = self.queue.write().await;
        if queue_lock.is_empty() {
            return Ok(None);
        }

        let size = queue_lock.len();
        let mut messages = Vec::with_capacity(size);
        let mut acks = Vec::with_capacity(size);

        while let Some((msg, ack)) = queue_lock.pop_back() {
            messages.push(msg);
            acks.push(ack);
        }

        if messages.is_empty() {
            return Ok(None);
        }

        let schema = messages[0].schema();
        let batches: Vec<RecordBatch> = messages.into_iter().map(|batch| batch.into()).collect();

        if batches.is_empty() {
            return Ok(None);
        }

        let new_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(VecAck(acks));
        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for SessionWindow {
    /// Writes a message batch to the session window buffer
    ///
    /// # Arguments
    /// * `msg` - The message batch to write
    /// * `ack` - The acknowledgment for the message batch
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let mut queue_lock = self.queue.write().await;
        queue_lock.push_front((msg, ack));
        // Update the last message timestamp to track session activity
        *self.last_message_time.write().await = Instant::now();
        Ok(())
    }

    /// Reads a message batch from the session window buffer
    /// Waits until either the session gap has elapsed or the buffer is closed
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the buffer is closed and empty
    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        loop {
            {
                let queue_arc = Arc::clone(&self.queue);
                let queue_lock = queue_arc.read().await;
                if !queue_lock.is_empty() {
                    let last_time = *self.last_message_time.read().await;
                    // Check if the session gap has elapsed since the last message
                    if last_time.elapsed() >= self.config.gap {
                        break;
                    }
                } else if self.close.is_cancelled() {
                    return Ok(None);
                }
            }

            // Wait for notification from timer or write operation
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        // Process and return the current session
        self.process_session().await
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

struct SessionWindowBuilder;

impl BufferBuilder for SessionWindowBuilder {
    /// Builds a session window buffer from the provided configuration
    ///
    /// # Arguments
    /// * `config` - JSON configuration for the session window
    ///
    /// # Returns
    /// * `Result<Arc<dyn Buffer>, Error>` - A new session window buffer instance or an error
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Session window configuration is missing".to_string(),
            ));
        }

        let config: SessionWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SessionWindow::new(config)?))
    }
}

/// Initializes the session window buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("session_window", Arc::new(SessionWindowBuilder))
}
