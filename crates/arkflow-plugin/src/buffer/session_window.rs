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

use crate::buffer::join::JoinConfig;
use crate::buffer::window::BaseWindow;
use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time;
use tokio::sync::{Notify, RwLock};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Configuration for the session window buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionWindowConfig {
    /// The maximum time gap between messages in a session
    /// If no new messages arrive within this duration, the session is considered complete
    #[serde(deserialize_with = "deserialize_duration")]
    gap: time::Duration,
    /// Optional join configuration for SQL join operations on message batches
    /// When specified, allows joining multiple message sources using SQL queries
    join: Option<JoinConfig>,
}

/// Session window buffer implementation
/// Groups messages into sessions based on timing gaps between messages
struct SessionWindow {
    /// Configuration parameters for the session window
    config: SessionWindowConfig,
    base_window: BaseWindow,
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
    fn new(config: SessionWindowConfig, resource: &Resource) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let gap = config.gap;
        let close = CancellationToken::new();
        let close_clone = close.clone();
        let last_message_time = Arc::new(RwLock::new(Instant::now()));
        let base_window = BaseWindow::new(
            config.join.clone(),
            notify_clone,
            close_clone,
            gap,
            resource,
        )?;

        Ok(Self {
            close,
            notify,
            config,
            base_window,
            last_message_time,
        })
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
        self.base_window.write(msg, ack).await?;
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
        if self.close.is_cancelled() {
            return Ok(None);
        }

        loop {
            {
                if !self.base_window.queue_is_empty().await {
                    let last_time = *self.last_message_time.read().await;
                    // Check if the session gap has elapsed since the last message
                    let duration = last_time.elapsed();
                    if duration >= self.config.gap {
                        break;
                    }
                }
            }

            // Wait for notification from timer or write operation
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        // Process and return the current session
        self.base_window.process_window().await
    }

    /// Flushes the buffer by cancelling the background task and notifying waiters
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn flush(&self) -> Result<(), Error> {
        self.base_window.flush().await
    }

    /// Closes the buffer by cancelling the background task
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn close(&self) -> Result<(), Error> {
        self.base_window.close().await
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
        resource: &Resource,
    ) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Session window configuration is missing".to_string(),
            ));
        }

        let config: SessionWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SessionWindow::new(config, resource)?))
    }
}

/// Initializes the session window buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("session_window", Arc::new(SessionWindowBuilder))
}

