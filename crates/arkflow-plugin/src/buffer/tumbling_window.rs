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

//! Tumbling Window Buffer Implementation
//!
//! This module implements a tumbling window buffer that groups messages into fixed-size,
//! non-overlapping time windows. Each window has a fixed duration, and when the window
//! period elapses, all accumulated messages are emitted as a single batch and a new
//! window begins immediately.

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
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Configuration for the tumbling window buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TumblingWindowConfig {
    /// The fixed duration of each window period
    /// When this interval elapses, all accumulated messages are emitted
    #[serde(deserialize_with = "deserialize_duration")]
    interval: time::Duration,
    /// Optional join configuration for SQL join operations on message batches
    /// When specified, allows joining multiple message sources using SQL queries
    join: Option<JoinConfig>,
}

/// Tumbling window buffer implementation
/// Groups messages into fixed-size, non-overlapping time windows
struct TumblingWindow {
    /// Thread-safe queue to store message batches and their acknowledgments
    base_window: BaseWindow,
    /// Notification mechanism for signaling between threads
    notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    close: CancellationToken,
}

impl TumblingWindow {
    /// Creates a new tumbling window buffer with the given configuration
    ///
    /// # Arguments
    /// * `config` - Configuration parameters for the tumbling window
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new tumbling window instance or an error
    fn new(config: TumblingWindowConfig, resource: &Resource) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let interval = config.interval;
        let close = CancellationToken::new();
        let close_clone = close.clone();
        let base_window = BaseWindow::new(
            config.join.clone(),
            notify_clone,
            close_clone,
            interval,
            resource,
        )?;

        Ok(Self {
            close,
            notify,
            base_window,
        })
    }
}

#[async_trait]
impl Buffer for TumblingWindow {
    /// Writes a message batch to the tumbling window buffer
    ///
    /// # Arguments
    /// * `msg` - The message batch to write
    /// * `ack` - The acknowledgment for the message batch
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        self.base_window.write(msg, ack).await
    }

    /// Reads a message batch from the tumbling window buffer
    /// Waits until either messages are available or the buffer is closed
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the buffer is closed and empty
    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        // If the buffer is closed, return None
        if self.close.is_cancelled() {
            return Ok(None);
        }

        loop {
            {
                // If there are messages available, break the loop and process them
                if !self.base_window.queue_is_empty().await {
                    break;
                }
            }
            // Wait for notification from timer, write operation, or close
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        // Process and return the current window
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

struct TumblingWindowBuilder;

impl BufferBuilder for TumblingWindowBuilder {
    /// Builds a tumbling window buffer from the provided configuration
    ///
    /// # Arguments
    /// * `config` - JSON configuration for the tumbling window
    ///
    /// # Returns
    /// * `Result<Arc<dyn Buffer>, Error>` - A new tumbling window buffer instance or an error
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Tumbling window configuration is missing".to_string(),
            ));
        }

        let config: TumblingWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(TumblingWindow::new(config, resource)?))
    }
}

/// Initializes the tumbling window buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("tumbling_window", Arc::new(TumblingWindowBuilder))
}
