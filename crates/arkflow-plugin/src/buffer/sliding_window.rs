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

//! Sliding Window Buffer Implementation
//!
//! This module implements a sliding window buffer that groups messages into overlapping
//! time windows. Each window has a fixed size, and windows slide forward by a configurable
//! number of messages after each window is emitted. This allows messages to be part of
//! multiple windows, providing a more continuous view of the data stream.

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
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Configuration for the sliding window buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlidingWindowConfig {
    /// The number of messages to include in each window
    window_size: u32,
    /// The time interval between window emissions
    #[serde(deserialize_with = "deserialize_duration")]
    interval: time::Duration,
    /// The number of messages to slide forward after each window emission
    /// This determines the overlap between consecutive windows
    slide_size: u32,
}

/// Sliding window buffer implementation
/// Groups messages into overlapping windows that slide forward over time
struct SlidingWindow {
    /// Configuration parameters for the sliding window
    config: SlidingWindowConfig,
    /// Thread-safe queue to store message batches and their acknowledgments
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    /// Notification mechanism for signaling between threads
    notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    close: CancellationToken,
}

impl SlidingWindow {
    /// Creates a new sliding window buffer with the given configuration
    ///
    /// # Arguments
    /// * `config` - Configuration parameters for the sliding window
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new sliding window instance or an error
    fn new(config: SlidingWindowConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let interval = config.interval;
        let close = CancellationToken::new();
        let close_clone = close.clone();

        tokio::spawn(async move {
            loop {
                let timer = sleep(interval);
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
        })
    }

    /// Processes the current window by merging messages and sliding forward
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if there aren't enough messages to form a window
    async fn process_slide(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut queue_lock = self.queue.write().await;
        if queue_lock.len() < self.config.window_size as usize {
            return Ok(None);
        }

        let window_messages: Vec<_> = queue_lock
            .iter()
            .take(self.config.window_size as usize)
            .cloned()
            .collect();
        let size = window_messages.len();
        let mut messages = Vec::with_capacity(size);
        let mut acks = Vec::with_capacity(size);

        for (msg, ack) in window_messages {
            messages.push(msg);
            acks.push(ack);
        }

        if messages.is_empty() {
            return Ok(None);
        }

        let schema = messages[0].schema();
        let batches: Vec<RecordBatch> = messages.into_iter().map(|batch| batch.into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(VecAck(acks));

        // Remove slide_size messages from the front of the queue
        // This slides the window forward by the configured amount
        for _ in 0..self.config.slide_size {
            if queue_lock.pop_front().is_none() {
                break;
            }
        }

        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for SlidingWindow {
    /// Writes a message batch to the sliding window buffer
    ///
    /// # Arguments
    /// * `msg` - The message batch to write
    /// * `ack` - The acknowledgment for the message batch
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or an error
    async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let mut queue_lock = self.queue.write().await;
        queue_lock.push_back((msg, ack));
        Ok(())
    }

    /// Reads a message batch from the sliding window buffer
    /// Waits until either enough messages are available to form a window or the buffer is closed
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
                let queue_arc = Arc::clone(&self.queue);
                let queue_lock = queue_arc.read().await;
                // If there are enough messages to form a window, break the loop and process them
                if queue_lock.len() >= self.config.window_size as usize {
                    break;
                }
                // If the buffer is closed, return None
            }
            // Wait for notification from timer, write operation, or close
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        // Process the current window and slide forward
        self.process_slide().await
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

struct SlidingWindowBuilder;

impl BufferBuilder for SlidingWindowBuilder {
    /// Builds a sliding window buffer from the provided configuration
    ///
    /// # Arguments
    /// * `config` - JSON configuration for the sliding window
    ///
    /// # Returns
    /// * `Result<Arc<dyn Buffer>, Error>` - A new sliding window buffer instance or an error
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Sliding window configuration is missing".to_string(),
            ));
        }

        let config: SlidingWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        // Validate configuration parameters
        if config.window_size == 0 {
            return Err(Error::Config(
                "Sliding window window_size must be greater than 0".to_string(),
            ));
        }
        if config.slide_size == 0 {
            return Err(Error::Config(
                "Sliding window slide_size must be greater than 0".to_string(),
            ));
        }
        if config.window_size < config.slide_size {
            return Err(Error::Config(
                "Sliding window window_size must be greater than slide_size".to_string(),
            ));
        }

        Ok(Arc::new(SlidingWindow::new(config)?))
    }
}

/// Initializes the sliding window buffer by registering its builder
///
/// # Returns
/// * `Result<(), Error>` - Success or an error
pub fn init() -> Result<(), Error> {
    register_buffer_builder("sliding_window", Arc::new(SlidingWindowBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;
    use std::time::Duration;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn test_sliding_window_config_deserialization() {
        let config_json = serde_json::json!({
            "window_size": 10,
            "interval": "1s",
            "slide_size": 5
        });

        let config: SlidingWindowConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.window_size, 10);
        assert_eq!(config.interval, Duration::from_secs(1));
        assert_eq!(config.slide_size, 5);
    }

    #[tokio::test]
    async fn test_sliding_window_builder_with_valid_config() {
        let builder = SlidingWindowBuilder;
        let config_json = serde_json::json!({
            "window_size": 10,
            "interval": "1s",
            "slide_size": 5
        });

        let result = builder.build(
            Some(&"test-buffer".to_string()),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_sliding_window_builder_with_invalid_window_size() {
        let builder = SlidingWindowBuilder;
        let config_json = serde_json::json!({
            "window_size": 0,
            "interval": "1s",
            "slide_size": 5
        });

        let result = builder.build(
            Some(&"test-buffer".to_string()),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_sliding_window_builder_with_invalid_slide_size() {
        let builder = SlidingWindowBuilder;
        let config_json = serde_json::json!({
            "window_size": 10,
            "interval": "1s",
            "slide_size": 0
        });

        let result = builder.build(
            Some(&"test-buffer".to_string()),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_sliding_window_builder_window_size_less_than_slide_size() {
        let builder = SlidingWindowBuilder;
        let config_json = serde_json::json!({
            "window_size": 5,
            "interval": "1s",
            "slide_size": 10
        });

        let result = builder.build(
            Some(&"test-buffer".to_string()),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_sliding_window_builder_without_config() {
        let builder = SlidingWindowBuilder;
        let result = builder.build(
            Some(&"test-buffer".to_string()),
            &None,
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn test_sliding_window_basic() {
        let config = SlidingWindowConfig {
            window_size: 3,
            interval: Duration::from_millis(100),
            slide_size: 2,
        };

        let buffer = SlidingWindow::new(config).unwrap();

        // Write 3 messages
        for i in 0..3 {
            let msg = MessageBatch::new_binary(vec![format!("msg{}", i).into_bytes()]).unwrap();
            buffer.write(msg, Arc::new(NoopAck)).await.unwrap();
        }

        // Read should return a window with 3 messages
        let result = buffer.read().await;
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_sliding_window_write_and_close() {
        let config = SlidingWindowConfig {
            window_size: 3,
            interval: Duration::from_millis(100),
            slide_size: 1,
        };

        let buffer = SlidingWindow::new(config).unwrap();

        // Write some messages
        for i in 0..2 {
            let msg = MessageBatch::new_binary(vec![format!("msg{}", i).into_bytes()]).unwrap();
            buffer.write(msg, Arc::new(NoopAck)).await.unwrap();
        }

        // Close the buffer
        buffer.close().await.unwrap();

        // Read should return None after close
        let result = buffer.read().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sliding_window_flush() {
        let config = SlidingWindowConfig {
            window_size: 3,
            interval: Duration::from_millis(100),
            slide_size: 1,
        };

        let buffer = SlidingWindow::new(config).unwrap();

        // Write some messages
        for i in 0..2 {
            let msg = MessageBatch::new_binary(vec![format!("msg{}", i).into_bytes()]).unwrap();
            buffer.write(msg, Arc::new(NoopAck)).await.unwrap();
        }

        // Flush the buffer
        buffer.flush().await.unwrap();
    }
}
