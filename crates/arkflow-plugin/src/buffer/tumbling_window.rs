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
use arkflow_core::component::{register_buffer_metadata, ComponentMetadata};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatchRef, Resource};
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
    async fn write(&self, msg: MessageBatchRef, ack: Arc<dyn Ack>) -> Result<(), Error> {
        self.base_window.write(msg, ack).await
    }

    /// Reads a message batch from the tumbling window buffer
    /// Waits until either messages are available or the buffer is closed
    ///
    /// # Returns
    /// * `Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error>` - The merged message batch and combined acknowledgment,
    ///   or None if the buffer is closed and empty
    async fn read(&self) -> Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error> {
        loop {
            // If there are messages available, break the loop and process
            // them (this also implements close semantics: a closed buffer
            // drains its remainder before ending).
            if !self.base_window.queue_is_empty().await {
                break;
            }
            if self.close.is_cancelled() {
                return Ok(None); // closed and drained
            }
            // Wait for notification from timer or write, racing with close:
            // a missed final notify_waiters must not park the reader forever.
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = self.close.cancelled() => {}
            }
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
        _name: Option<&str>,
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
    register_buffer_builder("tumbling_window", Arc::new(TumblingWindowBuilder))?;
    register_buffer_metadata(
        ComponentMetadata::with_schema(
            "tumbling_window",
            "Fixed-size, non-overlapping time windows. Supports SQL joins across sources.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "interval": {"type": "string", "description": "Window duration (humantime)."},
                    "join": {
                        "type": "object",
                        "description": "Optional SQL join across input sources.",
                        "properties": {
                            "query": {"type": "string"},
                            "value_field": {"type": "string"},
                            "codec": {"type": "object"},
                            "thread_num": {"type": "integer", "minimum": 1}
                        }
                    }
                },
                "required": ["interval"]
            }),
        )
        .with_example(serde_json::json!({"interval": "1m"})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;
    use arkflow_core::MessageBatch;
    use std::time::Duration;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn test_tumbling_window_config_deserialization() {
        let config_json = serde_json::json!({
            "interval": "5s"
        });

        let config: TumblingWindowConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.interval, Duration::from_secs(5));
        assert!(config.join.is_none());
    }

    #[test]
    fn test_tumbling_window_config_with_join() {
        let config_json = serde_json::json!({
            "interval": "1s",
            "join": {
                "query": "SELECT * FROM flow",
                "codec": {
                    "type": "json"
                }
            }
        });

        let config: TumblingWindowConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.interval, Duration::from_secs(1));
        assert!(config.join.is_some());
    }

    #[tokio::test]
    async fn test_tumbling_window_basic() {
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(100),
            join: None,
        };

        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        let msg = Arc::new(MessageBatch::new_binary(vec![b"test".to_vec()]).unwrap());
        buffer.write(msg, Arc::new(NoopAck)).await.unwrap();

        // Read should return the message
        let result = tokio::time::timeout(Duration::from_millis(200), buffer.read()).await;
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_tumbling_window_multiple_messages() {
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(100),
            join: None,
        };

        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        // Write multiple messages
        for i in 0..5 {
            let msg =
                Arc::new(MessageBatch::new_binary(vec![format!("msg{}", i).into_bytes()]).unwrap());
            buffer.write(msg, Arc::new(NoopAck)).await.unwrap();
        }

        // Read should return the messages
        let result = tokio::time::timeout(Duration::from_millis(200), buffer.read()).await;
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert!(batch.is_some());
    }

    #[tokio::test]
    async fn test_tumbling_window_close() {
        let config = TumblingWindowConfig {
            interval: Duration::from_secs(10),
            join: None,
        };

        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        let msg = Arc::new(MessageBatch::new_binary(vec![b"test".to_vec()]).unwrap());
        buffer.write(msg, Arc::new(NoopAck)).await.unwrap();

        buffer.close().await.unwrap();

        let result = buffer.read().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tumbling_window_flush() {
        let config = TumblingWindowConfig {
            interval: Duration::from_secs(10),
            join: None,
        };

        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        let msg = Arc::new(MessageBatch::new_binary(vec![b"flush-test".to_vec()]).unwrap());
        buffer.write(msg, Arc::new(NoopAck)).await.unwrap();

        buffer.flush().await.unwrap();

        // After flush, should be able to read the pending message
        let result = tokio::time::timeout(Duration::from_millis(100), buffer.read()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tumbling_window_builder_with_valid_config() {
        let builder = TumblingWindowBuilder;
        let config_json = serde_json::json!({
            "interval": "1s"
        });

        let result = builder.build(
            Some("test-buffer"),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_tumbling_window_builder_without_config() {
        let builder = TumblingWindowBuilder;
        let result = builder.build(Some("test-buffer"), &None, &create_test_resource());

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_tumbling_window_builder_with_invalid_interval() {
        let builder = TumblingWindowBuilder;
        let config_json = serde_json::json!({
            "interval": "invalid"
        });

        let result = builder.build(
            Some("test-buffer"),
            &Some(config_json),
            &create_test_resource(),
        );

        assert!(result.is_err());
    }

    fn arrow_batch(input_name: &str, columns: Vec<(&str, Vec<Option<&str>>)>) -> MessageBatch {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, datafusion::arrow::datatypes::DataType::Utf8, true))
            .collect();
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|(_, vals)| Arc::new(StringArray::from(vals.clone())) as ArrayRef)
            .collect();
        let rb =
            datafusion::arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .unwrap();
        let mut mb = MessageBatch::new_arrow(rb);
        mb.set_input_name(Some(input_name.to_string()));
        mb
    }

    #[tokio::test]
    async fn heterogeneous_input_schemas_null_fill_on_the_union() {
        // Input A carries `id`+`value`, input B only `id`: the window merge
        // must union the schemas and null-fill instead of failing.
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(50),
            join: None,
        };
        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        buffer
            .write(
                Arc::new(arrow_batch(
                    "a",
                    vec![("id", vec![Some("1")]), ("value", vec![Some("x")])],
                )),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();
        buffer
            .write(
                Arc::new(arrow_batch("b", vec![("id", vec![Some("2")])])),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();

        let (batch, _) = buffer.read().await.unwrap().expect("merged batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.record_batch().num_columns(), 2, "union schema");
        let value = batch
            .record_batch()
            .column_by_name("value")
            .expect("value column");
        use datafusion::arrow::array::Array;
        // Row order follows DashMap iteration and is not fixed; exactly one
        // row (input B's) must be null-filled.
        let nulls = (0..batch.len()).filter(|i| value.is_null(*i)).count();
        assert_eq!(nulls, 1, "input B's row must be null-filled");
    }

    #[tokio::test]
    async fn conflicting_types_error_preserves_queue_for_retry() {
        use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(50),
            join: None,
        };
        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        // `id` as Utf8 on input A, Int64 on input B: a genuine conflict.
        let a = {
            let rb = datafusion::arrow::array::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "id",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(StringArray::from(vec![Some("1")])) as ArrayRef],
            )
            .unwrap();
            let mut mb = MessageBatch::new_arrow(rb);
            mb.set_input_name(Some("a".to_string()));
            mb
        };
        let b = {
            let rb = datafusion::arrow::array::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "id",
                    datafusion::arrow::datatypes::DataType::Int64,
                    true,
                )])),
                vec![Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef],
            )
            .unwrap();
            let mut mb = MessageBatch::new_arrow(rb);
            mb.set_input_name(Some("b".to_string()));
            mb
        };
        buffer.write(Arc::new(a), Arc::new(NoopAck)).await.unwrap();
        buffer.write(Arc::new(b), Arc::new(NoopAck)).await.unwrap();

        let err = match buffer.read().await {
            Err(e) => e,
            Ok(_) => panic!("type conflict must error"),
        };
        assert!(
            format!("{err}").contains("`id`"),
            "error must name the column, got: {err}"
        );

        // The queues went back: the same messages are retryable (still an
        // error, but NOT "lost" — and per-input read still works).
        assert!(!buffer.base_window.queue_is_empty().await);
    }

    #[tokio::test]
    async fn per_input_failure_restores_previously_merged_inputs() {
        // Input "a" merges fine and is drained before input "b"'s internal
        // type conflict fails the round: a's message (merged form) must go
        // back to its queue alongside b's originals — nothing dropped, no
        // ack lost. Order-independent: whichever input the DashMap yields
        // first, both inputs end up fully restorable.
        use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(50),
            join: None,
        };
        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        let arrow = |name: &str, ty: datafusion::arrow::datatypes::DataType, arr: ArrayRef| {
            let rb = datafusion::arrow::array::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", ty, true)])),
                vec![arr],
            )
            .unwrap();
            let mut mb = MessageBatch::new_arrow(rb);
            mb.set_input_name(Some(name.to_string()));
            mb
        };

        buffer
            .write(
                Arc::new(arrow(
                    "a",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    Arc::new(StringArray::from(vec![Some("1")])) as ArrayRef,
                )),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();
        buffer
            .write(
                Arc::new(arrow(
                    "b",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    Arc::new(StringArray::from(vec![Some("1")])) as ArrayRef,
                )),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();
        buffer
            .write(
                Arc::new(arrow(
                    "b",
                    datafusion::arrow::datatypes::DataType::Int64,
                    Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef,
                )),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();

        assert!(buffer.read().await.is_err(), "b's conflict must error");

        let queue_len = |name: &str| {
            buffer
                .base_window
                .queue
                .get(name)
                .map(|q| q.len())
                .unwrap_or(0)
        };
        assert_eq!(queue_len("a"), 1, "input a must not be dropped");
        assert_eq!(queue_len("b"), 2, "input b's messages stay retryable");
    }

    #[tokio::test]
    async fn close_with_empty_queue_returns_promptly() {
        // A reader that misses the final notify_waiters must still be woken
        // by the close token: bounded wait, never a permanent park.
        let config = TumblingWindowConfig {
            interval: Duration::from_secs(3600), // timer never fires
            join: None,
        };
        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();

        let reader = buffer.read();
        tokio::time::sleep(Duration::from_millis(50)).await; // reader parks first
        buffer.close().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("close must wake a parked reader within the timeout");
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn close_drains_pending_messages_before_ending() {
        let config = TumblingWindowConfig {
            interval: Duration::from_millis(50),
            join: None,
        };
        let buffer = TumblingWindow::new(config, &create_test_resource()).unwrap();
        buffer
            .write(
                Arc::new(arrow_batch("a", vec![("id", vec![Some("1")])])),
                Arc::new(NoopAck),
            )
            .await
            .unwrap();
        buffer.close().await.unwrap();
        // First read after close drains the remainder...
        let (batch, _) = buffer.read().await.unwrap().expect("remainder drained");
        assert_eq!(batch.len(), 1);
        // ...the next read ends the stream.
        assert!(buffer.read().await.unwrap().is_none());
    }
}
