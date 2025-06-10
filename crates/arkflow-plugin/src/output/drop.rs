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

//! Drop output component
//!
//! This component discards all messages without performing any operations.
//! It's useful for testing or when you want to intentionally discard data.

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use std::sync::Arc;

/// Drop output component that discards all messages
///
/// This component implements the `Output` trait but doesn't perform any actual
/// output operations. All messages sent to this output are simply discarded.
struct DropOutput;

#[async_trait]
impl Output for DropOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, _: MessageBatch) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct DropOutputBuilder;
impl OutputBuilder for DropOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(DropOutput))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("drop", Arc::new(DropOutputBuilder))
}

#[cfg(test)]
mod tests {
    use crate::output::drop::DropOutput;

    use arkflow_core::output::Output;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_drop_output_connect() {
        // Create a DropOutput instance
        let drop_output = DropOutput;

        // Test connect method
        let result = drop_output.connect().await;
        assert!(result.is_ok(), "connect() should return Ok(())");
    }

    #[tokio::test]
    async fn test_drop_output_write_binary() {
        // Create a DropOutput instance
        let drop_output = DropOutput;

        // Create a binary message batch
        let binary_data = vec![b"test message".to_vec()];
        let message_batch = MessageBatch::new_binary(binary_data).unwrap();

        // Test write method with binary data
        let result = drop_output.write(message_batch).await;
        assert!(
            result.is_ok(),
            "write() should return Ok(()) for binary data"
        );
    }

    #[tokio::test]
    async fn test_drop_output_write_arrow() {
        // Create a DropOutput instance
        let drop_output = DropOutput;

        // Create an Arrow message batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap();

        let message_batch = MessageBatch::new_arrow(batch);

        // Test write method with Arrow data
        let result = drop_output.write(message_batch).await;
        assert!(
            result.is_ok(),
            "write() should return Ok(()) for Arrow data"
        );
    }

    #[tokio::test]
    async fn test_drop_output_close() {
        // Create a DropOutput instance
        let drop_output = DropOutput;

        // Test close method
        let result = drop_output.close().await;
        assert!(result.is_ok(), "close() should return Ok(())");
    }

    #[tokio::test]
    async fn test_drop_output_full_lifecycle() {
        // Create a DropOutput instance
        let drop_output = DropOutput;

        // Test the full lifecycle: connect -> write -> close
        let connect_result = drop_output.connect().await;
        assert!(connect_result.is_ok(), "connect() should return Ok(())");

        // Create a binary message batch
        let binary_data = vec![b"test message".to_vec()];
        let message_batch = MessageBatch::new_binary(binary_data).unwrap();

        // Write multiple messages
        for _ in 0..5 {
            let write_result = drop_output.write(message_batch.clone()).await;
            assert!(write_result.is_ok(), "write() should return Ok(())");
        }

        let close_result = drop_output.close().await;
        assert!(close_result.is_ok(), "close() should return Ok(())");
    }
}
