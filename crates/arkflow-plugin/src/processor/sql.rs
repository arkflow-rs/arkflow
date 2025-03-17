//! SQL processor component
//!
//! DataFusion is used to process data with SQL queries.

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const DEFAULT_TABLE_NAME: &str = "flow";
/// SQL processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL query statement
    pub query: String,

    /// Table name (used in SQL queries)
    pub table_name: Option<String>,
}

/// SQL processor component
pub struct SqlProcessor {
    config: SqlProcessorConfig,
}

impl SqlProcessor {
    /// Create a new SQL processor component.
    pub fn new(config: SqlProcessorConfig) -> Result<Self, Error> {
        Ok(Self { config })
    }

    /// Execute SQL query
    async fn execute_query(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        // Create a session context
        let ctx = SessionContext::new();
        let table_name = self
            .config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);

        ctx.register_batch(table_name, batch)
            .map_err(|e| Error::Processing(format!("Registration failed: {}", e)))?;

        // Execute the SQL query and collect the results.
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.config.query, sql_options)
            .await
            .map_err(|e| Error::Processing(format!("SQL query error: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Processing(format!("Collection query results error: {}", e)))?;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if result_batches.len() == 1 {
            return Ok(result_batches[0].clone());
        }

        Ok(
            arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Processing(format!("Batch merge failed: {}", e)))?,
        )
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // If the batch is empty, return an empty result.
        if msg_batch.is_empty() {
            return Ok(vec![]);
        }

        let batch: RecordBatch = match msg_batch.content {
            Content::Arrow(v) => v,
            Content::Binary(_) => {
                return Err(Error::Processing("Unsupported input format".to_string()))?;
            }
        };

        // Execute SQL query
        let result_batch = self.execute_query(batch).await?;
        Ok(vec![MessageBatch::new_arrow(result_batch)])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) struct SqlProcessorBuilder;
impl ProcessorBuilder for SqlProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: SqlProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SqlProcessor::new(config)?))
    }
}

pub fn init() {
    register_processor_builder("sql", Arc::new(SqlProcessorBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // Helper function to create a test record batch
    fn create_test_batch() -> RecordBatch {
        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create data
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

        // Create record batch
        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    #[tokio::test]
    async fn test_sql_processor_new() {
        // Test creating a new SQL processor
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config);
        assert!(processor.is_ok());
    }

    #[tokio::test]
    async fn test_sql_processor_process_simple_query() -> Result<(), Error> {
        // Test processing a simple SELECT query
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config)?;
        let batch = create_test_batch();
        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await?;

        // Verify the result
        assert_eq!(result.len(), 1);
        match &result[0].content {
            Content::Arrow(record_batch) => {
                // Check that all rows were returned
                assert_eq!(record_batch.num_rows(), 3);
                assert_eq!(record_batch.num_columns(), 2);

                // Check column values
                let id_array = record_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let name_array = record_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                assert_eq!(id_array.value(0), 1);
                assert_eq!(id_array.value(1), 2);
                assert_eq!(id_array.value(2), 3);
                assert_eq!(name_array.value(0), "Alice");
                assert_eq!(name_array.value(1), "Bob");
                assert_eq!(name_array.value(2), "Charlie");
            }
            _ => panic!("Expected Arrow content"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_processor_process_filter_query() -> Result<(), Error> {
        // Test processing a query with a filter
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow WHERE id > 1".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config)?;
        let batch = create_test_batch();
        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await?;

        // Verify the result
        assert_eq!(result.len(), 1);
        match &result[0].content {
            Content::Arrow(record_batch) => {
                // Check that only filtered rows were returned
                assert_eq!(record_batch.num_rows(), 2);
                assert_eq!(record_batch.num_columns(), 2);

                // Check column values
                let id_array = record_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();

                assert_eq!(id_array.value(0), 2);
                assert_eq!(id_array.value(1), 3);
            }
            _ => panic!("Expected Arrow content"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_processor_process_projection_query() -> Result<(), Error> {
        // Test processing a query with column projection
        let config = SqlProcessorConfig {
            query: "SELECT id FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config)?;
        let batch = create_test_batch();
        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await?;

        // Verify the result
        assert_eq!(result.len(), 1);
        match &result[0].content {
            Content::Arrow(record_batch) => {
                // Check that only the id column was returned
                assert_eq!(record_batch.num_rows(), 3);
                assert_eq!(record_batch.num_columns(), 1);

                // Check column values
                let id_array = record_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();

                assert_eq!(id_array.value(0), 1);
                assert_eq!(id_array.value(1), 2);
                assert_eq!(id_array.value(2), 3);
            }
            _ => panic!("Expected Arrow content"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_processor_process_empty_batch() -> Result<(), Error> {
        // Test processing an empty batch
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config)?;
        let msg_batch = MessageBatch::new_binary(vec![]);

        let result = processor.process(msg_batch).await?;

        // Verify that an empty result is returned
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_processor_process_binary_content() {
        // Test processing binary content (should return an error)
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config).unwrap();
        let msg_batch = MessageBatch::new_binary(vec![vec![1]]);

        let result = processor.process(msg_batch).await;

        // Verify that an error is returned
        assert!(matches!(result, Err(Error::Processing(_))));
    }

    #[tokio::test]
    async fn test_sql_processor_process_invalid_query() {
        // Test processing with an invalid SQL query
        let config = SqlProcessorConfig {
            query: "INVALID SQL".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config).unwrap();
        let batch = create_test_batch();
        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await;

        // Verify that an error is returned
        assert!(matches!(result, Err(Error::Processing(_))));
    }

    #[tokio::test]
    async fn test_sql_processor_process_custom_table_name() -> Result<(), Error> {
        // Test processing with a custom table name
        let config = SqlProcessorConfig {
            query: "SELECT * FROM custom_table".to_string(),
            table_name: Some("custom_table".to_string()),
        };
        let processor = SqlProcessor::new(config)?;
        let batch = create_test_batch();
        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await?;

        // Verify the result
        assert_eq!(result.len(), 1);
        match &result[0].content {
            Content::Arrow(record_batch) => {
                assert_eq!(record_batch.num_rows(), 3);
            }
            _ => panic!("Expected Arrow content"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_processor_close() {
        // Test closing the processor
        let config = SqlProcessorConfig {
            query: "SELECT * FROM flow".to_string(),
            table_name: None,
        };
        let processor = SqlProcessor::new(config).unwrap();

        // Verify that close returns Ok
        assert!(processor.close().await.is_ok());
    }
}
