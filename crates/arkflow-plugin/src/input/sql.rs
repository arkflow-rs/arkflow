use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::{SQLOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlInputConfig {
    select_sql: String,
    create_table_sql: String,
}

pub struct SqlInput {
    sql_config: SqlInputConfig,
    read: AtomicBool,
}

impl SqlInput {
    pub fn new(sql_config: SqlInputConfig) -> Result<Self, Error> {
        Ok(Self {
            sql_config,
            read: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if self.read.load(Ordering::Acquire) {
            return Err(Error::EOF);
        }

        let ctx = SessionContext::new();
        let sql_options = SQLOptions::new()
            .with_allow_ddl(true)
            .with_allow_dml(false)
            .with_allow_statements(false);
        ctx.sql_with_options(&self.sql_config.create_table_sql, sql_options)
            .await
            .map_err(|e| Error::Config(format!("Failed to execute create_table_sql: {}", e)))?;

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.sql_config.select_sql, sql_options)
            .await
            .map_err(|e| Error::Read(format!("Failed to execute select_sql: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Read(format!("Failed to collect data from SQL query: {}", e)))?;

        let x = match result_batches.len() {
            0 => RecordBatch::new_empty(Arc::new(Schema::empty())),
            1 => result_batches[0].clone(),
            _ => arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?,
        };

        self.read.store(true, Ordering::Release);
        Ok((MessageBatch::new_arrow(x), Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) struct SqlInputBuilder;
impl InputBuilder for SqlInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "SQL input configuration is missing".to_string(),
            ));
        }

        let config: SqlInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SqlInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("sql", Arc::new(SqlInputBuilder));
}
#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::Content;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sql_input_new() {
        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test".to_string(),
            create_table_sql:
                "CREATE EXTERNAL TABLE test (id INT, name STRING) STORED AS CSV LOCATION 'test.csv'"
                    .to_string(),
        };
        let input = SqlInput::new(config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_sql_input_connect() {
        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test".to_string(),
            create_table_sql:
                "CREATE EXTERNAL TABLE test (id INT, name STRING) STORED AS CSV LOCATION 'test.csv'"
                    .to_string(),
        };
        let input = SqlInput::new(config).unwrap();
        assert!(input.connect().await.is_ok());
    }

    #[tokio::test]
    async fn test_sql_input_read() -> Result<(), Error> {
        // 创建临时目录和测试数据文件
        let temp_dir = tempdir().unwrap();
        let csv_path = temp_dir.path().join("test.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "2,Bob").unwrap();

        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test".to_string(),
            create_table_sql: format!(
                "CREATE EXTERNAL TABLE test (id INT, name STRING) STORED AS CSV LOCATION '{}'",
                csv_path.to_str().unwrap()
            ),
        };

        let input = SqlInput::new(config)?;
        let (batch, _ack) = input.read().await?;

        // 验证返回的数据
        match batch.content {
            Content::Arrow(record_batch) => {
                assert_eq!(record_batch.num_rows(), 2);
                assert_eq!(record_batch.num_columns(), 2);

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
                assert_eq!(name_array.value(0), "Alice");
                assert_eq!(name_array.value(1), "Bob");
            }
            _ => panic!("Expected Arrow content"),
        }

        // Verify idempotency (second read should return Done error)
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_input_invalid_sql() {
        let config = SqlInputConfig {
            select_sql: "INVALID SQL".to_string(),
            create_table_sql:
                "CREATE EXTERNAL TABLE test (id INT, name STRING) STORED AS CSV LOCATION 'test.csv'"
                    .to_string(),
        };
        let input = SqlInput::new(config).unwrap();
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Read(_))));
    }

    #[tokio::test]
    async fn test_sql_input_close() {
        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test".to_string(),
            create_table_sql:
                "CREATE EXTERNAL TABLE test (id INT, name STRING) STORED AS CSV LOCATION 'test.csv'"
                    .to_string(),
        };
        let input = SqlInput::new(config).unwrap();
        assert!(input.close().await.is_ok());
    }
}
