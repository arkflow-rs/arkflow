use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};

use async_trait::async_trait;

use datafusion::execution::options::ArrowReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures_util::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

const DEFAULT_TABLE_NAME: &str = "flow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlInputConfig {
    select_sql: Option<String>,
    /// Table name (used in SQL queries)
    table_name: Option<String>,

    #[serde(flatten)]
    input_type: InputType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "input_type", rename_all = "lowercase")]
enum InputType {
    AVRO(AvroConfig),
    ARROW(ArrowConfig),
    JSON(JsonConfig),
    CSV(CsvConfig),
    PARQUET(ParquetConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct AvroConfig {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct ArrowConfig {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct JsonConfig {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct CsvConfig {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct ParquetConfig {
    path: String,
}

pub struct SqlInput {
    sql_config: SqlInputConfig,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl SqlInput {
    pub fn new(sql_config: SqlInputConfig) -> Result<Self, Error> {
        Ok(Self {
            sql_config,
            stream: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;

        let mut ctx = SessionContext::new();
        datafusion_functions_json::register_all(&mut ctx)
            .map_err(|e| Error::Process(format!("Registration JSON function failed: {}", e)))?;

        let table_name = self
            .sql_config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);

        match self.sql_config.input_type {
            InputType::AVRO(ref c) => {
                ctx.register_avro(table_name, &c.path, AvroReadOptions::default())
                    .await
            }
            InputType::ARROW(ref c) => {
                ctx.register_arrow(table_name, &c.path, ArrowReadOptions::default())
                    .await
            }
            InputType::JSON(ref c) => {
                ctx.register_json(table_name, &c.path, NdJsonReadOptions::default())
                    .await
            }
            InputType::CSV(ref c) => {
                ctx.register_csv(table_name, &c.path, CsvReadOptions::default())
                    .await
            }
            InputType::PARQUET(ref c) => {
                ctx.register_parquet(table_name, &c.path, ParquetReadOptions::default())
                    .await
            }
        }
        .map_err(|e| Error::Process(format!("Registration input failed: {}", e)))?;

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(
                &self
                    .sql_config
                    .select_sql
                    .as_deref()
                    .unwrap_or(format!("SELECT * FROM {}", table_name).as_str()),
                sql_options,
            )
            .await
            .map_err(|e| Error::Config(format!("Failed to execute select_table_sql: {}", e)))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Error::Process(format!("Failed to execute select_table_sql: {}", e)))?;

        *stream_lock = Some(stream);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;
        if stream_lock.is_none() {
            return Err(Error::Process("Stream is None".to_string()));
        }
        let stream_lock = stream_lock.as_mut().unwrap();

        let value_opt = stream_lock.as_mut().try_next().await.map_err(|e| {
            error!("Failed to read: {}", e);
            Error::EOF
        })?;
        let Some(value) = value_opt else {
            return Err(Error::EOF);
        };

        Ok((MessageBatch::new_arrow(value), Arc::new(NoopAck)))
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
