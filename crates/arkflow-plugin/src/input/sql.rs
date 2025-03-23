use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use std::collections::HashMap;

use async_trait::async_trait;

use datafusion::execution::options::ArrowReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, mysql::MySQLTableFactory,
    sql::db_connection_pool::mysqlpool::MySQLConnectionPool, util::secrets::to_secret_map,
};
use futures_util::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tracing::error;

const DEFAULT_TABLE_NAME: &str = "flow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlInputConfig {
    select_sql: Option<String>,

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
    MYSQL(MysqlConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AvroConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CsvConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParquetConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    path: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlConfig {
    name: Option<String>,
    uri: String,
    ssl: MysqlSslConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSslConfig {
    ssl_mode: String,
    root_cert: Option<String>,
}

pub struct SqlInput {
    sql_config: SqlInputConfig,
    close_tx: broadcast::Sender<()>,

    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl SqlInput {
    pub fn new(sql_config: SqlInputConfig) -> Result<Self, Error> {
        let (close_tx, _) = broadcast::channel::<()>(1);

        Ok(Self {
            sql_config,
            stream: Arc::new(Mutex::new(None)),
            close_tx,
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

        match self.sql_config.input_type {
            InputType::AVRO(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_TABLE_NAME);
                ctx.register_avro(table_name, &c.path, AvroReadOptions::default())
                    .await
            }
            InputType::ARROW(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_TABLE_NAME);
                ctx.register_arrow(table_name, &c.path, ArrowReadOptions::default())
                    .await
            }
            InputType::JSON(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_TABLE_NAME);
                ctx.register_json(table_name, &c.path, NdJsonReadOptions::default())
                    .await
            }
            InputType::CSV(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_TABLE_NAME);
                ctx.register_csv(table_name, &c.path, CsvReadOptions::default())
                    .await
            }
            InputType::PARQUET(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_TABLE_NAME);
                ctx.register_parquet(table_name, &c.path, ParquetReadOptions::default())
                    .await
            }
            InputType::MYSQL(ref c) => {
                let name = c.name.as_deref().unwrap_or("mysql");
                let mut params = HashMap::from([
                    ("connection_string".to_string(), c.uri.to_string()),
                    ("sslmode".to_string(), c.ssl.ssl_mode.to_string()),
                ]);

                if let Some(ref v) = c.ssl.root_cert {
                    params.insert("sslrootcert".to_string(), v.to_string());
                }

                let mysql_params = to_secret_map();
                let mysql_pool = Arc::new(
                    MySQLConnectionPool::new(mysql_params)
                        .await
                        .expect("unable to create MySQL connection pool"),
                );
                let catalog = DatabaseCatalogProvider::try_new(mysql_pool).await.unwrap();
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
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

        let mut close_rx = self.close_tx.subscribe();

        tokio::select! {
            _ = close_rx.recv() => {
                Err(Error::EOF)
            }
            Some(value) = stream_lock.as_mut().try_next() => {
                let value = value.map_err(|e| {
                    error!("Failed to read: {}", e);
                    Error::EOF
                })?;
                Ok((MessageBatch::new_arrow(value), Arc::new(NoopAck)))
            }

        }
    }

    async fn close(&self) -> Result<(), Error> {
        let _ = self.close_tx.send(());
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
