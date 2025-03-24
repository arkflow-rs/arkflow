use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use std::collections::HashMap;

use async_trait::async_trait;

use datafusion::execution::options::ArrowReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sql::db_connection_pool::Mode;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
    util::secrets::to_secret_map,
};
use duckdb::AccessMode;
use futures_util::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tracing::error;

const DEFAULT_TABLE_NAME: &str = "flow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlInputConfig {
    select_sql: String,

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
    DUCKDB(DuckDBConfig),
    POSTGRES(PostgresConfig),
    SQLITE(SqliteConfig),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DuckDBConfig {
    name: Option<String>,
    file_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresConfig {
    name: Option<String>,
    uri: String,
    ssl: PostgresSslConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresSslConfig {
    ssl_mode: String,
    root_cert: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqliteConfig {
    name: Option<String>,
    file_path: String,
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

        self.init_connect(&mut ctx).await?;

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.sql_config.select_sql, sql_options)
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
        // let result = stream_lock.as_mut().try_next().await;
        let mut stream_pin = stream_lock.as_mut();
        tokio::select! {
            _ = close_rx.recv() => {
                Err(Error::EOF)
            }
            result = stream_pin.try_next() => {
                 let value = result.map_err(|e| {
                    error!("Failed to read: {}:",e);
                    Error::EOF
                })?;
                let Some(x) = value else {
                    return Err(Error::EOF);
                };
                Ok((MessageBatch::new_arrow(x), Arc::new(NoopAck)))
            }

        }
    }

    async fn close(&self) -> Result<(), Error> {
        let _ = self.close_tx.send(());
        Ok(())
    }
}

impl SqlInput {
    async fn init_connect(&self, ctx: &mut SessionContext) -> Result<(), Error> {
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

                let mysql_params = to_secret_map(params);
                let mysql_pool =
                    Arc::new(MySQLConnectionPool::new(mysql_params).await.map_err(|e| {
                        Error::Config(format!("Failed to create mysql pool: {}", e))
                    })?);
                let catalog = DatabaseCatalogProvider::try_new(mysql_pool)
                    .await
                    .map_err(|e| Error::Config(format!("Failed to create mysql catalog: {}", e)))?;
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
            }
            InputType::DUCKDB(ref c) => {
                let duckdb_pool = Arc::new(
                    DuckDbConnectionPool::new_file(&c.file_path, &AccessMode::ReadOnly).map_err(
                        |e| {
                            return Error::Config(format!("Failed to create duckdb pool: {}", e));
                        },
                    )?,
                );

                let catalog = DatabaseCatalogProvider::try_new(duckdb_pool)
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create duckdb catalog: {}", e));
                    })?;
                let name = c.name.as_deref().unwrap_or("duckdb");
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
            }
            InputType::POSTGRES(ref c) => {
                let mut params = HashMap::from([
                    ("connection_string".to_string(), c.uri.to_string()),
                    ("sslmode".to_string(), c.ssl.ssl_mode.to_string()),
                ]);
                if let Some(ref v) = c.ssl.root_cert {
                    params.insert("sslrootcert".to_string(), v.to_string().parse().unwrap());
                }
                let postgres_params = to_secret_map(params);
                let postgres_pool = Arc::new(
                    PostgresConnectionPool::new(postgres_params)
                        .await
                        .map_err(|e| {
                            return Error::Config(format!("Failed to create postgres pool: {}", e));
                        })?,
                );

                let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create postgres catalog: {}", e));
                    })?;
                let name = c.name.as_deref().unwrap_or("postgres");
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
            }
            InputType::SQLITE(ref c) => {
                let sqlite_pool = Arc::new(
                    SqliteConnectionPoolFactory::new(
                        &c.file_path,
                        Mode::File,
                        Duration::from_millis(5000),
                    )
                    .build()
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create sqlite pool: {}", e));
                    })?,
                );

                let catalog_provider = DatabaseCatalogProvider::try_new(sqlite_pool)
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create sqlite catalog: {}", e));
                    })?;
                let name = c.name.as_deref().unwrap_or("sqlite");
                ctx.register_catalog(name, Arc::new(catalog_provider));
                Ok(())
            }
        }
        .map_err(|e| Error::Process(format!("Registration input failed: {}", e)))
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
