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

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch};

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlOutputConfig {
    /// SQL query statement
    table_name: String,
    #[serde(flatten)]
    output_type: OutputType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlConfig {
    name: Option<String>,
    uri: String,
    ssl: Option<MysqlSslConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSslConfig {
    // Path to the root CA certificate file used to verify server's identity
    root_cert: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "output_type", rename_all = "snake_case")]
enum OutputType {
    Mysql(MysqlConfig),
}

#[derive(Debug)]
enum DbConnection {
    Mysql(Conn),
}

impl DbConnection {
    // Executes a SQL query without returning results
    pub async fn execute_query(&mut self, query: &str) -> Result<(), Error> {
        match self {
            DbConnection::Mysql(conn) => conn
                .query_drop(query)
                .await
                .map_err(|e| Error::Process(format!("Failed to execute query: {}", e))),
        }
    }
    pub async fn begin_transaction(&mut self) -> Result<(), Error> {
        match self {
            DbConnection::Mysql(conn) => conn
                .query_drop("BEGIN")
                .await
                .map_err(|e| Error::Process(format!("Failed to begin transaction: {}", e))),
        }
    }
    pub async fn commit_transaction(&mut self) -> Result<(), Error> {
        match self {
            DbConnection::Mysql(conn) => conn
                .query_drop("COMMIT")
                .await
                .map_err(|e| Error::Process(format!("Failed to commit transaction: {}", e))),
        }
    }
}

pub struct SqlOutput {
    sql_config: SqlOutputConfig,
    db_connection: Arc<Mutex<Option<DbConnection>>>,
    connected: std::sync::atomic::AtomicBool,
    cancellation_token: CancellationToken,
}

//수정할 생각 필요
impl SqlOutput {
    pub fn new(sql_config: SqlOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            sql_config,
            db_connection: Arc::new(Mutex::new(None)),
            connected: std::sync::atomic::AtomicBool::new(false),
            cancellation_token,
        })
    }
}
#[async_trait]
impl Output for SqlOutput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        let conn = self.init_connect().await?;
        let mut guard = self.db_connection.lock().await;
        *guard = Some(conn);

        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let mut guard = self.db_connection.lock().await;
        let conn = guard.as_mut().expect("not connected");

        conn.begin_transaction().await?;
        self.insert_row(conn, &msg).await?;
        conn.commit_transaction().await?;

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        Ok(())
    }
}
impl SqlOutput {
    /// Initialize a new DB connection.  
    /// If `ssl` is configured, apply root certificates to the SSL options.
    async fn init_connect(&self) -> Result<DbConnection, Error> {
        match self.sql_config.output_type {
            OutputType::Mysql(ref c) => {
                let database_url = c.uri.clone();

                let base_opts = Opts::from_url(&database_url)
                    .map_err(|e| Error::Config(format!("Invalid MySQL URI: {}", e)))?;

                // Configure SSL/TLS if ssl configuration is provided
                // This enables secure connections with certificate validation
                let opts = if let Some(ref ssl_conf) = c.ssl {
                    let cert_path = Path::new(&ssl_conf.root_cert).to_path_buf();
                    let ssl_opts = SslOpts::default().with_root_certs(vec![cert_path.into()]);
                    OptsBuilder::from_opts(base_opts).ssl_opts(ssl_opts)
                } else {
                    OptsBuilder::from_opts(base_opts)
                };

                let pool = Pool::new(opts);
                let conn = pool
                    .get_conn()
                    .await
                    .map_err(|e| Error::Config(format!("Failed to connect to MySQL: {}", e)))?;
                Ok(DbConnection::Mysql(conn))
            }
        }
    }
    // insert_row implementation
    async fn insert_row(&self, conn: &mut DbConnection, msg: &MessageBatch) -> Result<(), Error> {
        let table_name = self.sql_config.table_name.clone();
        let schema = msg.schema();
        let num_rows = msg.len();
        let num_columns = schema.fields().len();

        for row_index in 0..num_rows {
            let mut columns = Vec::new();
            let mut values = Vec::new();

            for col_index in 0..num_columns {
                let field = schema.field(col_index);
                let column_name = field.name();
                columns.push(column_name.clone());

                let column = msg.column(col_index);

                let value = &self.matching_data_type(column, row_index).await;

                values.push(format!("'{}'", value));
            }
            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                columns.join(", "),
                values.join(", ")
            );

            conn.execute_query(&query)
                .await
                .map_err(|e| Error::Process(format!("Failed to insert row: {}", e)))?;
        }
        Ok(())
    }
    // Convert Arrow data types to SQL-compatible string representation
    async fn matching_data_type(&self, column: &dyn Array, row_index: usize) -> String {
        // Determine the data type of the column and convert to appropriate SQL format
        let column_type = column.data_type();
        match column_type {
            DataType::Utf8 => {
                let utf8_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                format!("{}", utf8_array.value(row_index))
            }
            DataType::Int64 => {
                let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                int_array.value(row_index).to_string()
            }
            DataType::UInt64 => {
                let uint_array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                uint_array.value(row_index).to_string()
            }
            DataType::Float64 => {
                let float_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                float_array.value(row_index).to_string()
            }
            DataType::Boolean => {
                let bool_array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if bool_array.value(row_index) {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            DataType::Null => "NULL".to_string(),
            _ => "NULL".to_string(),
        }
    }
}

pub(crate) struct SqlOutputBuilder;
impl OutputBuilder for SqlOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "SQL output configuration is missing".to_string(),
            ));
        }

        let config: SqlOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SqlOutput::new(config)?))
    }
}
pub fn init() -> Result<(), Error> {
    register_output_builder("sql", Arc::new(SqlOutputBuilder))
}
