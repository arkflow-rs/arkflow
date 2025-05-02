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
use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts, Statement};

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
    pub async fn prepare_statement(&mut self, query: String) -> Result<Statement, Error> {
        match self {
            DbConnection::Mysql(conn) => conn
                .prep(query)
                .await
                .map_err(|e| Error::Process(format!("Failed to prepare statement: {}", e))),
        }
    }

    pub async fn execute_batch(
        &mut self,
        stmt: &Statement,
        params: Vec<Vec<String>>,
    ) -> Result<(), Error> {
        match self {
            DbConnection::Mysql(conn) => conn
                .exec_batch(stmt, params)
                .await
                .map_err(|e| Error::Process(format!("Failed to execute prepared stmt: {}", e))),
        }
    }

}

pub struct SqlOutput {
    sql_config: SqlOutputConfig,
    db_connection: Arc<Mutex<Option<DbConnection>>>,
    connected: std::sync::atomic::AtomicBool,
    prepared_stmt: Arc<Mutex<Option<Statement>>>,
    cancellation_token: CancellationToken,
}

impl SqlOutput {
    pub fn new(sql_config: SqlOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            sql_config,
            db_connection: Arc::new(Mutex::new(None)),
            connected: std::sync::atomic::AtomicBool::new(false),
            prepared_stmt: Arc::new(Mutex::new(None)),
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

        self.insert_row(conn, &msg).await?;

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
        let schema = msg.schema();
        let num_rows = msg.len();
        let num_columns = schema.fields().len();
        let columns: Vec<String> = (0..num_columns)
            .map(|i| schema.field(i).name().clone())
            .collect();
        let stmt: Statement = {
            let mut guard = self.prepared_stmt.lock().await;
            if guard.is_none() {
                let query = self.generate_insert_query(&columns, num_columns).await;
                let prepared = conn.prepare_statement(query).await?;
                *guard = Some(prepared.clone());
                prepared
            } else {
                guard.as_ref().unwrap().clone()
            }
        };

        let mut batch_params = Vec::with_capacity(num_rows);
        for row_index in 0..num_rows {
            let mut row = Vec::with_capacity(num_columns);
            for col_index in 0..num_columns {
                let column = msg.column(col_index);
                let value = self.matching_data_type(column, row_index).await?;
                row.push(value);
            }
            batch_params.push(row);
        }
        conn.execute_batch(&stmt, batch_params).await?;

        Ok(())
    }
    // Convert Arrow data types to SQL-compatible string representation
    async fn matching_data_type(
        &self,
        column: &dyn Array,
        row_index: usize,
    ) -> Result<String, Error> {
        // Determine the data type of the column and convert to appropriate SQL format
        let column_type = column.data_type();
        match column_type {
            DataType::Utf8 => {
                let utf8_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(format!("{}", utf8_array.value(row_index)))
            }
            DataType::Int64 => {
                let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(int_array.value(row_index).to_string())
            }
            DataType::UInt64 => {
                let uint_array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(uint_array.value(row_index).to_string())
            }
            DataType::Float64 => {
                let float_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(float_array.value(row_index).to_string())
            }
            DataType::Boolean => {
                let bool_array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if bool_array.value(row_index) {
                    Ok("true".to_string())
                } else {
                    Ok("false".to_string())
                }
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type: {:?}",
                column_type
            ))),
        }
    }
    async fn generate_insert_query(&self, columns: &Vec<String>, num_columns: usize) -> String {
        let table_name = &self.sql_config.table_name;

        let escaped_columns: Vec<String> = columns.iter().map(|col| format!("`{}`", col)).collect();
        let placeholders = match self.sql_config.output_type {
            OutputType::Mysql(_) => vec!["?"; num_columns].join(", "),
        };
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            escaped_columns.join(", "),
            placeholders
        )
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
