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
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{codec::Codec, Error, MessageBatch, MessageBatchRef, Resource};

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array, StringArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{Connection, MySqlConnection, PgConnection, QueryBuilder};

#[derive(Debug, Clone, PartialEq)]
enum SqlValue {
    String(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DatabaseType {
    Mysql(MysqlConfig),
    Postgres(PostgresConfig),
    // Sqlite,
}

enum DatabaseConnection {
    Mysql(MySqlConnection),
    Postgres(PgConnection),
    // Sqlite(SqliteConnection),
}

impl DatabaseConnection {
    /// Closes the underlying connection explicitly (rather than waiting
    /// for the object to drop). Consumes the connection: sqlx `close` takes
    /// ownership.
    async fn close(self) -> Result<(), Error> {
        match self {
            DatabaseConnection::Mysql(conn) => conn
                .close()
                .await
                .map_err(|e| Error::Process(format!("Failed to close MySQL connection: {}", e))),
            DatabaseConnection::Postgres(conn) => conn.close().await.map_err(|e| {
                Error::Process(format!("Failed to close PostgreSQL connection: {}", e))
            }),
        }
    }

    /// Executes an INSERT query with the given columns and rows
    /// Handles type conversion and proper escaping for different database types
    /// Returns a Result indicating success or detailed error information
    async fn execute_insert(
        &mut self,
        output_config: &SqlOutputConfig,
        columns: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<(), Error> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                build_mysql_insert(output_config, columns, rows)
                    .build()
                    .execute(conn)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to execute MySQL query: {}", e)))?;
                Ok(())
            }
            DatabaseConnection::Postgres(conn) => {
                build_postgres_insert(output_config, columns, rows)
                    .build()
                    .execute(conn)
                    .await
                    .map_err(|e| {
                        Error::Process(format!("Failed to execute PostgresSQL query: {}", e))
                    })?;
                Ok(())
            }
        }
    }
}

/// One `write_batch` call is one SQL transaction: every batch's insert runs
/// inside a BEGIN…COMMIT pair, so a mid-batch failure rolls the whole ack
/// range back instead of leaving a partial write for recovery replays to
/// duplicate visibly.
async fn execute_insert_transactional(
    conn: &mut DatabaseConnection,
    output_config: &SqlOutputConfig,
    batches: &[(Vec<String>, Vec<Vec<SqlValue>>)],
) -> Result<(), Error> {
    use sqlx::Connection as _;
    match conn {
        DatabaseConnection::Mysql(conn) => {
            let mut transaction = conn
                .begin()
                .await
                .map_err(|e| Error::Process(format!("Failed to begin MySQL transaction: {}", e)))?;
            for (columns, rows) in batches {
                build_mysql_insert(output_config, columns, rows)
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to execute MySQL query: {}", e)))?;
            }
            transaction
                .commit()
                .await
                .map_err(|e| Error::Process(format!("Failed to commit MySQL transaction: {}", e)))
        }
        DatabaseConnection::Postgres(conn) => {
            let mut transaction = conn.begin().await.map_err(|e| {
                Error::Process(format!("Failed to begin Postgres transaction: {}", e))
            })?;
            for (columns, rows) in batches {
                build_postgres_insert(output_config, columns, rows)
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(|e| {
                        Error::Process(format!("Failed to execute PostgresSQL query: {}", e))
                    })?;
            }
            transaction.commit().await.map_err(|e| {
                Error::Process(format!("Failed to commit Postgres transaction: {}", e))
            })
        }
    }
}

/// Columns a conflicting row updates on upsert: every column except the upsert
/// keys. When every column is a key, the keys themselves are updated (a no-op
/// assignment) so the conflict clause stays valid in both dialects.
fn upsert_update_columns<'a>(columns: &'a [String], keys: &[String]) -> Vec<&'a String> {
    let updated: Vec<&String> = columns.iter().filter(|c| !keys.contains(c)).collect();
    if updated.is_empty() {
        columns.iter().collect()
    } else {
        updated
    }
}

fn mysql_upsert_clause(columns: &[String], keys: &[String]) -> String {
    let assignments: Vec<String> = upsert_update_columns(columns, keys)
        .into_iter()
        .map(|c| format!("`{}` = VALUES(`{}`)", c, c))
        .collect();
    format!(" ON DUPLICATE KEY UPDATE {}", assignments.join(", "))
}

fn postgres_upsert_clause(columns: &[String], keys: &[String]) -> String {
    let conflict: Vec<String> = keys.iter().map(|k| format!("\"{}\"", k)).collect();
    let assignments: Vec<String> = upsert_update_columns(columns, keys)
        .into_iter()
        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
        .collect();
    format!(
        " ON CONFLICT ({}) DO UPDATE SET {}",
        conflict.join(", "),
        assignments.join(", ")
    )
}

fn build_mysql_insert(
    output_config: &SqlOutputConfig,
    columns: &[String],
    rows: &[Vec<SqlValue>],
) -> QueryBuilder<sqlx::MySql> {
    let mut query_builder = QueryBuilder::<sqlx::MySql>::new(format!(
        "INSERT INTO {} ({})",
        output_config.table_name,
        columns
            .iter()
            .map(|c| format!("`{}`", c))
            .collect::<Vec<_>>()
            .join(", "),
    ));
    // Rows are borrowed: strings bind as `&str` (same wire encoding as an
    // owned `String`), so the transactional path binds every batch without
    // cloning the whole row table.
    query_builder.push_values(rows.iter(), |mut b, row| {
        for value in row {
            match value {
                SqlValue::String(s) => b.push_bind(s.as_str()),
                SqlValue::Int64(i) => b.push_bind(*i),
                SqlValue::UInt64(u) => b.push_bind(*u),
                SqlValue::Float64(f) => b.push_bind(*f),
                SqlValue::Boolean(bool) => b.push_bind(*bool),
                SqlValue::Null => b.push_bind(None::<String>),
            };
        }
    });
    if output_config.upsert {
        let keys = output_config.upsert_keys.as_deref().unwrap_or(&[]);
        let clause = mysql_upsert_clause(columns, keys);
        query_builder.push(clause.as_str());
    }
    query_builder
}

fn build_postgres_insert(
    output_config: &SqlOutputConfig,
    columns: &[String],
    rows: &[Vec<SqlValue>],
) -> QueryBuilder<sqlx::Postgres> {
    let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(format!(
        "INSERT INTO {} ({})",
        output_config.table_name,
        columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", "),
    ));
    query_builder.push_values(rows.iter(), |mut b, row| {
        for value in row {
            match value {
                SqlValue::String(s) => b.push_bind(s.as_str()),
                SqlValue::Int64(i) => b.push_bind(*i),
                SqlValue::UInt64(u) => b.push_bind(*u as i64),
                SqlValue::Float64(f) => b.push_bind(*f),
                SqlValue::Boolean(bool) => b.push_bind(*bool),
                SqlValue::Null => b.push_bind(None::<String>),
            };
        }
    });
    if output_config.upsert {
        let keys = output_config.upsert_keys.as_deref().unwrap_or(&[]);
        let clause = postgres_upsert_clause(columns, keys);
        query_builder.push(clause.as_str());
    }
    query_builder
}

/// Checks that every configured upsert key exists in the batch schema.
fn validate_upsert_keys(output_config: &SqlOutputConfig, columns: &[String]) -> Result<(), Error> {
    if !output_config.upsert {
        return Ok(());
    }
    let keys = output_config.upsert_keys.as_deref().unwrap_or(&[]);
    for key in keys {
        if !columns.contains(key) {
            return Err(Error::Process(format!(
                "upsert_keys column '{}' not found in the batch schema",
                key
            )));
        }
    }
    Ok(())
}

/// Convert a batch into `(column names, row-major values)` for binding.
///
/// Columnar conversion: every column is downcast exactly once and converted
/// in a tight loop ([`column_values`]), then the per-column vectors are
/// transposed into rows by moving each value once — no flat intermediate
/// buffer, no per-row re-clone, no per-cell dispatch.
fn batch_rows(
    output_config: &SqlOutputConfig,
    msg: &MessageBatch,
) -> Result<(Vec<String>, Vec<Vec<SqlValue>>), Error> {
    let schema = msg.schema();
    let num_rows = msg.len();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    validate_upsert_keys(output_config, &columns)?;

    let mut rows: Vec<Vec<SqlValue>> = (0..num_rows)
        .map(|_| Vec::with_capacity(columns.len()))
        .collect();
    for (col_index, column_name) in columns.iter().enumerate() {
        let values = column_values(column_name, msg.column(col_index))?;
        for (row, value) in rows.iter_mut().zip(values) {
            row.push(value);
        }
    }
    Ok((columns, rows))
}

/// Convert one Arrow column to SQL parameter values. The column name is
/// included in type errors so the offending column is identifiable.
///
/// Narrow integer widths widen to i64/u64, Float32 to f64, temporal values
/// to ISO strings; complex types are rejected with context. Null slots stay
/// SQL NULL (the temporal branches consult the null bitmap explicitly:
/// `value_as_date`/`value_as_datetime` read the raw slot, which usually
/// stores 0 and would otherwise insert the epoch instead of NULL).
fn column_values(name: &str, column: &dyn Array) -> Result<Vec<SqlValue>, Error> {
    use datafusion::arrow::array::{
        Float32Array, Int16Array, Int32Array, Int8Array, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt8Array,
    };
    // One concrete array: null stays SQL NULL; otherwise the closure
    // converts (and optionally widens) the native value.
    macro_rules! primitive {
        ($arr_ty:ty, $conv:expr) => {{
            let a = column.as_any().downcast_ref::<$arr_ty>().unwrap();
            Ok(a.iter()
                .map(|v| match v {
                    Some(v) => ($conv)(v),
                    None => SqlValue::Null,
                })
                .collect())
        }};
    }
    let column_type = column.data_type();
    match column_type {
        DataType::Utf8 => {
            let a = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(a.iter()
                .map(|v| match v {
                    Some(s) => SqlValue::String(s.to_string()),
                    None => SqlValue::Null,
                })
                .collect())
        }
        DataType::Int64 => primitive!(Int64Array, SqlValue::Int64),
        DataType::Int32 => primitive!(Int32Array, |v| SqlValue::Int64(v as i64)),
        DataType::Int16 => primitive!(Int16Array, |v| SqlValue::Int64(v as i64)),
        DataType::Int8 => primitive!(Int8Array, |v| SqlValue::Int64(v as i64)),
        DataType::UInt64 => primitive!(UInt64Array, SqlValue::UInt64),
        DataType::UInt32 => primitive!(UInt32Array, |v| SqlValue::UInt64(v as u64)),
        DataType::UInt16 => primitive!(UInt16Array, |v| SqlValue::UInt64(v as u64)),
        DataType::UInt8 => primitive!(UInt8Array, |v| SqlValue::UInt64(v as u64)),
        DataType::Float64 => primitive!(Float64Array, SqlValue::Float64),
        DataType::Float32 => primitive!(Float32Array, |v| SqlValue::Float64(v as f64)),
        DataType::Boolean => primitive!(BooleanArray, SqlValue::Boolean),
        DataType::Date32 => {
            let a = column.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        return SqlValue::Null;
                    }
                    match a.value_as_date(i) {
                        Some(d) => SqlValue::String(d.to_string()),
                        None => SqlValue::Null,
                    }
                })
                .collect())
        }
        DataType::Date64 => {
            let a = column.as_any().downcast_ref::<Date64Array>().unwrap();
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        return SqlValue::Null;
                    }
                    match a.value_as_date(i) {
                        Some(d) => SqlValue::String(d.to_string()),
                        None => SqlValue::Null,
                    }
                })
                .collect())
        }
        DataType::Timestamp(_, _) => {
            // Any unit formats as an RFC3339 string parameter.
            macro_rules! timestamps {
                ($arr_ty:ty) => {{
                    let a = column.as_any().downcast_ref::<$arr_ty>().unwrap();
                    (0..a.len())
                        .map(|i| {
                            if a.is_null(i) {
                                return SqlValue::Null;
                            }
                            match a.value_as_datetime(i) {
                                Some(dt) => SqlValue::String(dt.and_utc().to_rfc3339()),
                                None => SqlValue::Null,
                            }
                        })
                        .collect()
                }};
            }
            let values: Vec<SqlValue> = if column.as_any().is::<TimestampSecondArray>() {
                timestamps!(TimestampSecondArray)
            } else if column.as_any().is::<TimestampMillisecondArray>() {
                timestamps!(TimestampMillisecondArray)
            } else if column.as_any().is::<TimestampMicrosecondArray>() {
                timestamps!(TimestampMicrosecondArray)
            } else if column.as_any().is::<TimestampNanosecondArray>() {
                timestamps!(TimestampNanosecondArray)
            } else {
                vec![SqlValue::Null; column.len()]
            };
            Ok(values)
        }
        _ => Err(Error::Process(format!(
            "Unsupported data type for column `{}`: {} (supported: Utf8, Boolean, Int8-64, UInt8-64, Float32/64, Date32/64, Timestamp)",
            name, column_type
        ))),
    }
}

/// Configuration for SQL output
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqlOutputConfig {
    /// SQL query statement
    output_type: DatabaseType,
    table_name: String,
    /// Use upsert (MySQL `ON DUPLICATE KEY UPDATE` / PostgreSQL
    /// `ON CONFLICT DO UPDATE`) instead of a plain insert.
    #[serde(default)]
    upsert: bool,
    /// Columns used as the conflict target when `upsert` is enabled.
    #[serde(default)]
    upsert_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlConfig {
    uri: String,
    ssl: Option<SslConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresConfig {
    uri: String,
    ssl: Option<SslConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SslConfig {
    ssl_mode: String,
    root_cert: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
}

impl SslConfig {
    pub async fn generate_mysql_ssl_opts(
        &self,
        config: &MysqlConfig,
    ) -> Result<MySqlConnectOptions, Error> {
        let ssl_mode = match self.ssl_mode.to_lowercase().as_str() {
            "disable" => MySqlSslMode::Disabled,
            "prefer" => MySqlSslMode::Preferred,
            "require" => MySqlSslMode::Required,
            "verify_ca" => MySqlSslMode::VerifyCa,
            "verify_full" => MySqlSslMode::VerifyIdentity,
            _ => return Err(Error::Config("Invalid SSL mode".to_string())),
        };
        let mut opts = MySqlConnectOptions::from_str(&config.uri)
            .map_err(|e| Error::Config(format!("Invalid MySQL URI: {}", e)))?;
        opts = opts.ssl_mode(ssl_mode);

        if let Some(root_cert) = &self.root_cert {
            opts = opts.ssl_ca(Path::new(root_cert));
        }

        if let Some(client_cert) = &self.client_cert {
            if let Some(client_key) = &self.client_key {
                opts = opts.ssl_client_cert(Path::new(client_cert));
                opts = opts.ssl_client_key(Path::new(client_key));
            } else {
                warn!("Client certificate provided without private key - will be ignored");
            }
        } else if self.client_key.is_some() {
            warn!("Client key provided without certificate - will be ignored");
        }
        Ok(opts)
    }

    async fn generate_postgres_ssl_opts(
        &self,
        config: &PostgresConfig,
    ) -> Result<PgConnectOptions, Error> {
        let ssl_mode = match self.ssl_mode.to_lowercase().as_str() {
            "disable" => PgSslMode::Disable,
            "prefer" => PgSslMode::Prefer,
            "require" => PgSslMode::Require,
            "verify_ca" => PgSslMode::VerifyCa,
            "verify_full" => PgSslMode::VerifyFull,
            _ => return Err(Error::Config("Invalid SSL mode".to_string())),
        };
        let mut opts = PgConnectOptions::from_str(&config.uri)
            .map_err(|e| Error::Config(format!("Invalid PostgreSQL URI: {}", e)))?;
        opts = opts.ssl_mode(ssl_mode);

        if let Some(root_cert) = &self.root_cert {
            opts = opts.ssl_root_cert(Path::new(root_cert));
        }

        if let Some(client_cert) = &self.client_cert {
            if let Some(client_key) = &self.client_key {
                opts = opts.ssl_client_cert(Path::new(client_cert));
                opts = opts.ssl_client_key(Path::new(client_key));
            } else {
                warn!("Client certificate provided without private key - will be ignored");
            }
        } else if self.client_key.is_some() {
            warn!("Client key provided without certificate - will be ignored");
        }
        Ok(opts)
    }
}

struct SqlOutput {
    sql_config: SqlOutputConfig,
    conn_lock: Arc<Mutex<Option<DatabaseConnection>>>,
    cancellation_token: CancellationToken,
}

impl SqlOutput {
    fn new(sql_config: SqlOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            sql_config,
            conn_lock: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }
}

#[async_trait]
impl Output for SqlOutput {
    async fn connect(&self) -> Result<(), Error> {
        let conn = self.init_connect().await?;
        let mut conn_guard = self.conn_lock.lock().await;
        *conn_guard = Some(conn);

        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        let mut conn_guard = self.conn_lock.lock().await;
        let conn = conn_guard.as_mut().ok_or(Error::Disconnection)?;
        // SQL writes consume typed columns; a codec is rejected at build time.
        self.insert_row(conn, &msg).await?;
        Ok(())
    }

    async fn write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        // Transactional batch: the whole ack range commits atomically or not
        // at all. Per-message `write` (the trait default) would leave a
        // partial-write window the replay then duplicates visibly.
        let mut conn_guard = self.conn_lock.lock().await;
        let conn = conn_guard.as_mut().ok_or(Error::Disconnection)?;
        let mut batches = Vec::with_capacity(msgs.len());
        for msg in msgs {
            batches.push(batch_rows(&self.sql_config, msg)?);
        }
        execute_insert_transactional(conn, &self.sql_config, &batches).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        let mut conn_guard = self.conn_lock.lock().await;
        if let Some(conn) = conn_guard.take() {
            conn.close().await?;
        }
        Ok(())
    }
}

impl SqlOutput {
    /// Initialize a new DB connection.  
    /// If `ssl` is configured, apply root certificates to the SSL options.
    async fn init_connect(&self) -> Result<DatabaseConnection, Error> {
        let conn = match &self.sql_config.output_type {
            DatabaseType::Mysql(config) => self.generate_mysql_conn(config).await?,
            DatabaseType::Postgres(config) => self.generate_postgres_conn(config).await?,
        };
        Ok(conn)
    }

    /// Processes a batch of Arrow data and inserts it into the database
    /// 1. Extracts schema and column names
    /// 2. Converts the typed columns to SQL-compatible values (one downcast per column)
    /// 3. Executes the insert query with proper batching
    async fn insert_row(
        &self,
        conn: &mut DatabaseConnection,
        msg: &MessageBatch,
    ) -> Result<(), Error> {
        let (columns, rows) = batch_rows(&self.sql_config, msg)?;
        conn.execute_insert(&self.sql_config, &columns, &rows)
            .await?;
        Ok(())
    }

    /// Convert one Arrow cell to a SQL parameter value (test-facing single-cell
    /// view of `column_values`; production paths convert whole columns).
    #[cfg(test)]
    fn matching_data_type(
        &self,
        name: &str,
        column: &dyn Array,
        row_index: usize,
    ) -> Result<SqlValue, Error> {
        column_values(name, column).map(|mut v| v.swap_remove(row_index))
    }

    /// Generates MySQL SSL connection options based on configuration
    /// Validates SSL mode and sets up certificates if provided
    async fn generate_mysql_conn(&self, config: &MysqlConfig) -> Result<DatabaseConnection, Error> {
        let mysql_conn = if let Some(ssl) = &config.ssl {
            let opts = ssl.generate_mysql_ssl_opts(config).await?;
            MySqlConnection::connect_with(&opts)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to MySQL with SSL: {}", e)))?
        } else {
            MySqlConnection::connect(&config.uri)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to MySQL: {}", e)))?
        };
        Ok(DatabaseConnection::Mysql(mysql_conn))
    }

    async fn generate_postgres_conn(
        &self,
        config: &PostgresConfig,
    ) -> Result<DatabaseConnection, Error> {
        let postgres_conn = if let Some(ssl) = &config.ssl {
            let opts = ssl.generate_postgres_ssl_opts(config).await?;
            PgConnection::connect_with(&opts).await.map_err(|e| {
                Error::Config(format!("Failed to connect to PostgreSQL with SSL: {}", e))
            })?
        } else {
            PgConnection::connect(&config.uri)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to PostgreSQL: {}", e)))?
        };
        Ok(DatabaseConnection::Postgres(postgres_conn))
    }
}

struct SqlOutputBuilder;

impl OutputBuilder for SqlOutputBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: SqlOutputConfig = parse_config(config, "SqlOutput input")?;
        // SQL writes map typed Arrow columns to bound parameters; encoding
        // the batch through a codec would produce a Binary payload column
        // that can never insert. Reject at build time instead of failing
        // on every write.
        if codec.is_some() {
            return Err(Error::Config(
                "sql output does not support a codec: it writes typed columns directly".to_string(),
            ));
        }
        if config.upsert {
            if config
                .upsert_keys
                .as_ref()
                .is_none_or(|keys| keys.is_empty())
            {
                return Err(Error::Config(
                    "sql output: upsert = true requires a non-empty upsert_keys list".to_string(),
                ));
            }
            let keys = config
                .upsert_keys
                .as_deref()
                .expect("checked non-empty above");
            let mut seen = std::collections::HashSet::with_capacity(keys.len());
            if keys.iter().any(|key| !seen.insert(key)) {
                return Err(Error::Config(
                    "sql output: upsert_keys contains duplicate columns".to_string(),
                ));
            }
        }
        Ok(Arc::new(SqlOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("sql", Arc::new(SqlOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "sql",
        "Batch-inserts records into a MySQL or PostgreSQL database, with optional upsert (ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE) for idempotent writes.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "output_type": {"type": "object", "description": "Database connection settings, tagged by `type` (`mysql` or `postgres`), each with `uri` and optional `ssl`.", "properties": {
                    "type": {"type": "string", "enum": ["mysql", "postgres"]},
                    "uri": {"type": "string"},
                    "ssl": {"type": "object", "description": "Optional SSL configuration (ssl_mode, root_cert, client_cert, client_key)."}
                }, "required": ["type", "uri"]},
                "table_name": {"type": "string", "description": "Destination table."},
                "upsert": {"type": "boolean", "default": false, "description": "Use upsert (MySQL ON DUPLICATE KEY UPDATE / PostgreSQL ON CONFLICT DO UPDATE) instead of a plain insert."},
                "upsert_keys": {"type": "array", "items": {"type": "string"}, "description": "Columns used as the conflict target for upsert. Required (non-empty) when upsert is true."}
            },
            "required": ["output_type", "table_name"]
        }),
    ).with_example(serde_json::json!({
        "output_type": {"type": "postgres", "uri": "postgres://user:pass@localhost/db"},
        "table_name": "events",
        "upsert": true,
        "upsert_keys": ["id"]
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Execute;
    use std::cell::RefCell;
    use std::collections::HashMap;

    fn postgres_config(upsert: bool, keys: Option<Vec<&str>>) -> SqlOutputConfig {
        SqlOutputConfig {
            output_type: DatabaseType::Postgres(PostgresConfig {
                uri: "postgres://user:pass@localhost/db".to_string(),
                ssl: None,
            }),
            table_name: "events".to_string(),
            upsert,
            upsert_keys: keys.map(|ks| ks.into_iter().map(String::from).collect()),
        }
    }

    fn columns() -> Vec<String> {
        vec!["id".to_string(), "name".to_string()]
    }

    fn rows() -> Vec<Vec<SqlValue>> {
        vec![vec![SqlValue::Int64(1), SqlValue::String("a".to_string())]]
    }

    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: RefCell::new(vec![]),
        }
    }

    #[test]
    fn postgres_plain_insert_has_no_conflict_clause() {
        let sql = build_postgres_insert(&postgres_config(false, None), &columns(), &rows())
            .build()
            .sql()
            .as_str()
            .to_string();
        assert!(sql.starts_with("INSERT INTO events (\"id\", \"name\")"),);
        assert!(!sql.contains("ON CONFLICT"));
    }

    #[test]
    fn postgres_upsert_uses_on_conflict_do_update() {
        let sql = build_postgres_insert(
            &postgres_config(true, Some(vec!["id"])),
            &columns(),
            &rows(),
        )
        .build()
        .sql()
        .as_str()
        .to_string();
        assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE SET"));
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
        // key columns are not assigned in the update set
        assert!(!sql.contains("\"id\" = EXCLUDED"));
    }

    #[test]
    fn mysql_upsert_uses_on_duplicate_key_update() {
        let config = SqlOutputConfig {
            output_type: DatabaseType::Mysql(MysqlConfig {
                uri: "mysql://root@localhost/db".to_string(),
                ssl: None,
            }),
            ..postgres_config(true, Some(vec!["id"]))
        };
        let sql = build_mysql_insert(&config, &columns(), &rows())
            .build()
            .sql()
            .as_str()
            .to_string();
        assert!(sql.contains("INSERT INTO events (`id`, `name`)"));
        assert!(sql.contains(" ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"));
        assert!(!sql.contains("`id` = VALUES"));
    }

    #[test]
    fn all_key_columns_falls_back_to_identity_assignment() {
        // When every column is a key, the clause must still be valid: the keys
        // themselves are assigned (a no-op update).
        let sql = build_postgres_insert(
            &postgres_config(true, Some(vec!["id", "name"])),
            &columns(),
            &rows(),
        )
        .build()
        .sql()
        .as_str()
        .to_string();
        assert!(sql.contains("ON CONFLICT (\"id\", \"name\") DO UPDATE SET"));
        assert!(sql.contains("\"id\" = EXCLUDED.\"id\""));
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
    }

    #[test]
    fn rejects_upsert_without_keys() {
        let config = serde_json::json!({
            "output_type": {"type": "postgres", "uri": "postgres://user:pass@localhost/db"},
            "table_name": "events",
            "upsert": true
        });
        let err = match SqlOutputBuilder.build(None, &Some(config), None, &resource()) {
            Ok(_) => panic!("expected build to fail when upsert is set without upsert_keys"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("upsert_keys"),
            "expected upsert_keys in error, got: {msg}"
        );
    }

    #[test]
    fn rejects_empty_upsert_keys() {
        let config = serde_json::json!({
            "output_type": {"type": "postgres", "uri": "postgres://user:pass@localhost/db"},
            "table_name": "events",
            "upsert": true,
            "upsert_keys": []
        });
        assert!(SqlOutputBuilder
            .build(None, &Some(config), None, &resource())
            .is_err());
    }

    #[test]
    fn rejects_duplicate_upsert_keys() {
        let config = serde_json::json!({
            "output_type": {"type": "postgres", "uri": "postgres://user:pass@localhost/db"},
            "table_name": "events",
            "upsert": true,
            "upsert_keys": ["id", "id"]
        });
        let err = match SqlOutputBuilder.build(None, &Some(config), None, &resource()) {
            Ok(_) => panic!("expected build to fail when upsert_keys contains duplicates"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("duplicate"),
            "expected duplicate in error, got: {msg}"
        );
    }

    #[test]
    fn accepts_plain_config_without_upsert() {
        let config = serde_json::json!({
            "output_type": {"type": "mysql", "uri": "mysql://root@localhost/db"},
            "table_name": "events"
        });
        let _output = SqlOutputBuilder
            .build(None, &Some(config), None, &resource())
            .expect("plain config must build");
    }

    #[test]
    fn validate_upsert_keys_rejects_missing_column() {
        let config = postgres_config(true, Some(vec!["id", "missing"]));
        let err = validate_upsert_keys(&config, &columns()).unwrap_err();
        assert!(format!("{err}").contains("missing"));
    }

    #[test]
    fn validate_upsert_keys_passes_when_upsert_disabled() {
        // keys referencing unknown columns are irrelevant when upsert is off
        let config = postgres_config(false, Some(vec!["missing"]));
        assert!(validate_upsert_keys(&config, &columns()).is_ok());
    }

    #[tokio::test]
    async fn matching_data_type_widens_narrow_widths() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        use datafusion::arrow::array::{
            Date32Array, Float32Array, Int32Array, TimestampNanosecondArray,
        };

        let i32s = Int32Array::from(vec![Some(-7)]);
        assert!(matches!(
            output.matching_data_type("narrow", &i32s, 0).unwrap(),
            SqlValue::Int64(-7)
        ));

        let f32s = Float32Array::from(vec![Some(1.5f32)]);
        assert!(matches!(
            output.matching_data_type("f", &f32s, 0).unwrap(),
            SqlValue::Float64(v) if (v - 1.5f64).abs() < f64::EPSILON
        ));

        let dates = Date32Array::from(vec![Some(0)]);
        assert!(matches!(
            output.matching_data_type("d", &dates, 0).unwrap(),
            SqlValue::String(ref s) if s == "1970-01-01"
        ));

        let ts = TimestampNanosecondArray::from(vec![Some(0)]);
        assert!(matches!(
            output.matching_data_type("ts", &ts, 0).unwrap(),
            SqlValue::String(ref s) if s.starts_with("1970-01-01T00:00:00")
        ));
    }

    #[tokio::test]
    async fn matching_data_type_null_temporal_cells_stay_null() {
        // Regression (CR): value_as_date/value_as_datetime ignore the null
        // bitmap — a null slot stores 0 and used to insert the epoch
        // (1970-01-01 / 1970-01-01T00:00:00+00:00) instead of NULL.
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        use datafusion::arrow::array::{Date32Array, TimestampNanosecondArray};

        let dates = Date32Array::from(vec![Some(0), None]);
        assert!(matches!(
            output.matching_data_type("d", &dates, 1).unwrap(),
            SqlValue::Null
        ));

        let ts = TimestampNanosecondArray::from(vec![None]);
        assert!(matches!(
            output.matching_data_type("ts", &ts, 0).unwrap(),
            SqlValue::Null
        ));
    }

    #[tokio::test]
    async fn matching_data_type_rejects_complex_type_with_column_name() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        let list_field = datafusion::arrow::datatypes::Field::new(
            "tags",
            datafusion::arrow::datatypes::DataType::List(std::sync::Arc::new(
                datafusion::arrow::datatypes::Field::new("item", DataType::Utf8, true),
            )),
            true,
        );
        let schema =
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![list_field]));
        let rb = datafusion::arrow::array::RecordBatch::new_empty(schema);
        let batch = arkflow_core::MessageBatch::new_arrow(rb);
        let err = output
            .matching_data_type("tags", batch.column(0), 0)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("`tags`"),
            "error must name the column, got: {msg}"
        );
    }

    // ---- columnar conversion equivalence vs. the historical per-cell path ----

    /// The previous implementation: one cell conversion per (row, column)
    /// with a downcast per cell, then a flat buffer chunked into rows. Kept
    /// inline as the oracle for the columnar `batch_rows`.
    fn batch_rows_oracle(msg: &MessageBatch) -> Vec<Vec<SqlValue>> {
        use datafusion::arrow::array::{
            Float32Array, Int16Array, Int32Array, Int8Array, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
            UInt32Array, UInt8Array,
        };
        fn cell(column: &dyn Array, row_index: usize) -> SqlValue {
            macro_rules! cell {
                ($arr:expr, $i:expr, $conv:expr) => {{
                    let a = $arr;
                    if a.is_null($i) {
                        SqlValue::Null
                    } else {
                        ($conv)(a.value($i))
                    }
                }};
            }
            match column.data_type() {
                DataType::Utf8 => cell!(
                    column.as_any().downcast_ref::<StringArray>().unwrap(),
                    row_index,
                    |v: &str| SqlValue::String(v.to_string())
                ),
                DataType::Int64 => cell!(
                    column.as_any().downcast_ref::<Int64Array>().unwrap(),
                    row_index,
                    SqlValue::Int64
                ),
                DataType::Int32 => cell!(
                    column.as_any().downcast_ref::<Int32Array>().unwrap(),
                    row_index,
                    |v| SqlValue::Int64(v as i64)
                ),
                DataType::Int16 => cell!(
                    column.as_any().downcast_ref::<Int16Array>().unwrap(),
                    row_index,
                    |v| SqlValue::Int64(v as i64)
                ),
                DataType::Int8 => cell!(
                    column.as_any().downcast_ref::<Int8Array>().unwrap(),
                    row_index,
                    |v| SqlValue::Int64(v as i64)
                ),
                DataType::UInt64 => cell!(
                    column.as_any().downcast_ref::<UInt64Array>().unwrap(),
                    row_index,
                    SqlValue::UInt64
                ),
                DataType::UInt32 => cell!(
                    column.as_any().downcast_ref::<UInt32Array>().unwrap(),
                    row_index,
                    |v| SqlValue::UInt64(v as u64)
                ),
                DataType::UInt16 => cell!(
                    column.as_any().downcast_ref::<UInt16Array>().unwrap(),
                    row_index,
                    |v| SqlValue::UInt64(v as u64)
                ),
                DataType::UInt8 => cell!(
                    column.as_any().downcast_ref::<UInt8Array>().unwrap(),
                    row_index,
                    |v| SqlValue::UInt64(v as u64)
                ),
                DataType::Float64 => cell!(
                    column.as_any().downcast_ref::<Float64Array>().unwrap(),
                    row_index,
                    SqlValue::Float64
                ),
                DataType::Float32 => cell!(
                    column.as_any().downcast_ref::<Float32Array>().unwrap(),
                    row_index,
                    |v| SqlValue::Float64(v as f64)
                ),
                DataType::Boolean => cell!(
                    column.as_any().downcast_ref::<BooleanArray>().unwrap(),
                    row_index,
                    SqlValue::Boolean
                ),
                DataType::Date32 | DataType::Date64 => {
                    if column.is_null(row_index) {
                        return SqlValue::Null;
                    }
                    let date = if let Some(arr) = column.as_any().downcast_ref::<Date32Array>() {
                        arr.value_as_date(row_index)
                    } else if let Some(arr) = column.as_any().downcast_ref::<Date64Array>() {
                        arr.value_as_date(row_index)
                    } else {
                        None
                    };
                    match date {
                        Some(d) => SqlValue::String(d.to_string()),
                        None => SqlValue::Null,
                    }
                }
                DataType::Timestamp(_, _) => {
                    if column.is_null(row_index) {
                        return SqlValue::Null;
                    }
                    let dt = if let Some(a) = column.as_any().downcast_ref::<TimestampSecondArray>()
                    {
                        a.value_as_datetime(row_index)
                    } else if let Some(a) =
                        column.as_any().downcast_ref::<TimestampMillisecondArray>()
                    {
                        a.value_as_datetime(row_index)
                    } else if let Some(a) =
                        column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                    {
                        a.value_as_datetime(row_index)
                    } else if let Some(a) =
                        column.as_any().downcast_ref::<TimestampNanosecondArray>()
                    {
                        a.value_as_datetime(row_index)
                    } else {
                        None
                    };
                    match dt {
                        Some(dt) => SqlValue::String(dt.and_utc().to_rfc3339()),
                        None => SqlValue::Null,
                    }
                }
                other => panic!("oracle: unsupported {other}"),
            }
        }
        let schema = msg.schema();
        let num_rows = msg.len();
        let num_columns = schema.fields().len();
        let mut rows = Vec::with_capacity(num_columns * num_rows);
        for row_index in 0..num_rows {
            for col_index in 0..num_columns {
                rows.push(cell(msg.column(col_index), row_index));
            }
        }
        rows.chunks(num_columns).map(|c| c.to_vec()).collect()
    }

    /// Every supported column type with nulls sprinkled in.
    fn wide_typed_batch(rows: usize) -> MessageBatch {
        use datafusion::arrow::array::{
            Date32Array, Date64Array, Float32Array, Int16Array, Int32Array, Int8Array,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt16Array, UInt32Array, UInt8Array,
        };
        use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
        let present = |i: usize| i % 3 != 1;
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("d32", DataType::Date32, true),
            Field::new("d64", DataType::Date64, true),
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| present(i).then(|| format!("row_{i}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64 - 5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i32 - 5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int16Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i16 - 5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int8Array::from(
                (0..rows)
                    .map(|i| present(i).then_some((i % 100) as i8 - 5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(u64::MAX - i as u64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as u32 * 7))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt16Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as u16))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                (0..rows)
                    .map(|i| present(i).then_some((i % 256) as u8))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as f64 * 0.25))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as f32 * 0.5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                (0..rows)
                    .map(|i| present(i).then_some(i % 2 == 0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Date64Array::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64 * 86_400_000))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampSecondArray::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64 * 1_500))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64 * 1_000))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(
                (0..rows)
                    .map(|i| present(i).then_some(i as i64 * 1_000_000))
                    .collect::<Vec<_>>(),
            )),
        ];
        let rb = datafusion::arrow::array::RecordBatch::try_new(schema, columns).unwrap();
        MessageBatch::new_arrow(rb)
    }

    #[test]
    fn columnar_rows_match_per_cell_oracle_exactly() {
        for rows in [0usize, 1, 3, 7, 64] {
            let batch = wide_typed_batch(rows);
            let (columns, got) = batch_rows(&postgres_config(false, None), &batch).unwrap();
            assert_eq!(columns.len(), 18);
            let want = batch_rows_oracle(&batch);
            assert_eq!(
                got, want,
                "rows={rows}: columnar vs per-cell conversion diverged"
            );
        }
    }

    #[test]
    fn columnar_rows_validate_upsert_keys_before_converting() {
        let batch = wide_typed_batch(2);
        let err = batch_rows(&postgres_config(true, Some(vec!["missing"])), &batch).unwrap_err();
        assert!(format!("{err}").contains("missing"));
    }

    /// Ad-hoc release timing (NOT run by CI): columnar conversion vs the
    /// historical per-cell dispatch + flat-buffer chunking. Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored sql_output_conversion_timing --nocapture`
    #[test]
    #[ignore]
    fn sql_output_conversion_timing() {
        let batch = wide_typed_batch(1_000);
        let batches = 200usize;
        let config = postgres_config(false, None);

        let time = |name: &str, f: &mut dyn FnMut() -> usize| {
            let mut best = std::time::Duration::MAX;
            let mut cells = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                cells = f();
                best = best.min(start.elapsed());
            }
            println!(
                "{name}: {cells} cells in {best:?} ({:.0} cells/s)",
                cells as f64 / best.as_secs_f64()
            );
        };

        let mut columnar = || {
            let mut n = 0usize;
            for _ in 0..batches {
                let (_, rows) = batch_rows(&config, &batch).unwrap();
                n += rows.iter().map(Vec::len).sum::<usize>();
            }
            n
        };
        time(
            "columnar (one downcast per column + transpose)",
            &mut columnar,
        );

        let mut per_cell = || {
            let mut n = 0usize;
            for _ in 0..batches {
                let rows = batch_rows_oracle(&batch);
                n += rows.iter().map(Vec::len).sum::<usize>();
            }
            n
        };
        time(
            "per-cell dispatch + chunk re-clone (historical)",
            &mut per_cell,
        );
    }

    #[test]
    fn builder_rejects_codec() {
        let config = serde_json::json!({
            "output_type": {"type": "postgres", "uri": "postgres://user:pass@localhost/db"},
            "table_name": "events"
        });
        struct NoopCodec;
        #[async_trait]
        impl arkflow_core::codec::Encoder for NoopCodec {
            async fn encode(
                &self,
                _batch: arkflow_core::MessageBatch,
            ) -> Result<Vec<arkflow_core::Bytes>, Error> {
                Ok(Vec::new())
            }
        }
        #[async_trait]
        impl arkflow_core::codec::Decoder for NoopCodec {
            async fn decode(
                &self,
                _b: Vec<arkflow_core::Bytes>,
            ) -> Result<arkflow_core::MessageBatch, Error> {
                Err(Error::Process("noop".to_string()))
            }
        }
        let codec: Option<std::sync::Arc<dyn arkflow_core::codec::Codec>> =
            Some(std::sync::Arc::new(NoopCodec));
        let err = match SqlOutputBuilder.build(None, &Some(config.clone()), codec, &resource()) {
            Err(e) => e,
            Ok(_) => panic!("codec must be rejected at build time"),
        };
        assert!(
            format!("{err}").contains("codec"),
            "error must explain the codec rejection: {err}"
        );
        // without a codec, build succeeds (parse-only)
        assert!(SqlOutputBuilder
            .build(None, &Some(config), None, &resource())
            .is_ok());
    }

    #[tokio::test]
    async fn close_on_unconnected_output_is_ok() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        assert!(output.close().await.is_ok());
    }

    // ---- SSL option parsing (no network needed) ----

    fn ssl_config(mode: &str) -> SslConfig {
        SslConfig {
            ssl_mode: mode.to_string(),
            root_cert: None,
            client_cert: None,
            client_key: None,
        }
    }

    fn mysql_config(uri: &str) -> MysqlConfig {
        MysqlConfig {
            uri: uri.to_string(),
            ssl: None,
        }
    }

    fn pg_config(uri: &str) -> PostgresConfig {
        PostgresConfig {
            uri: uri.to_string(),
            ssl: None,
        }
    }

    #[tokio::test]
    async fn mysql_ssl_modes_parse_case_insensitively() {
        let config = mysql_config("mysql://root@127.0.0.1:3306/db");
        for mode in [
            "disable",
            "prefer",
            "require",
            "verify_ca",
            "verify_full",
            "DISABLE",
            "Prefer",
            "VERIFY_CA",
        ] {
            let result = ssl_config(mode).generate_mysql_ssl_opts(&config).await;
            assert!(
                result.is_ok(),
                "mysql ssl_mode '{mode}' must parse: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn mysql_ssl_opts_reject_unknown_mode_and_bad_uri() {
        let config = mysql_config("mysql://root@127.0.0.1:3306/db");
        let err = ssl_config("bogus")
            .generate_mysql_ssl_opts(&config)
            .await
            .expect_err("unknown ssl mode must be rejected");
        assert!(format!("{err}").contains("Invalid SSL mode"), "{err}");

        let bad_uri = mysql_config("not a mysql uri");
        let err = ssl_config("disable")
            .generate_mysql_ssl_opts(&bad_uri)
            .await
            .expect_err("malformed mysql uri must be rejected");
        assert!(format!("{err}").contains("Invalid MySQL URI"), "{err}");
    }

    #[tokio::test]
    async fn postgres_ssl_modes_parse_case_insensitively() {
        let config = pg_config("postgres://user:pass@127.0.0.1:5432/db");
        for mode in [
            "disable",
            "prefer",
            "require",
            "verify_ca",
            "verify_full",
            "DISABLE",
            "Prefer",
            "VERIFY_CA",
        ] {
            let result = ssl_config(mode).generate_postgres_ssl_opts(&config).await;
            assert!(
                result.is_ok(),
                "postgres ssl_mode '{mode}' must parse: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn postgres_ssl_opts_reject_unknown_mode_and_bad_uri() {
        let config = pg_config("postgres://user:pass@127.0.0.1:5432/db");
        let err = ssl_config("bogus")
            .generate_postgres_ssl_opts(&config)
            .await
            .expect_err("unknown ssl mode must be rejected");
        assert!(format!("{err}").contains("Invalid SSL mode"), "{err}");

        let bad_uri = pg_config("not a pg uri");
        let err = ssl_config("disable")
            .generate_postgres_ssl_opts(&bad_uri)
            .await
            .expect_err("malformed postgres uri must be rejected");
        assert!(format!("{err}").contains("Invalid PostgreSQL URI"), "{err}");
    }

    #[tokio::test]
    async fn ssl_opts_accept_certificates_and_warn_on_half_configured_identity() {
        // A full client identity, a lone root cert, and the two half-configured
        // identities (cert without key / key without cert) all produce options;
        // the halves only warn.
        let mysql = mysql_config("mysql://root@127.0.0.1:3306/db");
        let mut full = ssl_config("require");
        full.root_cert = Some("/nonexistent/root.pem".to_string());
        full.client_cert = Some("/nonexistent/client.pem".to_string());
        full.client_key = Some("/nonexistent/client.key".to_string());
        assert!(full.generate_mysql_ssl_opts(&mysql).await.is_ok());

        let mut cert_only = ssl_config("require");
        cert_only.client_cert = Some("/nonexistent/client.pem".to_string());
        assert!(cert_only.generate_mysql_ssl_opts(&mysql).await.is_ok());

        let mut key_only = ssl_config("require");
        key_only.client_key = Some("/nonexistent/client.key".to_string());
        assert!(key_only.generate_mysql_ssl_opts(&mysql).await.is_ok());

        let postgres = pg_config("postgres://user:pass@127.0.0.1:5432/db");
        let mut pg_full = ssl_config("require");
        pg_full.root_cert = Some("/nonexistent/root.pem".to_string());
        pg_full.client_cert = Some("/nonexistent/client.pem".to_string());
        pg_full.client_key = Some("/nonexistent/client.key".to_string());
        assert!(pg_full.generate_postgres_ssl_opts(&postgres).await.is_ok());

        let mut pg_cert_only = ssl_config("require");
        pg_cert_only.client_cert = Some("/nonexistent/client.pem".to_string());
        assert!(pg_cert_only
            .generate_postgres_ssl_opts(&postgres)
            .await
            .is_ok());

        let mut pg_key_only = ssl_config("require");
        pg_key_only.client_key = Some("/nonexistent/client.key".to_string());
        assert!(pg_key_only
            .generate_postgres_ssl_opts(&postgres)
            .await
            .is_ok());
    }

    // ---- connection failures against a guaranteed-closed local port ----

    fn closed_port() -> u16 {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        port
    }

    #[tokio::test]
    async fn mysql_connect_failure_surfaces_config_error() {
        let config = SqlOutputConfig {
            output_type: DatabaseType::Mysql(mysql_config(&format!(
                "mysql://root@127.0.0.1:{}/db",
                closed_port()
            ))),
            table_name: "events".to_string(),
            upsert: false,
            upsert_keys: None,
        };
        let output = SqlOutput::new(config).unwrap();
        let err = output.connect().await.unwrap_err();
        assert!(
            format!("{err}").contains("Failed to connect to MySQL"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn mysql_connect_failure_with_ssl_surfaces_ssl_error() {
        let config = SqlOutputConfig {
            output_type: DatabaseType::Mysql(MysqlConfig {
                uri: format!("mysql://root@127.0.0.1:{}/db", closed_port()),
                ssl: Some(ssl_config("disable")),
            }),
            table_name: "events".to_string(),
            upsert: false,
            upsert_keys: None,
        };
        let output = SqlOutput::new(config).unwrap();
        let err = output.connect().await.unwrap_err();
        assert!(
            format!("{err}").contains("Failed to connect to MySQL with SSL"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn postgres_connect_failure_surfaces_config_error() {
        let config = SqlOutputConfig {
            output_type: DatabaseType::Postgres(pg_config(&format!(
                "postgres://user:pass@127.0.0.1:{}/db",
                closed_port()
            ))),
            table_name: "events".to_string(),
            upsert: false,
            upsert_keys: None,
        };
        let output = SqlOutput::new(config).unwrap();
        let err = output.connect().await.unwrap_err();
        assert!(
            format!("{err}").contains("Failed to connect to PostgreSQL"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn postgres_connect_failure_with_ssl_surfaces_ssl_error() {
        let config = SqlOutputConfig {
            output_type: DatabaseType::Postgres(PostgresConfig {
                uri: format!("postgres://user:pass@127.0.0.1:{}/db", closed_port()),
                ssl: Some(ssl_config("disable")),
            }),
            table_name: "events".to_string(),
            upsert: false,
            upsert_keys: None,
        };
        let output = SqlOutput::new(config).unwrap();
        let err = output.connect().await.unwrap_err();
        assert!(
            format!("{err}").contains("Failed to connect to PostgreSQL with SSL"),
            "{err}"
        );
    }

    // ---- write paths before connect ----

    fn typed_batch() -> MessageBatchRef {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }

    #[tokio::test]
    async fn write_and_write_batch_before_connect_report_disconnection() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        let err = output.write(typed_batch()).await.unwrap_err();
        assert!(matches!(err, Error::Disconnection), "{err:?}");
        let err = output.write_batch(&[typed_batch()]).await.unwrap_err();
        assert!(matches!(err, Error::Disconnection), "{err:?}");
        // close stays a no-op success whether or not a connection existed
        assert!(output.close().await.is_ok());
    }

    // ---- remaining matching_data_type conversions ----

    #[tokio::test]
    async fn matching_data_type_widens_unsigned_and_signed_widths() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        use datafusion::arrow::array::{
            Int16Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        };

        let u8s = UInt8Array::from(vec![Some(250u8)]);
        assert!(matches!(
            output.matching_data_type("u8", &u8s, 0).unwrap(),
            SqlValue::UInt64(250)
        ));
        let u16s = UInt16Array::from(vec![Some(65_535u16)]);
        assert!(matches!(
            output.matching_data_type("u16", &u16s, 0).unwrap(),
            SqlValue::UInt64(65_535)
        ));
        let u32s = UInt32Array::from(vec![Some(4_000_000_000u32)]);
        assert!(matches!(
            output.matching_data_type("u32", &u32s, 0).unwrap(),
            SqlValue::UInt64(4_000_000_000)
        ));
        let u64s = UInt64Array::from(vec![Some(u64::MAX)]);
        assert!(matches!(
            output.matching_data_type("u64", &u64s, 0).unwrap(),
            SqlValue::UInt64(u64::MAX)
        ));
        let i8s = Int8Array::from(vec![Some(-128i8)]);
        assert!(matches!(
            output.matching_data_type("i8", &i8s, 0).unwrap(),
            SqlValue::Int64(-128)
        ));
        let i16s = Int16Array::from(vec![Some(-32_768i16)]);
        assert!(matches!(
            output.matching_data_type("i16", &i16s, 0).unwrap(),
            SqlValue::Int64(-32_768)
        ));
    }

    #[tokio::test]
    async fn matching_data_type_handles_bool_float64_and_nulls() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        use datafusion::arrow::array::{
            BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
        };

        let bools = BooleanArray::from(vec![Some(true)]);
        assert!(matches!(
            output.matching_data_type("b", &bools, 0).unwrap(),
            SqlValue::Boolean(true)
        ));
        let null_bools = BooleanArray::from(vec![None::<bool>]);
        assert!(matches!(
            output.matching_data_type("b", &null_bools, 0).unwrap(),
            SqlValue::Null
        ));

        let floats = Float64Array::from(vec![Some(2.25)]);
        assert!(matches!(
            output.matching_data_type("f", &floats, 0).unwrap(),
            SqlValue::Float64(v) if (v - 2.25).abs() < f64::EPSILON
        ));
        let null_floats = Float64Array::from(vec![None::<f64>]);
        assert!(matches!(
            output.matching_data_type("f", &null_floats, 0).unwrap(),
            SqlValue::Null
        ));

        let null_ints = Int64Array::from(vec![None::<i64>]);
        assert!(matches!(
            output.matching_data_type("i", &null_ints, 0).unwrap(),
            SqlValue::Null
        ));
        let null_uints = UInt64Array::from(vec![None::<u64>]);
        assert!(matches!(
            output.matching_data_type("u", &null_uints, 0).unwrap(),
            SqlValue::Null
        ));
        let null_strings = StringArray::from(vec![None::<&str>]);
        assert!(matches!(
            output.matching_data_type("s", &null_strings, 0).unwrap(),
            SqlValue::Null
        ));
    }

    #[tokio::test]
    async fn matching_data_type_formats_date64_and_all_timestamp_units() {
        let output = SqlOutput::new(postgres_config(false, None)).unwrap();
        use datafusion::arrow::array::{
            Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
        };

        let dates = Date64Array::from(vec![Some(86_400_000i64)]);
        assert!(matches!(
            output.matching_data_type("d", &dates, 0).unwrap(),
            SqlValue::String(ref s) if s == "1970-01-02"
        ));
        let null_dates = Date64Array::from(vec![None::<i64>]);
        assert!(matches!(
            output.matching_data_type("d", &null_dates, 0).unwrap(),
            SqlValue::Null
        ));

        let seconds = TimestampSecondArray::from(vec![Some(1i64)]);
        assert!(matches!(
            output.matching_data_type("ts", &seconds, 0).unwrap(),
            SqlValue::String(ref s) if s.starts_with("1970-01-01T00:00:01")
        ));
        let millis = TimestampMillisecondArray::from(vec![Some(1_500i64)]);
        assert!(matches!(
            output.matching_data_type("ts", &millis, 0).unwrap(),
            SqlValue::String(ref s) if s.starts_with("1970-01-01T00:00:01.500")
        ));
        let micros = TimestampMicrosecondArray::from(vec![Some(1_000i64)]);
        assert!(matches!(
            output.matching_data_type("ts", &micros, 0).unwrap(),
            SqlValue::String(ref s) if s.starts_with("1970-01-01T00:00:00.001")
        ));
    }
}
