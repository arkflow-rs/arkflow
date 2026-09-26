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
    Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
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

#[derive(Debug, Clone)]
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
    /// Executes an INSERT query with the given columns and rows
    /// Handles type conversion and proper escaping for different database types
    /// Returns a Result indicating success or detailed error information
    async fn execute_insert(
        &mut self,
        output_config: &SqlOutputConfig,
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<(), Error> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                build_mysql_insert(output_config, &columns, rows)
                    .build()
                    .execute(conn)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to execute MySQL query: {}", e)))?;
                Ok(())
            }
            DatabaseConnection::Postgres(conn) => {
                build_postgres_insert(output_config, &columns, rows)
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
                build_mysql_insert(output_config, columns, rows.clone())
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(|e| {
                        Error::Process(format!("Failed to execute MySQL query: {}", e))
                    })?;
            }
            transaction.commit().await.map_err(|e| {
                Error::Process(format!("Failed to commit MySQL transaction: {}", e))
            })
        }
        DatabaseConnection::Postgres(conn) => {
            let mut transaction = conn.begin().await.map_err(|e| {
                Error::Process(format!("Failed to begin Postgres transaction: {}", e))
            })?;
            for (columns, rows) in batches {
                build_postgres_insert(output_config, columns, rows.clone())
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
    rows: Vec<Vec<SqlValue>>,
) -> QueryBuilder<'static, sqlx::MySql> {
    let mut query_builder = QueryBuilder::<sqlx::MySql>::new(format!(
        "INSERT INTO {} ({})",
        output_config.table_name,
        columns
            .iter()
            .map(|c| format!("`{}`", c))
            .collect::<Vec<_>>()
            .join(", "),
    ));
    query_builder.push_values(rows, |mut b, row| {
        for value in row {
            match value {
                SqlValue::String(s) => b.push_bind(s),
                SqlValue::Int64(i) => b.push_bind(i),
                SqlValue::UInt64(u) => b.push_bind(u),
                SqlValue::Float64(f) => b.push_bind(f),
                SqlValue::Boolean(bool) => b.push_bind(bool),
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
    rows: Vec<Vec<SqlValue>>,
) -> QueryBuilder<'static, sqlx::Postgres> {
    let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(format!(
        "INSERT INTO {} ({})",
        output_config.table_name,
        columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", "),
    ));
    query_builder.push_values(rows, |mut b, row| {
        for value in row {
            match value {
                SqlValue::String(s) => b.push_bind(s),
                SqlValue::Int64(i) => b.push_bind(i),
                SqlValue::UInt64(u) => b.push_bind(u as i64),
                SqlValue::Float64(f) => b.push_bind(f),
                SqlValue::Boolean(bool) => b.push_bind(bool),
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
fn validate_upsert_keys(
    output_config: &SqlOutputConfig,
    columns: &[String],
) -> Result<(), Error> {
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
    codec: Option<Arc<dyn Codec>>,
}

impl SqlOutput {
    fn new(sql_config: SqlOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            sql_config,
            conn_lock: Arc::new(Mutex::new(None)),
            cancellation_token,
            codec,
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

        // Apply codec encoding if configured, otherwise use the message as-is
        let processed_msg = if let Some(codec) = &self.codec {
            let encoded = codec.encode((*msg).clone()).await?;
            // Convert encoded bytes back to MessageBatch for SQL insertion
            // This is a simplified approach - in practice, you might need more sophisticated handling
            MessageBatch::new_binary(encoded)?
        } else {
            (*msg).clone()
        };

        self.insert_row(conn, &processed_msg).await?;
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
            let processed: MessageBatch = if let Some(codec) = &self.codec {
                let encoded = codec.encode((**msg).clone()).await?;
                MessageBatch::new_binary(encoded)?
            } else {
                (**msg).clone()
            };
            let schema = processed.schema();
            let num_rows = processed.len();
            let num_columns = schema.fields().len();
            let columns: Vec<String> = (0..num_columns)
                .map(|i| schema.field(i).name().clone())
                .collect();
            validate_upsert_keys(&self.sql_config, &columns)?;
            let mut rows = Vec::with_capacity(num_columns * num_rows);
            for row_index in 0..num_rows {
                for col_index in 0..num_columns {
                    let column = processed.column(col_index);
                    let value = self.matching_data_type(column, row_index).await?;
                    rows.push(value);
                }
            }
            let rows: Vec<Vec<SqlValue>> = rows
                .chunks(num_columns)
                .map(|chunk| chunk.to_vec())
                .collect();
            batches.push((columns, rows));
        }
        execute_insert_transactional(conn, &self.sql_config, &batches).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
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
    /// 2. Converts each row to SQL-compatible values
    /// 3. Executes the insert query with proper batching
    async fn insert_row(
        &self,
        conn: &mut DatabaseConnection,
        msg: &MessageBatch,
    ) -> Result<(), Error> {
        let schema = msg.schema();
        let num_rows = msg.len();
        let num_columns = schema.fields().len();
        let columns: Vec<String> = (0..num_columns)
            .map(|i| schema.field(i).name().clone())
            .collect();

        validate_upsert_keys(&self.sql_config, &columns)?;

        let mut rows = Vec::with_capacity(num_columns * num_rows);
        for row_index in 0..num_rows {
            for col_index in 0..num_columns {
                let column = msg.column(col_index);

                let value = self.matching_data_type(column, row_index).await?;
                rows.push(value);
            }
        }
        let rows: Vec<Vec<SqlValue>> = rows
            .chunks(num_columns)
            .map(|chunk| chunk.to_vec())
            .collect();

        conn.execute_insert(&self.sql_config, columns, rows).await?;
        Ok(())
    }

    // Convert Arrow data types to SQL-compatible string representation
    async fn matching_data_type(
        &self,
        column: &dyn Array,
        row_index: usize,
    ) -> Result<SqlValue, Error> {
        // Determine the data type of the column and convert to appropriate SQL format
        let column_type = column.data_type();
        match column_type {
            DataType::Utf8 => {
                let utf8_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                if utf8_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::String(utf8_array.value(row_index).to_string()))
                }
            }
            DataType::Int64 => {
                let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                if int_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Int64(int_array.value(row_index)))
                }
            }
            DataType::UInt64 => {
                let uint_array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                if uint_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::UInt64(uint_array.value(row_index)))
                }
            }
            DataType::Float64 => {
                let float_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                if float_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Float64(float_array.value(row_index)))
                }
            }
            DataType::Boolean => {
                let bool_array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if bool_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(bool_array.value(row_index)))
                }
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type: {:?}",
                column_type
            ))),
        }
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
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: SqlOutputConfig = parse_config(config, "SqlOutput input")?;
        if config.upsert {
            if config.upsert_keys.as_ref().is_none_or(|keys| keys.is_empty()) {
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
        Ok(Arc::new(SqlOutput::new(config, codec)?))
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
        vec![vec![
            SqlValue::Int64(1),
            SqlValue::String("a".to_string()),
        ]]
    }

    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: RefCell::new(vec![]),
        }
    }

    #[test]
    fn postgres_plain_insert_has_no_conflict_clause() {
        let sql = build_postgres_insert(&postgres_config(false, None), &columns(), rows())
            .build()
            .sql()
            .to_string();
        assert!(sql.starts_with("INSERT INTO events (\"id\", \"name\")"),);
        assert!(!sql.contains("ON CONFLICT"));
    }

    #[test]
    fn postgres_upsert_uses_on_conflict_do_update() {
        let sql = build_postgres_insert(
            &postgres_config(true, Some(vec!["id"])),
            &columns(),
            rows(),
        )
        .build()
        .sql()
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
        let sql = build_mysql_insert(&config, &columns(), rows())
            .build()
            .sql()
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
            rows(),
        )
        .build()
        .sql()
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
}
