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

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use std::collections::HashMap;

use async_trait::async_trait;

use crate::udf;
use ballista::prelude::SessionContextExt;
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
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const DEFAULT_NAME: &str = "flow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlInputConfig {
    /// SQL query statement
    select_sql: String,
    /// Ballista helps us perform distributed computing
    ballista: Option<BallistaConfig>,
    input_type: InputType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaConfig {
    /// Ballista server url
    pub remote_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InputType {
    /// Mysql input
    Mysql(MysqlConfig),
    /// Duckdb input
    Duckdb(DuckDBConfig),
    /// Postgres input
    Postgres(PostgresConfig),
    /// Sqlite input
    Sqlite(SqliteConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlConfig {
    /// Table name (used in SQL queries)
    name: Option<String>,
    /// mysql uri
    uri: String,
    /// mysql ssl config
    ssl: MysqlSslConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSslConfig {
    /// mysql ssl mode
    ssl_mode: String,
    /// mysql ssl root cert
    root_cert: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DuckDBConfig {
    /// Table name (used in SQL queries)
    name: Option<String>,
    /// duckdb file path
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresConfig {
    /// Table name (used in SQL queries)
    name: Option<String>,
    /// postgres uri
    uri: String,
    /// postgres ssl config
    ssl: PostgresSslConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresSslConfig {
    /// postgres ssl mode
    ssl_mode: String,
    /// postgres ssl root cert
    root_cert: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqliteConfig {
    /// Table name (used in SQL queries)
    name: Option<String>,
    /// sqlite file path
    path: String,
}

pub struct SqlInput {
    input_name: Option<String>,
    sql_config: SqlInputConfig,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    /// Replaceable so a close→connect cycle starts with a live token; the old
    /// token stayed cancelled and the next read returned EOF immediately.
    cancellation_token: Arc<Mutex<CancellationToken>>,
}

impl SqlInput {
    pub fn new(name: Option<&str>, sql_config: SqlInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.map(str::to_string),
            sql_config,
            stream: Arc::new(Mutex::new(None)),
            cancellation_token: Arc::new(Mutex::new(cancellation_token)),
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
        // A reconnect replaces both the stream and the cancellation token.
        *self.cancellation_token.lock().await = CancellationToken::new();
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;

        let mut ctx = self.create_session_context().await?;

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

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;
        if stream_lock.is_none() {
            if self.cancellation_token.lock().await.is_cancelled() {
                return Err(Error::EOF);
            }
            return Err(Error::Process("Stream is None".to_string()));
        }

        let cancellation_token = self.cancellation_token.lock().await.clone();

        let stream_lock = stream_lock.as_mut().unwrap();
        let mut stream_pin = stream_lock.as_mut();
        tokio::select! {
            _ =  cancellation_token.cancelled() => {
                Err(Error::EOF)
            }
            result = stream_pin.try_next() => {
                 // A mid-stream failure is not a clean end-of-stream: `EOF`
                 // here would acknowledge a truncated result set as
                 // complete. Transient retrieval failures classify as
                 // Disconnection (reconnect); deterministic failures fail
                 // the run loudly instead of looping.
                 let value =
                    result.map_err(crate::input::codec_helper::classify_stream_error)?;
                let Some(x) = value else {
                    return Err(Error::EOF);
                };
                let mut msg = MessageBatch::new_arrow(x);
                msg.set_input_name(self.input_name.clone());

                Ok((Arc::new(msg), Arc::new(NoopAck)))
            }

        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.lock().await.cancel();
        // Release the installed DataFusion stream instead of leaving the plan
        // and its resources alive with the input instance.
        *self.stream.lock().await = None;
        Ok(())
    }
}

impl SqlInput {
    async fn init_connect(&self, ctx: &mut SessionContext) -> Result<(), Error> {
        match self.sql_config.input_type {
            InputType::Mysql(ref c) => {
                let name = c.name.as_deref().unwrap_or(DEFAULT_NAME);
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
            InputType::Duckdb(ref c) => {
                let duckdb_pool = Arc::new(
                    DuckDbConnectionPool::new_file(&c.path, &AccessMode::ReadOnly).map_err(
                        |e| Error::Config(format!("Failed to create duckdb pool: {}", e)),
                    )?,
                );

                let catalog = DatabaseCatalogProvider::try_new(duckdb_pool)
                    .await
                    .map_err(|e| {
                        Error::Config(format!("Failed to create duckdb catalog: {}", e))
                    })?;
                let name = c.name.as_deref().unwrap_or(DEFAULT_NAME);
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
            }
            InputType::Postgres(ref c) => {
                let mut params = HashMap::from([
                    ("connection_string".to_string(), c.uri.to_string()),
                    ("sslmode".to_string(), c.ssl.ssl_mode.to_string()),
                ]);
                if let Some(ref v) = c.ssl.root_cert {
                    params.insert("sslrootcert".to_string(), v.to_string());
                }
                let postgres_params = to_secret_map(params);
                let postgres_pool = Arc::new(
                    PostgresConnectionPool::new(postgres_params)
                        .await
                        .map_err(|e| {
                            Error::Config(format!("Failed to create postgres pool: {}", e))
                        })?,
                );

                let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
                    .await
                    .map_err(|e| {
                        Error::Config(format!("Failed to create postgres catalog: {}", e))
                    })?;
                let name = c.name.as_deref().unwrap_or(DEFAULT_NAME);
                ctx.register_catalog(name, Arc::new(catalog));
                Ok(())
            }
            InputType::Sqlite(ref c) => {
                let sqlite_pool = Arc::new(
                    SqliteConnectionPoolFactory::new(
                        &c.path,
                        Mode::File,
                        Duration::from_millis(5000),
                    )
                    .build()
                    .await
                    .map_err(|e| Error::Config(format!("Failed to create sqlite pool: {}", e)))?,
                );

                let catalog_provider = DatabaseCatalogProvider::try_new(sqlite_pool)
                    .await
                    .map_err(|e| {
                        Error::Config(format!("Failed to create sqlite catalog: {}", e))
                    })?;
                let name = c.name.as_deref().unwrap_or(DEFAULT_NAME);
                ctx.register_catalog(name, Arc::new(catalog_provider));
                Ok(())
            }
        }
    }

    /// Create a session context
    async fn create_session_context(&self) -> Result<SessionContext, Error> {
        let mut ctx = if let Some(ballista) = &self.sql_config.ballista {
            SessionContext::remote(&ballista.remote_url)
                .await
                .map_err(|e| Error::Process(format!("Create session context failed: {}", e)))?
        } else {
            SessionContext::new()
        };

        udf::init(&mut ctx)?;
        datafusion_functions_json::register_all(&mut ctx)
            .map_err(|e| Error::Process(format!("Registration JSON function failed: {}", e)))?;
        Ok(ctx)
    }
}

pub(crate) struct SqlInputBuilder;
impl InputBuilder for SqlInputBuilder {
    fn build(
        &self,
        name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        // Rows arrive typed from the SELECT result; a codec has no decode
        // integration point here. Reject instead of dropping it silently.
        if codec.is_some() {
            return Err(Error::Config(
                "sql input does not support a codec: rows come typed from the SELECT result"
                    .to_string(),
            ));
        }
        let config: SqlInputConfig = parse_config(config, "Sql input")?;
        Ok(Arc::new(SqlInput::new(name, config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("sql", Arc::new(SqlInputBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "sql",
        "Runs a one-shot SELECT against a SQL database (MySQL / PostgreSQL / SQLite / DuckDB) and emits the rows as batches.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "select_sql": {"type": "string", "description": "SELECT statement executed once at startup."},
                "ballista": {"type": "object", "description": "Optional Ballista distributed compute configuration."},
                "input_type": {"type": "object", "description": "Database connection settings."}
            },
            "required": ["select_sql", "input_type"]
        }),
    ).with_example(serde_json::json!({
        "select_sql": "SELECT id, name FROM users",
        "input_type": {"type": "sqlite", "name": "users", "path": "./data.db"}
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    fn build_sqlite(path: &str, select_sql: &str) -> Result<Arc<dyn Input>, Error> {
        let config = Some(serde_json::json!({
            "select_sql": select_sql,
            "input_type": {"type": "sqlite", "path": path}
        }));
        SqlInputBuilder.build(None, &config, None, &resource())
    }

    fn seeded_sqlite_file() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow.db");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sql = format!(
            "INSTALL sqlite; LOAD sqlite; ATTACH '{}' AS flow_db (TYPE sqlite);
             CREATE TABLE flow_db.events (id INTEGER PRIMARY KEY, kind TEXT);
             INSERT INTO flow_db.events (id, kind) VALUES (1, 'a'), (2, 'b'), (3, 'c');
             DETACH flow_db;",
            path.to_str().unwrap()
        );
        conn.execute_batch(&sql).unwrap();
        drop(conn);
        (dir, path.to_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn sqlite_input_reads_all_rows_until_eof() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let input = build_sqlite(&path, "SELECT id, kind FROM flow.main.events")?;
        input.connect().await?;
        let mut rows = 0usize;
        loop {
            match input.read().await {
                Ok((batch, _)) => rows += batch.num_rows(),
                Err(Error::EOF) => break,
                Err(e) => return Err(e),
            }
        }
        assert_eq!(rows, 3);
        input.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_before_connect_reports_missing_stream() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let input = build_sqlite(&path, "SELECT 1")?;
        match input.read().await {
            Err(err) => assert!(err.to_string().contains("Stream is None"), "{err}"),
            Ok(_) => panic!("read before connect must fail"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_database_file_fails_at_connect() {
        let result = build_sqlite("/nonexistent/arkflow/missing.db", "SELECT 1");
        if let Ok(input) = result {
            match input.connect().await {
                Err(err) => assert!(err.to_string().to_lowercase().contains("sqlite"), "{err}"),
                Ok(()) => panic!("a missing sqlite file must fail connect"),
            }
        }
    }

    #[test]
    fn builder_rejects_malformed_config() {
        let Err(err) = SqlInputBuilder.build(
            None,
            &Some(serde_json::json!({"unexpected": 1})),
            None,
            &resource(),
        ) else {
            panic!("malformed config must be rejected");
        };
        assert!(err.to_string().contains("Sql input"), "{err}");
    }

    /// A RecordBatchStream double that yields one good batch and then an
    /// injected error — the shape a failing DB connection or query
    /// produces after some rows have already flowed.
    struct MidStreamFailure {
        schema: datafusion::arrow::datatypes::SchemaRef,
        emitted: std::sync::atomic::AtomicBool,
        error: Option<datafusion::common::DataFusionError>,
    }

    impl datafusion::physical_plan::RecordBatchStream for MidStreamFailure {
        fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
            self.schema.clone()
        }
    }

    impl futures_util::Stream for MidStreamFailure {
        type Item = Result<
            datafusion::arrow::record_batch::RecordBatch,
            datafusion::common::DataFusionError,
        >;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if !this.emitted.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let schema = this.schema.clone();
                let batch = datafusion::arrow::record_batch::RecordBatch::new_empty(schema);
                return std::task::Poll::Ready(Some(Ok(batch)));
            }
            let error = this
                .error
                .take()
                .expect("failure polled twice; take a fresh double per case");
            std::task::Poll::Ready(Some(Err(error)))
        }
    }

    fn transient_object_store_error() -> datafusion::common::DataFusionError {
        datafusion::common::DataFusionError::ObjectStore(Box::new(object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("injected transient failure")),
        }))
    }

    fn postgres_transport_error() -> datafusion::common::DataFusionError {
        // The postgres table provider wraps post-first-chunk receive
        // failures in exactly this Execution shape — the inner Display is
        // `PostgresError::QueryError`'s (provider 0.13.1 conn.rs:261-267).
        // A dropped connection carries NO SQLSTATE (the server never saw
        // the query) and must reconnect, not wedge.
        datafusion::common::DataFusionError::Execution(
            "Failed to fetch batch: Query execution failed.\nconnection refused".into(),
        )
    }

    fn postgres_shutdown_error() -> datafusion::common::DataFusionError {
        // 57P01 admin_shutdown: the server told the client to go away —
        // carries a SQLSTATE but is transient (reconnect), unlike 22012.
        datafusion::common::DataFusionError::Execution(
            "Failed to fetch batch: Query execution failed.\nterminating connection due to \
             administrator command\nSQLSTATE: 57P01"
                .into(),
        )
    }

    fn postgres_db_error() -> datafusion::common::DataFusionError {
        // Same "Query execution failed." prefix, but the provider appended
        // a SQLSTATE (division by zero = 22012): a server-generated
        // database error is deterministic given the data — reconnecting
        // replays the same prefix into the same division by zero.
        datafusion::common::DataFusionError::Execution(
            "Failed to fetch batch: Query execution failed.\ndivision by zero\nSQLSTATE: 22012"
                .into(),
        )
    }

    fn postgres_conversion_error() -> datafusion::common::DataFusionError {
        // Same outer prefix, but the inner Display is `ConversionError`'s:
        // a deterministic conversion failure after the first chunk must
        // terminate instead of triggering reconnect + query replay.
        datafusion::common::DataFusionError::Execution(
            "Failed to fetch batch: Failed to convert query result to Arrow.\ninjected".into(),
        )
    }

    fn deterministic_decode_error() -> datafusion::common::DataFusionError {
        datafusion::common::DataFusionError::ArrowError(
            Box::new(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(std::io::Error::other("injected decode failure")),
            )),
            None,
        )
    }

    async fn install_failing_stream(
        input: &SqlInput,
        error: datafusion::common::DataFusionError,
    ) -> Result<(), Error> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        // Overwrite the installed stream with the failing double: read()
        // consumes whatever stream is installed, so this drives the exact
        // mapping under test without depending on a DB that can error
        // mid-scan on demand.
        {
            let mut stream_lock = input.stream.lock().await;
            *stream_lock = Some(Box::pin(MidStreamFailure {
                schema,
                emitted: std::sync::atomic::AtomicBool::new(false),
                error: Some(error),
            }));
        }
        Ok(())
    }

    /// Spec: connector-recovery-contract — 无界源的异常终止 SHALL 归类为
    /// Disconnection 而非 EOF。A transient retrieval failure (object store,
    /// IO, connector transport) after rows have flowed must surface
    /// `Error::Disconnection` (reconnect) — mapping it to `EOF` would
    /// broadcast EOS downstream and acknowledge a truncated result set as
    /// complete.
    #[tokio::test]
    async fn transient_mid_stream_failure_is_disconnection_not_eof() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, transient_object_store_error()).await?;

        // First read: the good batch flows.
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 0, "the double's first batch is empty");

        // Second read: the injected transient failure — Disconnection.
        match input.read().await {
            Err(Error::Disconnection) => {}
            Err(other) => panic!("transient failure must be Disconnection, got: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Spec: connector-recovery-contract — postgres provider 的传输失败
    /// 以 `Execution("Failed to fetch batch: ...")` 形态抵达,必须归类为
    /// Disconnection(重连)而非终端失败。
    #[tokio::test]
    async fn postgres_transport_failure_is_disconnection() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, postgres_transport_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Disconnection) => {}
            Err(other) => {
                panic!("a postgres receive failure must be Disconnection, got: {other:?}")
            }
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Regression (review round 4): `QueryError` also carries
    /// server-generated database errors (SQLSTATE present) — division by
    /// zero after the first chunk must terminate, not reconnect + replay.
    #[tokio::test]
    async fn postgres_db_error_is_terminal() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, postgres_db_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Process(message)) => {
                assert!(
                    message.contains("deterministically"),
                    "a SQLSTATE-carrying database error must terminate: {message}"
                );
            }
            Err(Error::Disconnection) => {
                panic!("a database execution error must not trigger reconnect + replay")
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Regression (review round 5): shutdown-class SQLSTATEs (57P01/
    /// 57P02/57P03) and the 08xxx connection-exception class are transient
    /// — a mid-stream 57P01 must reconnect, not terminate.
    #[tokio::test]
    async fn postgres_shutdown_sqlstate_is_disconnection() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, postgres_shutdown_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Disconnection) => {}
            Err(other) => {
                panic!("a shutdown SQLSTATE (57P01) must be Disconnection, got: {other:?}")
            }
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Regression (review round 3): the SAME outer prefix also wraps the
    /// provider's deterministic `ConversionError` — it must terminate, not
    /// reconnect.
    #[tokio::test]
    async fn postgres_conversion_failure_is_terminal() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, postgres_conversion_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Process(message)) => {
                assert!(
                    message.contains("deterministically"),
                    "a provider-wrapped conversion failure must terminate: {message}"
                );
            }
            Err(Error::Disconnection) => {
                panic!("a conversion failure must not trigger reconnect + query replay")
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Spec: connector-recovery-contract — 确定性失败 SHALL 走致命路径。
    /// A decode/format failure after rows have flowed must NOT be
    /// classified as Disconnection: reconnecting would replay the same
    /// prefix into the same corruption forever. It fails the run loudly
    /// (`Error::Process`) so an operator notices.
    #[tokio::test]
    async fn deterministic_mid_stream_failure_is_terminal_not_reconnect() -> Result<(), Error> {
        let (_dir, path) = seeded_sqlite_file();
        let sql_config = SqlInputConfig {
            select_sql: "SELECT id, kind FROM flow.main.events".to_string(),
            ballista: None,
            input_type: InputType::Sqlite(SqliteConfig {
                name: None,
                path: path.clone(),
            }),
        };
        let input = SqlInput::new(None, sql_config)?;
        input.connect().await?;
        install_failing_stream(&input, deterministic_decode_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Process(message)) => {
                assert!(
                    message.contains("deterministically"),
                    "the terminal error must explain why it is not a reconnect: {message}"
                );
            }
            Err(Error::Disconnection) => {
                panic!("a decode failure must not trigger endless reconnects")
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }
}
