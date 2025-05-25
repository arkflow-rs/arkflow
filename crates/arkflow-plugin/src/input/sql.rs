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

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch, Resource};
use std::collections::HashMap;

use async_trait::async_trait;

use crate::udf;
use ballista::prelude::SessionContextExt;
use datafusion::datasource::object_store::ObjectStoreUrl;
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
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;
use url::Url;

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
    /// Avro input
    Avro(AvroConfig),
    /// Arrow input
    Arrow(ArrowConfig),
    /// JSON input
    Json(JsonConfig),
    /// CSV input
    Csv(CsvConfig),
    /// Parquet input
    Parquet(ParquetConfig),
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
struct AvroConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    /// avro file path
    path: String,
    /// object store config
    object_store: Option<ObjectStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    /// arrow file path
    path: String,
    /// object store config
    object_store: Option<ObjectStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    /// json file path
    path: String,
    /// object store config
    object_store: Option<ObjectStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CsvConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    /// csv file path
    path: String,
    /// object store config
    object_store: Option<ObjectStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParquetConfig {
    /// Table name (used in SQL queries)
    table_name: Option<String>,
    /// parquet file path
    path: String,
    /// object store config
    object_store: Option<ObjectStore>,
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
#[serde(tag = "type", rename_all = "snake_case")]
enum ObjectStore {
    S3(AwsS3Config),
    GoogleCloudStorage(GoogleCloudStorageConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsS3Config {
    /// S3 endpoint URL (optional, uses AWS default if not specified)
    endpoint: Option<String>,
    /// AWS region
    region: Option<String>,
    /// S3 bucket name
    bucket_name: String,
    /// AWS access key ID
    access_key_id: String,
    /// AWS secret access key
    secret_access_key: String,
    /// Allow HTTP connections (defaults to false for security)
    #[serde(default = "default_allow_http")]
    allow_http: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoogleCloudStorageConfig {
    /// GCS bucket to connect to
    bucket_name: String,
    /// Optional custom endpoint (defaults to GCS public endpoint if `None`)
    url: Option<String>,
    /// Path to a service account JSON key file
    service_account_path: Option<String>,
    /// Raw JSON key contents
    service_account_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqliteConfig {
    /// Table name (used in SQL queries)
    name: Option<String>,
    /// sqlite file path
    path: String,
}

pub struct SqlInput {
    sql_config: SqlInputConfig,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    cancellation_token: CancellationToken,
}

impl SqlInput {
    pub fn new(sql_config: SqlInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            sql_config,
            stream: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
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

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;
        if stream_lock.is_none() {
            return Err(Error::Process("Stream is None".to_string()));
        }

        let cancellation_token = self.cancellation_token.clone();

        let stream_lock = stream_lock.as_mut().unwrap();
        let mut stream_pin = stream_lock.as_mut();
        tokio::select! {
            _ =  cancellation_token.cancelled() => {
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
        let _ = self.cancellation_token.clone().cancel();
        Ok(())
    }
}

impl SqlInput {
    async fn init_connect(&self, ctx: &mut SessionContext) -> Result<(), Error> {
        match self.sql_config.input_type {
            InputType::Avro(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_NAME);
                if let Some(object_store) = &c.object_store {
                    self.object_store(ctx, object_store).await?;
                }
                ctx.register_avro(table_name, &c.path, AvroReadOptions::default())
                    .await
            }
            InputType::Arrow(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_NAME);
                if let Some(object_store) = &c.object_store {
                    self.object_store(ctx, object_store).await?;
                }
                ctx.register_arrow(table_name, &c.path, ArrowReadOptions::default())
                    .await
            }
            InputType::Json(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_NAME);
                if let Some(object_store) = &c.object_store {
                    self.object_store(ctx, object_store).await?;
                }
                ctx.register_json(table_name, &c.path, NdJsonReadOptions::default())
                    .await
            }
            InputType::Csv(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_NAME);
                if let Some(object_store) = &c.object_store {
                    self.object_store(ctx, object_store).await?;
                }
                ctx.register_csv(table_name, &c.path, CsvReadOptions::default())
                    .await
            }
            InputType::Parquet(ref c) => {
                let table_name = c.table_name.as_deref().unwrap_or(DEFAULT_NAME);
                if let Some(object_store) = &c.object_store {
                    self.object_store(ctx, object_store).await?;
                }
                ctx.register_parquet(table_name, &c.path, ParquetReadOptions::default())
                    .await
            }
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
                            return Error::Config(format!("Failed to create postgres pool: {}", e));
                        })?,
                );

                let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create postgres catalog: {}", e));
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
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create sqlite pool: {}", e));
                    })?,
                );

                let catalog_provider = DatabaseCatalogProvider::try_new(sqlite_pool)
                    .await
                    .map_err(|e| {
                        return Error::Config(format!("Failed to create sqlite catalog: {}", e));
                    })?;
                let name = c.name.as_deref().unwrap_or(DEFAULT_NAME);
                ctx.register_catalog(name, Arc::new(catalog_provider));
                Ok(())
            }
        }
        .map_err(|e| Error::Process(format!("Registration input failed: {}", e)))
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

    /// Create an object store
    async fn object_store(
        &self,
        ctx: &SessionContext,
        object_store: &ObjectStore,
    ) -> Result<(), Error> {
        match object_store {
            ObjectStore::S3(aws_s3_config) => self.aws_s3_object_store(ctx, aws_s3_config).await,
            ObjectStore::GoogleCloudStorage(config) => self.google_cloud_storage(ctx, config).await,
        }
    }

    /// Create an AWS S3 object store
    async fn aws_s3_object_store(
        &self,
        ctx: &SessionContext,
        aws_s3_config: &AwsS3Config,
    ) -> Result<(), Error> {
        let mut s3_builder = AmazonS3Builder::new()
            .with_bucket_name(&aws_s3_config.bucket_name)
            .with_access_key_id(&aws_s3_config.access_key_id)
            .with_secret_access_key(&aws_s3_config.secret_access_key)
            .with_allow_http(aws_s3_config.allow_http);

        if let Some(endpoint) = aws_s3_config.endpoint.as_ref() {
            s3_builder = s3_builder.with_endpoint(endpoint);
        }
        if let Some(region) = aws_s3_config.region.as_ref() {
            s3_builder = s3_builder.with_region(region);
        }

        let s3 = s3_builder
            .build()
            .map_err(|e| Error::Config(format!("Failed to create S3 client: {}", e)))?;

        let s3_object_store_url =
            ObjectStoreUrl::parse(format!("s3://{}", &aws_s3_config.bucket_name))
                .map_err(|e| Error::Config(format!("Failed to parse S3 URL: {}", e)))?;
        let url: &Url = s3_object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(s3));
        Ok(())
    }
    async fn google_cloud_storage(
        &self,
        ctx: &SessionContext,
        config: &GoogleCloudStorageConfig,
    ) -> Result<(), Error> {
        let mut google_cloud_storage_builder = GoogleCloudStorageBuilder::new();
        google_cloud_storage_builder =
            google_cloud_storage_builder.with_bucket_name(&config.bucket_name);
        if let Some(url) = &config.url {
            google_cloud_storage_builder = google_cloud_storage_builder.with_url(url);
        }

        match (&config.service_account_path, &config.service_account_key) {
            (Some(path), None) => {
                google_cloud_storage_builder =
                    google_cloud_storage_builder.with_service_account_path(path)
            }
            (None, Some(key)) => {
                google_cloud_storage_builder =
                    google_cloud_storage_builder.with_service_account_key(key)
            }
            (None, None) => return Err(Error::Config("GCS auth is missing".into())),
            (Some(_), Some(_)) => {
                return Err(Error::Config(
                    "Specify either service_account_path or service_account_key, not both".into(),
                ))
            }
        };

        let google_cloud_storage = google_cloud_storage_builder
            .build()
            .map_err(|e| Error::Config(format!("Failed to create GCS client: {}", e)))?;

        let s3_object_store_url = ObjectStoreUrl::parse(format!("gs://{}", &config.bucket_name))
            .map_err(|e| Error::Config(format!("Failed to parse GCS URL: {}", e)))?;
        let url: &Url = s3_object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(google_cloud_storage));
        Ok(())
    }
}

pub(crate) struct SqlInputBuilder;
impl InputBuilder for SqlInputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "SQL input configuration is missing".to_string(),
            ));
        }

        let config: SqlInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SqlInput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("sql", Arc::new(SqlInputBuilder))
}

fn default_allow_http() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use tempfile::{tempdir, TempDir};
    fn create_test_data() -> (TempDir, String) {
        let temp_dir = tempdir().unwrap();
        let path_buf = temp_dir.path().join("test_path.json");
        let path = path_buf.as_path().to_str().unwrap().to_string();

        let _ = std::fs::write(path_buf, r#"{"name": "John", "age": 30}"#).unwrap();
        (temp_dir, path)
    }

    #[tokio::test]
    async fn test_sql_input_connect() {
        let (_x, path) = create_test_data();
        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test_table".to_string(),
            ballista: None,
            input_type: InputType::Json(JsonConfig {
                table_name: Some("test_table".to_string()),
                path,
                object_store: None,
            }),
        };
        let input = SqlInput::new(config).unwrap();
        assert!(input.connect().await.is_ok());
    }

    #[tokio::test]
    async fn test_sql_input_read() {
        let (_x, path) = create_test_data();
        let config = SqlInputConfig {
            select_sql: "SELECT * FROM test_table".to_string(),
            ballista: None,
            input_type: InputType::Json(JsonConfig {
                table_name: Some("test_table".to_string()),
                path,
                object_store: None,
            }),
        };
        let input = SqlInput::new(config).unwrap();
        input.connect().await.unwrap();

        let (msg, ack) = input.read().await.unwrap();
        let (i, _) = msg.schema().column_with_name("name").unwrap();
        let result = msg
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(result, "John");
        ack.ack().await;
        input.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_input_invalid_query() {
        let (_x, path) = create_test_data();
        let config = SqlInputConfig {
            select_sql: "SELECT invalid_column FROM test_table".to_string(),
            ballista: None,

            input_type: InputType::Json(JsonConfig {
                table_name: Some("test_table".to_string()),
                path,
                object_store: None,
            }),
        };
        let input = SqlInput::new(config).unwrap();
        assert!(input.connect().await.is_err());
    }
}
