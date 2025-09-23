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

use crate::udf;
use arkflow_core::input::{Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{input, Error, MessageBatch, Resource};
use async_trait::async_trait;
use ballista::prelude::SessionContextExt;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SQLOptions,
    SessionContext,
};
use futures_util::TryStreamExt;
use hdfs_native_object_store::HdfsObjectStoreBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileInputConfig {
    input_type: InputType,
    ballista: Option<BallistaConfig>,
    query: Option<QueryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BallistaConfig {
    /// Ballista server url
    remote_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryConfig {
    query: String,
    #[serde(default = "default_table")]
    table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InputType {
    /// Avro input
    Avro(FileFormatConfig),
    /// Arrow input
    Arrow(FileFormatConfig),
    /// JSON input
    Json(FileFormatConfig),
    /// CSV input
    Csv(FileFormatConfig),
    /// Parquet input
    Parquet(FileFormatConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileFormatConfig {
    /// file path
    path: String,
    /// object store config
    store: Option<Store>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Store {
    S3(AwsS3Config),
    Gs(GoogleCloudStorageConfig),
    Az(MicrosoftAzureConfig),
    Http(HttpConfig),
    Hdfs(HdfsConfig),
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
    #[serde(default = "default_disallow_http")]
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
struct MicrosoftAzureConfig {
    /// Azure blob endpoint URL (optional, uses Azure default if not specified)
    url: Option<String>,
    endpoint: Option<String>,
    /// Azure storage account name
    account: String,
    /// Azure shared access key
    access_key: Option<String>,
    /// Azure container name
    container_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HttpConfig {
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HdfsConfig {
    url: String,
    ha_config: Option<HashMap<String, String>>,
}

struct FileInput {
    input_name: Option<String>,
    config: FileInputConfig,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    cancellation_token: CancellationToken,
}

impl FileInput {
    fn new(name: Option<&String>, config: FileInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.cloned(),
            config,
            stream: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }

    async fn read_df(&self, ctx: &mut SessionContext) -> Result<DataFrame, Error> {
        // Register object store if configured
        let store = match &self.config.input_type {
            InputType::Avro(c)
            | InputType::Arrow(c)
            | InputType::Json(c)
            | InputType::Csv(c)
            | InputType::Parquet(c) => &c.store,
        };

        if let Some(object_store) = store {
            self.object_store(ctx, object_store)?;
        }

        let sql_options = SQLOptions::default()
            .with_allow_dml(false)
            .with_allow_ddl(false);

        match self.config.input_type {
            InputType::Avro(ref c) => {
                let options = AvroReadOptions::default();
                if let Some(ref query) = self.config.query {
                    ctx.register_avro(&query.table, &c.path, options)
                        .await
                        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))?;
                    ctx.sql_with_options(&query.query, sql_options).await
                } else {
                    ctx.read_avro(&c.path, options).await
                }
            }
            InputType::Arrow(ref c) => {
                let options = ArrowReadOptions::default();

                if let Some(ref query) = self.config.query {
                    ctx.register_arrow(&query.table, &c.path, options)
                        .await
                        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))?;
                    ctx.sql_with_options(&query.query, sql_options).await
                } else {
                    ctx.read_arrow(&c.path, options).await
                }
            }
            InputType::Json(ref c) => {
                let options = NdJsonReadOptions::default();
                if let Some(ref query) = self.config.query {
                    ctx.register_json(&query.table, &c.path, options)
                        .await
                        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))?;
                    ctx.sql_with_options(&query.query, sql_options).await
                } else {
                    ctx.read_json(&c.path, options).await
                }
            }
            InputType::Csv(ref c) => {
                let options = CsvReadOptions::default();
                if let Some(ref query) = self.config.query {
                    ctx.register_csv(&query.table, &c.path, options)
                        .await
                        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))?;
                    ctx.sql_with_options(&query.query, sql_options).await
                } else {
                    ctx.read_csv(&c.path, options).await
                }
            }
            InputType::Parquet(ref c) => {
                let options = ParquetReadOptions::default();
                if let Some(ref query) = self.config.query {
                    ctx.register_parquet(&query.table, &c.path, options)
                        .await
                        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))?;
                    ctx.sql_with_options(&query.query, sql_options).await
                } else {
                    ctx.read_parquet(&c.path, options).await
                }
            }
        }
        .map_err(|e| Error::Process(format!("Read input failed: {}", e)))
    }

    /// Create an object store
    fn object_store(&self, ctx: &SessionContext, object_store: &Store) -> Result<(), Error> {
        match object_store {
            Store::S3(config) => self.aws_s3_object_store(ctx, config),
            Store::Gs(config) => self.google_cloud_storage(ctx, config),
            Store::Az(config) => self.microsoft_azure_store(ctx, config),
            Store::Http(config) => self.http_store(ctx, config),
            Store::Hdfs(config) => self.hdfs_store(ctx, config),
        }
    }

    /// Create an AWS S3 object store
    fn aws_s3_object_store(
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

        let object_store_url =
            ObjectStoreUrl::parse(format!("s3://{}", &aws_s3_config.bucket_name))
                .map_err(|e| Error::Config(format!("Failed to parse S3 URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(s3));
        Ok(())
    }

    fn google_cloud_storage(
        &self,
        ctx: &SessionContext,
        config: &GoogleCloudStorageConfig,
    ) -> Result<(), Error> {
        let mut google_cloud_storage_builder =
            GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket_name);
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

        let object_store_url = ObjectStoreUrl::parse(format!("gs://{}", &config.bucket_name))
            .map_err(|e| Error::Config(format!("Failed to parse GCS URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(google_cloud_storage));
        Ok(())
    }

    fn microsoft_azure_store(
        &self,
        ctx: &SessionContext,
        config: &MicrosoftAzureConfig,
    ) -> Result<(), Error> {
        let mut azure_builder = MicrosoftAzureBuilder::new()
            .with_account(&config.account)
            .with_container_name(&config.container_name);

        if let Some(access_key) = &config.access_key {
            azure_builder = azure_builder.with_access_key(access_key);
        }
        if let Some(url) = &config.url {
            azure_builder = azure_builder.with_url(url);
        }
        if let Some(endpoint) = &config.endpoint {
            azure_builder = azure_builder
                .with_endpoint(endpoint.clone())
                .with_allow_http(true);
        }

        let azure_storage = azure_builder
            .build()
            .map_err(|e| Error::Config(format!("Failed to create AZ client: {}", e)))?;

        let object_store_url = ObjectStoreUrl::parse(format!("az://{}", &config.container_name))
            .map_err(|e| Error::Config(format!("Failed to parse AZ URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(azure_storage));
        Ok(())
    }

    fn http_store(&self, ctx: &SessionContext, config: &HttpConfig) -> Result<(), Error> {
        let http_builder = HttpBuilder::new().with_url(&config.url);
        let http_storage = http_builder
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HTTP client: {}", e)))?;
        let object_store_url = ObjectStoreUrl::parse(&config.url)
            .map_err(|e| Error::Config(format!("Failed to parse HTTP URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(http_storage));
        Ok(())
    }

    fn hdfs_store(&self, ctx: &SessionContext, config: &HdfsConfig) -> Result<(), Error> {
        let mut builder = HdfsObjectStoreBuilder::new().with_url(&config.url);
        if let Some(ha_config) = config.ha_config.as_ref().filter(|m| !m.is_empty()) {
            builder = builder.with_config(ha_config.clone());
        }
        let hdfs_storage = builder
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HDFS client: {}", e)))?;
        let object_store_url = ObjectStoreUrl::parse(&config.url)
            .map_err(|e| Error::Config(format!("Failed to parse HDFS URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(hdfs_storage));
        Ok(())
    }

    async fn create_session_context(&self) -> Result<SessionContext, Error> {
        let mut ctx = if let Some(ballista) = &self.config.ballista {
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

#[async_trait]
impl Input for FileInput {
    async fn connect(&self) -> Result<(), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;

        let mut ctx = self.create_session_context().await?;
        let df = self.read_df(&mut ctx).await?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Error::Process(format!("Failed to execute file stream: {}", e)))?;
        stream_lock.replace(stream);
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
                let mut msg = MessageBatch::new_arrow(x);
                msg.set_input_name(self.input_name.clone());

                Ok((msg, Arc::new(NoopAck)))
            }

        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.clone().cancel();
        Ok(())
    }
}

struct FileBuilder;

impl InputBuilder for FileBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "File input configuration is missing".to_string(),
            ));
        }

        let config: FileInputConfig = serde_json::from_value(config.clone().unwrap())
            .map_err(|e| Error::Config(format!("Failed to parse File input config: {}", e)))?;
        Ok(Arc::new(FileInput::new(name, config)?))
    }
}

pub fn init() -> Result<(), Error> {
    input::register_input_builder("file", Arc::new(FileBuilder))?;
    Ok(())
}

fn default_disallow_http() -> bool {
    false
}

fn default_table() -> String {
    "flow".to_string()
}
