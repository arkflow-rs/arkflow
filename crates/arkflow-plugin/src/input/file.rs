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
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    input::{Ack, Input, InputBuilder, NoopAck},
    Error, MessageBatch, MessageBatchRef, Resource,
};
use async_trait::async_trait;
#[cfg(feature = "connectors-sql")]
use ballista::prelude::SessionContextExt;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::options::{ArrowReadOptions, JsonReadOptions};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, DataFrame, ParquetReadOptions, SQLOptions, SessionContext,
};
use futures_util::TryStreamExt;
#[cfg(feature = "connectors-hdfs")]
use hdfs_native_object_store::HdfsObjectStoreBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "connectors-hdfs")]
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
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
    #[cfg(feature = "connectors-hdfs")]
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
#[cfg(feature = "connectors-hdfs")]
struct HdfsConfig {
    url: String,
    ha_config: Option<HashMap<String, String>>,
}

struct FileInput {
    input_name: Option<String>,
    config: FileInputConfig,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    cancellation_token: Arc<Mutex<CancellationToken>>,
}

impl FileInput {
    fn new(name: Option<&str>, config: FileInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.map(str::to_string),
            config,
            stream: Arc::new(Mutex::new(None)),
            cancellation_token: Arc::new(Mutex::new(cancellation_token)),
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
                let options = JsonReadOptions::default();
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
            #[cfg(feature = "connectors-hdfs")]
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

        let object_store_url = ObjectStoreUrl::parse(format!("s3://{}", aws_s3_config.bucket_name))
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
            // The bucket identity is `bucket_name` (DataFusion registers
            // `gs://{bucket_name}`); the URL only names the ENDPOINT.
            // `with_url` parses `gs://host/...` and OVERRIDES the bucket with
            // its host, so it is only safe when the host matches; plain
            // HTTPS endpoints (a local emulator) map onto `base_url` instead
            // of failing builder construction.
            if url.starts_with("gs://") {
                let host = url
                    .strip_prefix("gs://")
                    .and_then(|rest| rest.split('/').next())
                    .unwrap_or_default();
                if host != config.bucket_name {
                    return Err(Error::Config(format!(
                        "GCS url '{}' names bucket '{}' but bucket_name is '{}'; the endpoint URL must not rename the bucket",
                        url, host, config.bucket_name
                    )));
                }
                google_cloud_storage_builder = google_cloud_storage_builder.with_url(url);
            } else if url.starts_with("http://") || url.starts_with("https://") {
                google_cloud_storage_builder = google_cloud_storage_builder
                    .with_config(object_store::gcp::GoogleConfigKey::BaseUrl, url);
            } else {
                return Err(Error::Config(format!(
                    "GCS url '{}' must be gs://bucket or an http(s) endpoint",
                    url
                )));
            }
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

        let object_store_url = ObjectStoreUrl::parse(format!("gs://{}", config.bucket_name))
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

        let object_store_url = ObjectStoreUrl::parse(format!("az://{}", config.container_name))
            .map_err(|e| Error::Config(format!("Failed to parse AZ URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(azure_storage));
        Ok(())
    }

    fn http_store(&self, ctx: &SessionContext, config: &HttpConfig) -> Result<(), Error> {
        // `url` is the STORE ORIGIN (scheme + authority). A path prefix
        // belongs to the data path (the file URL), where DataFusion turns it
        // into the object key relative to this origin. A prefixed `url` was
        // previously forwarded verbatim to DataFusion's registration, which
        // rejects any path — failing initialization with a generic error;
        // it is now rejected with the contract spelled out.
        let mut origin = Url::parse(&config.url)
            .map_err(|e| Error::Config(format!("Failed to parse HTTP URL: {}", e)))?;
        let had_path =
            origin.path() != "/" || origin.query().is_some() || origin.fragment().is_some();
        origin.set_path("/");
        origin.set_query(None);
        origin.set_fragment(None);
        if had_path {
            return Err(Error::Config(format!(
                "http object store url '{}' must be an origin (scheme://host[:port]); put the path prefix in the data path instead (e.g. path: {}/prefix/file.parquet)",
                config.url, origin
            )));
        }
        let http_storage = HttpBuilder::new()
            .with_url(origin.as_str())
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HTTP client: {}", e)))?;
        let object_store_url = ObjectStoreUrl::parse(origin.as_str())
            .map_err(|e| Error::Config(format!("Failed to parse HTTP URL: {}", e)))?;
        let url: &Url = object_store_url.as_ref();
        ctx.register_object_store(url, Arc::new(http_storage));
        Ok(())
    }

    #[cfg(feature = "connectors-hdfs")]
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
        let mut ctx = self.new_session_context().await?;

        udf::init(&mut ctx)?;
        datafusion_functions_json::register_all(&mut ctx)
            .map_err(|e| Error::Process(format!("Registration JSON function failed: {}", e)))?;
        Ok(ctx)
    }

    /// Build the base session context: a remote Ballista context when
    /// `ballista` is configured (requires the `connectors-sql` feature),
    /// a local context otherwise. UDF/JSON function registration happens
    /// on either kind in [`Self::create_session_context`].
    async fn new_session_context(&self) -> Result<SessionContext, Error> {
        #[cfg(feature = "connectors-sql")]
        if let Some(ballista) = &self.config.ballista {
            return SessionContext::remote(&ballista.remote_url)
                .await
                .map_err(|e| Error::Process(format!("Create session context failed: {}", e)));
        }
        #[cfg(not(feature = "connectors-sql"))]
        if self.config.ballista.is_some() {
            return Err(Error::Config(
                "file input 'ballista' requires the 'connectors-sql' feature, which is not enabled in this build"
                    .to_string(),
            ));
        }
        Ok(SessionContext::new())
    }
}

#[async_trait]
impl Input for FileInput {
    async fn connect(&self) -> Result<(), Error> {
        *self.cancellation_token.lock().await = CancellationToken::new();
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

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let stream_arc = self.stream.clone();
        let mut stream_lock = stream_arc.lock().await;
        if stream_lock.is_none() {
            // A closed input reports EOF; an unconnected one is a usage error.
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
                 // here would acknowledge a truncated dataset as complete.
                 // Transient retrieval failures classify as Disconnection
                 // (reconnect); deterministic failures (corrupt file,
                 // schema errors) fail the run loudly instead of looping.
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
        // Drop the installed plan/stream; otherwise the query
        // keeps its resources alive while the input instance is retained,
        // and a concurrent connect could install a new stream after close.
        *self.stream.lock().await = None;
        Ok(())
    }
}

struct FileBuilder;

impl InputBuilder for FileBuilder {
    fn build(
        &self,
        name: Option<&str>,
        config: &Option<Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        // The file is parsed whole by its `format` (DataFusion reader); a
        // codec has no decode integration point here. Reject instead of
        // building a codec that is silently dropped.
        if codec.is_some() {
            return Err(Error::Config(
                "file input does not support a codec: the file is parsed by its `format`"
                    .to_string(),
            ));
        }
        let config: FileInputConfig = parse_config(config, "File input")?;
        Ok(Arc::new(FileInput::new(name, config)?))
    }
}

pub fn init() -> Result<(), Error> {
    arkflow_core::input::register_input_builder("file", Arc::new(FileBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "file",
        "Reads records from local or remote object storage (S3, GCS, Azure, HDFS) in CSV/JSON/Parquet/Avro/Arrow formats.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "input_type": {"type": "object", "description": "Format-specific input settings (type: csv/json/parquet/avro/arrow)."},
                "ballista": {"type": "object", "description": "Optional Ballista distributed compute configuration."},
                "query": {"type": "object", "description": "Optional SQL query and table name to filter the read."}
            },
            "required": ["input_type"]
        }),
    ).with_example(serde_json::json!({
        "input_type": {"type": "parquet", "path": "s3://bucket/data.parquet"}
    })))
}

fn default_disallow_http() -> bool {
    false
}

fn default_table() -> String {
    "flow".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::codec::{Decoder, Encoder};
    use arkflow_core::input::Input;
    use std::cell::RefCell;

    fn resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: RefCell::new(Vec::new()),
        }
    }

    /// Minimal codec to exercise the builder's codec-rejection branch.
    struct NoopCodec;
    #[async_trait::async_trait]
    impl Encoder for NoopCodec {
        async fn encode(
            &self,
            _: arkflow_core::MessageBatch,
        ) -> Result<Vec<arkflow_core::Bytes>, Error> {
            Ok(Vec::new())
        }
    }
    #[async_trait::async_trait]
    impl Decoder for NoopCodec {
        async fn decode(
            &self,
            _: Vec<arkflow_core::Bytes>,
        ) -> Result<arkflow_core::MessageBatch, Error> {
            unimplemented!("not needed for the rejection test")
        }
    }

    fn build(input_type: Value) -> Result<Arc<dyn Input>, Error> {
        let config = Some(serde_json::json!({ "input_type": input_type }));
        FileBuilder.build(None, &config, None, &resource())
    }

    fn build_with_query(input_type: Value, query: Value) -> Result<Arc<dyn Input>, Error> {
        let config = Some(serde_json::json!({
            "input_type": input_type,
            "query": query,
        }));
        FileBuilder.build(None, &config, None, &resource())
    }

    #[tokio::test]
    async fn csv_round_trip_reads_rows_until_eof() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "id,name\n1,alpha\n2,beta\n").unwrap();
        let input = build(serde_json::json!({
            "type": "csv",
            "path": path.to_str().unwrap(),
        }))?;
        input.connect().await?;
        let mut rows = 0usize;
        loop {
            match input.read().await {
                Ok((batch, _)) => rows += batch.num_rows(),
                Err(Error::EOF) => break,
                Err(e) => return Err(e),
            }
        }
        assert_eq!(rows, 2);
        input.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn json_round_trip_with_sql_query() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(&path, "{\"v\": 10}\n{\"v\": 20}\n{\"v\": 30}\n").unwrap();
        let input = build_with_query(
            serde_json::json!({"type": "json", "path": path.to_str().unwrap()}),
            serde_json::json!({
                "query": "SELECT COUNT(*) AS c FROM flow WHERE v > 5",
                "table": "flow",
            }),
        )?;
        input.connect().await?;
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_round_trip_reads_written_file() -> Result<(), Error> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter as ParquetWriter;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .map_err(|e| Error::Process(e.to_string()))?;
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ParquetWriter::try_new(file, batch.schema(), None)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer.close().map_err(|e| Error::Process(e.to_string()))?;
        }

        let input = build(serde_json::json!({
            "type": "parquet",
            "path": path.to_str().unwrap(),
        }))?;
        input.connect().await?;
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn arrow_round_trip_reads_ipc_file() -> Result<(), Error> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::FileWriter as IpcWriter;
        use datafusion::arrow::record_batch::RecordBatch;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![9]))])
            .map_err(|e| Error::Process(e.to_string()))?;
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = IpcWriter::try_new(file, schema.as_ref())
                .map_err(|e| Error::Process(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer.finish().map_err(|e| Error::Process(e.to_string()))?;
        }

        let input = build(serde_json::json!({
            "type": "arrow",
            "path": path.to_str().unwrap(),
        }))?;
        input.connect().await?;
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_round_trip_with_sql_query() -> Result<(), Error> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter as ParquetWriter;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))])
                .map_err(|e| Error::Process(e.to_string()))?;
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ParquetWriter::try_new(file, batch.schema(), None)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer.close().map_err(|e| Error::Process(e.to_string()))?;
        }

        let input = build_with_query(
            serde_json::json!({"type": "parquet", "path": path.to_str().unwrap()}),
            serde_json::json!({
                "query": "SELECT SUM(v) AS total FROM flow WHERE v > 1",
                "table": "flow",
            }),
        )?;
        input.connect().await?;
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn arrow_round_trip_with_sql_query() -> Result<(), Error> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::FileWriter as IpcWriter;
        use datafusion::arrow::record_batch::RecordBatch;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![5, 6]))])
                .map_err(|e| Error::Process(e.to_string()))?;
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = IpcWriter::try_new(file, schema.as_ref())
                .map_err(|e| Error::Process(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| Error::Process(e.to_string()))?;
            writer.finish().map_err(|e| Error::Process(e.to_string()))?;
        }

        let input = build_with_query(
            serde_json::json!({"type": "arrow", "path": path.to_str().unwrap()}),
            serde_json::json!({
                "query": "SELECT COUNT(*) AS c FROM flow",
                "table": "flow",
            }),
        )?;
        input.connect().await?;
        let (batch, _) = input.read().await?;
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn missing_file_fails_connect_with_process_error() {
        let input = build(serde_json::json!({
            "type": "csv",
            "path": "/nonexistent/arkflow/missing.csv",
        }))
        .unwrap();
        match input.connect().await {
            Err(err) => assert!(err.to_string().contains("Read input failed"), "{err}"),
            Ok(()) => panic!("reading a missing file must fail"),
        }
    }

    #[tokio::test]
    async fn avro_branch_reports_read_failure_for_missing_file() {
        let input = build(serde_json::json!({
            "type": "avro",
            "path": "/nonexistent/arkflow/missing.avro",
        }))
        .unwrap();
        match input.connect().await {
            Err(err) => assert!(err.to_string().contains("Read input failed"), "{err}"),
            Ok(()) => panic!("reading a missing file must fail"),
        }
    }

    #[tokio::test]
    async fn read_before_connect_and_close_then_eof() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.csv");
        std::fs::write(&path, "id\n1\n").unwrap();
        let input = build(serde_json::json!({
            "type": "csv",
            "path": path.to_str().unwrap(),
        }))?;

        match input.read().await {
            Err(err) => assert!(err.to_string().contains("Stream is None"), "{err}"),
            Ok(_) => panic!("read before connect must fail"),
        }

        input.connect().await?;
        input.read().await?;
        input.close().await?;
        assert!(matches!(input.read().await, Err(Error::EOF)));
        Ok(())
    }

    #[test]
    fn builder_rejects_codec_and_bad_config() -> Result<(), Error> {
        let Err(codec_rejected) = FileBuilder.build(
            None,
            &Some(serde_json::json!({"input_type": {"type": "csv", "path": "/tmp/x"}})),
            Some(Arc::new(NoopCodec)),
            &resource(),
        ) else {
            panic!("a codec must be rejected");
        };
        assert!(
            codec_rejected
                .to_string()
                .contains("does not support a codec"),
            "{codec_rejected}"
        );

        let Err(bad_config) = FileBuilder.build(
            None,
            &Some(serde_json::json!({"unexpected": 1})),
            None,
            &resource(),
        ) else {
            panic!("a malformed config must be rejected");
        };
        assert!(!bad_config.to_string().is_empty());
        Ok(())
    }

    #[test]
    fn object_store_registration_branches() -> Result<(), Error> {
        let input = FileInput::new(
            None,
            serde_json::from_value(serde_json::json!({
                "input_type": {"type": "csv", "path": "s3://does-not-matter/x.csv"}
            }))?,
        )?;
        let ctx = datafusion::prelude::SessionContext::new();

        // HTTP store registration is offline: only the URL shape is parsed.
        let http = Store::Http(HttpConfig {
            url: "https://example.invalid".into(),
        });
        input.object_store(&ctx, &http).unwrap();

        // S3 with a missing region still builds a client (credentials are
        // resolved lazily); a broken URL is rejected earlier by the builder.
        let s3 = Store::S3(AwsS3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            region: Some("us-east-1".into()),
            bucket_name: "bucket".into(),
            access_key_id: "key".into(),
            secret_access_key: "secret".into(),
            allow_http: true,
        });
        input.object_store(&ctx, &s3).unwrap();

        // GCS without credentials or an emulator env var refuses to build.
        let gcs = Store::Gs(GoogleCloudStorageConfig {
            bucket_name: "bucket".into(),
            url: Some("http://127.0.0.1:1".into()),
            service_account_path: None,
            service_account_key: None,
        });
        match input.object_store(&ctx, &gcs) {
            Err(err) => assert!(err.to_string().contains("GCS"), "{err}"),
            Ok(()) => panic!("credential-less GCS must be rejected"),
        }

        // Azure with an explicit non-HTTPS endpoint is rejected by the builder.
        let az = Store::Az(MicrosoftAzureConfig {
            url: Some("http://127.0.0.1:1".into()),
            endpoint: None,
            account: "account".into(),
            access_key: Some("key".into()),
            container_name: "container".into(),
        });
        match input.object_store(&ctx, &az) {
            Err(err) => assert!(err.to_string().to_lowercase().contains("azure"), "{err}"),
            Ok(()) => panic!("azure with an http endpoint must be rejected"),
        }
        Ok(())
    }

    /// A RecordBatchStream double that yields one good batch and then an
    /// injected error — the shape a failing object store or a corrupt
    /// file produces after some rows have already flowed.
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

    fn permanent_object_store_error() -> datafusion::common::DataFusionError {
        datafusion::common::DataFusionError::ObjectStore(Box::new(
            object_store::Error::PermissionDenied {
                path: "secret/warehouse.parquet".into(),
                source: Box::new(std::io::Error::other("injected denial")),
            },
        ))
    }

    fn deterministic_decode_error() -> datafusion::common::DataFusionError {
        datafusion::common::DataFusionError::ArrowError(
            Box::new(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(std::io::Error::other("injected decode failure")),
            )),
            None,
        )
    }

    /// Install the failing double into the input's stream slot: read()
    /// consumes whatever stream is installed, so this drives the exact
    /// mapping under test without depending on a storage backend that can
    /// error mid-scan on demand.
    async fn install_failing_stream(
        input: &FileInput,
        error: datafusion::common::DataFusionError,
    ) -> Result<(), Error> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "v",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
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

    fn json_input_for_test(dir: &tempfile::TempDir) -> Result<FileInput, Error> {
        let path = dir.path().join("data.json");
        std::fs::write(&path, "{\"v\": 10}\n").unwrap();
        let config = FileInputConfig {
            input_type: InputType::Json(FileFormatConfig {
                path: path.to_str().unwrap().to_string(),
                store: None,
            }),
            ballista: None,
            query: None,
        };
        FileInput::new(None, config)
    }

    /// Spec: connector-recovery-contract — 无界源的异常终止 SHALL 归类为
    /// Disconnection 而非 EOF(仅限暂态失败)。A transient retrieval
    /// failure (object store outage) after rows have flowed must surface
    /// `Error::Disconnection` (reconnect) — mapping it to `EOF` would
    /// broadcast EOS downstream and acknowledge a truncated dataset as
    /// complete.
    #[tokio::test]
    async fn transient_mid_stream_failure_is_disconnection_not_eof() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let input = json_input_for_test(&dir)?;
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

    /// Regression (review round 5): an object-store access denial is
    /// deterministic — reconnecting replays the same prefix into the same
    /// denial. It must terminate instead of returning Disconnection.
    #[tokio::test]
    async fn object_store_permission_denied_is_terminal() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let input = json_input_for_test(&dir)?;
        input.connect().await?;
        install_failing_stream(&input, permanent_object_store_error()).await?;

        let (_, _) = input.read().await?;
        match input.read().await {
            Err(Error::Process(message)) => {
                assert!(
                    message.contains("deterministic"),
                    "an access denial must terminate with the cause named: {message}"
                );
            }
            Err(Error::Disconnection) => {
                panic!("a permission-denied object store must not trigger endless reconnects")
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }

    /// Spec: connector-recovery-contract — 确定性失败 SHALL 走致命路径。
    /// A decode/format corruption after rows have flowed must NOT be
    /// classified as Disconnection: reconnecting rebuilds the scan from
    /// the first row and hits the same corruption forever, re-emitting the
    /// intact prefix each cycle. It fails the run loudly so an operator
    /// notices.
    #[tokio::test]
    async fn deterministic_mid_stream_failure_is_terminal_not_reconnect() -> Result<(), Error> {
        let dir = tempfile::tempdir().unwrap();
        let input = json_input_for_test(&dir)?;
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
                panic!("a corrupt file must not trigger endless reconnects")
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a failed stream must not yield data"),
        }
        input.close().await?;
        Ok(())
    }
}
