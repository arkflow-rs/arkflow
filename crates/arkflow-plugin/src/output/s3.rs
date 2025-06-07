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
use arkflow_core::{Error, MessageBatch, Resource};

use async_trait::async_trait;

use serde::{Deserialize, Serialize};
use serde_with::DeserializeFromStr;

use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path, ObjectStore};

use arrow_csv::WriterBuilder;
use arrow_json::LineDelimitedWriter;
use parquet::arrow::ArrowWriter;

use chrono::Local;

use bzip2::write::BzEncoder;
use bzip2::Compression as BzCompression;
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use std::io::{Cursor, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum ProviderCredentials {
    Aws(AwsCredentials),
    Gcp(GcpCredentials),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GcpCredentials {
    service_account_path: String,
}

#[derive(Debug, Clone, Serialize, DeserializeFromStr)]
#[serde(rename_all = "snake_case")]
pub enum S3OutputFormat {
    Json,
    Csv,
    Parquet,
}
impl FromStr for S3OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            _ => Err(format!("Invalid output format: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, DeserializeFromStr)]
#[serde(rename_all = "snake_case")]
pub enum S3Compression {
    Gzip,
    Bzip2,
}
impl FromStr for S3Compression {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(Self::Gzip),
            "bzip2" => Ok(Self::Bzip2),
            _ => Err(format!("Invalid compression type: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct S3OutputConfig {
    provider: String,
    bucket: String,
    region: String,
    credentials: ProviderCredentials,
    path: String,
    prefix: Option<String>,
    format: S3OutputFormat,
    compression: Option<S3Compression>,
}

pub struct S3Output {
    config: S3OutputConfig,
    s3_client: Arc<dyn ObjectStore>,
    cancellation_token: CancellationToken,
}

impl S3Output {
    pub fn new(config: S3OutputConfig) -> Result<Self, Error> {
        let builder = Self::create_storage_client(&config)?;
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            config,
            s3_client: builder,
            cancellation_token,
        })
    }
    fn create_storage_client(config: &S3OutputConfig) -> Result<Arc<dyn ObjectStore>, Error> {
        match config.provider.to_lowercase().as_str() {
            "aws" => Self::build_aws_client(config),
            "gcp" => Self::build_gcp_client(config),
            other => Err(Error::Config(format!(
                "Unsupported storage provider: {}",
                other
            ))),
        }
    }
    fn build_aws_client(config: &S3OutputConfig) -> Result<Arc<dyn ObjectStore>, Error> {
        match &config.credentials {
            ProviderCredentials::Aws(aws_creds) => {
                let builder = AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket)
                    .with_region(&config.region)
                    .with_access_key_id(&aws_creds.access_key_id)
                    .with_secret_access_key(&aws_creds.secret_access_key);

                Ok(Arc::new(builder.build().map_err(|e| {
                    Error::Config(format!("Failed to build AWS S3 client: {}", e))
                })?))
            }
            _ => Err(Error::Config(
                "Provider is 'aws', but AWS credentials are not provided or are of the wrong type."
                    .to_string(),
            )),
        }
    }
    fn build_gcp_client(config: &S3OutputConfig) -> Result<Arc<dyn ObjectStore>, Error> {
        match &config.credentials {
            ProviderCredentials::Gcp(gcp_creds) => {
                let builder = GoogleCloudStorageBuilder::new()
                    .with_bucket_name(&config.bucket)
                    .with_service_account_path(&gcp_creds.service_account_path);

                Ok(Arc::new(builder.build().map_err(|e| {
                    Error::Config(format!("Failed to build GCP GCS client: {}", e))
                })?))
            }
            _ => Err(Error::Config(
                "Provider is 'gcp', but GCP credentials are not provided or are of the wrong type."
                    .to_string(),
            )),
        }
    }
}
#[async_trait]
impl Output for S3Output {
    async fn connect(&self) -> Result<(), Error> {
        info!(
            "S3 client for bucket '{}' initialized. Performing a lightweight connection check (e.g., listing root).",
            self.config.bucket
        );
        Ok(())
    }
    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let s3_client_clone = Arc::clone(&self.s3_client);
        let config_clone = self.config.clone();
        let timestamp = Local::now().timestamp();

        let (object_key, data) =
            tokio::task::spawn_blocking(move || -> Result<(Path, Vec<u8>), Error> {
                let serialized_data = match config_clone.format {
                    S3OutputFormat::Json => S3Output::batch_to_json(&msg)?,
                    S3OutputFormat::Csv => S3Output::batch_to_csv(&msg)?,
                    S3OutputFormat::Parquet => S3Output::batch_to_parquet(&msg)?,
                };

                let processed_data = if let Some(compression) = config_clone.compression {
                    S3Output::compress_data(serialized_data, &compression)?
                } else {
                    serialized_data
                };
                let extension = S3Output::get_file_extension_from_config(&config_clone);

                let filename_prefix_part = config_clone
                    .prefix
                    .as_ref()
                    .map_or_else(|| "".to_string(), |p| format!("{}_", p.trim_matches('/')));
                let filename = format!("{}{}.{}", filename_prefix_part, timestamp, extension);
                let base_path_trimmed = config_clone.path.trim_matches('/');
                let path = format!("{}/{}", base_path_trimmed, filename);
                let object_key = Path::from(path);

                Ok((object_key, processed_data))
            })
            .await
            .map_err(|e| Error::Process(format!("Data processing task failed: {}", e)))??;

        s3_client_clone
            .put(&object_key, data.into())
            .await
            .map_err(|e| Error::Process(format!("Failed to write to S3: {}", e)))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        Ok(())
    }
}
impl S3Output {
    fn batch_to_json(msg: &MessageBatch) -> Result<Vec<u8>, Error> {
        let buf = Vec::new();
        let mut json_writer = LineDelimitedWriter::new(buf);

        json_writer
            .write_batches(&[msg])
            .map_err(|e| Error::Process(format!("Failed to write JSON: {}", e)))?;

        json_writer
            .finish()
            .map_err(|e| Error::Process(format!("Failed to finish JSON writer: {}", e)))?;

        Ok(json_writer.into_inner())
    }
    fn batch_to_csv(msg: &MessageBatch) -> Result<Vec<u8>, Error> {

        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let mut csv_writer = WriterBuilder::new().with_header(true).build(cursor);

        csv_writer
            .write(msg)
            .map_err(|e| Error::Process(format!("Failed to write CSV: {}", e)))?;

        let cursor = csv_writer.into_inner();
        Ok(cursor.into_inner())
    }
    fn batch_to_parquet(msg: &MessageBatch) -> Result<Vec<u8>, Error> {

        let mut parquet_buffer = Vec::new();

        let schema = msg.schema();
        let mut writer = ArrowWriter::try_new(&mut parquet_buffer, schema, None)
            .map_err(|e| Error::Process(format!("Failed to create ArrowWriter: {}", e)))?;

        writer.write(msg).map_err(|e| {
            Error::Process(format!("Failed to write RecordBatch to Parquet: {}", e))
        })?;

        writer
            .close()
            .map_err(|e| Error::Process(format!("Failed to close Parquet writer: {}", e)))?;

        Ok(parquet_buffer)
    }
    fn compress_data(data: Vec<u8>, compression: &S3Compression) -> Result<Vec<u8>, Error> {
        match compression {
            S3Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                encoder
                    .write_all(&data)
                    .map_err(|e| Error::Process(format!("Gzip write_all failed: {}", e)))?; // 에러 메시지 구체화
                encoder
                    .finish()
                    .map_err(|e| Error::Process(format!("Gzip finish failed: {}", e)))
            }
            S3Compression::Bzip2 => {
                let mut encoder = BzEncoder::new(Vec::new(), BzCompression::default());
                encoder
                    .write_all(&data)
                    .map_err(|e| Error::Process(format!("Bzip2 write_all failed: {}", e)))?; // 에러 메시지 구체화
                encoder
                    .finish()
                    .map_err(|e| Error::Process(format!("Bzip2 finish failed: {}", e)))
            }
        }
    }

    fn get_file_extension_from_config(config: &S3OutputConfig) -> &'static str {
        match (&config.format, &config.compression) {
            (S3OutputFormat::Json, Some(S3Compression::Gzip)) => "json.gz",
            (S3OutputFormat::Csv, Some(S3Compression::Gzip)) => "csv.gz",
            (S3OutputFormat::Json, Some(S3Compression::Bzip2)) => "json.bz2",
            (S3OutputFormat::Csv, Some(S3Compression::Bzip2)) => "csv.bz2",
            (S3OutputFormat::Json, None) => "json",
            (S3OutputFormat::Csv, None) => "csv",
            (S3OutputFormat::Parquet, _) => "parquet",
        }
    }
}

pub(crate) struct S3OutputBuilder;
impl OutputBuilder for S3OutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "S3 output configuration is missing".to_string(),
            ));
        }

        let config: S3OutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(S3Output::new(config)?))
    }
}
pub fn init() -> Result<(), Error> {
    register_output_builder("s3", Arc::new(S3OutputBuilder))
}
