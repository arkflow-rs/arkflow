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
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::config::{CsvOptions, JsonOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use serde_with::DeserializeFromStr;

use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use chrono::Local;
use url::Url;

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

#[derive(Debug, Clone, Serialize, DeserializeFromStr, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct S3OutputConfig {
    provider: String,
    bucket: String,
    region: String,
    credentials: ProviderCredentials,
    prefix_pattern: Option<String>,
    format: S3OutputFormat,
    format_options: Option<FormatOptions>,
}

#[derive(Debug, Clone)]
enum WriterOptions {
    Csv(CsvOptions),
    Json(JsonOptions),
    Parquet(TableParquetOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
struct FormatOptions {
    compression: Option<String>,
    //csv
    delimiter: Option<u8>,
    header: Option<bool>,
    escape: Option<u8>,
    //parquet
    skip_arrow_metadata: Option<bool>,
    max_row_group_size: Option<usize>,
    dictionary_enabled: Option<bool>,
    statistics_enabled: Option<String>,
}

pub struct S3Output {
    config: S3OutputConfig,
    s3_client: Arc<dyn ObjectStore>,
    cancellation_token: CancellationToken,
    ctx: Arc<SessionContext>,
    writer_options: Arc<Mutex<Option<WriterOptions>>>,
}

impl S3Output {
    pub fn new(config: S3OutputConfig) -> Result<Self, Error> {
        let builder = Self::create_storage_client(&config)?;
        let cancellation_token = CancellationToken::new();
        let ctx = Arc::new(SessionContext::new());

        Ok(Self {
            config,
            s3_client: builder,
            cancellation_token,
            ctx,
            writer_options: Arc::new(Mutex::new(None)),
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
        let url = self.get_url().await?;
        info!("Connecting to S3 at URL: {}", url);
        self.ctx.register_object_store(&url, self.s3_client.clone());
        let writer_options = self.get_writer_options().await?;
        *self.writer_options.lock().unwrap() = Some(writer_options);
        Ok(())
    }
    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        self.ctx
            .register_batch("batch", msg.into())
            .map_err(|e| Error::Process(format!("Failed to register batch: {}", e)))?;

        let df = self
            .ctx
            .sql("SELECT * FROM batch")
            .await
            .map_err(|e| Error::Process(format!("Failed to execute SQL: {}", e)))?;

        let writer_options = match self.writer_options.lock().unwrap().as_ref() {
            Some(options) => options.clone(),
            None => {
                // writer_options가 없으면 기본값으로 생성
                match self.config.format {
                    S3OutputFormat::Csv => WriterOptions::Csv(CsvOptions::default()),
                    S3OutputFormat::Json => WriterOptions::Json(JsonOptions::default()),
                    S3OutputFormat::Parquet => WriterOptions::Parquet(TableParquetOptions::new()),
                }
            }
        };

        let prefix = self.format_prefix_with_datetime().await;
        let url = self.get_url().await?;
        let save_path = url
            .join(&prefix)
            .map_err(|e| Error::Process(format!("Failed to join URL with prefix: {}", e)))?
            .to_string();
        let dataframe_options = DataFrameWriteOptions::new();

        match self.config.format {
            S3OutputFormat::Csv => match writer_options {
                WriterOptions::Csv(csv_options) => {
                    df.write_csv(&save_path, dataframe_options, Some(csv_options.clone()))
                        .await
                        .map_err(|e| Error::Process(format!("Failed to write CSV: {}", e)))?;
                    info!("Writing CSV to S3 at path: {}", save_path);
                }
                _ => {
                    return Err(Error::Process("Invalid options for CSV format".to_string()));
                }
            },
            S3OutputFormat::Json => match writer_options {
                WriterOptions::Json(json_options) => {
                    df.write_json(&save_path, dataframe_options, Some(json_options.clone()))
                        .await
                        .map_err(|e| Error::Process(format!("Failed to write JSON: {}", e)))?;
                }
                _ => {
                    return Err(Error::Process(
                        "Invalid options for JSON format".to_string(),
                    ));
                }
            },
            S3OutputFormat::Parquet => match writer_options {
                WriterOptions::Parquet(parquet_options) => {
                    df.write_parquet(&save_path, dataframe_options, Some(parquet_options.clone()))
                        .await
                        .map_err(|e| Error::Process(format!("Failed to write Parquet: {}", e)))?;
                }
                _ => {
                    return Err(Error::Process(
                        "Invalid options for Parquet format".to_string(),
                    ));
                }
            },
        };
        let _ = self.ctx.deregister_table("batch");
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        Ok(())
    }
}
impl S3Output {
    async fn get_writer_options(&self) -> Result<WriterOptions, Error> {
        match self.config.format {
            S3OutputFormat::Csv => {
                let csv_options = self.build_csv_options().await?;
                Ok(WriterOptions::Csv(csv_options))
            }
            S3OutputFormat::Json => {
                let json_options = self.build_json_options().await?;
                Ok(WriterOptions::Json(json_options))
            }
            S3OutputFormat::Parquet => {
                let parquet_options = self.build_parquet_options().await?;
                Ok(WriterOptions::Parquet(parquet_options))
            }
        }
    }

    async fn build_csv_options(&self) -> Result<CsvOptions, Error> {
        let mut csv_options = CsvOptions::default();

        if let Some(format_options) = &self.config.format_options {
            if let Some(delimiter) = format_options.delimiter {
                csv_options = csv_options.with_delimiter(delimiter);
            }
            if let Some(header) = format_options.header {
                csv_options = csv_options.with_has_header(header);
            }
            if let Some(compression_str) = &format_options.compression {
                // 문자열을 CompressionTypeVariant enum으로 변환
                let compression_type = match compression_str.to_lowercase().as_str() {
                    "gzip" => CompressionTypeVariant::GZIP,
                    "bzip2" => CompressionTypeVariant::BZIP2,
                    "xz" => CompressionTypeVariant::XZ,
                    "zstd" => CompressionTypeVariant::ZSTD,
                    unsupported => {
                        return Err(Error::Config(format!(
                            "Unsupported compression type: {}",
                            unsupported
                        )));
                    }
                };
                csv_options = csv_options.with_compression(compression_type);
            }
            if let Some(escape) = format_options.escape {
                csv_options = csv_options.with_escape(Some(escape));
            }
        }
        Ok(csv_options)
    }
    async fn build_json_options(&self) -> Result<JsonOptions, Error> {
        let mut json_options = JsonOptions::default();

        if let Some(format_options) = &self.config.format_options {
            if let Some(compression_str) = &format_options.compression {
                // 문자열을 CompressionTypeVariant enum으로 변환
                let compression_type = match compression_str.to_lowercase().as_str() {
                    "gzip" => CompressionTypeVariant::GZIP,
                    "bzip2" => CompressionTypeVariant::BZIP2,
                    "xz" => CompressionTypeVariant::XZ,
                    "zstd" => CompressionTypeVariant::ZSTD,
                    unsupported => {
                        return Err(Error::Config(format!(
                            "Unsupported compression type: {}",
                            unsupported
                        )));
                    }
                };
                json_options.compression = compression_type;
            }
        }
        Ok(json_options)
    }
    async fn build_parquet_options(&self) -> Result<TableParquetOptions, Error> {
        let mut parquet_table_options = TableParquetOptions::new();
        let mut parquet_options = ParquetOptions::default();

        if let Some(format_options) = &self.config.format_options {
            if let Some(compression_str) = &format_options.compression {
                parquet_options.compression = Some(compression_str.clone());
            }
            if let Some(skip_arrow_metadata) = format_options.skip_arrow_metadata {
                parquet_options.skip_arrow_metadata = skip_arrow_metadata;
            }
            if let Some(max_row_group_size) = format_options.max_row_group_size {
                parquet_options.max_row_group_size = max_row_group_size;
            }
            if let Some(dictionary_enabled) = format_options.dictionary_enabled {
                parquet_options.dictionary_enabled = Some(dictionary_enabled);
            }
            if let Some(statistics_enabled) = &format_options.statistics_enabled {
                parquet_options.statistics_enabled = Some(statistics_enabled.clone());
            }
        }

        parquet_table_options.global = parquet_options;

        Ok(parquet_table_options)
    }
    async fn format_prefix_with_datetime(&self) -> String {
        let datetime = Local::now();
        let path = if let Some(prefix_pattern) = &self.config.prefix_pattern {
            prefix_pattern
                .replace("{year}", &datetime.format("%Y").to_string())
                .replace("{month}", &datetime.format("%m").to_string())
                .replace("{day}", &datetime.format("%d").to_string())
                .replace("{hour}", &datetime.format("%H").to_string())
                .replace("{minute}", &datetime.format("%M").to_string())
                .replace("{second}", &datetime.format("%S").to_string())
                .replace("{timestamp}", &datetime.timestamp().to_string())
        } else {
            datetime.timestamp().to_string()
        };

        let extension = match self.config.format {
            S3OutputFormat::Json => "json",
            S3OutputFormat::Csv => "csv",
            S3OutputFormat::Parquet => "parquet",
        };
        let mut file_path = format!("{}.{}", path, extension);

        if self.config.format != S3OutputFormat::Parquet {
            if let Some(format_options) = &self.config.format_options {
                if let Some(compression) = &format_options.compression {
                    let compression_ext = match compression.to_lowercase().as_str() {
                        "gzip" => ".gz",
                        "bzip2" => ".bz2",
                        "xz" => ".xz",
                        "zstd" => ".zst",
                        _ => "",
                    };
                    if !compression_ext.is_empty() {
                        file_path = format!("{}{}", file_path, compression_ext);
                    }
                }
            }
        }

        file_path
    }

    async fn get_url(&self) -> Result<Url, Error> {
        let url_str = match self.config.provider.to_lowercase().as_str() {
            "aws" => format!("s3://{}", self.config.bucket),
            "gcp" => format!("gs://{}", self.config.bucket),
            provider => {
                return Err(Error::Config(format!(
                    "Unsupported storage provider: {}",
                    provider
                )))
            }
        };
        info!("S3 Output URL: {}", url_str);
        Url::parse(&url_str).map_err(|e| Error::Config(format!("Invalid URL: {}", e)))
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
