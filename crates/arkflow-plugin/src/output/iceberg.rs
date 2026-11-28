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

//! Iceberg output component
//!
//! Appends `MessageBatch`es to an existing Apache Iceberg table through a REST
//! catalog. One `write_batch` call is one atomic Iceberg append: the call
//! returns `Ok` only after the catalog accepted the commit, so the kernel's
//! acknowledgement never outruns the table. The output holds no buffer or
//! commit timer across calls — commit cadence and file size follow the batches
//! the upstream pipeline delivers, in practice a `batch` processor.
//!
//! With `exactly_once: true`, a per-`(topic, partition)` Kafka offset
//! watermark is committed atomically with each append (snapshot summary
//! property `arkflow.writer.<writer_id>.offsets`) and replayed rows below it
//! are dropped, so a crash between the table commit and the acknowledgement
//! produces no duplicates. The guarantee requires every row to reach the
//! output with `__meta_partition`, `__meta_offset`, and `__meta_ext.topic`
//! and an ordered single chain from the Kafka input; otherwise the output is
//! at-least-once.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    codec::Codec,
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatchRef, Resource,
};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef, TimeUnit};
use iceberg::arrow::schema_to_arrow_schema;

use iceberg::io::{S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION};
use iceberg::spec::PartitionKey;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalStorageFactory, ProvideCredential,
};
use parquet::basic::Compression as ParquetCompressionCodec;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Snapshot summary property carrying this writer's committed-through offsets.
fn watermark_property(writer_id: &str) -> String {
    format!("arkflow.writer.{writer_id}.offsets")
}

/// Watermark JSON: `{"<topic>#<partition>": <exclusive next offset>}`.
fn watermark_json(watermark: &Watermark) -> String {
    let flat: BTreeMap<String, u64> = watermark
        .iter()
        .map(|((topic, partition), next)| (format!("{topic}#{partition}"), *next))
        .collect();
    serde_json::to_string(&flat).unwrap_or_default()
}

fn watermark_from_json(raw: &str) -> Option<Watermark> {
    let flat: BTreeMap<String, u64> = serde_json::from_str(raw).ok()?;
    let mut watermark = Watermark::new();
    for (key, next) in flat {
        let (topic, partition) = key.rsplit_once('#')?;
        let partition: u32 = partition.parse().ok()?;
        watermark.insert((topic.to_string(), partition), next);
    }
    Some(watermark)
}

/// REST catalog connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergCatalogConfig {
    /// REST catalog endpoint, e.g. `http://lakekeeper:8181/catalog`.
    uri: String,
    /// Warehouse (catalog) name.
    warehouse: String,
    /// Static bearer token. May reference external secrets
    /// (`${env:VAR}`, `${file:/path}`, `${secret:...}`).
    #[serde(default)]
    token: Option<String>,
}

/// S3 storage overrides. When absent, storage configuration comes from the
/// catalog server (Lakekeeper sends `s3.*` config with the table) and
/// credentials fall back to the environment / instance profile. When present,
/// `endpoint` and `region` are sent as client properties (they override the
/// server defaults) and explicit credentials are used for signing — no
/// environment lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergS3Config {
    /// S3 endpoint, e.g. `http://minio:9000`. The host running ArkFlow must
    /// be able to reach it: data files are uploaded from this process.
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    /// Path-style addressing (required by MinIO/Silo). Default `true`.
    #[serde(default = "default_true")]
    path_style_access: bool,
}

fn default_true() -> bool {
    true
}

/// Parquet codec override. When unset the table's `write.parquet.*`
/// properties decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ParquetCodec {
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl From<ParquetCodec> for ParquetCompressionCodec {
    fn from(codec: ParquetCodec) -> Self {
        match codec {
            ParquetCodec::Snappy => ParquetCompressionCodec::SNAPPY,
            ParquetCodec::Gzip => ParquetCompressionCodec::GZIP(arrow_variant_gzip_level()),
            ParquetCodec::Zstd => ParquetCompressionCodec::ZSTD(arrow_variant_zstd_level()),
            ParquetCodec::Lz4 => ParquetCompressionCodec::LZ4_RAW,
        }
    }
}

fn arrow_variant_gzip_level() -> parquet::basic::GzipLevel {
    parquet::basic::GzipLevel::default()
}

fn arrow_variant_zstd_level() -> parquet::basic::ZstdLevel {
    parquet::basic::ZstdLevel::default()
}

/// Write tuning. Unset values follow the table's own `write.*` properties.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct IcebergWriteConfig {
    /// Roll the data file after this many bytes. Defaults to the table's
    /// `write.target-file-size-bytes` (Iceberg default 512 MiB). This only
    /// splits files inside one commit; it does not drive commit cadence.
    #[serde(default)]
    target_file_size_bytes: Option<usize>,
    /// Parquet compression codec. Defaults to the table's
    /// `write.parquet.compression-codec` (parquet default snappy).
    #[serde(default)]
    compression: Option<ParquetCodec>,
}

/// Iceberg output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergOutputConfig {
    catalog: IcebergCatalogConfig,
    /// Namespace containing the table. Split on `.` for multi-level namespaces.
    namespace: String,
    /// Table name. The table must already exist; the output never creates
    /// tables or evolves schemas.
    table: String,
    /// Drop replayed rows below the committed offset watermark. Requires a
    /// replayable Kafka source position on every row. Default `false`
    /// (at-least-once).
    #[serde(default)]
    exactly_once: bool,
    /// Writer identity for the watermark. Required exactly when
    /// `exactly_once` is true; use the Kafka consumer group id.
    #[serde(default)]
    writer_id: Option<String>,
    #[serde(default)]
    write: IcebergWriteConfig,
    #[serde(default)]
    s3: Option<IcebergS3Config>,
}

impl IcebergOutputConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.catalog.uri.trim().is_empty() {
            return Err(Error::Config(
                "iceberg output: catalog.uri must not be empty".to_string(),
            ));
        }
        if self.catalog.warehouse.trim().is_empty() {
            return Err(Error::Config(
                "iceberg output: catalog.warehouse must not be empty".to_string(),
            ));
        }
        if self.namespace.trim().is_empty() {
            return Err(Error::Config(
                "iceberg output: namespace must not be empty".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(Error::Config(
                "iceberg output: table must not be empty".to_string(),
            ));
        }
        if self.exactly_once
            && self
                .writer_id
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(Error::Config(
                "iceberg output: writer_id is required for exactly-once delivery".to_string(),
            ));
        }
        if !self.exactly_once && self.writer_id.is_some() {
            return Err(Error::Config(
                "iceberg output: writer_id is only meaningful with exactly_once: true".to_string(),
            ));
        }
        if let Some(size) = self.write.target_file_size_bytes {
            if size == 0 {
                return Err(Error::Config(
                    "iceberg output: write.target_file_size_bytes must be greater than zero"
                        .to_string(),
                ));
            }
        }
        if let Some(s3) = &self.s3 {
            let has_credential = s3.access_key_id.is_some() || s3.secret_access_key.is_some();
            if has_credential && (s3.access_key_id.is_none() || s3.secret_access_key.is_none()) {
                return Err(Error::Config(
                    "iceberg output: s3.access_key_id and s3.secret_access_key must be set together"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    fn namespace_ident(&self) -> Result<NamespaceIdent, Error> {
        let parts: Vec<&str> = self
            .namespace
            .split('.')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        if parts.is_empty() {
            return Err(Error::Config(format!(
                "iceberg output: invalid namespace {:?}",
                self.namespace
            )));
        }
        NamespaceIdent::from_strs(parts)
            .map_err(|error| Error::Config(format!("iceberg output: invalid namespace: {error}")))
    }
}

/// Builder for the `iceberg` output. Building is side-effect free: no network
/// connection, no catalog contact — everything remote happens in
/// [`IcebergOutput::connect`].
struct IcebergOutputBuilder;

impl OutputBuilder for IcebergOutputBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if codec.is_some() {
            return Err(Error::Config(
                "iceberg output: codec is not supported — rows are written with the table's \
                 own Parquet schema"
                    .to_string(),
            ));
        }
        let config: IcebergOutputConfig = parse_config(config, "Iceberg output")?;
        config.validate()?;
        Ok(Arc::new(IcebergOutput::new(config)))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("iceberg", Arc::new(IcebergOutputBuilder))?;
    register_output_metadata(
        ComponentMetadata::with_schema(
            "iceberg",
            "Appends rows to an existing Apache Iceberg table through a REST catalog. \
             One write_batch call is one atomic table append; a batch processor in front \
             of this output controls commit cadence and file size.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "catalog": {
                        "type": "object",
                        "description": "REST catalog connection.",
                        "properties": {
                            "uri": {"type": "string", "description": "REST catalog endpoint, e.g. http://lakekeeper:8181/catalog."},
                            "warehouse": {"type": "string", "description": "Warehouse (catalog) name."},
                            "token": {"type": "string", "description": "Static bearer token; may reference ${env:VAR} / ${file:/path} / ${secret:...}."}
                        },
                        "required": ["uri", "warehouse"],
                        "additionalProperties": false
                    },
                    "namespace": {"type": "string", "description": "Namespace containing the table; split on '.' for multi-level namespaces."},
                    "table": {"type": "string", "description": "Table name. The table must already exist; the output never creates tables or evolves schemas."},
                    "exactly_once": {"type": "boolean", "description": "Drop replayed rows below the committed Kafka offset watermark. Requires a replayable source position on every row and an ordered single chain from the Kafka input.", "default": false},
                    "writer_id": {"type": "string", "description": "Watermark writer identity; required exactly when exactly_once is true. Use the Kafka consumer group id."},
                    "write": {
                        "type": "object",
                        "description": "Write tuning; unset values follow the table's own write.* properties.",
                        "properties": {
                            "target_file_size_bytes": {"type": "integer", "minimum": 1, "description": "Roll the data file after this many bytes. Only splits files inside one commit; does not drive commit cadence. Default: the table's write.target-file-size-bytes (512 MiB)."},
                            "compression": {"enum": ["snappy", "gzip", "zstd", "lz4"], "description": "Parquet codec. Default: the table's write.parquet.compression-codec (snappy)."}
                        },
                        "additionalProperties": false
                    },
                    "s3": {
                        "type": "object",
                        "description": "S3 storage overrides. When absent, storage config comes from the catalog server and credentials fall back to the environment.",
                        "properties": {
                            "endpoint": {"type": "string", "description": "S3 endpoint reachable from the host running ArkFlow; sent as a client property overriding the server default."},
                            "region": {"type": "string"},
                            "access_key_id": {"type": "string", "description": "Static access key; set together with secret_access_key to avoid environment credential lookup."},
                            "secret_access_key": {"type": "string", "description": "Static secret key; may reference ${env:VAR} / ${file:/path} / ${secret:...}."},
                            "path_style_access": {"type": "boolean", "description": "Path-style addressing (required by MinIO).", "default": true}
                        },
                        "additionalProperties": false
                    }
                },
                "required": ["catalog", "namespace", "table"]
            }),
        )
        .with_example(serde_json::json!({
            "catalog": {"uri": "http://lakekeeper:8181/catalog", "warehouse": "demo"},
            "namespace": "analytics",
            "table": "user_events",
            "exactly_once": true,
            "writer_id": "arkflow-iceberg-ingest",
            "write": {"target_file_size_bytes": 33554432, "compression": "zstd"},
            "s3": {"endpoint": "http://minio:9000", "region": "local-01", "path_style_access": true}
        })),
    )?;
    Ok(())
}

/// Committed-through source offsets: `((topic, partition), exclusive next offset)`.
type Watermark = HashMap<(String, u32), u64>;

/// Everything `connect()` established. Held behind one mutex: writers take it
/// for the whole transaction, a transient-failure reload replaces it.
struct Connected {
    catalog: Arc<RestCatalog>,
    table: Table,
    /// `None` in at-least-once mode.
    watermark: Option<Watermark>,
}

struct IcebergOutput {
    config: IcebergOutputConfig,
    /// Connected state; `None` until `connect()` succeeds, emptied by `close()`.
    inner: tokio::sync::Mutex<Option<Connected>>,
    /// Completion timestamps of the last commits, for the commit-rate warning.
    commit_times: std::sync::Mutex<VecDeque<Instant>>,
    /// Last time the commit-rate warning fired (rate-limited to one/minute).
    last_rate_warn: std::sync::Mutex<Option<Instant>>,
    /// Unique-per-process id for data file name prefixes.
    file_prefix_seq: AtomicU64,
}

impl IcebergOutput {
    fn new(config: IcebergOutputConfig) -> Self {
        Self {
            config,
            inner: tokio::sync::Mutex::new(None),
            commit_times: std::sync::Mutex::new(VecDeque::new()),
            last_rate_warn: std::sync::Mutex::new(None),
            file_prefix_seq: AtomicU64::new(0),
        }
    }

    /// Config-driven static S3 credentials. Present only when the `s3` block
    /// carries both keys — otherwise the upstream default chain (env vars,
    /// instance profile) applies, which the docs must not imply.
    fn s3_factory(config: &Option<IcebergS3Config>) -> Arc<OpenDalStorageFactory> {
        let customized = config.as_ref().and_then(|s3| {
            let access_key_id = s3.access_key_id.as_ref()?;
            let secret_access_key = s3.secret_access_key.as_ref()?;
            Some(CustomAwsCredentialLoader::new(StaticS3Credentials {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
            }))
        });
        Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: customized,
        })
    }

    fn client_storage_props(s3: &Option<IcebergS3Config>) -> Vec<(String, String)> {
        let mut props = Vec::new();
        if let Some(s3) = s3 {
            // Client props override the server-provided defaults: this is how
            // a NAT'd endpoint reaches the data path from the host process.
            if let Some(endpoint) = &s3.endpoint {
                props.push((S3_ENDPOINT.to_string(), endpoint.clone()));
            }
            if let Some(region) = &s3.region {
                props.push((S3_REGION.to_string(), region.clone()));
            }
            props.push((
                S3_PATH_STYLE_ACCESS.to_string(),
                s3.path_style_access.to_string(),
            ));
        }
        props
    }

    async fn build_catalog(config: &IcebergOutputConfig) -> Result<Arc<RestCatalog>, Error> {
        let mut props = HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_string(),
                config.catalog.uri.clone(),
            ),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                config.catalog.warehouse.clone(),
            ),
        ]);
        if let Some(token) = &config.catalog.token {
            props.insert("token".to_string(), token.clone());
        }
        for (key, value) in Self::client_storage_props(&config.s3) {
            props.insert(key, value);
        }
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Self::s3_factory(&config.s3))
            .load("arkflow-iceberg-output", props)
            .await
            .map_err(|error| {
                Error::Connection(format!(
                    "iceberg output: catalog connection failed: {error}"
                ))
            })?;
        Ok(Arc::new(catalog))
    }

    /// Recover this writer's watermark from the snapshot ancestry, bounded.
    async fn recover_watermark(table: &Table, writer_id: &str) -> Option<Watermark> {
        let property = watermark_property(writer_id);
        let metadata = table.metadata();
        let mut current = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let mut visited = 0usize;
        while let Some(snapshot_id) = current {
            visited += 1;
            if visited > 1000 {
                warn!(
                    writer_id,
                    visited,
                    "iceberg output: watermark not found within 1000 snapshots; \
                     starting without one — replayed rows may be duplicated"
                );
                return None;
            }
            let snapshot = match metadata.snapshot_by_id(snapshot_id) {
                Some(snapshot) => snapshot,
                None => break,
            };
            if let Some(raw) = snapshot.summary().additional_properties.get(&property) {
                match watermark_from_json(raw) {
                    Some(watermark) => {
                        debug!(
                            writer_id,
                            visited,
                            "iceberg output: recovered watermark from snapshot {snapshot_id}"
                        );
                        return Some(watermark);
                    }
                    None => {
                        warn!(
                            writer_id,
                            snapshot_id,
                            "iceberg output: undecodable watermark property on snapshot; \
                             starting without one — replayed rows may be duplicated"
                        );
                        return None;
                    }
                }
            }
            current = snapshot.parent_snapshot_id();
        }
        None
    }
}

impl IcebergOutput {
    async fn connect_state(&self) -> Result<(), Error> {
        let catalog = Self::build_catalog(&self.config).await?;
        let namespace = self.config.namespace_ident()?;
        let ident = TableIdent::new(namespace, self.config.table.clone());
        let table = catalog.load_table(&ident).await.map_err(|error| {
            let message = error.to_string();
            if message.contains("NoSuchTableException")
                || message.to_lowercase().contains("does not exist")
                || message.to_lowercase().contains("not found")
            {
                Error::Process(format!(
                    "iceberg output: table {}.{} not found in warehouse {:?} — create it \
                     before starting the stream ({message})",
                    self.config.namespace, self.config.table, self.config.catalog.warehouse
                ))
            } else {
                Error::Connection(format!("iceberg output: failed to load table: {message}"))
            }
        })?;

        // Reject a partition spec this output cannot evaluate before the
        // first write — never fall back to writing as if unpartitioned.
        let spec = table.metadata().default_partition_spec();
        if !spec.is_unpartitioned() {
            iceberg::arrow::partition_value_calculator::PartitionValueCalculator::try_new(
                spec,
                table.metadata().current_schema(),
            )
            .map_err(|error| {
                Error::Process(format!(
                    "iceberg output: table {} has a partition spec this output cannot \
                     evaluate: {error}",
                    self.config.table
                ))
            })?;
        }

        let watermark = if self.config.exactly_once {
            let writer_id = self.config.writer_id.as_deref().expect("validated");
            Self::recover_watermark(&table, writer_id).await
        } else {
            None
        };
        let watermark_rows = watermark.as_ref().map(|w| w.len()).unwrap_or(0);
        info!(
            table = %self.config.table,
            exactly_once = self.config.exactly_once,
            watermark_groups = watermark_rows,
            "iceberg output connected"
        );
        *self.inner.lock().await = Some(Connected {
            catalog,
            table,
            watermark,
        });
        Ok(())
    }

    async fn close_state(&self) -> Result<(), Error> {
        // Nothing to flush: every write_batch committed before returning Ok.
        *self.inner.lock().await = None;
        Ok(())
    }
}

/// The table's `write.parquet.compression-codec` (parquet default snappy).
fn table_compression(properties: &HashMap<String, String>) -> ParquetCompressionCodec {
    match properties
        .get("write.parquet.compression-codec")
        .map(|value| value.to_lowercase())
        .as_deref()
    {
        Some("gzip") => ParquetCompressionCodec::GZIP(arrow_variant_gzip_level()),
        Some("zstd") => ParquetCompressionCodec::ZSTD(arrow_variant_zstd_level()),
        Some("lz4") => ParquetCompressionCodec::LZ4_RAW,
        _ => ParquetCompressionCodec::SNAPPY,
    }
}

/// The table's `write.target-file-size-bytes` (Iceberg default 512 MiB).
fn table_target_file_size(properties: &HashMap<String, String>) -> usize {
    properties
        .get("write.target-file-size-bytes")
        .and_then(|value| value.parse().ok())
        .unwrap_or(512 * 1024 * 1024)
}

/// Transient transport failures surface as the bare `Error::Disconnection`
/// (documentation-grade on the sink path) with the details logged.
fn transient_error(context: &str, error: impl std::fmt::Display) -> Error {
    tracing::error!(
        error = %error,
        "iceberg output: {context} — transient transport failure, retrying on a fresh          attempt may succeed"
    );
    Error::Disconnection
}

/// Config-driven static S3 credentials for the signing chain.
#[derive(Debug)]
struct StaticS3Credentials {
    access_key_id: String,
    secret_access_key: String,
}

impl ProvideCredential for StaticS3Credentials {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<AwsCredential>> {
        Ok(Some(AwsCredential {
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            session_token: None,
            expires_in: None,
        }))
    }
}

/// True when retrying the same logical write on a fresh connection can
/// succeed: transport failures, timeouts, and rate/availability responses.
/// Everything else (schema mismatch, missing table, decode errors) is
/// deterministic and must not be retried.
fn is_transient(message: &str) -> bool {
    let lower = message.to_lowercase();
    const TRANSIENT_PHRASES: &[&str] = &[
        "timed out",
        "timeout",
        "connection reset",
        "connection closed",
        "connection refused",
        "broken pipe",
        "temporarily unavailable",
        "service unavailable",
        "slow down",
        "internal error",
        "bad gateway",
        "gateway timeout",
        " 429",
        " 502",
        " 503",
        " 504",
    ];
    TRANSIENT_PHRASES
        .iter()
        .any(|phrase| lower.contains(phrase))
}

/// Per-batch position view: the raw column slices plus the topic per row.
#[derive(Debug)]
struct PositionColumns {
    partitions: Vec<Option<u32>>,
    offsets: Vec<Option<u64>>,
    topics: Vec<Option<String>>,
}

/// Extract `__meta_partition` (UInt32-family), `__meta_offset` (UInt64-family)
/// and `__meta_ext.topic` from one batch. Returns `Err` naming the first
/// offending column when values are missing or have unexpected types.
fn extract_positions(batch: &RecordBatch) -> Result<PositionColumns, Error> {
    let rows = batch.num_rows();
    let partition_array = batch
        .column_by_name(arkflow_core::meta_columns::PARTITION)
        .ok_or_else(|| missing_column(arkflow_core::meta_columns::PARTITION))?;
    let offset_array = batch
        .column_by_name(arkflow_core::meta_columns::OFFSET)
        .ok_or_else(|| missing_column(arkflow_core::meta_columns::OFFSET))?;
    let ext_array = batch
        .column_by_name(arkflow_core::meta_columns::EXT)
        .ok_or_else(|| missing_column(arkflow_core::meta_columns::EXT))?;

    let partitions = u32_values(partition_array, arkflow_core::meta_columns::PARTITION, rows)?;
    let offsets = u64_values(offset_array, arkflow_core::meta_columns::OFFSET, rows)?;

    let mut topics = Vec::with_capacity(rows);
    let ext = ext_array
        .as_any()
        .downcast_ref::<arrow_array::MapArray>()
        .ok_or_else(|| {
            Error::Process(format!(
                "iceberg output: {} column has an unexpected type (expected a map)",
                arkflow_core::meta_columns::EXT
            ))
        })?;
    for row in 0..rows {
        let mut topic = None;
        let entries = ext.value(row);
        if let Some(structs) = entries.as_any().downcast_ref::<arrow_array::StructArray>() {
            if let (Some(keys), Some(values)) = (
                structs.column_by_name("key"),
                structs.column_by_name("value"),
            ) {
                let keys = keys.as_string::<i32>();
                let values = values.as_string::<i32>();
                for entry in 0..keys.len() {
                    if !keys.is_null(entry) && keys.value(entry) == "topic" {
                        if !values.is_null(entry) {
                            topic = Some(values.value(entry).to_string());
                        }
                        break;
                    }
                }
            }
        }
        if topic.is_none() {
            return Err(missing_column(&format!(
                "{}.topic",
                arkflow_core::meta_columns::EXT
            )));
        }
        topics.push(topic);
    }

    Ok(PositionColumns {
        partitions,
        offsets,
        topics,
    })
}

fn missing_column(name: &str) -> Error {
    Error::Process(format!(
        "iceberg output: rows are missing the {name} column — a processor dropped the \
         __meta_* columns, which exactly-once delivery requires to deduplicate replays"
    ))
}

fn u32_values(array: &ArrayRef, name: &str, rows: usize) -> Result<Vec<Option<u32>>, Error> {
    let cast = cast_array(array.clone(), &DataType::UInt32, name)?;
    let values = cast.as_primitive::<arrow_array::types::UInt32Type>();
    if values.len() != rows {
        return Err(Error::Process(format!(
            "iceberg output: {name} column length mismatch"
        )));
    }
    Ok((0..rows)
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

fn u64_values(array: &ArrayRef, name: &str, rows: usize) -> Result<Vec<Option<u64>>, Error> {
    let cast = cast_array(array.clone(), &DataType::UInt64, name)?;
    let values = cast.as_primitive::<arrow_array::types::UInt64Type>();
    if values.len() != rows {
        return Err(Error::Process(format!(
            "iceberg output: {name} column length mismatch"
        )));
    }
    Ok((0..rows)
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

/// Compute the keep mask and the watermark updates for one batch under
/// exactly-once. Rows at or above their `(topic, partition)` watermark are
/// kept; rows below it are already committed and dropped. Rows expanded from
/// one Kafka record share an offset and are kept or dropped together.
fn position_filter(
    positions: &PositionColumns,
    watermark: &Watermark,
) -> Result<(Vec<bool>, Watermark), Error> {
    let rows = positions.partitions.len();
    if positions.offsets.len() != rows || positions.topics.len() != rows {
        return Err(Error::Process(
            "iceberg output: position columns length mismatch".to_string(),
        ));
    }
    let mut keep = vec![false; rows];
    let mut updates: Watermark = HashMap::new();
    for row in 0..rows {
        let (partition, offset, topic) = match (
            positions.partitions[row],
            positions.offsets[row],
            &positions.topics[row],
        ) {
            (Some(partition), Some(offset), Some(topic)) => (partition, offset, topic),
            _ => return Err(missing_column("source position (a __meta_* value is null)")),
        };
        let group = (topic.clone(), partition);
        let committed_through = watermark.get(&group).copied().unwrap_or(0);
        if offset >= committed_through {
            keep[row] = true;
            let next = updates.entry(group).or_insert(committed_through);
            if offset + 1 > *next {
                *next = offset + 1;
            }
        }
    }
    Ok((keep, updates))
}

/// Apply the keep mask to a batch, returning the surviving rows as new
/// column slices in the same column order.
fn take_rows(batch: &RecordBatch, keep: &[bool]) -> Result<RecordBatch, Error> {
    let indices: arrow_array::UInt32Array = keep
        .iter()
        .enumerate()
        .filter(|(_, kept)| **kept)
        .map(|(row, _)| row as u32)
        .collect();
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|column| {
            arrow_select::take::take(column, &indices, None).map_err(|error| {
                Error::Process(format!("iceberg output: row filter failed: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| Error::Process(format!("iceberg output: row filter failed: {error}")))
}

/// Merge per-batch watermark updates into the running watermark.
fn merge_watermark(base: &mut Watermark, updates: &Watermark) {
    for (group, next) in updates {
        let slot = base.entry(group.clone()).or_insert(0);
        if *next > *slot {
            *slot = *next;
        }
    }
}

/// Cast a column to `target` with non-safe options: overflow-checked for
/// integer narrowing and errors on impossible conversions.
fn cast_array(column: ArrayRef, target: &DataType, name: &str) -> Result<ArrayRef, Error> {
    let options = arrow_cast::cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    arrow_cast::cast::cast_with_options(&column, target, &options).map_err(|error| {
        Error::Process(format!(
            "iceberg output: column {name} cannot be converted to {}: {error}",
            type_name(target)
        ))
    })
}

fn type_name(data_type: &DataType) -> String {
    data_type.to_string()
}

/// True when casting `from` to `to` never loses information. Widening
/// integer conversions, widening timestamp units (same timezone-ness), and
/// f32→f64 are allowed; narrowing, integer↔float, and timestamp unit
/// narrowing are not.
fn is_lossless_cast(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }
    use DataType::*;
    match (from, to) {
        // Signed widening.
        (Int8, Int16 | Int32 | Int64) => true,
        (Int16, Int32 | Int64) => true,
        (Int32, Int64) => true,
        // Unsigned widening.
        (UInt8, UInt16 | UInt32 | UInt64) => true,
        (UInt16, UInt32 | UInt64) => true,
        (UInt32, UInt64) => true,
        // Unsigned → signed, value-lossless for the narrow widths.
        (UInt8, Int16 | Int32 | Int64) => true,
        (UInt16, Int32 | Int64) => true,
        (UInt32, Int64) => true,
        // Float widening.
        (Float16, Float32 | Float64) => true,
        (Float32, Float64) => true,
        // Timestamps: widening units only, timezone-ness must match.
        (Timestamp(from_unit, from_tz), Timestamp(to_unit, to_tz)) => {
            from_tz == to_tz && unit_rank(from_unit) <= unit_rank(to_unit)
        }
        (Date32, Date64) => true,
        (Time32(from_unit), Time64(to_unit)) => unit_rank(from_unit) <= unit_rank(to_unit),
        // Strings and binaries grow, never shrink.
        (Utf8, LargeUtf8) => true,
        (Binary, LargeBinary) => true,
        _ => false,
    }
}

fn unit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// Map one raw input batch onto the table's canonical Arrow schema:
/// name-matched columns, `__meta_*` columns silently dropped, unknown batch
/// columns rejected, required table columns enforced, lossless casts applied.
/// Optional table columns missing from the batch are null-filled (Iceberg
/// data files may omit optional columns).
fn map_to_canonical(
    batch: &RecordBatch,
    canonical: &ArrowSchemaRef,
    iceberg_schema: &iceberg::spec::Schema,
) -> Result<RecordBatch, Error> {
    let canonical_fields = canonical.fields();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(canonical_fields.len());

    for field in canonical_fields.iter() {
        let name = field.name();
        match batch.column_by_name(name) {
            Some(column) => {
                if !field.is_nullable() && column.null_count() > 0 {
                    return Err(Error::Process(format!(
                        "iceberg output: column {name} is non-nullable in the table but the \
                         batch contains nulls"
                    )));
                }
                let column = if column.data_type() == field.data_type() {
                    column.clone()
                } else if is_lossless_cast(column.data_type(), field.data_type()) {
                    cast_array(column.clone(), field.data_type(), name)?
                } else {
                    return Err(Error::Process(format!(
                        "iceberg output: column {name} has type {} but the table requires {} \
                         and no lossless conversion exists",
                        type_name(column.data_type()),
                        type_name(field.data_type())
                    )));
                };
                columns.push(column);
            }
            None => {
                let required = iceberg_schema
                    .field_by_name(name)
                    .map(|field| field.required)
                    .unwrap_or(false);
                if required {
                    return Err(Error::Process(format!(
                        "iceberg output: batch is missing required table column {name}"
                    )));
                }
                columns.push(arrow_array::new_null_array(
                    field.data_type(),
                    batch.num_rows(),
                ));
            }
        }
    }

    // Unknown columns are a pipeline mistake — fail loudly instead of
    // silently dropping data. `__meta_*` columns are infrastructure.
    for field in batch.schema().fields() {
        let name = field.name();
        if canonical.field_with_name(name).is_err() && !name.starts_with("__meta_") {
            return Err(Error::Process(format!(
                "iceberg output: batch column {name} is not a table column — remove it from \
                 the projection or add it to the table"
            )));
        }
    }

    RecordBatch::try_new(Arc::new(canonical.as_ref().clone()), columns).map_err(|error| {
        Error::Process(format!(
            "iceberg output: mapping rows onto the table schema failed: {error}"
        ))
    })
}

/// Immutable snapshot of the connected state used for one attempt.
#[derive(Clone)]
struct AttemptState {
    catalog: Arc<RestCatalog>,
    table: Table,
    watermark: Option<Watermark>,
}

impl IcebergOutput {
    async fn connected_state(&self) -> Result<AttemptState, Error> {
        let inner = self.inner.lock().await;
        let connected = inner.as_ref().ok_or_else(|| {
            Error::Connection(
                "iceberg output: write attempted before connect() succeeded".to_string(),
            )
        })?;
        Ok(AttemptState {
            catalog: Arc::clone(&connected.catalog),
            table: connected.table.clone(),
            watermark: connected.watermark.clone(),
        })
    }

    /// Replace the connected table (and watermark) with catalog truth after a
    /// transient failure: a commit whose outcome is unknown may have been
    /// accepted server-side, so the catalog is the only source of truth.
    async fn reload_state(&self) -> Result<AttemptState, Error> {
        let catalog = Self::build_catalog(&self.config).await?;
        let namespace = self.config.namespace_ident()?;
        let ident = TableIdent::new(namespace, self.config.table.clone());
        let table = catalog.load_table(&ident).await.map_err(|error| {
            Error::Connection(format!(
                "iceberg output: reloading table state failed: {error}"
            ))
        })?;
        let watermark = if self.config.exactly_once {
            let writer_id = self.config.writer_id.as_deref().expect("validated");
            Self::recover_watermark(&table, writer_id).await
        } else {
            None
        };
        let state = AttemptState {
            catalog: Arc::clone(&catalog),
            table: table.clone(),
            watermark,
        };
        *self.inner.lock().await = Some(Connected {
            catalog,
            table,
            watermark: state.watermark.clone(),
        });
        Ok(state)
    }

    async fn persist_state(
        &self,
        catalog: Arc<RestCatalog>,
        table: Table,
        watermark: Option<Watermark>,
    ) {
        *self.inner.lock().await = Some(Connected {
            catalog,
            table,
            watermark,
        });
    }

    /// Warning contract: ten commits inside two seconds means the pipeline is
    /// missing a `batch` processor in front of this output. Fires at most
    /// once per minute and never alters delivery.
    fn note_commit_and_warn(&self) {
        let now = Instant::now();
        let mut times = self.commit_times.lock().expect("commit times lock");
        times.push_back(now);
        while let Some(front) = times.front() {
            if now.duration_since(*front).as_secs() >= 2 {
                times.pop_front();
            } else {
                break;
            }
        }
        if times.len() >= 10 {
            let mut last = self.last_rate_warn.lock().expect("rate warn lock");
            let should_warn = match *last {
                Some(previous) => now.duration_since(previous).as_secs() >= 60,
                None => true,
            };
            if should_warn {
                *last = Some(now);
                warn!(
                    "iceberg output: {} commits in the last 2 seconds — put a `batch` \
                     processor (count + timeout_ms) in front of this output to control \
                     commit cadence and data file size",
                    times.len()
                );
            }
        }
    }

    /// Encode and upload the surviving rows as data files, routed by the
    /// table's partition spec.
    async fn encode_batches(
        &self,
        table: &Table,
        mapped: Vec<RecordBatch>,
        file_prefix: &str,
    ) -> Result<Vec<iceberg::spec::DataFile>, Error> {
        let iceberg_schema = table.metadata().current_schema();
        let mut data_files = Vec::new();
        for batch in mapped {
            let spec = table.metadata().default_partition_spec();
            let partitioned = !spec.is_unpartitioned();
            let parts: Vec<(Option<PartitionKey>, RecordBatch)> = if partitioned {
                let calculator =
                    iceberg::arrow::partition_value_calculator::PartitionValueCalculator::try_new(
                        spec,
                        iceberg_schema,
                    )
                    .map_err(|error| {
                        Error::Process(format!(
                            "iceberg output: partition spec cannot be evaluated: {error}"
                        ))
                    })?;
                let splitter =
                    iceberg::arrow::record_batch_partition_splitter::RecordBatchPartitionSplitter::try_new(
                        iceberg_schema.clone(),
                        spec.clone(),
                        Some(calculator),
                    )
                    .map_err(|error| {
                        Error::Process(format!(
                            "iceberg output: partition splitting failed: {error}"
                        ))
                    })?;
                splitter
                    .split(&batch)
                    .map_err(|error| {
                        Error::Process(format!(
                            "iceberg output: partition splitting failed: {error}"
                        ))
                    })?
                    .into_iter()
                    .map(|(key, batch)| (Some(key), batch))
                    .collect()
            } else {
                vec![(None, batch)]
            };

            for (partition_key, part) in parts {
                if part.num_rows() == 0 {
                    continue;
                }
                let codec = self
                    .config
                    .write
                    .compression
                    .map(ParquetCompressionCodec::from)
                    .unwrap_or_else(|| table_compression(table.metadata().properties()));
                let parquet_builder = ParquetWriterBuilder::new(
                    WriterProperties::builder().set_compression(codec).build(),
                    iceberg_schema.clone(),
                );
                let target_size = self
                    .config
                    .write
                    .target_file_size_bytes
                    .unwrap_or_else(|| table_target_file_size(table.metadata().properties()));
                let location_gen =
                    DefaultLocationGenerator::new(table.metadata()).map_err(|error| {
                        Error::Process(format!(
                            "iceberg output: table location is invalid: {error}"
                        ))
                    })?;
                let file_name_gen = DefaultFileNameGenerator::new(
                    // Unique per attempt and per file slot: the duplicate-file
                    // check rejects a path already referenced by the table and
                    // the generator's numbering is deterministic per prefix.
                    format!(
                        "{file_prefix}-{}",
                        self.file_prefix_seq.fetch_add(1, Ordering::Relaxed)
                    ),
                    None,
                    iceberg::spec::DataFileFormat::Parquet,
                );
                let rolling = RollingFileWriterBuilder::new(
                    parquet_builder,
                    target_size,
                    table.file_io().clone(),
                    location_gen,
                    file_name_gen,
                );
                let mut writer = DataFileWriterBuilder::new(rolling)
                    .build(partition_key)
                    .await
                    .map_err(|error| transient_error("opening a data file", error))?;
                writer
                    .write(part)
                    .await
                    .map_err(|error| transient_error("writing rows to a data file", error))?;
                let files = writer
                    .close()
                    .await
                    .map_err(|error| transient_error("finishing a data file", error))?;
                data_files.extend(files);
            }
        }
        Ok(data_files)
    }

    /// Map every input batch onto the table schema; under exactly-once,
    /// filter replayed rows against the attempt's watermark and collect the
    /// watermark updates the surviving rows imply.
    fn map_and_filter(
        &self,
        table: &Table,
        msgs: &[MessageBatchRef],
        watermark: &Watermark,
    ) -> Result<(Vec<RecordBatch>, Watermark, usize), Error> {
        let canonical =
            schema_to_arrow_schema(table.metadata().current_schema()).map_err(|error| {
                Error::Process(format!(
                    "iceberg output: table schema has no Arrow projection: {error}"
                ))
            })?;
        let canonical = Arc::new(canonical);
        let exactly_once = self.config.exactly_once;
        let mut mapped: Vec<RecordBatch> = Vec::with_capacity(msgs.len());
        let mut updates_total: Watermark = HashMap::new();
        let mut kept_rows = 0usize;
        for msg in msgs {
            let batch = msg.record_batch();
            if batch.num_rows() == 0 {
                continue;
            }
            // Positions come from the RAW batch: the canonical mapping keeps
            // only table columns, so `__meta_*` is stripped there.
            let positions = if exactly_once {
                Some(extract_positions(batch)?)
            } else {
                None
            };
            let mapped_batch =
                map_to_canonical(batch, &canonical, table.metadata().current_schema())?;
            if let Some(positions) = positions {
                let (keep, updates) = position_filter(&positions, watermark)?;
                merge_watermark(&mut updates_total, &updates);
                let filtered = take_rows(&mapped_batch, &keep)?;
                kept_rows += filtered.num_rows();
                mapped.push(filtered);
            } else {
                kept_rows += mapped_batch.num_rows();
                mapped.push(mapped_batch);
            }
        }
        Ok((mapped, updates_total, kept_rows))
    }

    async fn write_impl(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        if msgs.is_empty() {
            return Ok(());
        }
        // Attempt loop: attempt 1 uses the connected state; every transient
        // failure reloads table + watermark from the catalog before the next
        // attempt, so the filter runs against catalog truth — a commit whose
        // outcome was unknown cannot produce duplicates on retry.
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            let state = if attempt == 1 {
                self.connected_state().await?
            } else {
                if attempt > 3 {
                    warn!(
                        attempt,
                        "iceberg output: exhausted transient retries — the acknowledgement \
                         is withheld and the range replays on recovery"
                    );
                    return Err(Error::Disconnection);
                }
                warn!(
                    attempt,
                    "iceberg output: transient failure — reloading table state before \
                     retrying"
                );
                match self.reload_state().await {
                    Ok(state) => state,
                    Err(error) if attempt >= 3 => return Err(error),
                    Err(error) => return Err(error),
                }
            };

            let (mapped, updates_total, kept_rows) = self.map_and_filter(
                &state.table,
                msgs,
                &state.watermark.clone().unwrap_or_default(),
            )?;
            if kept_rows == 0 && updates_total.is_empty() {
                // Everything was already committed (or the call carried no
                // rows): acknowledge without creating a snapshot.
                return Ok(());
            }

            let file_prefix = format!(
                "arkflow-{}-{attempt}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_nanos())
                    .unwrap_or(0)
            );
            let data_files = match self
                .encode_batches(&state.table, mapped, &file_prefix)
                .await
            {
                Ok(files) => files,
                Err(error) if error_is_transient(&error) => {
                    if attempt >= 3 {
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            if data_files.is_empty() {
                return Ok(());
            }

            let transaction = Transaction::new(&state.table);
            let append = transaction.fast_append().with_check_duplicate(true);
            let mut merged = state.watermark.clone().unwrap_or_default();
            merge_watermark(&mut merged, &updates_total);
            let append = if self.config.exactly_once {
                let writer_id = self.config.writer_id.as_deref().expect("validated");
                let mut summary = HashMap::new();
                summary.insert(watermark_property(writer_id), watermark_json(&merged));
                append.set_snapshot_properties(summary)
            } else {
                append
            };
            let transaction = append
                .add_data_files(data_files)
                .apply(transaction)
                .map_err(|error| {
                    Error::Process(format!(
                        "iceberg output: preparing the commit failed: {error}"
                    ))
                })?;
            match transaction.commit(state.catalog.as_ref()).await {
                Ok(committed) => {
                    let watermark = if self.config.exactly_once {
                        Some(merged)
                    } else {
                        None
                    };
                    self.persist_state(state.catalog.clone(), committed, watermark)
                        .await;
                    debug!(
                        rows = kept_rows,
                        attempt, "iceberg output: committed one snapshot"
                    );
                    self.note_commit_and_warn();
                    return Ok(());
                }
                Err(error) => {
                    let mapped_error = commit_error(error);
                    if error_is_transient(&mapped_error) && attempt < 3 {
                        warn!(
                            attempt,
                            error = %mapped_error,
                            "iceberg output: transient commit failure — the commit may or \
                             may not have been accepted; reloading catalog truth before \
                             retrying"
                        );
                        continue;
                    }
                    return Err(mapped_error);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Output for IcebergOutput {
    async fn connect(&self) -> Result<(), Error> {
        self.connect_state().await
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        self.write_impl(std::slice::from_ref(&msg)).await
    }

    /// One call, one atomic Iceberg append: the kernel settles the source
    /// acknowledgements only after this returns `Ok`.
    async fn write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        self.write_impl(msgs).await
    }

    async fn close(&self) -> Result<(), Error> {
        // Nothing to flush: every write_batch committed before returning Ok.
        self.close_state().await
    }
}

fn error_is_transient(error: &Error) -> bool {
    let message = error.to_string();
    is_transient(&message)
}

fn commit_error(error: iceberg::Error) -> Error {
    let message = error.to_string();
    if message.contains("Cannot add files that are already referenced by table") {
        // The duplicate-file safety net fired: this attempt re-used a file
        // path already committed. Deterministic — file naming is at fault.
        Error::Process(format!(
            "iceberg output: duplicate data file commit rejected: {message}"
        ))
    } else if is_transient(&message)
        || message.contains("Commit Failed")
        || message.contains("Retries exceeded")
    {
        // Optimistic-concurrency conflicts and transport failures are retried
        // by the attempt loop against reloaded state.
        transient_error("catalog commit failed", &message)
    } else {
        Error::Process(format!("iceberg output: commit rejected: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Int64Array, MapArray, StringArray, StructArray as ArrowStructArray, UInt32Array,
        UInt64Array,
    };
    use arrow_buffer::OffsetBuffer as ArrowOffsetBuffer;
    use arrow_buffer::ScalarBuffer as ArrowScalarBuffer;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};

    fn config_with(exactly_once: bool, writer_id: Option<&str>) -> IcebergOutputConfig {
        serde_json::from_value(serde_json::json!({
            "catalog": {"uri": "http://catalog:8181/catalog", "warehouse": "demo"},
            "namespace": "analytics",
            "table": "events",
            "exactly_once": exactly_once,
            "writer_id": writer_id,
        }))
        .expect("config")
    }

    #[test]
    fn exactly_once_requires_writer_id() {
        let config = config_with(true, None);
        assert!(config.validate().is_err());
        let config = config_with(true, Some("group-1"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn writer_id_without_exactly_once_is_rejected() {
        let config = config_with(false, Some("group-1"));
        assert!(config
            .validate()
            .err()
            .map(|error| error.to_string().contains("exactly_once"))
            .unwrap_or(false));
    }

    #[test]
    fn empty_fields_are_rejected() {
        let mut config = config_with(false, None);
        config.catalog.uri = "  ".to_string();
        assert!(config.validate().is_err());
        let mut config = config_with(false, None);
        config.table = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn target_file_size_must_be_positive() {
        let mut config = config_with(false, None);
        config.write.target_file_size_bytes = Some(0);
        assert!(config.validate().is_err());
        config.write.target_file_size_bytes = Some(1024);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn s3_credentials_must_come_in_pairs() {
        let mut config = config_with(false, None);
        config.s3 = Some(
            serde_json::from_value(serde_json::json!({
                "access_key_id": "ak"
            }))
            .expect("s3 config"),
        );
        assert!(config.validate().is_err());
        config.s3 = Some(
            serde_json::from_value(serde_json::json!({
                "access_key_id": "ak",
                "secret_access_key": "sk",
                "path_style_access": true
            }))
            .expect("s3 config"),
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn watermark_json_round_trips() {
        let mut watermark = Watermark::new();
        watermark.insert(("events".to_string(), 0), 42);
        watermark.insert(("events".to_string(), 3), 7);
        watermark.insert(("other".to_string(), 11), 1);
        let raw = watermark_json(&watermark);
        let decoded = watermark_from_json(&raw).expect("decode");
        assert_eq!(watermark, decoded);
        assert!(watermark_from_json("not json").is_none());
    }

    fn positions_from(
        partitions: Vec<Option<u32>>,
        offsets: Vec<Option<u64>>,
        topics: Vec<Option<&str>>,
    ) -> PositionColumns {
        PositionColumns {
            partitions,
            offsets,
            topics: topics
                .into_iter()
                .map(|topic| topic.map(str::to_string))
                .collect(),
        }
    }

    #[test]
    fn position_filter_keeps_rows_at_or_above_the_watermark() {
        let mut watermark = Watermark::new();
        watermark.insert(("events".to_string(), 0), 151);
        let positions = positions_from(
            vec![Some(0), Some(0), Some(0), Some(1)],
            vec![Some(100), Some(150), Some(150), Some(200)],
            vec![
                Some("events"),
                Some("events"),
                Some("events"),
                Some("events"),
            ],
        );
        let (keep, updates) = position_filter(&positions, &watermark).expect("filter");
        // The watermark is the exclusive next offset 151: offsets 100 and 150
        // (both copies) are committed and dropped; only partition 1's row survives.
        assert_eq!(keep, vec![false, false, false, true]);
        // Partition 1 advances to 201; partition 0 has no surviving rows.
        assert_eq!(updates.get(&("events".to_string(), 0)), None);
        assert_eq!(updates.get(&("events".to_string(), 1)), Some(&201));
    }

    #[test]
    fn position_filter_null_positions_fail_closed() {
        let positions = positions_from(vec![Some(0)], vec![None], vec![Some("events")]);
        let error = position_filter(&positions, &Watermark::new()).unwrap_err();
        assert!(error.to_string().contains("position"));
    }

    #[test]
    fn merge_watermark_only_advances() {
        let mut base = Watermark::new();
        base.insert(("events".to_string(), 0), 100);
        let mut updates = Watermark::new();
        updates.insert(("events".to_string(), 0), 50);
        updates.insert(("events".to_string(), 2), 9);
        merge_watermark(&mut base, &updates);
        assert_eq!(base.get(&("events".to_string(), 0)), Some(&100));
        assert_eq!(base.get(&("events".to_string(), 2)), Some(&9));
    }

    fn batch_with_positions(rows: usize, with_ext: bool) -> RecordBatch {
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        fields.push(ArrowField::new("id", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(vec![1i64; rows])));
        fields.push(ArrowField::new(
            arkflow_core::meta_columns::PARTITION,
            DataType::UInt32,
            false,
        ));
        columns.push(Arc::new(UInt32Array::from(vec![0u32; rows])));
        fields.push(ArrowField::new(
            arkflow_core::meta_columns::OFFSET,
            DataType::UInt64,
            false,
        ));
        columns.push(Arc::new(UInt64Array::from(
            (0..rows as u64).collect::<Vec<_>>(),
        )));
        if with_ext {
            let entries_struct = ArrowStructArray::from(vec![
                (
                    Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                    Arc::new(StringArray::from(vec!["topic"; rows])) as ArrayRef,
                ),
                (
                    Arc::new(ArrowField::new("value", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["events"; rows])) as ArrayRef,
                ),
            ]);
            let entry_offsets = ArrowOffsetBuffer::new(ArrowScalarBuffer::from(
                (0..=rows as i32).collect::<Vec<_>>(),
            ));
            fields.push(ArrowField::new(
                arkflow_core::meta_columns::EXT,
                DataType::Map(
                    Arc::new(ArrowField::new(
                        "entries",
                        entries_struct.data_type().clone(),
                        false,
                    )),
                    false,
                ),
                false,
            ));
            columns.push(Arc::new(MapArray::new(
                Arc::new(ArrowField::new(
                    "entries",
                    entries_struct.data_type().clone(),
                    false,
                )),
                entry_offsets,
                entries_struct,
                None,
                false,
            )));
        }
        let schema = ArrowSchema::new(fields);
        RecordBatch::try_new(Arc::new(schema), columns).expect("batch")
    }

    #[test]
    fn extract_positions_reads_the_meta_columns() {
        let batch = batch_with_positions(3, true);
        let positions = extract_positions(&batch).expect("positions");
        assert_eq!(positions.partitions, vec![Some(0), Some(0), Some(0)]);
        assert_eq!(positions.offsets, vec![Some(0), Some(1), Some(2)]);
        assert_eq!(
            positions.topics,
            vec![
                Some("events".to_string()),
                Some("events".to_string()),
                Some("events".to_string())
            ]
        );
    }

    #[test]
    fn extract_positions_without_ext_fails_closed() {
        let batch = batch_with_positions(2, false);
        let error = extract_positions(&batch).unwrap_err();
        assert!(error.to_string().contains("__meta_ext"));
    }

    #[test]
    fn lossless_cast_table() {
        use DataType::*;
        assert!(is_lossless_cast(&Int32, &Int64));
        assert!(is_lossless_cast(&UInt16, &Int64));
        assert!(!is_lossless_cast(&Int64, &Int32));
        assert!(!is_lossless_cast(&Int64, &Float64));
        assert!(!is_lossless_cast(&Float64, &Int32));
        assert!(is_lossless_cast(
            &Timestamp(TimeUnit::Microsecond, None),
            &Timestamp(TimeUnit::Nanosecond, None)
        ));
        assert!(!is_lossless_cast(
            &Timestamp(TimeUnit::Nanosecond, None),
            &Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(!is_lossless_cast(
            &Timestamp(TimeUnit::Microsecond, None),
            &Timestamp(TimeUnit::Microsecond, Some("+01:00".into()))
        ));
        assert!(is_lossless_cast(&Utf8, &LargeUtf8));
        assert!(!is_lossless_cast(&Utf8, &Int64));
    }

    #[test]
    fn transient_classifier_matches_phrases_not_numbers() {
        assert!(is_transient("request timed out after 30s"));
        assert!(is_transient("HTTP 503 Service Unavailable"));
        assert!(is_transient("connection reset by peer"));
        // Deterministic failures must not be classified as transient.
        assert!(!is_transient("HTTP 400 Bad Request: schema mismatch"));
        assert!(!is_transient("column id cannot be converted"));
        assert!(!is_transient("table not found"));
    }

    #[test]
    fn watermark_property_naming_is_stable() {
        assert_eq!(
            watermark_property("group-1"),
            "arkflow.writer.group-1.offsets"
        );
    }

    #[test]
    fn commit_rate_burst_warns_once_per_minute() {
        let output = IcebergOutput::new(config_with(false, None));
        // Ten commits within the two-second window trip the warning once.
        for _ in 0..10 {
            output.note_commit_and_warn();
        }
        assert!(output.last_rate_warn.lock().expect("lock").is_some());
        // More commits inside the same minute stay silent.
        for _ in 0..10 {
            output.note_commit_and_warn();
        }
        let warned = *output.last_rate_warn.lock().expect("lock");
        assert!(warned.is_some());
    }

    #[test]
    fn commit_rate_below_threshold_stays_silent() {
        let output = IcebergOutput::new(config_with(false, None));
        // Nine commits inside the window — one below the threshold — never warn.
        for _ in 0..9 {
            output.note_commit_and_warn();
        }
        assert!(output.last_rate_warn.lock().expect("lock").is_none());
    }
}
