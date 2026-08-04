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

//! InfluxDB output component
//!
//! Write the processed data to InfluxDB 2.x

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// InfluxDB output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDBOutputConfig {
    /// InfluxDB server URL
    pub url: String,
    /// Organization name
    pub org: String,
    /// Bucket name
    pub bucket: String,
    /// Authentication token
    pub token: String,

    /// Measurement name
    pub measurement: String,

    /// Tag mappings (indexed fields)
    pub tags: Option<Vec<TagMapping>>,

    /// Field mappings (value fields)
    pub fields: Vec<FieldMapping>,

    /// Timestamp field name
    pub timestamp_field: Option<String>,

    /// Batch size for writes
    pub batch_size: Option<usize>,

    /// Flush interval in seconds
    pub flush_interval: Option<u64>,

    /// Retry count on failure
    pub retry_count: Option<u32>,

    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

/// Tag field mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagMapping {
    /// Field name in MessageBatch
    pub field: String,
    /// Tag name in InfluxDB
    pub tag_name: String,
}

/// Field mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Field name in MessageBatch
    pub field: String,
    /// Field name in InfluxDB
    pub field_name: String,
    /// Field type (optional)
    pub field_type: Option<FieldType>,
}

/// Field type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Float,
    Integer,
    Boolean,
    String,
}

/// InfluxDB output component
pub struct InfluxDBOutput {
    config: InfluxDBOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    batch_buffer: Arc<Mutex<Vec<String>>>,
    last_flush: Arc<Mutex<Instant>>,
    connected: AtomicBool,
}

impl InfluxDBOutput {
    /// Create a new InfluxDB output component
    pub fn new(config: InfluxDBOutputConfig) -> Result<Self, Error> {
        if config.batch_size == Some(0) {
            return Err(Error::Config(
                "InfluxDB batch_size must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            batch_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            connected: AtomicBool::new(false),
        })
    }

    /// Get column index by name
    fn get_column_index(msg: &MessageBatch, field_name: &str) -> Option<usize> {
        let schema = msg.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            if field.name() == field_name {
                return Some(i);
            }
        }
        None
    }

    /// Convert MessageBatch to InfluxDB Line Protocol
    fn convert_to_line_protocol(&self, msg: &MessageBatch) -> Result<Vec<String>, Error> {
        let mut lines = Vec::new();

        // Get measurement
        let measurement = escape_identifier(&self.config.measurement);

        // Get row count
        let num_rows = msg.len();

        for row_idx in 0..num_rows {
            let mut line_parts = Vec::new();

            // 1. Add measurement
            line_parts.push(measurement.clone());

            // 2. Add tags
            let mut tag_pairs = Vec::new();
            if let Some(ref tags) = self.config.tags {
                for tag_mapping in tags {
                    if let Some(col_idx) = Self::get_column_index(msg, &tag_mapping.field) {
                        let column = msg.column(col_idx);
                        if let Some(value) = Self::get_string_value(column, row_idx) {
                            let escaped_key = escape_identifier(&tag_mapping.tag_name);
                            let escaped_value = escape_tag_value(&value);
                            tag_pairs.push(format!("{}={}", escaped_key, escaped_value));
                        }
                    }
                }
            }

            if !tag_pairs.is_empty() {
                line_parts[0].push(',');
                line_parts[0].push_str(&tag_pairs.join(","));
            }

            // 3. Add fields
            let mut field_pairs = Vec::new();
            for field_mapping in &self.config.fields {
                if let Some(col_idx) = Self::get_column_index(msg, &field_mapping.field) {
                    let column = msg.column(col_idx);

                    match field_mapping.field_type.as_ref() {
                        Some(FieldType::Float) => {
                            if let Some(value) = Self::get_float_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}", escaped_key, value));
                            }
                        }
                        Some(FieldType::Integer) => {
                            if let Some(value) = Self::get_int_value(column, row_idx)? {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}i", escaped_key, value));
                            }
                        }
                        Some(FieldType::Boolean) => {
                            if let Some(value) = Self::get_bool_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}", escaped_key, value));
                            }
                        }
                        Some(FieldType::String) | None => {
                            if let Some(value) = Self::get_string_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                let escaped_value = escape_field_value(&value);
                                field_pairs.push(format!("{}=\"{}\"", escaped_key, escaped_value));
                            }
                        }
                    }
                }
            }

            if field_pairs.is_empty() {
                continue; // Skip rows with no fields
            }

            line_parts.push(field_pairs.join(","));

            // 4. Add timestamp
            if let Some(ref ts_field) = self.config.timestamp_field {
                if let Some(col_idx) = Self::get_column_index(msg, ts_field) {
                    let column = msg.column(col_idx);
                    if let Some(ts) = Self::get_int_value(column, row_idx)? {
                        // Assume timestamp is in nanoseconds
                        line_parts.push(format!("{}", ts));
                    } else {
                        // Use current time
                        line_parts.push(format!("{}", Self::current_timestamp_nanos()));
                    }
                } else {
                    line_parts.push(format!("{}", Self::current_timestamp_nanos()));
                }
            } else {
                line_parts.push(format!("{}", Self::current_timestamp_nanos()));
            }

            lines.push(line_parts.join(" "));
        }

        Ok(lines)
    }

    /// Get string value from column
    fn get_string_value(column: &dyn Array, row_index: usize) -> Option<String> {
        macro_rules! primitive_string {
            ($array:ty) => {{
                let array = column
                    .as_any()
                    .downcast_ref::<$array>()
                    .expect("Arrow data type and array type must match");
                if array.is_null(row_index) {
                    None
                } else {
                    Some(array.value(row_index).to_string())
                }
            }};
        }

        let data_type = column.data_type();
        match data_type {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                if array.is_null(row_index) {
                    None
                } else {
                    Some(array.value(row_index).to_string())
                }
            }
            DataType::LargeUtf8 => {
                let array = column
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                    .unwrap();
                (!array.is_null(row_index)).then(|| array.value(row_index).to_string())
            }
            DataType::Int8 => primitive_string!(Int8Array),
            DataType::Int16 => primitive_string!(Int16Array),
            DataType::Int32 => primitive_string!(Int32Array),
            DataType::Int64 => primitive_string!(Int64Array),
            DataType::UInt8 => primitive_string!(UInt8Array),
            DataType::UInt16 => primitive_string!(UInt16Array),
            DataType::UInt32 => primitive_string!(UInt32Array),
            DataType::UInt64 => primitive_string!(UInt64Array),
            DataType::Float32 => primitive_string!(Float32Array),
            DataType::Float64 => primitive_string!(Float64Array),
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.is_null(row_index) {
                    None
                } else {
                    Some(array.value(row_index).to_string())
                }
            }
            _ => None,
        }
    }

    /// Get float value from column
    fn get_float_value(column: &dyn Array, row_index: usize) -> Option<f64> {
        macro_rules! primitive_float {
            ($array:ty) => {
                if let Some(array) = column.as_any().downcast_ref::<$array>() {
                    return (!array.is_null(row_index)).then(|| array.value(row_index) as f64);
                }
            };
        }

        primitive_float!(Float32Array);
        primitive_float!(Float64Array);
        primitive_float!(Int8Array);
        primitive_float!(Int16Array);
        primitive_float!(Int32Array);
        primitive_float!(Int64Array);
        primitive_float!(UInt8Array);
        primitive_float!(UInt16Array);
        primitive_float!(UInt32Array);
        primitive_float!(UInt64Array);
        None
    }

    /// Get int value from column
    fn get_int_value(column: &dyn Array, row_index: usize) -> Result<Option<i64>, Error> {
        macro_rules! signed_int {
            ($array:ty) => {
                if let Some(array) = column.as_any().downcast_ref::<$array>() {
                    return Ok((!array.is_null(row_index)).then(|| array.value(row_index) as i64));
                }
            };
        }
        macro_rules! unsigned_int {
            ($array:ty) => {
                if let Some(array) = column.as_any().downcast_ref::<$array>() {
                    if array.is_null(row_index) {
                        return Ok(None);
                    }
                    return i64::try_from(array.value(row_index))
                        .map(Some)
                        .map_err(|_| {
                            Error::Process(
                                "Unsigned Arrow integer exceeds InfluxDB's signed i64 range"
                                    .to_string(),
                            )
                        });
                }
            };
        }

        signed_int!(Int8Array);
        signed_int!(Int16Array);
        signed_int!(Int32Array);
        signed_int!(Int64Array);
        unsigned_int!(UInt8Array);
        unsigned_int!(UInt16Array);
        unsigned_int!(UInt32Array);
        unsigned_int!(UInt64Array);
        Ok(None)
    }

    /// Get bool value from column
    fn get_bool_value(column: &dyn Array, row_index: usize) -> Option<bool> {
        if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
            return (!array.is_null(row_index)).then(|| array.value(row_index));
        }
        None
    }

    /// Get current timestamp in nanoseconds
    fn current_timestamp_nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }

    /// Check if buffer should be flushed
    async fn should_flush(&self) -> bool {
        let buffer = self.batch_buffer.lock().await;
        let batch_size = self.config.batch_size.unwrap_or(1000);

        // Flush if buffer size exceeds batch size
        if buffer.len() >= batch_size {
            return true;
        }

        // Flush if flush interval exceeded
        if let Some(interval_secs) = self.config.flush_interval {
            let last_flush = self.last_flush.lock().await;
            let elapsed = last_flush.elapsed().as_secs();
            if elapsed >= interval_secs {
                return true;
            }
        }

        false
    }

    /// Flush batch buffer to InfluxDB
    async fn flush(&self) -> Result<(), Error> {
        let mut buffer = self.batch_buffer.lock().await;

        if buffer.is_empty() {
            return Ok(());
        }

        let client_guard = self.client.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client not initialized".to_string()))?;

        // Build URL
        let url = format!("{}/api/v2/write", self.config.url.trim_end_matches('/'));

        // Join lines with newline
        let body = buffer.join("\n");

        // Retry logic
        let retry_count = self.config.retry_count.unwrap_or(3);
        let mut last_error = None;

        let attempts = retry_count.max(1);
        for attempt in 0..attempts {
            let response = client
                .post(&url)
                .query(&[
                    ("org", self.config.org.as_str()),
                    ("bucket", self.config.bucket.as_str()),
                    ("precision", "ns"),
                ])
                .header("Authorization", format!("Token {}", self.config.token))
                .header("Content-Type", "text/plain")
                .body(body.clone())
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        buffer.clear();
                        *self.last_flush.lock().await = Instant::now();
                        return Ok(());
                    } else {
                        let status = resp.status();
                        let error_body = resp.text().await.unwrap_or_default();
                        last_error = Some(Error::Connection(format!(
                            "InfluxDB write failed: {} - {}",
                            status, error_body
                        )));
                    }
                }
                Err(e) => {
                    last_error = Some(Error::Connection(format!("InfluxDB request failed: {}", e)));
                }
            }

            // Exponential backoff
            if attempt + 1 < attempts {
                tokio::time::sleep(std::time::Duration::from_millis(100 * 2_u64.pow(attempt)))
                    .await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            Error::Connection("InfluxDB write failed after retries".to_string())
        }))
    }
}

#[async_trait]
impl Output for InfluxDBOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Create HTTP client
        let timeout = std::time::Duration::from_millis(self.config.timeout_ms.unwrap_or(5000));
        let client_builder = Client::builder().timeout(timeout);

        let client_arc = self.client.clone();
        client_arc.lock().await.replace(
            client_builder
                .build()
                .map_err(|e| Error::Connection(format!("Failed to create HTTP client: {}", e)))?,
        );

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        self.write_batch(&[msg]).await
    }

    async fn write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection(
                "InfluxDB output not connected".to_string(),
            ));
        }

        let mut lines = Vec::new();
        for msg in msgs {
            lines.extend(self.convert_to_line_protocol(msg)?);
        }
        if !lines.is_empty() {
            let mut buffer = self.batch_buffer.lock().await;
            buffer.extend(lines);
            drop(buffer);

            // Check once per acknowledgement range so one write_batch call is
            // not split into multiple requests merely because it has several
            // MessageBatch values.
            if self.should_flush().await {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Flush any remaining data
        self.flush().await?;

        // Close client
        let mut client_guard = self.client.lock().await;
        *client_guard = None;

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

/// Escape measurement/tag/field keys
fn escape_identifier(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

/// Escape tag values
fn escape_tag_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

/// Escape field string values
fn escape_field_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

pub(crate) struct InfluxDBOutputBuilder;

impl OutputBuilder for InfluxDBOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if codec.is_some() {
            return Err(Error::Config(
                "InfluxDB output requires typed Arrow fields and does not support codecs"
                    .to_string(),
            ));
        }
        let config: InfluxDBOutputConfig = parse_config(config, "InfluxDBOutput config")?;

        Ok(Arc::new(InfluxDBOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("influxdb", Arc::new(InfluxDBOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "influxdb",
        "Writes time-series data to InfluxDB v2.x using the Line Protocol.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "InfluxDB server URL (e.g. http://localhost:8086)."},
                "org": {"type": "string", "description": "Organization name."},
                "bucket": {"type": "string", "description": "Destination bucket."},
                "token": {"type": "string", "description": "Authentication token."},
                "measurement": {"type": "string", "description": "Measurement name."},
                "tags": {"type": "array", "items": {"type": "object", "properties": {"field": {"type": "string"}, "tag_name": {"type": "string"}}, "required": ["field", "tag_name"]}, "description": "Tag mappings (label fields)."},
                "fields": {"type": "array", "items": {"type": "object", "properties": {"field": {"type": "string"}, "field_name": {"type": "string"}, "field_type": {"enum": ["float", "integer", "boolean", "string"]}}, "required": ["field", "field_name"]}, "description": "Field mappings (value fields)."},
                "timestamp_field": {"type": "string", "description": "Source field for the point timestamp."},
                "batch_size": {"type": "integer", "minimum": 1, "description": "Batch size for write requests."},
                "flush_interval": {"type": "integer", "minimum": 1, "description": "Maximum seconds to wait before flushing a partial batch."},
                "retry_count": {"type": "integer", "minimum": 1, "description": "Number of HTTP attempts per flush."},
                "timeout_ms": {"type": "integer", "minimum": 1, "description": "HTTP request timeout in milliseconds."}
            },
            "required": ["url", "org", "bucket", "token", "measurement", "fields"]
        }),
    ).with_example(serde_json::json!({
        "url": "http://localhost:8086",
        "org": "arkflow",
        "bucket": "metrics",
        "token": "${INFLUX_TOKEN}",
        "measurement": "sensor",
        "tags": [{"field": "sensor", "tag_name": "sensor"}],
        "fields": [{"field": "value", "field_name": "value", "field_type": "float"}],
        "timestamp_field": "timestamp"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn typed_batch() -> MessageBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sensor", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![Some("lab 1")])),
                    Arc::new(Float64Array::from(vec![42.5])),
                    Arc::new(Int64Array::from(vec![1_700_000_000_000_000_000i64])),
                ],
            )
            .unwrap(),
        )
    }

    fn typed_config(url: String, batch_size: usize) -> InfluxDBOutputConfig {
        InfluxDBOutputConfig {
            url,
            org: "my org".into(),
            bucket: "metrics".into(),
            token: "token".into(),
            measurement: "sensor data".into(),
            tags: Some(vec![TagMapping {
                field: "sensor".into(),
                tag_name: "device".into(),
            }]),
            fields: vec![FieldMapping {
                field: "value".into(),
                field_name: "reading".into(),
                field_type: Some(FieldType::Float),
            }],
            timestamp_field: Some("timestamp".into()),
            batch_size: Some(batch_size),
            flush_interval: None,
            retry_count: Some(1),
            timeout_ms: Some(1000),
        }
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("test"), "test");
        assert_eq!(escape_identifier("test value"), "test\\ value");
        assert_eq!(escape_identifier("test,value"), "test\\,value");
        assert_eq!(escape_identifier("test\\value"), "test\\\\value");
        assert_eq!(escape_identifier("test=value"), "test\\=value");
    }

    #[test]
    fn test_escape_tag_value() {
        assert_eq!(escape_tag_value("value"), "value");
        assert_eq!(escape_tag_value("value space"), "value\\ space");
        assert_eq!(escape_tag_value("value=equals"), "value\\=equals");
    }

    #[test]
    fn test_escape_field_value() {
        assert_eq!(escape_field_value("value"), "value");
        assert_eq!(escape_field_value("value\"quote"), "value\\\"quote");
        assert_eq!(escape_field_value("value\\slash"), "value\\\\slash");
    }

    #[test]
    fn test_builder_missing_config() {
        let builder = InfluxDBOutputBuilder;
        let resource = Resource {
            temporary: Default::default(),
            input_names: std::cell::RefCell::new(Default::default()),
        };
        let result = builder.build(None, &None, None, &resource);
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_convert_typed_batch_to_line_protocol() {
        let batch = typed_batch();
        let output = InfluxDBOutput::new(typed_config("http://localhost:8086".into(), 10)).unwrap();

        assert_eq!(
            output.convert_to_line_protocol(&batch).unwrap(),
            vec!["sensor\\ data,device=lab\\ 1 reading=42.5 1700000000000000000"]
        );
    }

    #[test]
    fn test_all_arrow_integer_widths_are_mapped_without_unsigned_wrap() {
        let float_inputs: Vec<(Box<dyn Array>, f64)> = vec![
            (Box::new(Int8Array::from(vec![-8])), -8.0),
            (Box::new(Int16Array::from(vec![-16])), -16.0),
            (Box::new(Int32Array::from(vec![-32])), -32.0),
            (Box::new(Int64Array::from(vec![-64])), -64.0),
            (Box::new(UInt8Array::from(vec![8])), 8.0),
            (Box::new(UInt16Array::from(vec![16])), 16.0),
            (Box::new(UInt32Array::from(vec![32])), 32.0),
            (Box::new(UInt64Array::from(vec![64])), 64.0),
        ];
        for (array, expected) in float_inputs {
            assert_eq!(
                InfluxDBOutput::get_float_value(array.as_ref(), 0),
                Some(expected)
            );
        }

        let integer_inputs: Vec<(Box<dyn Array>, i64)> = vec![
            (Box::new(Int8Array::from(vec![-8])), -8),
            (Box::new(Int16Array::from(vec![-16])), -16),
            (Box::new(Int32Array::from(vec![-32])), -32),
            (Box::new(Int64Array::from(vec![-64])), -64),
            (Box::new(UInt8Array::from(vec![8])), 8),
            (Box::new(UInt16Array::from(vec![16])), 16),
            (Box::new(UInt32Array::from(vec![32])), 32),
            (Box::new(UInt64Array::from(vec![64])), 64),
        ];
        for (array, expected) in integer_inputs {
            assert_eq!(
                InfluxDBOutput::get_int_value(array.as_ref(), 0).unwrap(),
                Some(expected)
            );
        }

        let too_large = UInt64Array::from(vec![u64::MAX]);
        assert!(matches!(
            InfluxDBOutput::get_int_value(&too_large, 0),
            Err(Error::Process(message)) if message.contains("signed i64 range")
        ));
    }

    #[test]
    fn test_builder_accepts_typed_config() {
        let resource = Resource {
            temporary: Default::default(),
            input_names: std::cell::RefCell::new(Default::default()),
        };
        let codec = None;
        let config = Some(serde_json::json!({
            "url": "http://localhost:8086",
            "org": "org",
            "bucket": "bucket",
            "token": "token",
            "measurement": "measurement",
            "fields": [{"field": "value", "field_name": "value"}]
        }));
        let builder = InfluxDBOutputBuilder;
        assert!(builder.build(None, &config, codec, &resource).is_ok());
    }

    #[tokio::test]
    #[ignore = "sandbox disallows binding a local TCP listener; run in CI or a normal host"]
    async fn test_successful_http_flush_contains_encoded_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let count = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..count]);
                let complete = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|end| request.len() >= end + 4 + 70)
                    .unwrap_or(false);
                if complete || count == 0 {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            request
        });

        let output = InfluxDBOutput::new(typed_config(format!("http://{}", address), 1)).unwrap();
        output.connect().await.unwrap();
        output.write(Arc::new(typed_batch())).await.unwrap();
        let request = String::from_utf8(server.await.unwrap()).unwrap();
        assert!(request.starts_with("POST /api/v2/write?"));
        assert!(request.contains("org=my+org"));
        assert!(request.contains("bucket=metrics"));
        assert!(request.contains("Authorization: Token token"));
        assert!(request.contains("sensor\\ data,device=lab\\ 1 reading=42.5"));
    }

    #[tokio::test]
    async fn test_failed_flush_retains_buffered_lines() {
        let output = InfluxDBOutput::new(InfluxDBOutputConfig {
            url: "http://127.0.0.1:1".into(),
            ..typed_config("http://127.0.0.1:1".into(), 1)
        })
        .unwrap();
        output.connect().await.unwrap();
        assert!(output.write(Arc::new(typed_batch())).await.is_err());
        assert_eq!(output.batch_buffer.lock().await.len(), 1);
    }
}
