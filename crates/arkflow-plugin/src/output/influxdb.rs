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
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray,
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
    codec: Option<Arc<dyn Codec>>,
    batch_buffer: Arc<Mutex<Vec<String>>>,
    last_flush: Arc<Mutex<Instant>>,
    connected: AtomicBool,
}

impl InfluxDBOutput {
    /// Create a new InfluxDB output component
    pub fn new(
        config: InfluxDBOutputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            codec,
            batch_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            connected: AtomicBool::new(false),
        })
    }

    /// Get column index by name
    fn get_column_index(msg: &MessageBatch, field_name: &str) -> Result<Option<usize>, Error> {
        let schema = msg.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            if field.name() == field_name {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Convert MessageBatch to InfluxDB Line Protocol
    fn convert_to_line_protocol(
        &self,
        msg: &MessageBatch,
    ) -> Result<Vec<String>, Error> {
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
                    if let Ok(Some(col_idx)) = Self::get_column_index(msg, &tag_mapping.field) {
                        let column = msg.column(col_idx);
                        if let Ok(Some(value)) = Self::get_string_value(column, row_idx) {
                            let escaped_key = escape_identifier(&tag_mapping.tag_name);
                            let escaped_value = escape_tag_value(&value);
                            tag_pairs.push(format!("{}={}", escaped_key, escaped_value));
                        }
                    }
                }
            }

            if !tag_pairs.is_empty() {
                line_parts.push(tag_pairs.join(","));
            }

            // 3. Add fields
            let mut field_pairs = Vec::new();
            for field_mapping in &self.config.fields {
                if let Ok(Some(col_idx)) = Self::get_column_index(msg, &field_mapping.field) {
                    let column = msg.column(col_idx);

                    match field_mapping.field_type.as_ref() {
                        Some(FieldType::Float) => {
                            if let Ok(Some(value)) = Self::get_float_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}", escaped_key, value));
                            }
                        }
                        Some(FieldType::Integer) => {
                            if let Ok(Some(value)) = Self::get_int_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}i", escaped_key, value));
                            }
                        }
                        Some(FieldType::Boolean) => {
                            if let Ok(Some(value)) = Self::get_bool_value(column, row_idx) {
                                let escaped_key = escape_identifier(&field_mapping.field_name);
                                field_pairs.push(format!("{}={}", escaped_key, value));
                            }
                        }
                        Some(FieldType::String) | None => {
                            if let Ok(Some(value)) = Self::get_string_value(column, row_idx) {
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
                if let Ok(Some(col_idx)) = Self::get_column_index(msg, ts_field) {
                    let column = msg.column(col_idx);
                    if let Ok(Some(ts)) = Self::get_int_value(column, row_idx) {
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
    fn get_string_value(column: &dyn Array, row_index: usize) -> Result<Option<String>, Error> {
        let data_type = column.data_type();
        match data_type {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                if array.is_null(row_index) {
                    Ok(None)
                } else {
                    Ok(Some(array.value(row_index).to_string()))
                }
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                if array.is_null(row_index) {
                    Ok(None)
                } else {
                    Ok(Some(array.value(row_index).to_string()))
                }
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                if array.is_null(row_index) {
                    Ok(None)
                } else {
                    Ok(Some(array.value(row_index).to_string()))
                }
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.is_null(row_index) {
                    Ok(None)
                } else {
                    Ok(Some(array.value(row_index).to_string()))
                }
            }
            _ => Ok(None),
        }
    }

    /// Get float value from column
    fn get_float_value(column: &dyn Array, row_index: usize) -> Result<Option<f64>, Error> {
        if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
            if array.is_null(row_index) {
                Ok(None)
            } else {
                Ok(Some(array.value(row_index)))
            }
        } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
            if array.is_null(row_index) {
                Ok(None)
            } else {
                Ok(Some(array.value(row_index) as f64))
            }
        } else {
            Ok(None)
        }
    }

    /// Get int value from column
    fn get_int_value(column: &dyn Array, row_index: usize) -> Result<Option<i64>, Error> {
        if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
            if array.is_null(row_index) {
                Ok(None)
            } else {
                Ok(Some(array.value(row_index)))
            }
        } else {
            Ok(None)
        }
    }

    /// Get bool value from column
    fn get_bool_value(column: &dyn Array, row_index: usize) -> Result<Option<bool>, Error> {
        if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
            if array.is_null(row_index) {
                Ok(None)
            } else {
                Ok(Some(array.value(row_index)))
            }
        } else {
            Ok(None)
        }
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
            if elapsed >= interval_secs as u64 {
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
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Connection("InfluxDB client not initialized".to_string())
        })?;

        // Build URL
        let url = format!(
            "{}/api/v2/write?org={}&bucket={}",
            self.config.url, self.config.org, self.config.bucket
        );

        // Join lines with newline
        let body = buffer.join("\n");

        // Retry logic
        let retry_count = self.config.retry_count.unwrap_or(3);
        let mut last_error = None;

        for attempt in 0..retry_count {
            let response = client
                .post(&url)
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
            if attempt < retry_count - 1 {
                tokio::time::sleep(std::time::Duration::from_millis(100 * 2_u64.pow(attempt))).await;
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
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("InfluxDB output not connected".to_string()));
        }

        // Apply codec encoding if configured
        let processed_msgs = if let Some(codec) = &self.codec {
            codec.encode((*msg).clone())?
        } else {
            // No codec: extract binary data and re-create MessageBatch
            let binary_data = msg.to_binary("")?;
            binary_data.into_iter().map(|b| b.to_vec()).collect()
        };

        // Process each payload
        for payload in processed_msgs {
            // Create MessageBatch from payload for line protocol conversion
            let batch = MessageBatch::new_binary(vec![payload])?;
            let lines = self.convert_to_line_protocol(&batch)?;

            // Add to batch buffer
            let mut buffer = self.batch_buffer.lock().await;
            buffer.extend(lines);
            drop(buffer);

            // Check if we should flush
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
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
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
        if config.is_none() {
            return Err(Error::Config(
                "InfluxDB output configuration is missing".to_string(),
            ));
        }

        let config: InfluxDBOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(InfluxDBOutput::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("influxdb", Arc::new(InfluxDBOutputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("test"), "test");
        assert_eq!(escape_identifier("test value"), "test\\ value");
        assert_eq!(escape_identifier("test,value"), "test\\,value");
        assert_eq!(escape_identifier("test\\value"), "test\\\\value");
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
}
