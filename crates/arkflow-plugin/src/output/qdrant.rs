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

//! Qdrant output component
//!
//! Upserts each batch's rows as Qdrant points through the REST API: the
//! vector comes from a `FixedSizeList(Float32)`/`List(Float32)` column
//! (typically produced by the `embedding` processor), an optional id column
//! keys the point, and every remaining column lands in the point payload.
//! `api_key` supports secret references (`${env:...}`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, FixedSizeListArray, Float32Array, LargeStringArray, ListArray, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::error;

pub fn init() -> Result<(), Error> {
    register_output_builder("qdrant", Arc::new(QdrantOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "qdrant",
        "Upserts batch rows as Qdrant points over the REST API: a vector column, an optional id column, and all remaining columns as payload.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Qdrant base URL, e.g. http://localhost:6333."},
                "collection": {"type": "string", "description": "Target collection name."},
                "vector_field": {"type": "string", "description": "Name of the vector column (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "id_field": {"type": "string", "description": "Column used as the point id (unsigned integer or string). When omitted, Qdrant generates ids."},
                "payload_fields": {"type": "array", "items": {"type": "string"}, "description": "Columns to include in the point payload. Defaults to every column except the vector and id columns."},
                "api_key": {"type": "string", "description": "API key sent as 'Authorization: Bearer'; supports secret references."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "retry_count": {"type": "integer", "description": "Retry attempts for connection errors and 5xx responses. Defaults to 3."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers."}
            },
            "required": ["url", "collection"]
        }),
    )
    .with_example(serde_json::json!({
        "url": "http://localhost:6333",
        "collection": "documents",
        "vector_field": "embedding",
        "id_field": "doc_id"
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QdrantOutputConfig {
    url: String,
    collection: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default)]
    id_field: Option<String>,
    #[serde(default)]
    payload_fields: Option<Vec<String>>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_retry_count")]
    retry_count: u32,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

fn default_vector_field() -> String {
    "embedding".to_string()
}
fn default_timeout_ms() -> u64 {
    30000
}
fn default_retry_count() -> u32 {
    3
}

struct QdrantOutput {
    config: QdrantOutputConfig,
    client: Client,
}

#[async_trait]
impl Output for QdrantOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        let rows = msg.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let vectors = extract_vectors(&msg, &self.config.vector_field)?;
        let ids = extract_ids(&msg, &self.config.id_field)?;
        let payloads = extract_payloads(&msg, &self.config)?;

        let points: Vec<Value> = (0..rows)
            .map(|row| {
                let mut point = Map::new();
                match ids.as_ref().and_then(|ids| ids.get(row)) {
                    Some(id) => {
                        point.insert("id".to_string(), id.clone());
                    }
                    // Qdrant requires every point to carry an id: generate a
                    // UUID v4 per row so the request is valid (each retry
                    // inserts a fresh point — prefer id_field for
                    // at-least-once idempotency).
                    None => {
                        point.insert("id".to_string(), json!(random_uuid_v4()));
                    }
                }
                point.insert("vector".to_string(), json!(vectors[row]));
                if let Some(payload) = payloads.get(row) {
                    point.insert("payload".to_string(), payload.clone());
                }
                Value::Object(point)
            })
            .collect();

        self.upsert(&points).await
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl QdrantOutput {
    async fn upsert(&self, points: &[Value]) -> Result<(), Error> {
        let url = format!(
            "{}/collections/{}/points?wait=true",
            self.config.url.trim_end_matches('/'),
            self.config.collection
        );
        let mut request = self.client.put(&url).json(&json!({ "points": points }));
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                request = request.header(name, value);
            }
        }

        let mut attempt = 0u32;
        loop {
            match request.try_clone().expect("request body is JSON").send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return Ok(());
                    }
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<unreadable body>".to_string());
                    // Client errors are deterministic: retrying cannot help.
                    if status.is_client_error() {
                        return Err(Error::Process(format!(
                            "Qdrant returned {}: {}",
                            status,
                            truncate_body(&body)
                        )));
                    }
                    error!("Qdrant upsert attempt {} failed: {} {}", attempt + 1, status, truncate_body(&body));
                    if attempt >= self.config.retry_count {
                        return Err(Error::Process(format!(
                            "Qdrant returned {} after {} attempts: {}",
                            status,
                            attempt + 1,
                            truncate_body(&body)
                        )));
                    }
                }
                Err(e) => {
                    if attempt >= self.config.retry_count {
                        return Err(Error::Connection(format!("Qdrant request failed: {}", e)));
                    }
                }
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt - 1))).await;
        }
    }
}

fn extract_vectors(batch: &MessageBatchRef, field: &str) -> Result<Vec<Vec<f32>>, Error> {
    let column = find_column(batch, field)?;
    let rows = column.len();
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(rows);
    match column.data_type() {
        DataType::FixedSizeList(_, dim) => {
            let list = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| missing_vector_type(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| missing_vector_type(field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(field, row));
                }
                let start = row as i64 * *dim as i64;
                vectors.push(
                    (start..start + *dim as i64)
                        .map(|i| values.value(i as usize))
                        .collect(),
                );
            }
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| missing_vector_type(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| missing_vector_type(field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(field, row));
                }
                let offsets = list.value_offsets();
                vectors.push(
                    (offsets[row]..offsets[row + 1])
                        .map(|i| values.value(i as usize))
                        .collect(),
                );
            }
        }
        other => {
            return Err(Error::Process(format!(
                "qdrant output: column '{}' must be FixedSizeList(Float32) or List(Float32), got {:?}",
                field, other
            )));
        }
    }
    if let Some((row, empty)) = vectors.iter().enumerate().find(|(_, v)| v.is_empty()) {
        let _ = empty;
        return Err(Error::Process(format!(
            "qdrant output: column '{}' has an empty vector at row {row}",
            field
        )));
    }
    Ok(vectors)
}

fn missing_vector_type(field: &str) -> Error {
    Error::Process(format!(
        "qdrant output: column '{}' is not a Float32 vector list",
        field
    ))
}

fn null_vector_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "qdrant output: column '{}' has a null vector at row {row}",
        field
    ))
}

fn extract_ids(batch: &MessageBatchRef, field: &Option<String>) -> Result<Option<Vec<Value>>, Error> {
    let field = match field {
        Some(field) if !field.trim().is_empty() => field.clone(),
        _ => return Ok(None),
    };
    let column = find_column(batch, &field)?;
    let mut ids = Vec::with_capacity(column.len());
    match column.data_type() {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(Error::Process(format!("qdrant output: id column '{field}' has a null value at row {row}")));
                }
                let value = array.value(row);
                let id = u64::try_from(value).map_err(|_| {
                    Error::Process(format!(
                        "qdrant output: id column '{field}' has a negative value {value} at row {row}"
                    ))
                })?;
                ids.push(json!(id));
            }
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<datafusion::arrow::array::Int32Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(Error::Process(format!("qdrant output: id column '{field}' has a null value at row {row}")));
                }
                ids.push(json!(u64::try_from(array.value(row)).map_err(|_| Error::Process(format!(
                    "qdrant output: id column '{field}' has a negative value at row {row}"
                )))?));
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            for row in 0..column.len() {
                if column.is_null(row) {
                    return Err(Error::Process(format!("qdrant output: id column '{field}' has a null value at row {row}")));
                }
                let value = downcast_string_value(column, row)?;
                ids.push(json!(value));
            }
        }
        other => {
            return Err(Error::Process(format!(
                "qdrant output: id column '{}' must be Int64/Int32 or Utf8, got {:?}",
                field, other
            )));
        }
    }
    Ok(Some(ids))
}

fn downcast_string_value(column: &Arc<dyn Array>, row: usize) -> Result<&str, Error> {
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = column
        .as_any()
        .downcast_ref::<LargeStringArray>()
    {
        Ok(array.value(row))
    } else {
        Err(Error::Process(
            "qdrant output: unexpected string array type".to_string(),
        ))
    }
}

fn extract_payloads(batch: &MessageBatchRef, config: &QdrantOutputConfig) -> Result<Vec<Value>, Error> {
    let all_fields: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let payload_columns: Vec<String> = match &config.payload_fields {
        Some(fields) => {
            for field in fields {
                if !all_fields.contains(field) {
                    return Err(Error::Process(format!(
                        "qdrant output: payload column '{}' not found",
                        field
                    )));
                }
            }
            fields.clone()
        }
        None => all_fields
            .iter()
            .filter(|name| {
                name.as_str() != config.vector_field && Some(name.as_str()) != config.id_field.as_deref()
            })
            .cloned()
            .collect(),
    };
    if payload_columns.is_empty() {
        return Ok(vec![Value::Object(Map::new()); batch.num_rows()]);
    }

    let filtered =
        batch.filter_columns(&payload_columns.iter().cloned().collect::<std::collections::HashSet<_>>())?;
    let mut buffer = Vec::new();
    let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buffer);
    writer
        .write(&filtered)
        .map_err(|e| Error::Process(format!("qdrant output: payload serialization failed: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("qdrant output: payload serialization failed: {}", e)))?;

    let text = String::from_utf8(buffer).map_err(|e| {
        Error::Process(format!("qdrant output: payload is not UTF-8: {}", e))
    })?;
    text.lines()
        .map(|line| {
            serde_json::from_str(line).map_err(|e| {
                Error::Process(format!("qdrant output: payload parse failed: {}", e))
            })
        })
        .collect()
}

fn find_column<'a>(batch: &'a MessageBatchRef, field: &str) -> Result<&'a Arc<dyn Array>, Error> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| {
            Error::Process(format!("qdrant output: column '{}' not found", field))
        })
}

/// Formats a random UUID v4 (no uuid crate).
fn random_uuid_v4() -> String {
        let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&rand::random::<u64>().to_be_bytes());
    bytes[8..16].copy_from_slice(&rand::random::<u64>().to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

struct QdrantOutputBuilder;
impl OutputBuilder for QdrantOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _codec: Option<Arc<dyn arkflow_core::codec::Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: QdrantOutputConfig = parse_config(config, "qdrant output")?;
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "qdrant output: 'url' must not be empty".to_string(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(Error::Config(
                "qdrant output: 'collection' must not be empty".to_string(),
            ));
        }
        // Loopback endpoints (local Qdrant dev instances, tests) bypass a
        // system proxy — proxying localhost is never what a user means.
        let is_loopback = reqwest::Url::parse(&config.url)
            .ok()
            .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
            .map(|host| host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]")
            .unwrap_or(false);
        let mut builder = Client::builder().timeout(Duration::from_millis(config.timeout_ms));
        if is_loopback {
            builder = builder.no_proxy();
        }
        let client = builder
            .build()
            .map_err(|e| Error::Config(format!("Unable to create HTTP client: {}", e)))?;
        Ok(Arc::new(QdrantOutput { config, client }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    struct MockQdrant {
        addr: std::net::SocketAddr,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    }

    impl MockQdrant {
        fn spawn<F>(handler: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            let handler = Arc::new(handler);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let request_log = requests.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let handler = handler.clone();
                    let request_log = request_log.clone();
                    let mut buffer = Vec::new();
                    let mut byte = [0u8; 1];
                    loop {
                        use std::io::Read;
                        if stream.read_exact(&mut byte).is_err() {
                            break;
                        }
                        buffer.push(byte[0]);
                        if buffer.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let head = String::from_utf8_lossy(&buffer).to_string();
                    let content_length = head
                        .to_ascii_lowercase()
                        .split("content-length:")
                        .nth(1)
                        .and_then(|rest| rest.split("\r\n").next())
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let mut body_bytes = vec![0u8; content_length];
                    if content_length > 0 {
                        use std::io::Read;
                        let _ = stream.read_exact(&mut body_bytes);
                    }
                    let body = String::from_utf8_lossy(&body_bytes).to_string();
                    request_log.lock().unwrap().push((head.clone(), body.clone()));

                    let (status, response_body) = handler(&body);
                    let response = format!(
                        "HTTP/1.1 {status} MOCK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                        response_body.len()
                    );
                    use std::io::Write;
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
            });
            Self { addr, requests }
        }

        fn last_request(&self) -> (String, String) {
            self.requests.lock().unwrap().last().cloned().unwrap()
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    fn build_output(config: Value) -> Arc<dyn Output> {
        QdrantOutputBuilder
            .build(None, &Some(config), None, &test_resource())
            .unwrap()
    }

    fn sample_batch() -> MessageBatchRef {
        let dim = 2i32;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let flat = Float32Array::from(vec![1.0f32, 2.0, 3.0, 4.0]);
        let vectors = Arc::new(FixedSizeListArray::new(
            item_field,
            dim,
            Arc::new(flat),
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Int64, false),
            Field::new("embedding", DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim), true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![10i64, 20])),
            vectors,
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ];
        Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, columns).unwrap(),
        ))
    }

    fn base_config(addr: std::net::SocketAddr, extra: Value) -> Value {
        let mut config = serde_json::json!({
            "url": format!("http://{addr}"),
            "collection": "docs",
            "api_key": "secret-key",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    #[tokio::test]
    async fn upserts_points_with_vector_payload_and_id() {
        let mock = MockQdrant::spawn(|_body| (200, "{\"result\":{}}".to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"id_field": "doc_id"}),
        ));
        output.connect().await.unwrap();
        output.write(sample_batch()).await.unwrap();

        let (head, body) = mock.last_request();
        assert!(
            head.starts_with("PUT /collections/docs/points?wait=true "),
            "{head}"
        );
        assert!(head.contains("authorization: Bearer secret-key"), "{head}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let points = parsed["points"].as_array().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0]["id"], 10);
        assert_eq!(points[0]["vector"], serde_json::json!([1.0, 2.0]));
        assert_eq!(points[0]["payload"]["text"], "hello");
        assert_eq!(points[1]["id"], 20);
        assert_eq!(points[1]["payload"]["text"], "world");
        // vector/id columns must not leak into the payload
        assert!(points[0]["payload"].get("embedding").is_none());
        assert!(points[0]["payload"].get("doc_id").is_none());
    }

    #[tokio::test]
    async fn generates_uuid_ids_when_id_field_not_configured() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        output.write(sample_batch()).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        for point in parsed["points"].as_array().unwrap() {
            let id = point["id"].as_str().expect("uuid string id");
            assert_eq!(id.len(), 36, "uuid v4 shape: {id}");
        }
    }

    #[tokio::test]
    async fn string_id_column_is_supported() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"id_field": "text"}),
        ));
        output.write(sample_batch()).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["points"][0]["id"], "hello");
    }

    #[tokio::test]
    async fn missing_id_field_errors() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"id_field": "nope"}),
        ));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("'nope'"), "{err}");
    }

    #[tokio::test]
    async fn payload_fields_subset_is_respected() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"payload_fields": ["text"]}),
        ));
        output.write(sample_batch()).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let payload = parsed["points"][0]["payload"].as_object().unwrap();
        assert_eq!(payload.len(), 1);
        assert_eq!(payload.get("text"), Some(&json!("hello")));
    }

    #[tokio::test]
    async fn no_api_key_sends_no_auth_header() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = QdrantOutputBuilder
            .build(
                None,
                &Some(serde_json::json!({
                    "url": format!("http://{}", mock.addr),
                    "collection": "docs",
                })),
                None,
                &test_resource(),
            )
            .unwrap();
        output.write(sample_batch()).await.unwrap();
        let (head, _) = mock.last_request();
        assert!(!head.to_ascii_lowercase().contains("authorization:"), "{head}");
    }

    #[tokio::test]
    async fn client_errors_fail_without_retry() {
        let mock = MockQdrant::spawn(|_body| (400, "{\"error\":\"bad dim\"}".to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("400"), "{err}");
        assert!(err.contains("bad dim"), "{err}");
        assert_eq!(mock.request_count(), 1, "4xx must not retry");
    }

    #[tokio::test]
    async fn server_errors_retry_then_fail() {
        let mock = MockQdrant::spawn(|_body| (500, "boom".to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"retry_count": 2}),
        ));
        let start = std::time::Instant::now();
        let result = output.write(sample_batch()).await;
        assert!(result.is_err());
        assert!(start.elapsed() >= Duration::from_millis(100 + 200));
        assert_eq!(mock.request_count(), 3, "1 initial + 2 retries");
    }

    #[tokio::test]
    async fn empty_batch_writes_nothing() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))])
                .unwrap(),
        ));
        output.write(batch).await.unwrap();
        assert_eq!(mock.request_count(), 0);
    }

    #[test]
    fn invalid_configs_rejected() {
        let mock = MockQdrant::spawn(|_body| (200, "{}".to_string()));
        for bad in [
            serde_json::json!({"collection": "c"}),
            serde_json::json!({"url": format!("http://{}", mock.addr)}),
            serde_json::json!({"url": " ", "collection": "c"}),
        ] {
            assert!(
                QdrantOutputBuilder
                    .build(None, &Some(bad), None, &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }
}
