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

//! Milvus output component
//!
//! Upserts each batch's rows into a Milvus collection over the REST v2
//! vectordb API (`POST /v2/vectordb/entities/upsert`, Milvus 2.4+): a
//! Float32 list column becomes the vector, every remaining column packs
//! into a JSON payload field, and an optional id column keys the row
//! (omit it for auto-id collections). Milvus reports failures as HTTP
//! 200 with a non-zero `code` — that is treated as an error here.
//! `api_key` supports secret references (`${env:...}`); Milvus REST
//! convention is `Authorization: Bearer <user>:<password>`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, LargeStringArray, ListArray,
    StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::json::LineDelimitedWriter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::error;

pub fn init() -> Result<(), Error> {
    register_output_builder("milvus", Arc::new(MilvusOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "milvus",
        "Upserts batch rows into a Milvus collection over the REST v2 vectordb API: a Float32 list column becomes the vector, other columns pack into a JSON payload field, and an optional id column keys the row.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Milvus base URL, e.g. http://localhost:19530."},
                "collection": {"type": "string", "description": "Target collection name (schema must already exist)."},
                "vector_field": {"type": "string", "description": "Name of the vector field (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "id_field": {"type": "string", "description": "Field carrying the row id (integer or string). When omitted the id key is left out (auto-id collections)."},
                "payload_field": {"type": "string", "description": "JSON field receiving every remaining column as a per-row object. Defaults to 'payload'; set to an empty string to disable."},
                "api_key": {"type": "string", "description": "Sent as 'Authorization: Bearer' (Milvus convention: <user>:<password>); supports secret references."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers."}
            },
            "required": ["url", "collection"]
        }),
    )
    .with_example(serde_json::json!({
        "url": "http://localhost:19530",
        "collection": "documents",
        "vector_field": "embedding",
        "id_field": "doc_id",
        "api_key": "${env:MILVUS_CREDENTIALS}"
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MilvusOutputConfig {
    url: String,
    collection: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default)]
    id_field: Option<String>,
    #[serde(default = "default_payload_field")]
    payload_field: String,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

fn default_vector_field() -> String {
    "embedding".to_string()
}
fn default_payload_field() -> String {
    "payload".to_string()
}
fn default_timeout_ms() -> u64 {
    30000
}

struct MilvusOutput {
    config: MilvusOutputConfig,
    client: Client,
}

#[async_trait]
impl Output for MilvusOutput {
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

        let data: Vec<Value> = (0..rows)
            .map(|row| {
                let mut object = Map::new();
                if let Some(Some(id)) = ids.as_ref().map(|ids| ids.get(row)) {
                    object.insert(self.config.id_field.clone().expect("ids imply id_field"), id.clone());
                }
                object.insert(self.config.vector_field.clone(), json!(vectors[row]));
                if let Some(payload) = payloads.as_ref().and_then(|payloads| payloads.get(row)) {
                    object.insert(self.config.payload_field.clone(), payload.clone());
                }
                Value::Object(object)
            })
            .collect();

        self.upsert(data).await
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl MilvusOutput {
    async fn upsert(&self, data: Vec<Value>) -> Result<(), Error> {
        let url = format!(
            "{}/v2/vectordb/entities/upsert",
            self.config.url.trim_end_matches('/')
        );
        let mut request = self
            .client
            .post(&url)
            .json(&json!({"collectionName": self.config.collection, "data": data}));
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                request = request.header(name, value);
            }
        }

        let response = request.send().await.map_err(|e| {
            error!("Milvus request failed: {}", e);
            Error::Process(format!("Milvus request failed: {}", e))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Process(format!("Milvus response read failed: {}", e)))?;
        if !status.is_success() {
            return Err(Error::Process(format!(
                "Milvus returned {}: {}",
                status,
                truncate_body(&body)
            )));
        }

        // Milvus reports failures as HTTP 200 with a non-zero `code`.
        let parsed: Value = serde_json::from_str(&body)
            .map_err(|e| Error::Process(format!("Milvus response parse failed: {}", e)))?;
        match parsed.get("code") {
            None => Ok(()), // Lenient with older builds that omit the code.
            Some(code) if code.as_i64() == Some(0) => Ok(()),
            Some(code) => {
                let message = parsed
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("<no message>");
                Err(Error::Process(format!(
                    "Milvus upsert failed with code {}: {}",
                    code,
                    truncate_body(message)
                )))
            }
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
                .ok_or_else(|| not_a_vector_error(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(field))?;
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
                .ok_or_else(|| not_a_vector_error(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(field))?;
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
                "milvus output: column '{}' must be FixedSizeList(Float32) or List(Float32), got {:?}",
                field, other
            )));
        }
    }
    if let Some((row, _)) = vectors.iter().enumerate().find(|(_, v)| v.is_empty()) {
        return Err(Error::Process(format!(
            "milvus output: column '{}' has an empty vector at row {row}",
            field
        )));
    }
    Ok(vectors)
}

fn not_a_vector_error(field: &str) -> Error {
    Error::Process(format!(
        "milvus output: column '{}' is not a Float32 vector list",
        field
    ))
}

fn null_vector_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "milvus output: column '{}' has a null vector at row {row}",
        field
    ))
}

fn extract_ids(
    batch: &MessageBatchRef,
    field: &Option<String>,
) -> Result<Option<Vec<Value>>, Error> {
    let field = match field {
        Some(field) if !field.trim().is_empty() => field.clone(),
        _ => return Ok(None),
    };
    let column = find_column(batch, &field)?;
    let mut ids = Vec::with_capacity(column.len());
    match column.data_type() {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                ids.push(json!(array.value(row)));
            }
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                ids.push(json!(array.value(row)));
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            for row in 0..column.len() {
                if column.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                ids.push(json!(downcast_string_value(column, row)?));
            }
        }
        other => {
            return Err(Error::Process(format!(
                "milvus output: id column '{}' must be Int64/Int32 or Utf8, got {:?}",
                field, other
            )));
        }
    }
    Ok(Some(ids))
}

fn null_id_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "milvus output: id column '{field}' has a null value at row {row}"
    ))
}

fn downcast_string_value(column: &Arc<dyn Array>, row: usize) -> Result<&str, Error> {
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
        Ok(array.value(row))
    } else {
        Err(Error::Process(
            "milvus output: unexpected string array type".to_string(),
        ))
    }
}

fn extract_payloads(
    batch: &MessageBatchRef,
    config: &MilvusOutputConfig,
) -> Result<Option<Vec<Value>>, Error> {
    if config.payload_field.is_empty() {
        return Ok(None);
    }
    let excluded: Vec<String> = vec![Some(config.vector_field.clone()), config.id_field.clone()]
        .into_iter()
        .flatten()
        .collect();
    let payload_columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !excluded.contains(name))
        .collect();

    if payload_columns.is_empty() {
        return Ok(Some(vec![json!({}); batch.num_rows()]));
    }

    let filtered = batch
        .filter_columns(&payload_columns.iter().cloned().collect::<std::collections::HashSet<_>>())?;
    let mut buffer = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut buffer);
    writer
        .write(&filtered)
        .map_err(|e| Error::Process(format!("milvus output: payload serialization failed: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("milvus output: payload serialization failed: {}", e)))?;
    let text = String::from_utf8(buffer)
        .map_err(|e| Error::Process(format!("milvus output: payload is not UTF-8: {}", e)))?;
    text.lines()
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|e| Error::Process(format!("milvus output: payload parse failed: {}", e)))
        })
        .collect::<Result<Vec<Value>, Error>>()
        .map(Some)
}

fn find_column<'a>(
    batch: &'a MessageBatchRef,
    field: &str,
) -> Result<&'a Arc<dyn Array>, Error> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| Error::Process(format!("milvus output: column '{}' not found", field)))
}

fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

struct MilvusOutputBuilder;
impl OutputBuilder for MilvusOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _codec: Option<Arc<dyn arkflow_core::codec::Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let mut config: MilvusOutputConfig = parse_config(config, "milvus output")?;
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "milvus output: 'url' must not be empty".to_string(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(Error::Config(
                "milvus output: 'collection' must not be empty".to_string(),
            ));
        }
        config.id_field = config.id_field.filter(|field| !field.trim().is_empty());
        // Loopback endpoints (local Milvus dev instances, tests) bypass a
        // system proxy — proxying localhost is never what a user means.
        let is_loopback = reqwest::Url::parse(&format!("{}/", config.url.trim_end_matches('/')))
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
        Ok(Arc::new(MilvusOutput { config, client }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    struct MockMilvus {
        addr: std::net::SocketAddr,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    }

    impl MockMilvus {
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
    }

    fn build_output(config: Value) -> Arc<dyn Output> {
        MilvusOutputBuilder
            .build(None, &Some(config), None, &test_resource())
            .unwrap()
    }

    fn sample_batch() -> MessageBatchRef {
        let dim = 2i32;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let flat = Float32Array::from(vec![1.5f32, 2.0, -3.0, 4.25]);
        let vectors = Arc::new(FixedSizeListArray::new(item_field, dim, Arc::new(flat), None));
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

    fn base_config(addr: std::net::SocketAddr, mut extra: Value) -> Value {
        let mut config = serde_json::json!({
            "url": format!("http://{addr}"),
            "collection": "docs",
            "id_field": "doc_id",
            "api_key": "root:Milvus-pw",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object_mut().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    #[tokio::test]
    async fn upserts_rows_with_vector_payload_and_id() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":{"upsertCount":2}}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        output.write(sample_batch()).await.unwrap();

        let (head, body) = mock.last_request();
        assert!(
            head.starts_with("POST /v2/vectordb/entities/upsert "),
            "{head}"
        );
        assert!(head.contains("authorization: Bearer root:Milvus-pw"), "{head}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["collectionName"], "docs");
        let data = parsed["data"].as_array().unwrap();
        assert_eq!(data.len(), 2, "one request carries the whole batch");
        assert_eq!(data[0]["doc_id"], 10);
        assert_eq!(data[0]["embedding"], serde_json::json!([1.5, 2.0]));
        assert_eq!(data[0]["payload"]["text"], "hello");
        assert_eq!(data[1]["embedding"], serde_json::json!([-3.0, 4.25]));
        // id and vector must not leak into the payload
        assert!(data[0]["payload"].get("doc_id").is_none());
        assert!(data[0]["payload"].get("embedding").is_none());
    }

    #[tokio::test]
    async fn omits_id_when_id_field_not_configured() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({"id_field": ""})));
        output.write(sample_batch()).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        for row in parsed["data"].as_array().unwrap() {
            assert!(row.get("doc_id").is_none(), "auto-id rows must omit the id key");
        }
    }

    #[tokio::test]
    async fn payload_field_can_be_disabled() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(base_config(
            mock.addr,
            serde_json::json!({"payload_field": ""}),
        ));
        output.write(sample_batch()).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        for row in parsed["data"].as_array().unwrap() {
            assert!(row.get("payload").is_none());
            assert!(row.get("embedding").is_some());
        }
    }

    #[tokio::test]
    async fn http_200_with_nonzero_code_fails() {
        let mock = MockMilvus::spawn(|_body| {
            (200, r#"{"code":100,"message":"collection not found"}"#.to_string())
        });
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("100"), "{err}");
        assert!(err.contains("collection not found"), "{err}");
    }

    #[tokio::test]
    async fn http_200_with_zero_code_succeeds() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":{}}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        assert!(output.write(sample_batch()).await.is_ok());
    }

    #[tokio::test]
    async fn http_200_without_code_is_lenient() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"status":"ok"}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        assert!(output.write(sample_batch()).await.is_ok());
    }

    #[tokio::test]
    async fn non_2xx_is_surfaced_with_status_and_body() {
        let mock = MockMilvus::spawn(|_body| (401, "unauthorized".to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("401"), "{err}");
        assert!(err.contains("unauthorized"), "{err}");
    }

    #[tokio::test]
    async fn no_api_key_sends_no_auth_header() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(serde_json::json!({
            "url": format!("http://{}", mock.addr),
            "collection": "docs",
        }));
        output.write(sample_batch()).await.unwrap();
        let (head, _) = mock.last_request();
        assert!(!head.to_ascii_lowercase().contains("authorization:"), "{head}");
    }

    #[tokio::test]
    async fn null_vector_row_errors_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let dim = 2i32;
        let with_null = FixedSizeListArray::from_iter_primitive::<
            datafusion::arrow::datatypes::Float32Type,
            _,
            _,
        >(vec![Some(vec![Some(1.0), Some(2.0)]), None], dim);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        )]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(with_null)]).unwrap(),
        ));
        let err = output.write(batch).await.unwrap_err().to_string();
        assert!(err.contains("null vector at row 1"), "{err}");
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(base_config(mock.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))],
            )
            .unwrap(),
        ));
        output.write(batch).await.unwrap();
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"collection": "c"}),
            serde_json::json!({"url": "http://localhost"}),
            serde_json::json!({"url": " ", "collection": "c"}),
        ] {
            assert!(
                MilvusOutputBuilder
                    .build(None, &Some(bad), None, &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }
}
