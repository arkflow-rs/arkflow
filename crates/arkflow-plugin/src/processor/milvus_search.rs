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

//! Milvus similarity search processor
//!
//! Reads a Float32 list column from the batch (typically produced by the
//! `embedding` processor), searches a Milvus collection with one batched
//! REST v2 request (`POST /v2/vectordb/entities/search` — the `data`
//! array carries every row's query vector), and appends the matches as a
//! JSON array text column keyed by position. Failure semantics follow the
//! `milvus` output: HTTP 200 with a non-zero `code` is an error. The
//! `api_key` supports secret references (`${env:...}`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::error;

pub fn init() -> Result<(), Error> {
    register_processor_builder("milvus_search", Arc::new(MilvusSearchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "milvus_search",
        "Searches a Milvus collection for the top-k nearest neighbors of each row's vector with one batched REST v2 request and appends the matches as a JSON array text column.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Milvus base URL, e.g. http://localhost:19530."},
                "collection": {"type": "string", "description": "Collection to search."},
                "vector_field": {"type": "string", "description": "Name of the query vector column (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "target_field": {"type": "string", "description": "Name of the appended matches column (JSON array text). Defaults to 'matches'."},
                "id_field": {"type": "string", "description": "Collection field returned as the match id. When omitted the id key is left out (auto-id collections)."},
                "payload_field": {"type": "string", "description": "Collection field included in each match as a payload object. Defaults to 'payload'; set to an empty string to disable."},
                "metric": {"type": "string", "enum": ["COSINE", "L2", "IP"], "description": "Metric type; must match the collection schema. Defaults to COSINE."},
                "top_k": {"type": "integer", "description": "Number of neighbors per row. Defaults to 5."},
                "api_key": {"type": "string", "description": "API key sent as 'Authorization: Bearer' (Milvus convention: <user>:<password>); supports secret references."},
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
        "top_k": 5
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MilvusSearchProcessorConfig {
    url: String,
    collection: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default = "default_target_field")]
    target_field: String,
    #[serde(default)]
    id_field: Option<String>,
    #[serde(default = "default_payload_field")]
    payload_field: String,
    #[serde(default)]
    metric: Metric,
    #[serde(default = "default_top_k")]
    top_k: usize,
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
fn default_target_field() -> String {
    "matches".to_string()
}
fn default_payload_field() -> String {
    "payload".to_string()
}
fn default_top_k() -> usize {
    5
}
fn default_timeout_ms() -> u64 {
    30000
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
enum Metric {
    #[default]
    #[serde(rename = "COSINE")]
    Cosine,
    #[serde(rename = "L2")]
    L2,
    #[serde(rename = "IP")]
    InnerProduct,
}

impl Metric {
    fn as_str(self) -> &'static str {
        match self {
            Metric::Cosine => "COSINE",
            Metric::L2 => "L2",
            Metric::InnerProduct => "IP",
        }
    }
}

struct MilvusSearchProcessor {
    config: MilvusSearchProcessorConfig,
    client: Client,
}

#[async_trait]
impl Processor for MilvusSearchProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let vectors = extract_vectors(&msg_batch, &self.config.vector_field)?;
        let matches = self.search_all(vectors).await?;
        let batch = append_column(&msg_batch, &self.config.target_field, &matches)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl MilvusSearchProcessor {
    /// One batched request: the `data` array carries every row's query
    /// vector and the response's `data[i]` maps back to input row i.
    async fn search_all(&self, vectors: Vec<Vec<f32>>) -> Result<Vec<String>, Error> {
        let url = format!(
            "{}/v2/vectordb/entities/search",
            self.config.url.trim_end_matches('/')
        );
        let data: Vec<Value> = vectors.iter().map(|vector| json!({"vector": vector})).collect();
        let mut output_fields: Vec<String> = Vec::with_capacity(2);
        if let Some(id_field) = &self.config.id_field {
            output_fields.push(id_field.clone());
        }
        if !self.config.payload_field.is_empty() {
            output_fields.push(self.config.payload_field.clone());
        }
        let mut body = json!({
            "collectionName": self.config.collection,
            "data": data,
            "limit": self.config.top_k,
            "searchParams": {"metricType": self.config.metric.as_str(), "params": {}},
        });
        if !output_fields.is_empty() {
            body["outputFields"] = json!(output_fields);
        }

        let mut request = self.client.post(&url).json(&body);
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                request = request.header(name, value);
            }
        }

        let response = request.send().await.map_err(|e| {
            error!("Milvus search request failed: {}", e);
            Error::Process(format!("Milvus search request failed: {}", e))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Process(format!("Milvus search response read failed: {}", e)))?;
        if !status.is_success() {
            return Err(Error::Process(format!(
                "Milvus search returned {}: {}",
                status,
                truncate_body(&body)
            )));
        }

        let parsed: Value = serde_json::from_str(&body)
            .map_err(|e| Error::Process(format!("Milvus search response parse failed: {}", e)))?;
        match parsed.get("code") {
            None => {}
            Some(code) if code.as_i64() == Some(0) => {}
            Some(code) => {
                let message = parsed
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("<no message>");
                return Err(Error::Process(format!(
                    "Milvus search failed with code {}: {}",
                    code,
                    truncate_body(message)
                )));
            }
        }

        let groups = parsed
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::Process("Milvus search response has no 'data' array".to_string())
            })?;
        if groups.len() != vectors.len() {
            return Err(Error::Process(format!(
                "Milvus search returned {} result groups for {} input rows",
                groups.len(),
                vectors.len()
            )));
        }

        groups
            .iter()
            .map(|group| match_matches(group, &self.config))
            .collect()
    }
}

/// Normalizes one row's matches: rename the configured collection fields to
/// the canonical `id`/`distance`/`payload` keys used by the other search
/// processors, so downstream prompts and tooling stay backend-agnostic.
fn match_matches(group: &Value, config: &MilvusSearchProcessorConfig) -> Result<String, Error> {
    let hits = group.as_array().ok_or_else(|| {
        Error::Process("Milvus search response group is not an array".to_string())
    })?;
    let normalized: Vec<Value> = hits
        .iter()
        .map(|hit| {
            let mut object = Map::new();
            if let Some(id_field) = &config.id_field {
                if let Some(id) = hit.get(id_field.as_str()) {
                    object.insert("id".to_string(), id.clone());
                }
            }
            if let Some(distance) = hit.get("distance") {
                object.insert("distance".to_string(), distance.clone());
            }
            if !config.payload_field.is_empty() {
                if let Some(payload) = hit.get(config.payload_field.as_str()) {
                    object.insert("payload".to_string(), payload.clone());
                }
            }
            Value::Object(object)
        })
        .collect();
    serde_json::to_string(&normalized)
        .map_err(|e| Error::Process(format!("Milvus search serialization failed: {}", e)))
}

fn extract_vectors(batch: &MessageBatchRef, field: &str) -> Result<Vec<Vec<f32>>, Error> {
    let column = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| {
            Error::Process(format!(
                "milvus_search processor: input column '{}' not found",
                field
            ))
        })?;
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
                "milvus_search processor: column '{}' must be FixedSizeList(Float32) or List(Float32), got {:?}",
                field, other
            )));
        }
    }
    if let Some((row, _)) = vectors.iter().enumerate().find(|(_, v)| v.is_empty()) {
        return Err(Error::Process(format!(
            "milvus_search processor: column '{}' has an empty vector at row {row}",
            field
        )));
    }
    Ok(vectors)
}

fn not_a_vector_error(field: &str) -> Error {
    Error::Process(format!(
        "milvus_search processor: column '{}' is not a Float32 vector list",
        field
    ))
}

fn null_vector_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "milvus_search processor: column '{}' has a null vector at row {row}",
        field
    ))
}

fn append_column(
    batch: &MessageBatch,
    target_field: &str,
    matches: &[String],
) -> Result<MessageBatch, Error> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(target_field, DataType::Utf8, true)));
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|index| batch.column(index).clone())
        .collect();
    columns.push(Arc::new(StringArray::from(matches.to_vec())));

    let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(
        |e| Error::Process(format!("milvus_search processor: batch rebuild failed: {e}")),
    )?;
    Ok(MessageBatch::new_arrow(record_batch))
}

fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

struct MilvusSearchProcessorBuilder;
impl ProcessorBuilder for MilvusSearchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let mut config: MilvusSearchProcessorConfig =
            parse_config(config, "milvus_search processor")?;
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "milvus_search processor: 'url' must not be empty".to_string(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(Error::Config(
                "milvus_search processor: 'collection' must not be empty".to_string(),
            ));
        }
        if config.top_k == 0 {
            return Err(Error::Config(
                "milvus_search processor: 'top_k' must be at least 1".to_string(),
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
        Ok(Arc::new(MilvusSearchProcessor { config, client }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        MilvusSearchProcessorBuilder
            .build(None, &Some(config), &test_resource())
            .unwrap()
    }

    fn vector_batch(vectors: Vec<Vec<f32>>) -> MessageBatchRef {
        let dim = vectors[0].len() as i32;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let list = Arc::new(FixedSizeListArray::new(
            item_field,
            dim,
            Arc::new(Float32Array::from(flat)),
            None,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        )]));
        Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![list]).unwrap(),
        ))
    }

    fn base_config(addr: std::net::SocketAddr, extra: Value) -> Value {
        let mut config = serde_json::json!({
            "url": format!("http://{addr}"),
            "collection": "docs",
            "id_field": "doc_id",
            "api_key": "root:Milvus-pw",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    fn search_response() -> String {
        json!({
            "code": 0,
            "data": [
                [
                    {"doc_id": 7, "distance": 0.1, "payload": {"text": "a"}},
                    {"doc_id": 8, "distance": 0.4, "payload": {"text": "b"}}
                ],
                [
                    {"doc_id": 9, "distance": 0.2, "payload": {"text": "c"}}
                ]
            ]
        })
        .to_string()
    }

    #[tokio::test]
    async fn batched_search_maps_rows_in_order() {
        let mock = MockMilvus::spawn(|_body| (200, search_response()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let batch = vector_batch(vec![vec![1.0, 0.0], vec![0.0, 1.0]]);

        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        assert_eq!(output.num_rows(), 2);
        let matches = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let row0: Vec<Value> = serde_json::from_str(matches.value(0)).unwrap();
        assert_eq!(row0.len(), 2);
        assert_eq!(row0[0]["id"], 7);
        assert_eq!(row0[0]["distance"], 0.1);
        assert_eq!(row0[0]["payload"]["text"], "a");
        let row1: Vec<Value> = serde_json::from_str(matches.value(1)).unwrap();
        assert_eq!(row1[0]["id"], 9);
        assert_eq!(row1[0]["payload"]["text"], "c");

        let (head, body) = mock.last_request();
        assert!(head.starts_with("POST /v2/vectordb/entities/search "), "{head}");
        assert!(head.contains("authorization: Bearer root:Milvus-pw"), "{head}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["collectionName"], "docs");
        assert_eq!(parsed["data"].as_array().unwrap().len(), 2);
        assert_eq!(parsed["data"][0]["vector"], serde_json::json!([1.0, 0.0]));
        assert_eq!(parsed["limit"], 5);
        assert_eq!(parsed["searchParams"]["metricType"], "COSINE");
        let fields = parsed["outputFields"].as_array().unwrap();
        assert!(fields.contains(&json!("doc_id")));
        assert!(fields.contains(&json!("payload")));
    }

    #[tokio::test]
    async fn auto_id_omits_id_from_output_fields_and_matches() {
        let mock = MockMilvus::spawn(|_body| {
            (200, r#"{"code":0,"data":[[{"distance":0.3,"payload":{"text":"x"}}]]}"#.to_string())
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({"id_field": ""})));
        let batch = vector_batch(vec![vec![1.0, 2.0]]);
        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        let matches = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parsed: Value = serde_json::from_str(matches.value(0)).unwrap();
        assert!(parsed[0].get("id").is_none());
        assert_eq!(parsed[0]["payload"]["text"], "x");

        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert!(
            !parsed["outputFields"].as_array().unwrap().contains(&json!("doc_id")),
            "auto-id collections must not request the id field"
        );
    }

    #[tokio::test]
    async fn metric_type_is_configurable() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":[[{"distance":1.0}]]}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({"metric": "IP"})));
        processor.process(vector_batch(vec![vec![1.0]])).await.unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["searchParams"]["metricType"], "IP");
    }

    #[tokio::test]
    async fn http_200_with_nonzero_code_fails() {
        let mock = MockMilvus::spawn(|_body| {
            (200, r#"{"code":100,"message":"collection not found"}"#.to_string())
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("100"), "{err}");
        assert!(err.contains("collection not found"), "{err}");
    }

    #[tokio::test]
    async fn http_error_is_surfaced() {
        let mock = MockMilvus::spawn(|_body| (404, "no route".to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("404"), "{err}");
    }

    #[tokio::test]
    async fn mismatched_result_groups_error() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":[[{"distance":0.1}]]}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0], vec![2.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 result groups for 2 input rows"), "{err}");
    }

    #[tokio::test]
    async fn null_vector_row_errors_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
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
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("null vector at row 1"), "{err}");
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("embedding", DataType::Utf8, true)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))],
            )
            .unwrap(),
        ));
        let result = processor.process(batch).await.unwrap();
        assert!(matches!(result, ProcessResult::None));
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"collection": "c"}),
            serde_json::json!({"url": "http://localhost"}),
            serde_json::json!({"url": "http://localhost", "collection": "c", "top_k": 0}),
        ] {
            assert!(
                MilvusSearchProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }
}
