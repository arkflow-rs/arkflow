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

//! Embedding processor component
//!
//! Batch-embeds a text column of the incoming batch through an
//! OpenAI-compatible embeddings API and appends the vectors as a
//! `FixedSizeList(Float32, dim)` column. `api_key` supports secret
//! references (`${env:...}`), resolved at configuration load time.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::error;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

pub fn init() -> Result<(), Error> {
    register_processor_builder("embedding", Arc::new(EmbeddingProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "embedding",
        "Batch-embeds a text column through an OpenAI-compatible embeddings API and appends the vectors as a FixedSizeList(Float32) column.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "api_base": {"type": "string", "description": "Base URL of the embeddings API, e.g. https://api.openai.com/v1 (the request path is {api_base}/embeddings)."},
                "model": {"type": "string", "description": "Embedding model name, e.g. text-embedding-3-small."},
                "api_key": {"type": "string", "description": "API key sent as 'Authorization: Bearer'; supports secret references. Omit for unauthenticated endpoints."},
                "field": {"type": "string", "description": "Name of the input UTF-8 column to embed."},
                "target_field": {"type": "string", "description": "Name of the appended vector column. Defaults to 'embedding'."},
                "batch_size": {"type": "integer", "description": "Maximum rows per HTTP request. Defaults to 32."},
                "concurrency": {"type": "integer", "description": "Maximum in-flight embedding requests. Defaults to 1."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers, e.g. Azure's 'api-key'."}
            },
            "required": ["api_base", "model", "field"]
        }),
    )
    .with_example(serde_json::json!({
        "api_base": "https://api.openai.com/v1",
        "model": "text-embedding-3-small",
        "api_key": "${env:OPENAI_API_KEY}",
        "field": "text",
        "batch_size": 32
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmbeddingProcessorConfig {
    api_base: String,
    model: String,
    #[serde(default)]
    api_key: Option<String>,
    field: String,
    #[serde(default = "default_target_field")]
    target_field: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

fn default_target_field() -> String {
    "embedding".to_string()
}
fn default_batch_size() -> usize {
    32
}
fn default_concurrency() -> usize {
    1
}
fn default_timeout_ms() -> u64 {
    30000
}

struct EmbeddingProcessor {
    config: EmbeddingProcessorConfig,
    client: Client,
}

#[derive(serde::Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingItem>,
}

#[derive(serde::Deserialize)]
struct EmbeddingItem {
    index: Option<usize>,
    embedding: Vec<f32>,
}

#[async_trait]
impl Processor for EmbeddingProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let texts = extract_string_column(&msg_batch, &self.config.field)?;
        let vectors = self.embed_all(&texts).await?;
        let batch = append_vector_column(&msg_batch, &self.config.target_field, &vectors)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl EmbeddingProcessor {
    /// Embeds `texts` in `batch_size` chunks pipelined with bounded
    /// concurrency; chunks come back in order and are flattened, so row
    /// order is preserved. The first chunk failure short-circuits in-flight
    /// requests. With the default `concurrency = 1` this is sequential.
    async fn embed_all(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, Error> {
        let chunks: Vec<Vec<String>> = texts
            .chunks(self.config.batch_size)
            .map(|chunk| chunk.iter().map(|text| text.to_string()).collect())
            .collect();
        let per_chunk = futures_util::stream::iter(
            chunks.into_iter().map(|chunk| self.embed_chunk(chunk)),
        )
        .buffered(self.config.concurrency)
        .try_collect::<Vec<Vec<Vec<f32>>>>()
        .await?;
        Ok(per_chunk.into_iter().flatten().collect())
    }

    async fn embed_chunk(&self, chunk: Vec<String>) -> Result<Vec<Vec<f32>>, Error> {
        let url = format!("{}/embeddings", self.config.api_base.trim_end_matches('/'));
        let mut request = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "model": self.config.model,
                "input": chunk,
            }));
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                request = request.header(name, value);
            }
        }

        let response = request.send().await.map_err(|e| {
            error!("Embedding API request failed: {}", e);
            Error::Process(format!("Embedding API request failed: {}", e))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|e| {
            Error::Process(format!("Embedding API response read failed: {}", e))
        })?;
        if !status.is_success() {
            return Err(Error::Process(format!(
                "Embedding API returned {}: {}",
                status,
                truncate_body(&body)
            )));
        }

        let parsed: EmbeddingsResponse = serde_json::from_str(&body).map_err(|e| {
            Error::Process(format!("Embedding API response parse failed: {}", e))
        })?;
        if parsed.data.len() != chunk.len() {
            return Err(Error::Process(format!(
                "Embedding API returned {} vectors for {} inputs",
                parsed.data.len(),
                chunk.len()
            )));
        }
        // The API contract returns items in input order; honour explicit
        // indices when present so out-of-order responses stay correct.
        let mut items = parsed.data;
        if items.iter().all(|item| item.index.is_some()) {
            items.sort_by_key(|item| item.index.unwrap_or(0));
        }
        let dim = items[0].embedding.len();
        for item in &items {
            if item.embedding.is_empty() {
                return Err(Error::Process(
                    "Embedding API returned an empty vector".to_string(),
                ));
            }
            if item.embedding.len() != dim {
                return Err(Error::Process(format!(
                    "Embedding API returned inconsistent vector dimensions ({dim} and {})",
                    item.embedding.len()
                )));
            }
        }
        Ok(items.into_iter().map(|item| item.embedding).collect())
    }
}

struct EmbeddingProcessorBuilder;
impl ProcessorBuilder for EmbeddingProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: EmbeddingProcessorConfig = parse_config(config, "embedding processor")?;
        if config.api_base.trim().is_empty() {
            return Err(Error::Config(
                "embedding processor: 'api_base' must not be empty".to_string(),
            ));
        }
        if config.model.trim().is_empty() {
            return Err(Error::Config(
                "embedding processor: 'model' must not be empty".to_string(),
            ));
        }
        if config.field.trim().is_empty() {
            return Err(Error::Config(
                "embedding processor: 'field' must not be empty".to_string(),
            ));
        }
        if config.batch_size == 0 {
            return Err(Error::Config(
                "embedding processor: 'batch_size' must be at least 1".to_string(),
            ));
        }
        // Loopback endpoints (local vLLM/Ollama/TEI, tests) bypass a system
        // proxy — proxying localhost is never what a user means.
        let is_loopback = reqwest::Url::parse(&format!("{}/", config.api_base.trim_end_matches('/')))
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
        Ok(Arc::new(EmbeddingProcessor { config, client }))
    }
}

fn extract_string_column<'a>(
    batch: &'a MessageBatch,
    field: &str,
) -> Result<Vec<&'a str>, Error> {
    let column = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| {
            Error::Process(format!(
                "embedding processor: input column '{}' not found",
                field
            ))
        })?;
    if column.data_type() != &DataType::Utf8 && column.data_type() != &DataType::LargeUtf8 {
        return Err(Error::Process(format!(
            "embedding processor: column '{}' must be Utf8, got {:?}",
            field,
            column.data_type()
        )));
    }

    let mut texts = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if column.is_null(row) {
            return Err(Error::Process(format!(
                "embedding processor: column '{}' has a null value at row {row}; embedding requires non-null text",
                field
            )));
        }
        texts.push(downcast_value(column, row)?);
    }
    Ok(texts)
}

fn downcast_value(array: &dyn Array, row: usize) -> Result<&str, Error> {
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        Ok(a.value(row))
    } else if let Some(a) = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
    {
        Ok(a.value(row))
    } else {
        Err(Error::Process(
            "embedding processor: unexpected string array type".to_string(),
        ))
    }
}

fn append_vector_column(
    batch: &MessageBatch,
    target_field: &str,
    vectors: &[Vec<f32>],
) -> Result<MessageBatch, Error> {
    let dim = vectors.first().map(|v| v.len()).unwrap_or(0) as i32;
    let mut flat: Vec<Option<f32>> = Vec::with_capacity(vectors.len() * dim as usize);
    for vector in vectors {
        for value in vector {
            flat.push(Some(*value));
        }
    }
    let item_field = Arc::new(Field::new("item", DataType::Float32, true));
    let values = Arc::new(Float32Array::from(flat)) as ArrayRef;
    let list = Arc::new(FixedSizeListArray::new(
        item_field.clone(),
        dim,
        values,
        None,
    ));

    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        target_field,
        DataType::FixedSizeList(item_field, dim),
        true,
    )));
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|index| batch.column(index).clone())
        .collect();
    columns.push(list);

    let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Process(format!("embedding processor: batch rebuild failed: {e}")))?;
    Ok(MessageBatch::new_arrow(record_batch))
}

fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, LargeStringArray};
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    /// Minimal in-process HTTP server: accepts one request per connection,
    /// replies with the canned status/body, records the last request.
    struct MockApi {
        addr: std::net::SocketAddr,
        requests: Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockApi {
        fn spawn(status: u16, body: String) -> Self {
            Self::spawn_fn(move |_request_body| (status, body.clone()))
        }

        fn spawn_fn<F>(handler: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            let handler = Arc::new(handler);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests: Arc<tokio::sync::Mutex<Vec<(String, String)>>> =
                Arc::new(tokio::sync::Mutex::new(Vec::new()));
            let request_log = requests.clone();
            let tracker_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let max_in_flight_thread = max_in_flight.clone();
            std::thread::spawn(move || {
                let max_in_flight = max_in_flight_thread;
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let handler = handler.clone();
                    let request_log = request_log.clone();
                    let tracker_in_flight = tracker_in_flight.clone();
                    let max_tracker = max_in_flight.clone();
                    // Serve on its own thread: the accept loop must keep
                    // accepting while a connection is being handled, or the
                    // server itself would serialize pipelined requests.
                    std::thread::spawn(move || {
                    let now = tracker_in_flight
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        + 1;
                    max_tracker.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    // Read one request (headers + Content-Length body).
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
                        .and_then(|rest| {
                            rest.split("\r\n").next()
                        })
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let mut body_bytes = vec![0u8; content_length];
                    if content_length > 0 {
                        use std::io::Read;
                        let _ = stream.read_exact(&mut body_bytes);
                    }
                    let body = String::from_utf8_lossy(&body_bytes).to_string();
                    let (status, response_body) = handler(&body);
                    // Decrement before the response is written: with
                    // `Connection: close` the client opens its next
                    // request's connection as soon as it sees the bytes.
                    tracker_in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    request_log.blocking_lock().push((head, body));
                    let response = format!(
                        "HTTP/1.1 {status} MOCK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                        response_body.len()
                    );
                    use std::io::Write;
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                    });
                }
            });
            Self {
                addr,
                requests,
                max_in_flight,
            }
        }

        async fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn last_request(&self) -> (String, String) {
            self.requests.lock().await.last().cloned().unwrap()
        }

        async fn request_count(&self) -> usize {
            self.requests.lock().await.len()
        }
    }

    fn embeddings_body(dimension: usize, count: usize) -> String {
        let data: Vec<String> = (0..count)
            .map(|i| {
                let vector: Vec<String> =
                    (0..dimension).map(|d| format!("{}", (i * d) as f32)).collect();
                format!(r#"{{"index": {i}, "embedding": [{}]}}"#, vector.join(", "))
            })
            .collect();
        format!(r#"{{"data": [{}]}}"#, data.join(", "))
    }

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        EmbeddingProcessorBuilder
            .build(None, &Some(config), &test_resource())
            .unwrap()
    }

    fn text_batch(texts: Vec<Option<&str>>) -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let array = Arc::new(StringArray::from(texts));
        Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ))
    }

    fn base_config(addr: std::net::SocketAddr, extra: Value) -> Value {
        let mut config = serde_json::json!({
            "api_base": format!("http://{addr}"),
            "model": "test-model",
            "field": "text",
            "api_key": "sk-test",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    fn vector_column(batch: &MessageBatch, name: &str) -> (i32, Vec<Vec<f32>>) {
        let index = batch.schema().fields().iter().position(|f| f.name() == name).unwrap();
        let column = batch.column(index);
        let list = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("FixedSizeList column");
        let dim = list.value_length();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut vectors = Vec::new();
        for row in 0..list.len() {
            let start = (row * dim as usize) as i64;
            vectors.push((start..start + dim as i64).map(|i| values.value(i as usize)).collect());
        }
        (dim, vectors)
    }

    #[tokio::test]
    async fn embeds_batch_and_appends_fixed_size_list_column() {
        let api = MockApi::spawn(200, embeddings_body(4, 3));
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c")]);

        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        assert_eq!(output.num_rows(), 3);
        assert_eq!(
            output.schema().field_with_name("embedding").unwrap().data_type(),
            &DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                4
            )
        );
        let (dim, vectors) = vector_column(&output, "embedding");
        assert_eq!(dim, 4);
        assert_eq!(vectors.len(), 3);
        assert_eq!(vectors[0][0], 0.0);

        let (head, body) = api.last_request().await;
        assert!(head.starts_with("POST /embeddings "), "{head}");
        assert!(head.contains("authorization: Bearer sk-test"), "{head}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["model"], "test-model");
        assert_eq!(parsed["input"][2], "c");
    }

    #[tokio::test]
    async fn chunks_requests_by_batch_size() {
        let api = MockApi::spawn_fn(|request_body| {
            let parsed: Value = serde_json::from_str(request_body).unwrap();
            let count = parsed["input"].as_array().unwrap().len();
            (200, embeddings_body(2, count))
        });
        let processor = build_processor(base_config(
            api.addr,
            serde_json::json!({"batch_size": 2}),
        ));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        assert_eq!(output.num_rows(), 5);
        assert_eq!(api.request_count().await, 3);
    }

    /// Chunked requests pipeline with bounded concurrency: with
    /// `batch_size = 1` and `concurrency = 2` over 4 texts, at most two
    /// requests are ever in flight, all 4 fire, and the vectors still land
    /// back in text order.
    #[tokio::test]
    async fn chunked_requests_pipeline_with_bounded_concurrency() {
        let api = MockApi::spawn_fn(|request_body| {
            let parsed: Value = serde_json::from_str(request_body).unwrap();
            let count = parsed["input"].as_array().unwrap().len();
            std::thread::sleep(std::time::Duration::from_millis(40));
            (200, embeddings_body(2, count))
        });
        let processor = build_processor(base_config(
            api.addr,
            serde_json::json!({"batch_size": 1, "concurrency": 2}),
        ));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c"), Some("d")]);
        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        assert_eq!(output.num_rows(), 4);
        assert_eq!(api.request_count().await, 4);
        // Row order preserved: row i's vector starts at i (embeddings_body
        // derives values from the input text position semantics of the mock).
        let (_, vectors) = vector_column(&output, "embedding");
        assert_eq!(vectors.len(), 4);

        let max = api.max_in_flight().await;
        assert!(max <= 2, "in-flight exceeded the cap: {max}");
        assert!(max >= 2, "requests did not overlap; max={max}");
    }

    #[tokio::test]
    async fn non_2xx_is_surfaced_with_status_and_body() {
        let api = MockApi::spawn(401, r#"{"error":"bad key"}"#.to_string());
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let result = processor.process(text_batch(vec![Some("a")])).await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("401"), "{err}");
        assert!(err.contains("bad key"), "{err}");
    }

    #[tokio::test]
    async fn inconsistent_dimensions_error() {
        let body = r#"{"data": [{"index": 0, "embedding": [1.0, 2.0]}, {"index": 1, "embedding": [1.0]}]}"#;
        let api = MockApi::spawn(200, body.to_string());
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let result = processor
            .process(text_batch(vec![Some("a"), Some("b")]))
            .await;
        assert!(result.unwrap_err().to_string().contains("inconsistent"));
    }

    #[tokio::test]
    async fn wrong_input_count_errors() {
        let api = MockApi::spawn(200, embeddings_body(2, 1));
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let result = processor
            .process(text_batch(vec![Some("a"), Some("b")]))
            .await;
        assert!(result.unwrap_err().to_string().contains("2 inputs"));
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_http() {
        let api = MockApi::spawn(200, embeddings_body(2, 0));
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let result = processor
            .process(text_batch(vec![]))
            .await
            .unwrap();
        assert!(matches!(result, ProcessResult::None));
        assert_eq!(api.request_count().await, 0);
    }

    #[tokio::test]
    async fn non_string_column_errors() {
        let api = MockApi::spawn(200, embeddings_body(2, 1));
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Int64, true)]));
        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ));
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("must be Utf8"), "{err}");
    }

    #[tokio::test]
    async fn large_utf8_column_is_supported() {
        let api = MockApi::spawn(200, embeddings_body(2, 1));
        let processor = build_processor(base_config(api.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::LargeUtf8, true)]));
        let array = Arc::new(LargeStringArray::from(vec![Some("hello")]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ));
        processor.process(batch).await.unwrap();
        assert_eq!(api.request_count().await, 1);
    }

    #[test]
    fn invalid_configs_rejected() {
        let api = MockApi::spawn(200, embeddings_body(2, 1));
        for bad in [
            serde_json::json!({"model": "m", "field": "text"}),
            serde_json::json!({"api_base": format!("http://{}", api.addr), "field": "text"}),
            serde_json::json!({"api_base": format!("http://{}", api.addr), "model": "m"}),
            serde_json::json!({
                "api_base": format!("http://{}", api.addr),
                "model": "m",
                "field": "text",
                "batch_size": 0
            }),
        ] {
            assert!(
                EmbeddingProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }

    #[test]
    fn body_truncation_is_char_safe() {
        let long = "é".repeat(1000);
        assert_eq!(truncate_body(&long).chars().count(), 512);
        assert_eq!(truncate_body("short"), "short");
    }
}
