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

//! Vector similarity search processor
//!
//! Reads a Float32 list column from the batch (typically produced by the
//! `embedding` processor), searches a Qdrant collection for the top-k
//! nearest neighbors of each row's vector, and appends the matches as a
//! JSON array text column. Requests run with bounded, order-preserving
//! concurrency. `api_key` supports secret references (`${env:...}`).

use std::collections::HashMap;
use std::sync::Arc;

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::error;

pub fn init() -> Result<(), Error> {
    register_processor_builder("vector_search", Arc::new(VectorSearchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "vector_search",
        "Searches a Qdrant collection for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Qdrant base URL, e.g. http://localhost:6333."},
                "collection": {"type": "string", "description": "Collection to search."},
                "vector_field": {"type": "string", "description": "Name of the query vector column (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "target_field": {"type": "string", "description": "Name of the appended matches column (JSON array text). Defaults to 'matches'."},
                "top_k": {"type": "integer", "description": "Number of neighbors per row. Defaults to 5."},
                "score_threshold": {"type": "number", "description": "Minimum similarity score; omitted from the request when unset."},
                "concurrency": {"type": "integer", "description": "Maximum in-flight requests. Defaults to 4."},
                "api_key": {"type": "string", "description": "API key sent as 'Authorization: Bearer'; supports secret references."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers."}
            },
            "required": ["url", "collection"]
        }),
    )
    .with_example(serde_json::json!({
        "url": "http://localhost:6333",
        "collection": "documents",
        "vector_field": "embedding",
        "top_k": 5
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorSearchProcessorConfig {
    url: String,
    collection: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default = "default_target_field")]
    target_field: String,
    #[serde(default = "default_top_k")]
    top_k: usize,
    #[serde(default)]
    score_threshold: Option<f64>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
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
fn default_top_k() -> usize {
    5
}
fn default_concurrency() -> usize {
    4
}
fn default_timeout_ms() -> u64 {
    30000
}

struct VectorSearchProcessor {
    config: VectorSearchProcessorConfig,
    client: Client,
}

#[async_trait]
impl Processor for VectorSearchProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let vectors = vector_util::extract_vectors("vector_search processor", &msg_batch, &self.config.vector_field)?;
        let matches = self.search_all(&vectors).await?;
        let batch = vector_util::append_column("vector_search processor", &msg_batch, &self.config.target_field, &matches)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl VectorSearchProcessor {
    /// One search per row; results are collected in row order while at
    /// most `concurrency` requests are in flight. The first row failure
    /// short-circuits: in-flight requests are dropped rather than awaited.
    async fn search_all(&self, vectors: &[Vec<f32>]) -> Result<Vec<String>, Error> {
        let owned: Vec<Vec<f32>> = vectors.to_vec();
        futures_util::stream::iter(owned.into_iter().map(|vector| self.search(vector)))
            .buffered(self.config.concurrency)
            .try_collect()
            .await
    }

    async fn search(&self, vector: Vec<f32>) -> Result<String, Error> {
        let url = format!(
            "{}/collections/{}/points/search",
            self.config.url.trim_end_matches('/'),
            vector_util::encode_path_segment(&self.config.collection)
        );
        let mut body = json!({
            "vector": vector,
            "limit": self.config.top_k,
            "with_payload": true,
        });
        if let Some(threshold) = self.config.score_threshold {
            body["score_threshold"] = json!(threshold);
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
            error!("Vector search request failed: {}", e);
            Error::Process(format!("Vector search request failed: {}", e))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Process(format!("Vector search response read failed: {}", e)))?;
        if !status.is_success() {
            return Err(Error::Process(format!(
                "Vector search returned {}: {}",
                status,
                vector_util::truncate_body(&body)
            )));
        }

        let parsed: Value = serde_json::from_str(&body)
            .map_err(|e| Error::Process(format!("Vector search response parse failed: {}", e)))?;
        let result = parsed.get("result").ok_or_else(|| {
            Error::Process("Vector search response has no 'result' array".to_string())
        })?;
        if !result.is_array() {
            return Err(Error::Process(
                "Vector search response 'result' is not an array".to_string(),
            ));
        }
        // Compact serialization: the column feeds llm prompt templates and
        // downstream JSON tooling, so keep it dense and predictable.
        let compact = serde_json::to_string(result)
            .map_err(|e| Error::Process(format!("Vector search result serialization failed: {}", e)))?;
        Ok(compact)
    }
}

struct VectorSearchProcessorBuilder;
impl ProcessorBuilder for VectorSearchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: VectorSearchProcessorConfig = parse_config(config, "vector_search processor")?;
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "vector_search processor: 'url' must not be empty".to_string(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(Error::Config(
                "vector_search processor: 'collection' must not be empty".to_string(),
            ));
        }
        if config.vector_field.trim().is_empty() {
            return Err(Error::Config(
                "vector_search processor: 'vector_field' must not be empty".to_string(),
            ));
        }
        if config.top_k == 0 {
            return Err(Error::Config(
                "vector_search processor: 'top_k' must be at least 1".to_string(),
            ));
        }
        if config.concurrency == 0 {
            return Err(Error::Config(
                "vector_search processor: 'concurrency' must be at least 1".to_string(),
            ));
        }
        let client = vector_util::build_http_client(config.timeout_ms, &config.url)?;
        Ok(Arc::new(VectorSearchProcessor { config, client }))
    }
}

use crate::vector_util;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_util::test_support::MockApi;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{
        Array, FixedSizeListArray, Float32Array, ListArray, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn search_body(marker: &str) -> String {
        json!({
            "result": [
                {"id": 1, "score": 0.9, "payload": {"text": format!("{marker}-a")}},
                {"id": 2, "score": 0.5, "payload": {"text": format!("{marker}-b")}}
            ]
        })
        .to_string()
    }

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        VectorSearchProcessorBuilder
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
            "api_key": "sk-q",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    #[tokio::test]
    async fn searches_rows_in_order_and_appends_json_column() {
        let mock = MockApi::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            let first = parsed["vector"][0].as_f64().unwrap();
            (200, search_body(&format!("q{first}")))
        });
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
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
        let row0: Value = serde_json::from_str(matches.value(0)).unwrap();
        let row1: Value = serde_json::from_str(matches.value(1)).unwrap();
        assert_eq!(row0[0]["payload"]["text"], "q1-a");
        assert_eq!(row1[0]["payload"]["text"], "q0-a");
        assert_eq!(row0[1]["score"], 0.5, "qdrant score order preserved");
        assert!(output.schema().field_with_name("matches").is_ok());
    }

    #[tokio::test]
    async fn request_body_has_limit_and_optional_threshold() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({"top_k": 3})));
        processor.process(vector_batch(vec![vec![1.0, 2.0]])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["limit"], 3);
        assert_eq!(parsed["with_payload"], true);
        assert!(parsed.get("score_threshold").is_none());

        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"top_k": 2, "score_threshold": 0.8}),
        ));
        processor.process(vector_batch(vec![vec![1.0, 2.0]])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["limit"], 2);
        assert_eq!(parsed["score_threshold"], 0.8);
    }

    #[tokio::test]
    async fn bounded_concurrency_preserves_order() {
        let mock = MockApi::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(100));
            let first = parsed["vector"][0].as_f64().unwrap();
            (200, search_body(&format!("q{}", first as i64)))
        });
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"concurrency": 2}),
        ));
        let batch = vector_batch(vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0]]);

        let start = std::time::Instant::now();
        let result = processor.process(batch).await.unwrap();
        let elapsed = start.elapsed();

        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        let matches = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, first) in [1.0, 2.0, 3.0, 4.0].iter().enumerate() {
            let parsed: Value = serde_json::from_str(matches.value(row)).unwrap();
            assert_eq!(
                parsed[0]["payload"]["text"],
                format!("q{first}-a"),
                "row order must hold"
            );
        }
        assert!(elapsed < std::time::Duration::from_millis(320), "no overlap: took {elapsed:?}");
        assert_eq!(mock.requests().len(), 4);
    }

    /// The first row failure short-circuits: rows whose requests have not
    /// started yet must never hit the backend instead of dooming every
    /// remaining row to a request that cannot save the batch.
    #[tokio::test]
    async fn first_failure_short_circuits_pending_rows() {
        let mock = MockApi::spawn(|_body| (500, "boom".to_string()));
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"concurrency": 2}),
        ));
        let batch = vector_batch(vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0], vec![5.0], vec![6.0]]);
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("500"), "{err}");
        let sent = mock.requests().len();
        assert!(
            sent < 6,
            "all 6 rows were sent despite the first-row failure: {sent}"
        );
    }

    #[tokio::test]
    async fn in_flight_requests_respect_concurrency_cap() {
        let mock = MockApi::spawn(|_body| {
            std::thread::sleep(std::time::Duration::from_millis(30));
            (200, search_body("x"))
        });
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"concurrency": 2}),
        ));
        let batch = vector_batch(vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0], vec![5.0], vec![6.0]]);
        processor.process(batch).await.unwrap();
        let max = mock.max_in_flight();
        assert!(max <= 2, "in-flight requests exceeded the cap: {max}");
        assert!(max >= 2, "requests did not overlap; expected pipelining, max={max}");
    }

    /// Ordinary collection names keep their URL byte-for-byte; names with
    /// path-hostile characters are percent-encoded as one segment so the
    /// request still addresses the same collection.
    #[test]
    fn collection_names_are_encoded_as_a_single_path_segment() {
        assert_eq!(
            vector_util::encode_path_segment("documents-2.x"),
            "documents-2.x",
            "unreserved characters must pass through unchanged"
        );
        assert_eq!(
            vector_util::encode_path_segment("docs/2024?x y#f"),
            "docs%2F2024%3Fx%20y%23f"
        );
        assert_eq!(vector_util::encode_path_segment("a~b_c-d.e"), "a~b_c-d.e");

        // End-to-end: the encoded name lands in the request line.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mock = MockApi::spawn(|_body| (200, search_body("x")));
            let processor = build_processor(base_config(
                mock.addr(),
                serde_json::json!({"collection": "eu/docs 1"}),
            ));
            processor
                .process(vector_batch(vec![vec![1.0]]))
                .await
                .unwrap();
            let (head, _) = mock.requests().remove(0);
            assert!(
                head.starts_with("POST /collections/eu%2Fdocs%201/points/search "),
                "{head}"
            );
        });
    }

    #[tokio::test]
    async fn api_key_sent_as_bearer() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        processor.process(vector_batch(vec![vec![1.0]])).await.unwrap();
        let (head, _) = mock.requests().remove(0);
        assert!(head.contains("authorization: Bearer sk-q"), "{head}");
    }

    #[tokio::test]
    async fn no_api_key_sends_no_auth_header() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(serde_json::json!({
            "url": format!("http://{}", mock.addr()),
            "collection": "docs",
        }));
        processor.process(vector_batch(vec![vec![1.0]])).await.unwrap();
        let (head, _) = mock.requests().remove(0);
        assert!(!head.to_ascii_lowercase().contains("authorization:"), "{head}");
    }

    #[tokio::test]
    async fn non_2xx_is_surfaced_with_status_and_body() {
        let mock = MockApi::spawn(|_body| (404, r#"{"status":{"error":"collection not found"}}"#.to_string()));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("404"), "{err}");
        assert!(err.contains("not found"), "{err}");
    }

    #[tokio::test]
    async fn missing_result_key_errors() {
        let mock = MockApi::spawn(|_body| (200, r#"{"status":"ok"}"#.to_string()));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no 'result' array"), "{err}");
    }

    #[tokio::test]
    async fn null_vector_row_errors() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
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
    async fn non_list_column_errors() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("embedding", DataType::Utf8, true)]));
        let array = Arc::new(StringArray::from(vec!["not a vector"]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ));
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("must be FixedSizeList(Float32) or List(Float32)"), "{err}");
    }

    #[tokio::test]
    async fn large_list_column_is_supported() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let values = Float32Array::from(vec![1.0f32, 2.0]);
        let offsets = datafusion::arrow::buffer::OffsetBuffer::new(
            datafusion::arrow::buffer::ScalarBuffer::from(vec![0i32, 2]),
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap(),
        ));
        processor.process(batch).await.unwrap();
        assert_eq!(mock.requests().len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_http() {
        let mock = MockApi::spawn(|_body| (200, search_body("x")));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
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
        assert_eq!(mock.requests().len(), 0);
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"collection": "c"}),
            serde_json::json!({"url": "http://localhost"}),
            serde_json::json!({"url": "http://localhost", "collection": "c", "vector_field": ""}),
            serde_json::json!({"url": "http://localhost", "collection": "c", "top_k": 0}),
            serde_json::json!({"url": "http://localhost", "collection": "c", "concurrency": 0}),
        ] {
            assert!(
                VectorSearchProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }
}
