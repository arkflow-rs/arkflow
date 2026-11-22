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
//! `embedding` processor), searches a Milvus collection, and appends the
//! matches as a JSON array text column keyed by position. Each row issues
//! its own `POST /v2/vectordb/entities/search` with `data: [one-vector]` —
//! Milvus 2.4's REST v2 search flattens multi-vector responses, which loses
//! per-query grouping — and rows run with bounded, order-preserving
//! concurrency. Failure semantics follow the `milvus` output: HTTP 200 with
//! a non-zero `code` is an error. The `api_key` supports secret references
//! (`${env:...}`).

use std::collections::HashMap;
use std::sync::Arc;

use crate::vector_util;
use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub fn init() -> Result<(), Error> {
    register_processor_builder("milvus_search", Arc::new(MilvusSearchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "milvus_search",
        "Searches a Milvus collection for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column. Rows are searched with bounded, order-preserving concurrency.",
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
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    /// Retries for transient failures (429/5xx/transport). 0 = single
    /// attempt.
    #[serde(default = "default_retry_count")]
    retry_count: u32,
}

fn default_retry_count() -> u32 {
    3
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
fn default_concurrency() -> usize {
    4
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

        let vectors = vector_util::extract_vectors(
            "milvus_search processor",
            &msg_batch,
            &self.config.vector_field,
        )?;
        let matches = self.search_all(vectors).await?;
        let batch = vector_util::append_column(
            "milvus_search processor",
            &msg_batch,
            &self.config.target_field,
            &matches,
        )?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl MilvusSearchProcessor {
    fn output_fields(&self) -> Vec<String> {
        let mut fields: Vec<String> = Vec::with_capacity(2);
        if let Some(id_field) = &self.config.id_field {
            fields.push(id_field.clone());
        }
        if !self.config.payload_field.is_empty() {
            fields.push(self.config.payload_field.clone());
        }
        fields
    }

    /// One request per row: Milvus 2.4's REST v2 search flattens multi-
    /// vector responses, which loses per-query grouping, so each row issues
    /// its own `data: [one-vector]` search. Rows run with bounded
    /// concurrency and results map back in row order; the first row failure
    /// short-circuits (in-flight requests are dropped rather than awaited).
    /// One search per row; at most `concurrency` requests are in flight.
    /// The first failure surfaces as soon as its own request completes and
    /// cancels the rest, so an early slow row cannot hide a later error;
    /// successful results keep row order.
    async fn search_all(&self, vectors: Vec<Vec<f32>>) -> Result<Vec<String>, Error> {
        let mut results: Vec<Option<String>> = (0..vectors.len()).map(|_| None).collect();
        let mut pending = futures_util::stream::iter(
            vectors
                .into_iter()
                .enumerate()
                .map(|(index, vector)| async move { (index, self.search(vector).await) }),
        )
        .buffer_unordered(self.config.concurrency);
        while let Some((index, result)) = pending.next().await {
            results[index] = Some(result?);
        }
        Ok(results
            .into_iter()
            .map(|result| result.expect("every row completed"))
            .collect())
    }

    async fn search(&self, vector: Vec<f32>) -> Result<String, Error> {
        let url = crate::http_util::join_url_base(&self.config.url, "v2/vectordb/entities/search");
        let output_fields: Vec<String> = self.output_fields();
        let mut body = json!({
            "collectionName": self.config.collection,
            // REST v2 search takes RAW query vectors in `data` (one vector
            // per request) and names the target field via `annsField`. The
            // previous `{"vector": [...]}` shape is the INSERT entity
            // format — with no annsField every search failed.
            "data": [vector],
            "annsField": self.config.vector_field,
            "limit": self.config.top_k,
            "searchParams": {"metricType": self.config.metric.as_str(), "params": {}},
        });
        if !output_fields.is_empty() {
            body["outputFields"] = json!(output_fields);
        }

        let retry_config = crate::retry::RetryConfig {
            max_attempts: self.config.retry_count.saturating_add(1),
            ..Default::default()
        };
        let body = crate::http_util::send_with_retry(
            || {
                let mut request = self.client.post(&url).json(&body);
                if let Some(key) = &self.config.api_key {
                    request = request.bearer_auth(key);
                }
                crate::http_util::apply_headers(request, self.config.headers.as_ref())
            },
            &retry_config,
            "milvus search",
            "Milvus search",
        )
        .await?;

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
                    crate::http_util::truncate_body(message)
                )));
            }
        }

        // nq = 1: the response `data` is this row's flat hit list.
        let hits = parsed
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::Process("Milvus search response has no 'data' array".to_string())
            })?;
        let normalized: Vec<Value> = hits
            .iter()
            .map(|hit| match_match(hit, &self.config))
            .collect::<Result<Vec<Value>, Error>>()?;
        serde_json::to_string(&normalized)
            .map_err(|e| Error::Process(format!("Milvus search serialization failed: {}", e)))
    }
}

/// Normalizes one hit: rename the configured collection fields to the
/// canonical `id`/`distance`/`payload` keys used by the other search
/// processors, so downstream prompts and tooling stay backend-agnostic.
fn match_match(hit: &Value, config: &MilvusSearchProcessorConfig) -> Result<Value, Error> {
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
    Ok(Value::Object(object))
}

struct MilvusSearchProcessorBuilder;
impl ProcessorBuilder for MilvusSearchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
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
        if config.concurrency == 0 {
            return Err(Error::Config(
                "milvus_search processor: 'concurrency' must be at least 1".to_string(),
            ));
        }
        let client = crate::http_util::build_http_client(config.timeout_ms, &config.url)?;
        Ok(Arc::new(MilvusSearchProcessor { config, client }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_util::test_support::MockApi as MockMilvus;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{Array, FixedSizeListArray, Float32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
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

    /// nq = 1 response: `data` is this row's flat hit list.
    fn hits_response(first_id: i64) -> String {
        json!({
            "code": 0,
            "data": [
                {"doc_id": first_id, "distance": 0.1, "payload": {"text": "p1"}},
                {"doc_id": first_id + 1, "distance": 0.4, "payload": {"text": "p2"}}
            ]
        })
        .to_string()
    }

    #[tokio::test]
    async fn searches_each_row_in_order_with_normalized_matches() {
        let mock = MockMilvus::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            let first = parsed["data"][0][0].as_f64().unwrap() as i64;
            (200, hits_response(first))
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

        let row0: Vec<Value> = serde_json::from_str(matches.value(0)).unwrap();
        assert_eq!(row0.len(), 2);
        assert_eq!(row0[0]["id"], 1);
        assert_eq!(row0[0]["distance"], 0.1);
        assert_eq!(row0[0]["payload"]["text"], "p1");
        let row1: Vec<Value> = serde_json::from_str(matches.value(1)).unwrap();
        assert_eq!(row1[0]["id"], 0);
        assert_eq!(row1[0]["payload"]["text"], "p1");

        let requests = mock.requests();
        assert_eq!(requests.len(), 2, "one request per row");
        // Requests reach the mock in any order (buffered concurrency only
        // guarantees ordered results); locate row 0's request by its vector.
        let (head, body) = requests
            .iter()
            .find(|(_, body)| {
                serde_json::from_str::<Value>(body).unwrap()["data"][0]
                    == serde_json::json!([1.0, 0.0])
            })
            .expect("row 0's request carries the [1.0, 0.0] vector")
            .clone();
        assert!(
            head.starts_with("POST /v2/vectordb/entities/search "),
            "{head}"
        );
        assert!(
            head.contains("authorization: Bearer root:Milvus-pw"),
            "{head}"
        );
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["collectionName"], "docs");
        assert_eq!(parsed["data"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["data"][0], serde_json::json!([1.0, 0.0]));
        assert_eq!(parsed["annsField"], json!("embedding"));
        assert_eq!(parsed["limit"], 5);
        assert_eq!(parsed["searchParams"]["metricType"], "COSINE");
        let fields = parsed["outputFields"].as_array().unwrap();
        assert!(fields.contains(&json!("doc_id")));
        assert!(fields.contains(&json!("payload")));
    }

    #[tokio::test]
    async fn auto_id_omits_id_from_output_fields_and_matches() {
        let mock = MockMilvus::spawn(|_body| {
            (
                200,
                r#"{"code":0,"data":[{"distance":0.3,"payload":{"text":"x"}}]}"#.to_string(),
            )
        });
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"id_field": ""}),
        ));
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
            !parsed["outputFields"]
                .as_array()
                .unwrap()
                .contains(&json!("doc_id")),
            "auto-id collections must not request the id field"
        );
    }

    #[tokio::test]
    async fn metric_type_is_configurable() {
        let mock = MockMilvus::spawn(|_body| {
            (200, r#"{"code":0,"data":[[{"distance":1.0}]]}"#.to_string())
        });
        let processor = build_processor(base_config(
            mock.addr(),
            serde_json::json!({"metric": "IP"}),
        ));
        processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap();
        let (_, body) = mock.last_request();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["searchParams"]["metricType"], "IP");
    }

    #[tokio::test]
    async fn http_200_with_nonzero_code_fails() {
        let mock = MockMilvus::spawn(|_body| {
            (
                200,
                r#"{"code":100,"message":"collection not found"}"#.to_string(),
            )
        });
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
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
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let err = processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("404"), "{err}");
    }

    #[tokio::test]
    async fn null_vector_row_errors_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
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
    async fn empty_batch_short_circuits_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::Utf8,
            true,
        )]));
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
    #[tokio::test]
    async fn transient_429_is_retried_then_succeeds() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        let mock = MockMilvus::spawn(move |_body| {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 2 {
                (429, r#"{"message":"rate limited"}"#.to_string())
            } else {
                (200, r#"{"code":0,"data":[]}"#.to_string())
            }
        });
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        let result = processor.process(vector_batch(vec![vec![1.0]])).await;
        assert!(
            result.is_ok(),
            "429s must be retried to success: {result:?}"
        );
        assert_eq!(mock.requests().len(), 3);
    }

    #[tokio::test]
    async fn deterministic_4xx_is_not_retried() {
        let mock = MockMilvus::spawn(|_body| (400, r#"{"message":"bad request"}"#.to_string()));
        let processor = build_processor(base_config(mock.addr(), serde_json::json!({})));
        processor
            .process(vector_batch(vec![vec![1.0]]))
            .await
            .unwrap_err();
        assert_eq!(
            mock.requests().len(),
            1,
            "4xx rejections must not be retried"
        );
    }
}
