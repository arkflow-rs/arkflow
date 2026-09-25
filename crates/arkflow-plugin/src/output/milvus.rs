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
use datafusion::arrow::array::{Array, Int32Array, Int64Array, LargeStringArray, StringArray};
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
                "retry_count": {"type": "integer", "description": "Retry attempts for connection errors and 5xx responses. Defaults to 0."},
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
    #[serde(default = "default_retry_count")]
    retry_count: u32,
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
fn default_retry_count() -> u32 {
    0
}

/// Rows per upsert request: keeps each JSON body well below REST request
/// size limits. Batches at or below this size stay a single request.
const MILVUS_ROWS_PER_REQUEST: usize = 1000;

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
        let vectors = crate::vector_util::extract_vectors("milvus output", &msg, &self.config.vector_field)?;
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

        // Large batches are split into bounded requests so a single JSON
        // body cannot exceed the REST request size limits; slices preserve
        // row order.
        for chunk in data.chunks(MILVUS_ROWS_PER_REQUEST) {
            self.upsert(chunk).await?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl MilvusOutput {
    async fn upsert(&self, data: &[Value]) -> Result<(), Error> {
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

        let mut attempt = 0u32;
        loop {
            match request.try_clone().expect("request body is JSON").send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<unreadable body>".to_string());
                    if status.is_success() {
                        // Milvus reports failures as HTTP 200 with a
                        // non-zero `code`; those are deterministic, so no
                        // retry.
                        let parsed: Value = serde_json::from_str(&body).map_err(|e| {
                            Error::Process(format!("Milvus response parse failed: {}", e))
                        })?;
                        match parsed.get("code") {
                            None => return Ok(()), // Lenient with older builds that omit the code.
                            Some(code) if code.as_i64() == Some(0) => return Ok(()),
                            Some(code) => {
                                let message = parsed
                                    .get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or("<no message>");
                                return Err(Error::Process(format!(
                                    "Milvus upsert failed with code {}: {}",
                                    code,
                                    crate::vector_util::truncate_body(message)
                                )));
                            }
                        }
                    }
                    // Client errors are deterministic: retrying cannot help.
                    if status.is_client_error() {
                        return Err(Error::Process(format!(
                            "Milvus returned {}: {}",
                            status,
                            crate::vector_util::truncate_body(&body)
                        )));
                    }
                    error!(
                        "Milvus upsert attempt {} failed: {} {}",
                        attempt + 1,
                        status,
                        crate::vector_util::truncate_body(&body)
                    );
                    if attempt >= self.config.retry_count {
                        return Err(Error::Process(format!(
                            "Milvus returned {} after {} attempts: {}",
                            status,
                            attempt + 1,
                            crate::vector_util::truncate_body(&body)
                        )));
                    }
                }
                Err(e) => {
                    error!("Milvus request failed: {}", e);
                    if attempt >= self.config.retry_count {
                        return Err(Error::Connection(format!("Milvus request failed: {}", e)));
                    }
                }
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt - 1))).await;
        }
    }
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
    use crate::vector_util::test_support::MockApi as MockMilvus;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{FixedSizeListArray, Float32Array};
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
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
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
        let output = build_output(base_config(mock.addr(), serde_json::json!({"id_field": ""})));
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
            mock.addr(),
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
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("100"), "{err}");
        assert!(err.contains("collection not found"), "{err}");
    }

    #[tokio::test]
    async fn http_200_with_zero_code_succeeds() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":{}}"#.to_string()));
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
        assert!(output.write(sample_batch()).await.is_ok());
    }

    #[tokio::test]
    async fn http_200_without_code_is_lenient() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"status":"ok"}"#.to_string()));
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
        assert!(output.write(sample_batch()).await.is_ok());
    }

    #[tokio::test]
    async fn non_2xx_is_surfaced_with_status_and_body() {
        let mock = MockMilvus::spawn(|_body| (401, "unauthorized".to_string()));
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.contains("401"), "{err}");
        assert!(err.contains("unauthorized"), "{err}");
    }

    #[tokio::test]
    async fn no_api_key_sends_no_auth_header() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(serde_json::json!({
            "url": format!("http://{}", mock.addr()),
            "collection": "docs",
        }));
        output.write(sample_batch()).await.unwrap();
        let (head, _) = mock.last_request();
        assert!(!head.to_ascii_lowercase().contains("authorization:"), "{head}");
    }

    #[tokio::test]
    async fn null_vector_row_errors_without_request() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0}"#.to_string()));
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
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
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
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

    #[tokio::test]
    async fn transient_5xx_is_retried_with_backoff() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        let mock = MockMilvus::spawn(move |_body| {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 2 {
                (503, r#"{"message":"overloaded"}"#.to_string())
            } else {
                (200, r#"{"code":0,"data":{"upsertCount":1}}"#.to_string())
            }
        });
        let output = build_output(base_config(mock.addr(), serde_json::json!({"retry_count": 2})));
        output.write(sample_batch()).await.unwrap();
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "two 503s then success"
        );
    }

    #[tokio::test]
    async fn client_errors_are_not_retried() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        let mock = MockMilvus::spawn(move |_body| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (400, r#"{"message":"bad request"}"#.to_string())
        });
        let output = build_output(base_config(mock.addr(), serde_json::json!({"retry_count": 3})));
        let err = output
            .write(sample_batch())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("400"), "{err}");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn large_batches_are_split_into_bounded_requests() {
        let mock = MockMilvus::spawn(|_body| (200, r#"{"code":0,"data":{"upsertCount":1}}"#.to_string()));
        let output = build_output(base_config(mock.addr(), serde_json::json!({})));
        output.write(sample_batch_rows(2500)).await.unwrap();

        let requests = mock.requests();
        assert_eq!(requests.len(), 3, "2500 rows -> 3 requests");
        let sizes: Vec<usize> = requests
            .iter()
            .map(|(_, body)| {
                let parsed: Value = serde_json::from_str(body).unwrap();
                parsed["data"].as_array().unwrap().len()
            })
            .collect();
        assert_eq!(sizes, vec![1000, 1000, 500], "slices must preserve row order");
    }

    /// Builds a batch with `rows` rows: an id column, a 2-dim vector column,
    /// and a text column packed into the payload.
    fn sample_batch_rows(rows: usize) -> MessageBatchRef {
        use datafusion::arrow::array::{FixedSizeListArray, Float32Array, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field as F};
        let ids = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let flat = Float32Array::from((0..rows as i64).flat_map(|i| vec![i as f32, 1.0]).collect::<Vec<_>>());
        let item_field = Arc::new(F::new("item", DataType::Float32, true));
        let vectors = FixedSizeListArray::new(item_field, 2, Arc::new(flat), None);
        let texts = StringArray::from(vec!["t"; rows]);
        let schema = Arc::new(Schema::new(vec![
            F::new("doc_id", DataType::Int64, false),
            F::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(F::new("item", DataType::Float32, true)), 2),
                false,
            ),
            F::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(vectors), Arc::new(texts)])
            .unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }
}
