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

//! LLM processor component
//!
//! Sends each row of a text column to an OpenAI-compatible chat
//! completions API and appends the completion as a Utf8 column. Requests
//! run with bounded, order-preserving concurrency. `api_key` supports
//! secret references (`${env:...}`), resolved at configuration load time.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::error;

pub fn init() -> Result<(), Error> {
    register_processor_builder("llm", Arc::new(LlmProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "llm",
        "Sends each row of a text column to an OpenAI-compatible chat completions API and appends the completion as a Utf8 column, with bounded ordered concurrency.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "api_base": {"type": "string", "description": "Base URL of the API, e.g. https://api.openai.com/v1 (the request path is {api_base}/chat/completions)."},
                "model": {"type": "string", "description": "Chat model name, e.g. gpt-4o-mini."},
                "api_key": {"type": "string", "description": "API key sent as 'Authorization: Bearer'; supports secret references. Omit for unauthenticated endpoints."},
                "field": {"type": "string", "description": "Name of the input UTF-8 column to send."},
                "target_field": {"type": "string", "description": "Name of the appended completion column. Defaults to 'response'."},
                "system_prompt": {"type": "string", "description": "Optional system message prepended to each request."},
                "prompt_template": {"type": "string", "description": "Optional user-message template; '{{value}}' is replaced with the row text. When omitted the row text is the user message."},
                "temperature": {"type": "number", "description": "Sampling temperature; omitted from the request when unset."},
                "max_tokens": {"type": "integer", "description": "Completion token cap; omitted from the request when unset."},
                "concurrency": {"type": "integer", "description": "Maximum in-flight requests. Defaults to 4."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers."}
            },
            "required": ["api_base", "model", "field"]
        }),
    )
    .with_example(serde_json::json!({
        "api_base": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
        "api_key": "${env:OPENAI_API_KEY}",
        "field": "text",
        "system_prompt": "You classify support tickets with one word.",
        "concurrency": 4
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LlmProcessorConfig {
    api_base: String,
    model: String,
    field: String,
    #[serde(default = "default_target_field")]
    target_field: String,
    #[serde(default)]
    system_prompt: Option<String>,
    #[serde(default)]
    prompt_template: Option<String>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

fn default_target_field() -> String {
    "response".to_string()
}
fn default_concurrency() -> usize {
    4
}
fn default_timeout_ms() -> u64 {
    30000
}

struct LlmProcessor {
    config: LlmProcessorConfig,
    client: Client,
}

#[derive(serde::Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<Choice>,
}

#[derive(serde::Deserialize)]
struct Choice {
    message: ChatMessage,
}

#[derive(serde::Deserialize)]
struct ChatMessage {
    content: Option<String>,
}

#[async_trait]
impl Processor for LlmProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let texts = extract_string_column(&msg_batch, &self.config.field)?;
        let completions = self.complete_all(&texts).await?;
        let batch = append_column(&msg_batch, &self.config.target_field, &completions)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl LlmProcessor {
    /// One request per row; results are collected in row order while at
    /// most `concurrency` requests are in flight. Requests already in
    /// flight complete even if an earlier row failed — the batch fails as
    /// a whole either way.
    async fn complete_all(&self, texts: &[&str]) -> Result<Vec<String>, Error> {
        let owned: Vec<String> = texts.iter().map(|text| text.to_string()).collect();
        futures_util::stream::iter(owned.into_iter().map(|text| self.complete(text)))
            .buffered(self.config.concurrency)
            .collect::<Vec<Result<String, Error>>>()
            .await
            .into_iter()
            .collect()
    }

    async fn complete(&self, text: String) -> Result<String, Error> {
        let mut messages = Vec::new();
        if let Some(system_prompt) = &self.config.system_prompt {
            messages.push(json!({"role": "system", "content": system_prompt}));
        }
        let user_content = match &self.config.prompt_template {
            Some(template) => template.replace("{{value}}", &text),
            None => text,
        };
        messages.push(json!({"role": "user", "content": user_content}));

        let mut body = json!({"model": self.config.model, "messages": messages});
        if let Some(temperature) = self.config.temperature {
            body["temperature"] = json!(temperature);
        }
        if let Some(max_tokens) = self.config.max_tokens {
            body["max_tokens"] = json!(max_tokens);
        }

        let url = format!(
            "{}/chat/completions",
            self.config.api_base.trim_end_matches('/')
        );
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
            error!("LLM API request failed: {}", e);
            Error::Process(format!("LLM API request failed: {}", e))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Process(format!("LLM API response read failed: {}", e)))?;
        if !status.is_success() {
            return Err(Error::Process(format!(
                "LLM API returned {}: {}",
                status,
                truncate_body(&body)
            )));
        }

        let parsed: ChatCompletionResponse = serde_json::from_str(&body).map_err(|e| {
            Error::Process(format!("LLM API response parse failed: {}", e))
        })?;
        let content = parsed
            .choices
            .first()
            .and_then(|choice| choice.message.content.clone())
            .ok_or_else(|| {
                Error::Process("LLM API response has no choice content".to_string())
            })?;
        Ok(content)
    }
}

struct LlmProcessorBuilder;
impl ProcessorBuilder for LlmProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: LlmProcessorConfig = parse_config(config, "llm processor")?;
        if config.api_base.trim().is_empty() {
            return Err(Error::Config(
                "llm processor: 'api_base' must not be empty".to_string(),
            ));
        }
        if config.model.trim().is_empty() {
            return Err(Error::Config(
                "llm processor: 'model' must not be empty".to_string(),
            ));
        }
        if config.field.trim().is_empty() {
            return Err(Error::Config(
                "llm processor: 'field' must not be empty".to_string(),
            ));
        }
        if config.concurrency == 0 {
            return Err(Error::Config(
                "llm processor: 'concurrency' must be at least 1".to_string(),
            ));
        }
        // Loopback endpoints (local vLLM/Ollama gateways, tests) bypass a
        // system proxy — proxying localhost is never what a user means.
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
        Ok(Arc::new(LlmProcessor { config, client }))
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
            Error::Process(format!("llm processor: input column '{}' not found", field))
        })?;
    if column.data_type() != &DataType::Utf8 && column.data_type() != &DataType::LargeUtf8 {
        return Err(Error::Process(format!(
            "llm processor: column '{}' must be Utf8, got {:?}",
            field,
            column.data_type()
        )));
    }

    let mut texts = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if column.is_null(row) {
            return Err(Error::Process(format!(
                "llm processor: column '{}' has a null value at row {row}; LLM calls require non-null text",
                field
            )));
        }
        let value = downcast_value(column, row)?;
        texts.push(value);
    }
    Ok(texts)
}

fn downcast_value(column: &Arc<dyn Array>, row: usize) -> Result<&str, Error> {
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
    {
        Ok(array.value(row))
    } else {
        Err(Error::Process(
            "llm processor: unexpected string array type".to_string(),
        ))
    }
}

fn append_column(
    batch: &MessageBatch,
    target_field: &str,
    completions: &[String],
) -> Result<MessageBatch, Error> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(target_field, DataType::Utf8, true)));
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|index| batch.column(index).clone())
        .collect();
    columns.push(Arc::new(StringArray::from(completions.to_vec())));

    let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Process(format!("llm processor: batch rebuild failed: {e}")))?;
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

    /// Minimal in-process HTTP server: one request per connection, canned
    /// status/body per request, logs every request.
    struct MockApi {
        addr: std::net::SocketAddr,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockApi {
        fn spawn<F>(handler: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            let handler = Arc::new(handler);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let tracker_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let tracker_max = max_in_flight.clone();
            let request_log = requests.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let handler = handler.clone();
                    let request_log = request_log.clone();
                    let tracker_in_flight = tracker_in_flight.clone();
                    let tracker_max = tracker_max.clone();
                    std::thread::spawn(move || {
                        let now_in_flight = tracker_in_flight
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                            + 1;
                        tracker_max.fetch_max(now_in_flight, std::sync::atomic::Ordering::SeqCst);

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
                        tracker_in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    });
                }
            });
            Self {
                addr,
                requests,
                max_in_flight,
            }
        }

        fn requests(&self) -> Vec<(String, String)> {
            self.requests.lock().unwrap().clone()
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn completion_body(text: &str) -> String {
        json!({
            "choices": [{"message": {"role": "assistant", "content": text}}]
        })
        .to_string()
    }

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        LlmProcessorBuilder
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

    #[tokio::test]
    async fn completes_rows_in_order_and_keeps_original_column() {
        let mock = MockApi::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            let text = parsed["messages"][0]["content"].as_str().unwrap().to_string();
            (200, completion_body(&format!("out-{text}")))
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c")]);

        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        assert_eq!(output.num_rows(), 3);
        let text_col = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let response_col = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text_col.value(1), "b", "original column must be untouched");
        for row in 0..3 {
            assert_eq!(response_col.value(row), format!("out-{}", ["a", "b", "c"][row]));
        }
        assert_eq!(output.schema().field_with_name("response").unwrap().data_type(), &DataType::Utf8);
    }

    #[tokio::test]
    async fn message_construction_three_states() {
        // system_prompt set
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"system_prompt": "be brief"}),
        ));
        processor.process(text_batch(vec![Some("x")])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["messages"][0]["role"], "system");
        assert_eq!(parsed["messages"][0]["content"], "be brief");
        assert_eq!(parsed["messages"][1]["content"], "x");

        // prompt_template substitution
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"prompt_template": "Translate: {{value}}"}),
        ));
        processor.process(text_batch(vec![Some("hi")])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["messages"].as_array().unwrap().len(), 1, "no system message when unset");
        assert_eq!(parsed["messages"][0]["content"], "Translate: hi");

        // neither set: raw text is the user message
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        processor.process(text_batch(vec![Some("raw")])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["messages"][0]["content"], "raw");
    }

    #[tokio::test]
    async fn optional_params_omitted_when_unset_and_sent_when_set() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        processor.process(text_batch(vec![Some("x")])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert!(parsed.get("temperature").is_none());
        assert!(parsed.get("max_tokens").is_none());

        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"temperature": 0.2, "max_tokens": 64}),
        ));
        processor.process(text_batch(vec![Some("x")])).await.unwrap();
        let (_, body) = mock.requests().remove(0);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["temperature"], 0.2);
        assert_eq!(parsed["max_tokens"], 64);
    }

    #[tokio::test]
    async fn bounded_concurrency_preserves_order() {
        // Four 100ms rows at concurrency 2 finish in ~2 waves (~200ms);
        // serial execution would need ~400ms.
        let mock = MockApi::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(100));
            let text = parsed["messages"][0]["content"].as_str().unwrap();
            (200, completion_body(&format!("done-{text}")))
        });
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"concurrency": 2}),
        ));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c"), Some("d")]);

        let start = std::time::Instant::now();
        let result = processor.process(batch).await.unwrap();
        let elapsed = start.elapsed();

        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        let response_col = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, name) in ["a", "b", "c", "d"].iter().enumerate() {
            assert_eq!(response_col.value(row), format!("done-{name}"), "row order must hold");
        }
        assert!(elapsed < std::time::Duration::from_millis(320), "no overlap: took {elapsed:?}");
        assert_eq!(mock.requests().len(), 4);
    }

    #[tokio::test]
    async fn in_flight_requests_respect_concurrency_cap() {
        let mock = MockApi::spawn(|body| {
            let parsed: Value = serde_json::from_str(body).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));
            (200, completion_body(parsed["messages"][0]["content"].as_str().unwrap()))
        });
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"concurrency": 2}),
        ));
        let batch = text_batch(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")]);
        processor.process(batch).await.unwrap();
        let max = mock.max_in_flight();
        assert!(max <= 2, "in-flight requests exceeded the cap: {max}");
        assert!(max >= 2, "requests did not overlap; expected pipelining, max={max}");
    }

    #[tokio::test]
    async fn api_key_sent_as_bearer() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        processor.process(text_batch(vec![Some("x")])).await.unwrap();
        let (head, _) = mock.requests().remove(0);
        assert!(head.contains("authorization: Bearer sk-test"), "{head}");
    }

    #[tokio::test]
    async fn no_api_key_sends_no_auth_header() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(
            serde_json::json!({
                "api_base": format!("http://{}", mock.addr),
                "model": "m",
                "field": "text",
            }),
        );
        processor.process(text_batch(vec![Some("x")])).await.unwrap();
        let (head, _) = mock.requests().remove(0);
        assert!(!head.to_ascii_lowercase().contains("authorization:"), "{head}");
    }

    #[tokio::test]
    async fn non_2xx_is_surfaced_with_status_and_body() {
        let mock = MockApi::spawn(|_body| (429, r#"{"error":"rate limited"}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(text_batch(vec![Some("x")]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("429"), "{err}");
        assert!(err.contains("rate limited"), "{err}");
    }

    #[tokio::test]
    async fn missing_content_errors() {
        let mock = MockApi::spawn(|_body| (200, r#"{"choices": [{"message": {"role": "assistant", "content": null}}]}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(text_batch(vec![Some("x")]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no choice content"), "{err}");

        let mock = MockApi::spawn(|_body| (200, r#"{"choices": []}"#.to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(text_batch(vec![Some("x")]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no choice content"), "{err}");
    }

    #[tokio::test]
    async fn null_input_row_errors() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(text_batch(vec![Some("a"), None]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("row 1"), "{err}");
    }

    #[tokio::test]
    async fn non_string_column_errors() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
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
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::LargeUtf8, true)]));
        let array = Arc::new(LargeStringArray::from(vec![Some("hello")]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ));
        processor.process(batch).await.unwrap();
        assert_eq!(mock.requests().len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_http() {
        let mock = MockApi::spawn(|_body| (200, completion_body("ok")));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let result = processor.process(text_batch(vec![])).await.unwrap();
        assert!(matches!(result, ProcessResult::None));
        assert_eq!(mock.requests().len(), 0);
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"model": "m", "field": "text"}),
            serde_json::json!({"api_base": "http://localhost", "field": "text"}),
            serde_json::json!({"api_base": "http://localhost", "model": "m"}),
            serde_json::json!({
                "api_base": "http://localhost",
                "model": "m",
                "field": "text",
                "concurrency": 0
            }),
        ] {
            assert!(
                LlmProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }
}
