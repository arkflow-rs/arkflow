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

//! HTTP output component
//!
//! Send the processed data to the HTTP endpoint

use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    codec::Codec,
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatchRef, Resource,
};
use async_trait::async_trait;
use base64::Engine;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Authentication type
#[derive(Debug, Clone, Serialize, Deserialize)]
enum AuthType {
    /// Basic authentication
    Basic { username: String, password: String },
    /// Bearer token authentication
    Bearer { token: String },
}

/// HTTP output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HttpOutputConfig {
    /// Destination URL
    url: String,
    /// HTTP method
    method: String,
    /// Timeout Period (ms)
    timeout_ms: u64,
    /// Number of retries
    retry_count: u32,
    /// Request header
    headers: Option<std::collections::HashMap<String, String>>,
    /// Body type
    body_field: Option<String>,
    /// Authentication configuration
    auth: Option<AuthType>,
}

/// HTTP output component
struct HttpOutput {
    config: HttpOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    connected: AtomicBool,
    auth: Option<AuthType>,
    codec: Option<Arc<dyn Codec>>,
}

impl HttpOutput {
    /// Create a new HTTP output component
    fn new(config: HttpOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        let auth = config.auth.clone();
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            auth,
            codec,
        })
    }
}

#[async_trait]
impl Output for HttpOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Create an HTTP client
        let client_builder =
            Client::builder().timeout(std::time::Duration::from_millis(self.config.timeout_ms));
        let client_arc = self.client.clone();
        client_arc.lock().await.replace(
            client_builder.build().map_err(|e| {
                Error::Connection(format!("Unable to create an HTTP client: {}", e))
            })?,
        );

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        // Apply codec encoding if configured
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec).await?;
        if payloads.is_empty() {
            return Ok(());
        }

        for x in payloads {
            self.send(&x).await?
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }
}

impl HttpOutput {
    async fn send(&self, data: &[u8]) -> Result<(), Error> {
        let client_arc = self.client.clone();
        let client_arc_guard = client_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || client_arc_guard.is_none() {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let client = client_arc_guard.as_ref().unwrap();
        // Build the request
        let mut request_builder = match self.config.method.to_uppercase().as_str() {
            "GET" => client.get(&self.config.url),
            "POST" => client.post(&self.config.url).body(data.to_vec()), // Content-Type由统一逻辑添加
            "PUT" => client.put(&self.config.url).body(data.to_vec()),
            "DELETE" => client.delete(&self.config.url),
            "PATCH" => client.patch(&self.config.url).body(data.to_vec()),
            _ => {
                return Err(Error::Config(format!(
                    "HTTP methods that are not supported: {}",
                    self.config.method
                )))
            }
        };

        // Add authentication header if configured
        if let Some(auth_config) = &self.auth {
            match auth_config {
                AuthType::Basic { username, password } => {
                    let credentials = format!("{}:{}", username, password);
                    let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
                    request_builder =
                        request_builder.header(header::AUTHORIZATION, format!("Basic {}", encoded));
                }
                AuthType::Bearer { token } => {
                    request_builder =
                        request_builder.header(header::AUTHORIZATION, format!("Bearer {}", token));
                }
            }
        }

        // Add request headers
        if let Some(headers) = &self.config.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // Add content type header (if not specified)
        // 始终添加Content-Type头（如果未指定）
        if let Some(headers) = &self.config.headers {
            if !headers.contains_key("Content-Type") {
                request_builder = request_builder.header(header::CONTENT_TYPE, "application/json");
            }
        } else {
            request_builder = request_builder.header(header::CONTENT_TYPE, "application/json");
        }

        // Send a request
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count <= self.config.retry_count {
            match request_builder.try_clone().unwrap().send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    } else {
                        let status = response.status();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "<Unable to read response body>".to_string());
                        last_error = Some(Error::Process(format!(
                            "HTTP Request Failed: Status code {}, response: {}",
                            status, body
                        )));
                    }
                }
                Err(e) => {
                    last_error = Some(Error::Connection(format!("HTTP request error: {}", e)));
                }
            }

            retry_count += 1;
            if retry_count <= self.config.retry_count {
                // Index backoff retry
                tokio::time::sleep(std::time::Duration::from_millis(
                    100 * 2u64.pow(retry_count - 1),
                ))
                .await;
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Unknown("Unknown HTTP error".to_string())))
    }
}
pub(crate) struct HttpOutputBuilder;
impl OutputBuilder for HttpOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: HttpOutputConfig = parse_config(config, "HttpOutput input")?;

        Ok(Arc::new(HttpOutput::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("http", Arc::new(HttpOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "http",
        "Posts each batch to an HTTP endpoint. Supports custom headers, retry, and auth.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Destination URL."},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"], "default": "POST", "description": "HTTP method."},
                "timeout_ms": {"type": "integer", "minimum": 1, "description": "Request timeout in milliseconds."},
                "retry_count": {"type": "integer", "minimum": 0, "description": "Number of retry attempts on failure."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Custom HTTP headers."},
                "body_field": {"type": "string", "description": "Record field that holds the request body."},
                "auth": {"type": "object", "description": "Authentication configuration."}
            },
            "required": ["url"]
        }),
    ).with_example(serde_json::json!({
        "url": "https://example.com/ingest",
        "method": "POST",
        "timeout_ms": 5000,
        "retry_count": 3
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    fn build(config: serde_json::Value) -> Result<Arc<dyn Output>, Error> {
        HttpOutputBuilder.build(None, &Some(config), None, &resource())
    }

    fn base_config(url: String) -> serde_json::Value {
        serde_json::json!({
            "url": url,
            "method": "POST",
            "timeout_ms": 2_000,
            "retry_count": 0,
        })
    }

    fn batch() -> MessageBatchRef {
        Arc::new(MessageBatch::new_binary(vec![b"{\"k\":1}".to_vec()]).unwrap())
    }

    #[tokio::test]
    async fn post_delivers_the_batch() -> Result<(), Error> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let output = build(base_config(format!("{}/ingest", server.uri())))?;
        output.connect().await?;
        output.write(batch()).await?;
        output.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn each_supported_method_reaches_the_endpoint() -> Result<(), Error> {
        for verb in ["GET", "PUT", "PATCH", "DELETE"] {
            let server = MockServer::start().await;
            Mock::given(method(verb))
                .and(path("/sink"))
                .respond_with(ResponseTemplate::new(200))
                .expect(1)
                .mount(&server)
                .await;
            let config = serde_json::json!({
                "url": format!("{}/sink", server.uri()),
                "method": verb,
                "timeout_ms": 2_000,
                "retry_count": 0,
            });
            let output = build(config)?;
            output.connect().await?;
            output.write(batch()).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn basic_and_bearer_auth_headers_are_sent() -> Result<(), Error> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth"))
            .and(wiremock::matchers::header(
                "Authorization",
                "Basic dXNlcjpwYXNz",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = base_config(format!("{}/auth", server.uri()));
        config["auth"] =
            serde_json::json!({"Basic": {"username": "user", "password": "pass"}});
        let output = build(config)?;
        output.connect().await?;
        output.write(batch()).await?;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth"))
            .and(wiremock::matchers::header(
                "Authorization",
                "Bearer token-1",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = base_config(format!("{}/auth", server.uri()));
        config["auth"] = serde_json::json!({"Bearer": {"token": "token-1"}});
        let output = build(config)?;
        output.connect().await?;
        output.write(batch()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn custom_header_and_default_content_type() -> Result<(), Error> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hdr"))
            .and(wiremock::matchers::header("X-Custom", "1"))
            .and(wiremock::matchers::header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = base_config(format!("{}/hdr", server.uri()));
        config["headers"] = serde_json::json!({"X-Custom": "1"});
        let output = build(config)?;
        output.connect().await?;
        output.write(batch()).await?;

        // An explicit Content-Type is preserved, not overwritten.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hdr"))
            .and(wiremock::matchers::header("Content-Type", "text/plain"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = base_config(format!("{}/hdr", server.uri()));
        config["headers"] = serde_json::json!({"Content-Type": "text/plain"});
        let output = build(config)?;
        output.connect().await?;
        output.write(batch()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_error_is_retried_then_reported() -> Result<(), Error> {
        let server = MockServer::start().await;
        // One failure then one success with retry_count=1.
        Mock::given(method("POST"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = base_config(format!("{}/flaky", server.uri()));
        config["retry_count"] = serde_json::json!(1);
        let output = build(config)?;
        output.connect().await?;
        output.write(batch()).await?;

        // Exhausted retries surface the failure.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/down"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let output = build(base_config(format!("{}/down", server.uri())))?;
        output.connect().await?;
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            output.write(batch()),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(err.to_string().contains("503"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn unreachable_endpoint_reports_a_connection_error() -> Result<(), Error> {
        // Port 1 refuses connections immediately.
        let output = build(base_config("http://127.0.0.1:1/sink".into()))?;
        output.connect().await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            output.write(batch()),
        )
        .await;
        match result {
            Err(_elapsed) => panic!("write must not hang on a refused connection"),
            Ok(Err(err)) => assert!(matches!(err, Error::Connection(_)), "{err}"),
            Ok(Ok(())) => panic!("a refused connection must not report success"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn send_without_connect_and_after_close_is_rejected() -> Result<(), Error> {
        let output = build(base_config("http://127.0.0.1:1/sink".into()))?;
        let err = output.write(batch()).await.unwrap_err();
        assert!(matches!(err, Error::Connection(_)), "{err}");

        let server = MockServer::start().await;
        let output = build(base_config(format!("{}/x", server.uri())))?;
        output.connect().await?;
        output.close().await?;
        let err = output.write(batch()).await.unwrap_err();
        assert!(matches!(err, Error::Connection(_)), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_method_is_rejected_at_send_time() -> Result<(), Error> {
        let mut config = base_config("http://127.0.0.1:1/sink".into());
        config["method"] = serde_json::json!("TRACE");
        let output = build(config)?;
        output.connect().await?;
        let err = output.write(batch()).await.unwrap_err();
        assert!(err.to_string().contains("not supported"), "{err}");
        Ok(())
    }
}
