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

use arkflow_core::{
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatchRef, Resource, DEFAULT_BINARY_VALUE_FIELD,
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
}

impl HttpOutput {
    /// Create a new HTTP output component
    fn new(config: HttpOutputConfig) -> Result<Self, Error> {
        let auth = config.auth.clone();
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            auth,
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
        let body_field = self
            .config
            .body_field
            .as_deref()
            .unwrap_or(DEFAULT_BINARY_VALUE_FIELD);
        let content = msg.to_binary(body_field)?;
        if content.is_empty() {
            return Ok(());
        }

        for x in content {
            self.send(x).await?
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
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "HTTP output configuration is missing".to_string(),
            ));
        }
        let config: HttpOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(HttpOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("http", Arc::new(HttpOutputBuilder))
}
