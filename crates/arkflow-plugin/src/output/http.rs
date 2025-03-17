//! HTTP output component
//!
//! Send the processed data to the HTTP endpoint

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// HTTP output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOutputConfig {
    /// Destination URL
    pub url: String,
    /// HTTP method
    pub method: String,
    /// Timeout Period (ms)
    pub timeout_ms: u64,
    /// Number of retries
    pub retry_count: u32,
    /// Request header
    pub headers: Option<std::collections::HashMap<String, String>>,
}

/// HTTP output component
pub struct HttpOutput {
    config: HttpOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    connected: AtomicBool,
}

impl HttpOutput {
    /// Create a new HTTP output component
    pub fn new(config: HttpOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
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

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let client_arc = self.client.clone();
        let client_arc_guard = client_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || client_arc_guard.is_none() {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let client = client_arc_guard.as_ref().unwrap();
        let content = msg.as_string()?;
        if content.is_empty() {
            return Ok(());
        }
        let body;
        if content.len() == 1 {
            body = content[0].clone();
        } else {
            body = serde_json::to_string(&content)
                .map_err(|_| Error::Processing("Unable to serialize message".to_string()))?;
        }

        // Build the request
        let mut request_builder = match self.config.method.to_uppercase().as_str() {
            "GET" => client.get(&self.config.url),
            "POST" => client.post(&self.config.url).body(body), // Content-Type由统一逻辑添加
            "PUT" => client.put(&self.config.url).body(body),
            "DELETE" => client.delete(&self.config.url),
            "PATCH" => client.patch(&self.config.url).body(body),
            _ => {
                return Err(Error::Config(format!(
                    "HTTP methods that are not supported: {}",
                    self.config.method
                )))
            }
        };

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
                        last_error = Some(Error::Processing(format!(
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

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }
}

pub(crate) struct HttpOutputBuilder;
impl OutputBuilder for HttpOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "HTTP output configuration is missing".to_string(),
            ));
        }
        let config: HttpOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(HttpOutput::new(config)?))
    }
}

pub fn init() {
    register_output_builder("http", Arc::new(HttpOutputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Json,
        http::StatusCode,
        response::IntoResponse,
        routing::{delete, get, patch, post, put},
        Router,
    };
    use serde_json::json;
    use std::net::SocketAddr;

    /// Test creating a new HTTP output component
    #[tokio::test]
    async fn test_http_output_new() {
        // Create a basic configuration
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create a new HTTP output component
        let output = HttpOutput::new(config);
        assert!(output.is_ok(), "Failed to create HTTP output component");
    }

    /// Test connecting to the HTTP output
    #[tokio::test]
    async fn test_http_output_connect() {
        // Create a basic configuration
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create and connect the HTTP output
        let output = HttpOutput::new(config).unwrap();
        let result = output.connect().await;
        assert!(result.is_ok(), "Failed to connect HTTP output");
        assert!(
            output.connected.load(Ordering::SeqCst),
            "Connected flag not set"
        );

        // Verify client is initialized
        let client_guard = output.client.lock().await;
        assert!(client_guard.is_some(), "HTTP client not initialized");
    }

    /// Test closing the HTTP output
    #[tokio::test]
    async fn test_http_output_close() {
        // Create a basic configuration
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create, connect, and close the HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();
        let result = output.close().await;

        assert!(result.is_ok(), "Failed to close HTTP output");
        assert!(
            !output.connected.load(Ordering::SeqCst),
            "Connected flag not reset"
        );

        // Verify client is cleared
        let client_guard = output.client.lock().await;
        assert!(client_guard.is_none(), "HTTP client not cleared");
    }

    /// Test writing to HTTP output without connecting first
    #[tokio::test]
    async fn test_http_output_write_without_connect() {
        // Create a basic configuration
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create HTTP output without connecting
        let output = HttpOutput::new(config).unwrap();
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;

        // Should return connection error
        assert!(result.is_err(), "Write should fail when not connected");
        match result {
            Err(Error::Connection(_)) => {} // Expected error
            _ => panic!("Expected Connection error"),
        }
    }

    /// Test writing with empty message
    #[tokio::test]
    async fn test_http_output_write_empty_message() {
        // Create a basic configuration
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create and connect HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();

        // Create empty message
        let msg = MessageBatch::new_binary(vec![]);
        let result = output.write(&msg).await;

        // Should succeed with empty message
        assert!(result.is_ok(), "Write should succeed with empty message");
    }

    /// Test HTTP output with custom headers
    #[tokio::test]
    async fn test_http_output_with_custom_headers() {
        // Create a configuration with custom headers
        let mut headers = std::collections::HashMap::new();
        headers.insert("X-Custom-Header".to_string(), "test-value".to_string());
        headers.insert("Content-Type".to_string(), "application/xml".to_string());

        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: Some(headers),
        };

        // Create and connect HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();

        // We can't easily test the actual headers sent without a mock server,
        // but we can verify the component initializes correctly
        assert!(output.connected.load(Ordering::SeqCst));
    }

    /// Test HTTP output with unsupported method
    #[tokio::test]
    async fn test_http_output_unsupported_method() {
        // Create a configuration with unsupported method
        let config = HttpOutputConfig {
            url: "http://example.com".to_string(),
            method: "CONNECT".to_string(), // Unsupported method
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create and connect HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();

        // Try to write a message
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;

        // Should return config error
        assert!(result.is_err(), "Write should fail with unsupported method");
        match result {
            Err(Error::Config(_)) => {} // Expected error
            _ => panic!("Expected Config error, got {:?}", result),
        }
    }

    /// Helper function to create a test HTTP server
    async fn setup_test_server() -> (String, tokio::task::JoinHandle<()>) {
        // Create a router with endpoints for different HTTP methods
        let app = Router::new()
            .route("/get", get(|| async { StatusCode::OK }))
            .route(
                "/post",
                post(|_: Json<serde_json::Value>| async { StatusCode::CREATED }),
            )
            .route(
                "/put",
                put(|_: Json<serde_json::Value>| async { StatusCode::OK }),
            )
            .route("/delete", delete(|| async { StatusCode::NO_CONTENT }))
            .route(
                "/patch",
                patch(|_: Json<serde_json::Value>| async { StatusCode::OK }),
            )
            .route(
                "/error",
                get(|| async { StatusCode::INTERNAL_SERVER_ERROR.into_response() }),
            );

        // Find an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();
        drop(listener); // Release the port for the server to use

        // Start the server in a separate task
        let server_handle = tokio::spawn(async move {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        // Give the server a moment to start up
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Return the base URL and server handle
        (format!("http://127.0.0.1:{}", port), server_handle)
    }

    /// Test HTTP output with successful GET request
    #[tokio::test]
    async fn test_http_output_get_request() {
        // Start a test server
        let (base_url, server_handle) = setup_test_server().await;
        let url = format!("{}/get", base_url);

        // Create configuration for GET request
        let config = HttpOutputConfig {
            url,
            method: "GET".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create, connect, and write to HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;

        // Should succeed with 200 OK response
        assert!(result.is_ok(), "Write should succeed with 200 OK response");

        // Clean up
        output.close().await.unwrap();
        server_handle.abort();
    }

    /// Test HTTP output with successful POST request
    #[tokio::test]
    async fn test_http_output_post_request() {
        // Start a test server
        let (base_url, server_handle) = setup_test_server().await;
        let url = format!("{}/post", base_url);

        // Create configuration for POST request
        let config = HttpOutputConfig {
            url,
            method: "POST".to_string(),
            timeout_ms: 5000,
            retry_count: 3,
            headers: None,
        };

        // Create, connect, and write to HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();

        // Use a valid JSON string for the message
        let msg = MessageBatch::from_string("{\"test\": \"message\"}");

        // Add a small delay to ensure server is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let result = output.write(&msg).await;

        // Should succeed with 201 Created response
        assert!(
            result.is_ok(),
            "Write should succeed with 201 Created response"
        );

        // Clean up
        output.close().await.unwrap();
        server_handle.abort();
    }

    /// Test HTTP output with error response
    #[tokio::test]
    async fn test_http_output_error_response() {
        // Start a test server
        let (base_url, server_handle) = setup_test_server().await;
        let url = format!("{}/error", base_url);

        // Create configuration with no retries
        let config = HttpOutputConfig {
            url,
            method: "GET".to_string(),
            timeout_ms: 5000,
            retry_count: 0, // No retries
            headers: None,
        };

        // Create, connect, and write to HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;

        // Should fail with either Processing or Connection error
        assert!(result.is_err(), "Write should fail with error response");
        match result {
            Err(Error::Processing(_)) => {} // Expected error type 1
            Err(Error::Connection(_)) => {} // Also acceptable error type
            _ => panic!("Expected Processing or Connection error, got {:?}", result),
        }

        // Clean up
        output.close().await.unwrap();
        server_handle.abort();
    }

    /// Test HTTP output retry mechanism
    #[tokio::test]
    async fn test_http_output_retry() {
        // Start a test server
        let (base_url, server_handle) = setup_test_server().await;

        // Use a non-existent endpoint to force connection errors
        let url = format!("{}/nonexistent", base_url);

        // Create configuration with retries
        let config = HttpOutputConfig {
            url,
            method: "GET".to_string(),
            timeout_ms: 1000,
            retry_count: 2, // 2 retries
            headers: None,
        };

        // Create, connect, and write to HTTP output
        let output = HttpOutput::new(config).unwrap();
        output.connect().await.unwrap();
        let msg = MessageBatch::from_string("test message");

        // Measure time to verify retry delay
        let start = std::time::Instant::now();
        let result = output.write(&msg).await;
        let elapsed = start.elapsed();

        // Should fail after retries
        assert!(result.is_err(), "Write should fail after retries");

        // Verify that some time has passed for retries (at least 300ms for 2 retries)
        // First retry: 100ms, Second retry: 200ms
        assert!(
            elapsed.as_millis() >= 300,
            "Retry mechanism not working properly"
        );

        // Clean up
        output.close().await.unwrap();
        server_handle.abort();
    }

    /// Test HTTP output builder
    #[tokio::test]
    async fn test_http_output_builder() {
        // Create a valid configuration
        let config = json!({
            "url": "http://example.com",
            "method": "POST",
            "timeout_ms": 5000,
            "retry_count": 3
        });

        // Create builder and build output
        let builder = HttpOutputBuilder;
        let result = builder.build(&Some(config));
        assert!(
            result.is_ok(),
            "Builder should create output with valid config"
        );

        // Test with missing configuration
        let result = builder.build(&None);
        assert!(result.is_err(), "Builder should fail with missing config");

        // Test with invalid configuration
        let invalid_config = json!({"invalid": "config"});
        let result = builder.build(&Some(invalid_config));
        assert!(result.is_err(), "Builder should fail with invalid config");
    }
}
