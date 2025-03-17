//! HTTP input component
//!
//! Receive data from HTTP endpoints

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use axum::{extract::State, http::StatusCode, routing::post, Router};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// HTTP input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpInputConfig {
    /// Listening address
    pub address: String,
    /// Path
    pub path: String,
    /// Whether CORS is enabled
    pub cors_enabled: Option<bool>,
}

/// HTTP input component
pub struct HttpInput {
    config: HttpInputConfig,
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<Result<(), Error>>>>>,
    connected: AtomicBool,
}

type AppState = Arc<Mutex<VecDeque<MessageBatch>>>;

impl HttpInput {
    pub fn new(config: HttpInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            server_handle: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }

    async fn handle_request(
        State(state): State<AppState>,
        body: axum::extract::Json<serde_json::Value>,
    ) -> StatusCode {
        let msg = match MessageBatch::from_json(&body.0) {
            Ok(msg) => msg,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        let mut queue = state.lock().await;
        queue.push_back(msg);
        StatusCode::OK
    }
}

#[async_trait]
impl Input for HttpInput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(Ordering::SeqCst) {
            return Ok(());
        }

        let queue = self.queue.clone();
        let path = self.config.path.clone();
        let address = self.config.address.clone();

        let app = Router::new()
            .route(&path, post(Self::handle_request))
            .with_state(queue);

        let addr: SocketAddr = address
            .parse()
            .map_err(|e| Error::Config(format!("Invalid address {}: {}", address, e)))?;

        let server_handle = tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(|e| Error::Connection(format!("HTTP server error: {}", e)))
        });

        let server_handle_arc = self.server_handle.clone();
        let mut server_handle_arc_mutex = server_handle_arc.lock().await;
        *server_handle_arc_mutex = Some(server_handle);
        self.connected.store(true, Ordering::SeqCst);

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        // Try to get a message from the queue
        let msg_option;
        {
            let mut queue = self.queue.lock().await;
            msg_option = queue.pop_front();
        }

        if let Some(msg) = msg_option {
            Ok((msg, Arc::new(NoopAck)))
        } else {
            // If the queue is empty, an error is returned after waiting for a while
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Err(Error::Processing("The queue is empty".to_string()))
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut server_handle_guard = self.server_handle.lock().await;
        if let Some(handle) = server_handle_guard.take() {
            handle.abort();
        }

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) struct HttpInputBuilder;
impl InputBuilder for HttpInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Http input configuration is missing".to_string(),
            ));
        }

        let config: HttpInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(HttpInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("http", Arc::new(HttpInputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use serde_json::json;

    #[tokio::test]
    async fn test_http_input_new() {
        let config = HttpInputConfig {
            address: "127.0.0.1:0".to_string(), // Use random port
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_http_input_connect() {
        let config = HttpInputConfig {
            address: "127.0.0.1:0".to_string(), // Use random port
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config).unwrap();
        let result = input.connect().await;
        assert!(result.is_ok());

        // Test repeated connection
        let result = input.connect().await;
        assert!(result.is_ok());

        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_http_input_read_without_connect() {
        let config = HttpInputConfig {
            address: "127.0.0.1:0".to_string(),
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config).unwrap();
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // Expected error type
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_http_input_read_empty_queue() {
        let config = HttpInputConfig {
            address: "127.0.0.1:0".to_string(),
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config).unwrap();
        assert!(input.connect().await.is_ok());

        // Queue is empty, should return Processing error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Processing(_)) => {} // Expected error type
            _ => panic!("Expected Processing error"),
        }

        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_http_input_invalid_address() {
        let config = HttpInputConfig {
            address: "invalid-address".to_string(), // Invalid address
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config).unwrap();
        let result = input.connect().await;
        assert!(result.is_err());
        match result {
            Err(Error::Config(_)) => {} // Expected error type
            _ => panic!("Expected Config error"),
        }
    }

    #[tokio::test]
    async fn test_http_input_receive_message() {
        // Create a TCP listener to get an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // Release the listener so the HTTP server can use this port
        drop(listener);

        // Create HTTP input component using the obtained port
        let config = HttpInputConfig {
            address: format!("127.0.0.1:{}", port),
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input = HttpInput::new(config.clone()).unwrap();
        assert!(input.connect().await.is_ok());

        // Wait for server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create an HTTP client and send request
        let client = Client::new();
        let test_message = json!({"data": "test message"});

        // Send request and verify response
        let response = client
            .post(format!("http://127.0.0.1:{}{}", port, config.path))
            .json(&test_message)
            .send()
            .await;

        assert!(
            response.is_ok(),
            "HTTP request failed: {:?}",
            response.err()
        );
        let response = response.unwrap();
        assert!(
            response.status().is_success(),
            "HTTP response status is not success: {}",
            response.status()
        );

        // Verify message was received correctly
        let read_result = input.read().await;
        assert!(
            read_result.is_ok(),
            "Failed to read message: {:?}",
            read_result.err()
        );

        let (msg, ack) = read_result.unwrap();
        let content = msg.as_string().unwrap();
        assert_eq!(content, vec![test_message.to_string()]);
        ack.ack().await;

        // Close connection
        assert!(input.close().await.is_ok());
    }
}
