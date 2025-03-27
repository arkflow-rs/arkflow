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
            Err(Error::Process("The queue is empty".to_string()))
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
