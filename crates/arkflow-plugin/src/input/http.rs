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

//! HTTP input component
//!
//! Receive data from HTTP endpoints

use arkflow_core::codec::Codec;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use axum::http::header;
use axum::http::header::HeaderMap;
use axum::{extract::State, http::StatusCode, routing::post, Router};
use base64::Engine;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

/// HTTP input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthType {
    /// Basic authentication
    Basic { username: String, password: String },
    /// Bearer token authentication
    Bearer { token: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpInputConfig {
    /// Listening address
    pub address: String,
    /// Path
    pub path: String,
    /// Whether CORS is enabled
    pub cors_enabled: Option<bool>,
    /// Authentication configuration
    pub auth: Option<AuthType>,
}

/// HTTP input component
pub struct HttpInput {
    input_name: Option<String>,
    config: HttpInputConfig,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<Result<(), Error>>>>>,
    sender: Arc<Sender<MessageBatch>>,
    receiver: Arc<Receiver<MessageBatch>>,
    connected: AtomicBool,
    auth: Option<AuthType>,
    codec: Option<Arc<dyn Codec>>,
}

struct AppStateInner {
    sender: Sender<MessageBatch>,
    auth: Option<AuthType>,
    codec: Option<Arc<dyn Codec>>,
}

type AppState = Arc<AppStateInner>;

impl HttpInput {
    pub fn new(
        name: Option<&String>,
        config: HttpInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<MessageBatch>(1000);
        let auth = config.auth.clone();

        Ok(Self {
            input_name: name.cloned(),
            config,
            server_handle: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            sender: Arc::new(sender),
            receiver: Arc::new(receiver),
            auth,
            codec,
        })
    }

    async fn handle_request(
        State(state): State<AppState>,
        headers: HeaderMap,
        body: axum::extract::Json<serde_json::Value>,
    ) -> StatusCode {
        if let Some(auth_config) = &state.auth {
            if !validate_auth(&headers, auth_config).await {
                return StatusCode::UNAUTHORIZED;
            }
        }

        // Convert JSON to bytes for codec processing
        let json_bytes = match serde_json::to_vec(&body.0) {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        // Apply codec if configured
        let msg =
            match crate::input::codec_helper::apply_codec_to_payload(&json_bytes, &state.codec) {
                Ok(msg) => msg,
                Err(_) => return StatusCode::BAD_REQUEST,
            };

        let _ = state.sender.send_async(msg).await;

        StatusCode::OK
    }
}

#[async_trait]
impl Input for HttpInput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(Ordering::SeqCst) {
            return Ok(());
        }

        let path = self.config.path.clone();
        let address = self.config.address.clone();

        let app_state = Arc::new(AppStateInner {
            sender: self.sender.as_ref().clone(),
            auth: self.auth.clone(),
            codec: self.codec.clone(),
        });

        let mut app = Router::new()
            .route(&path, post(Self::handle_request))
            .with_state(app_state);

        if self.config.cors_enabled.unwrap_or(false) {
            app = app.layer(CorsLayer::very_permissive());
        }

        let addr: SocketAddr = address
            .parse()
            .map_err(|e| Error::Config(format!("Invalid address {}: {}", address, e)))?;

        let server_handle = tokio::spawn(async move {
            let server = axum::serve(
                TcpListener::bind(&addr).await.expect("bind error"),
                app.into_make_service(),
            );
            server
                .await
                .map_err(|e| Error::Connection(format!("HTTP server error: {}", e)))
        });

        let server_handle_arc = self.server_handle.clone();
        let mut server_handle_arc_mutex = server_handle_arc.lock().await;
        *server_handle_arc_mutex = Some(server_handle);
        self.connected.store(true, Ordering::SeqCst);

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        if let Ok(mut msg) = self.receiver.recv_async().await {
            msg.set_input_name(self.input_name.clone());
            Ok((Arc::new(msg), Arc::new(NoopAck)))
        } else {
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
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Http input configuration is missing".to_string(),
            ));
        }

        let config: HttpInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(HttpInput::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("http", Arc::new(HttpInputBuilder))
}

async fn validate_auth(headers: &HeaderMap, auth_config: &AuthType) -> bool {
    let Some(auth_header) = headers.get(header::AUTHORIZATION) else {
        return false;
    };

    match auth_config {
        AuthType::Basic { username, password } => {
            let Ok(auth_str) = auth_header.to_str() else {
                return false;
            };

            if !auth_str.starts_with("Basic ") {
                return false;
            }

            let credentials = auth_str.trim_start_matches("Basic ");
            let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(credentials) else {
                return false;
            };

            if let Ok(auth_string) = String::from_utf8(decoded) {
                let parts: Vec<&str> = auth_string.splitn(2, ':').collect();
                return parts.len() == 2 && parts[0] == username && parts[1] == password;
            }

            false
        }
        AuthType::Bearer { token } => {
            if let Ok(auth_str) = auth_header.to_str() {
                if auth_str.starts_with("Bearer ") {
                    let received_token = auth_str.trim_start_matches("Bearer ");
                    return received_token == token;
                }
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use serde_json::json;
    use tower::util::ServiceExt;
    // for `oneshot` method

    #[tokio::test]
    async fn test_handle_request_ok() {
        let config = HttpInputConfig {
            address: "127.0.0.1:3000".to_string(),
            path: "/test".to_string(),
            cors_enabled: Some(false),
            auth: None,
        };
        let input = HttpInput::new(None, config, None).unwrap();
        let app_state = Arc::new(AppStateInner {
            sender: input.sender.as_ref().clone(),
            auth: input.auth.clone(),
            codec: None,
        });

        let app = Router::new()
            .route("/test", axum::routing::post(HttpInput::handle_request))
            .with_state(app_state);

        let request = Request::builder()
            .method("POST")
            .uri("/test")
            .header("content-type", "application/json")
            .body(Body::from(json!({"key": "value"}).to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_handle_request_unauthorized() {
        let config = HttpInputConfig {
            address: "127.0.0.1:3000".to_string(),
            path: "/test".to_string(),
            cors_enabled: Some(false),
            auth: Some(AuthType::Basic {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
        };
        let input = HttpInput::new(None, config, None).unwrap();
        let app_state = Arc::new(AppStateInner {
            sender: input.sender.as_ref().clone(),
            auth: input.auth.clone(),
            codec: None,
        });

        let app = Router::new()
            .route("/test", axum::routing::post(HttpInput::handle_request))
            .with_state(app_state);

        let request = Request::builder()
            .method("POST")
            .uri("/test")
            .header("content-type", "application/json")
            .body(Body::from(json!({"key": "value"}).to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
