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

//! WebSocket input component
//!
//! Receive data from a WebSocket server

use crate::input::codec_helper::Delivery;
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatchRef, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;

/// WebSocket input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketInputConfig {
    /// WebSocket server URL
    pub url: String,
    /// Headers to include in the WebSocket handshake (optional)
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// Connection timeout in seconds (optional)
    pub timeout: Option<u64>,
}

/// WebSocket message types
// The channel carries finished `Delivery` values decoded by the reader
// task (cancellation-safety contract; see codec_helper).
/// WebSocket input component
pub struct WebSocketInput {
    #[allow(unused)]
    input_name: Option<String>,
    config: WebSocketInputConfig,
    sender: Sender<Delivery>,
    receiver: Receiver<Delivery>,
    #[allow(clippy::type_complexity)]
    writer: Arc<Mutex<Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

impl WebSocketInput {
    /// Create a new WebSocket input component
    pub fn new(
        name: Option<&str>,
        config: WebSocketInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<Delivery>(1000);
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.map(str::to_string),
            config,
            sender,
            receiver,
            writer: Arc::new(Mutex::new(None)),
            cancellation_token,
            codec,
        })
    }
}

#[async_trait]
impl Input for WebSocketInput {
    async fn connect(&self) -> Result<(), Error> {
        // Parse the WebSocket URL
        let url = Url::parse(&self.config.url).map_err(|e| {
            Error::Connection(format!("Invalid WebSocket URL {}: {}", self.config.url, e))
        })?;

        // Build the handshake request, carrying the configured headers
        // (previously parsed but silently dropped from the handshake).
        let mut request = url.to_string().into_client_request().map_err(|e| {
            Error::Connection(format!(
                "Invalid WebSocket request for {}: {}",
                self.config.url, e
            ))
        })?;
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                let header_name =
                    tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|e| {
                        Error::Config(format!("Invalid WebSocket header name '{name}': {e}"))
                    })?;
                let header_value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(
                    value,
                )
                .map_err(|e| {
                    Error::Config(format!("Invalid WebSocket header value for '{name}': {e}"))
                })?;
                request.headers_mut().insert(header_name, header_value);
            }
        }

        // Set up connection timeout if specified
        let connect_future = connect_async(request);
        let connect_result = if let Some(timeout_secs) = self.config.timeout {
            let timeout_duration = std::time::Duration::from_secs(timeout_secs);
            tokio::time::timeout(timeout_duration, connect_future)
                .await
                .map_err(|_| Error::Connection("WebSocket connection timeout".to_string()))?
        } else {
            connect_future.await
        };

        // Establish the WebSocket connection
        let (ws_stream, _) = connect_result.map_err(|e| {
            Error::Connection(format!("Failed to connect to WebSocket server: {}", e))
        })?;

        info!("Connected to websocket server: {}", self.config.url);

        // Split the WebSocket stream into reader and writer parts
        let (writer, reader) = ws_stream.split();

        // Store the writer for later use
        let writer_arc = Arc::clone(&self.writer);
        let mut writer_guard = writer_arc.lock().await;
        *writer_guard = Some(writer);

        // Clone the sender and cancellation token for the reader task
        let sender_clone = Sender::clone(&self.sender);
        let cancellation_token = self.cancellation_token.clone();
        let codec = self.codec.clone();
        let input_name = self.input_name.clone();

        // Spawn a task to handle incoming WebSocket messages. It decodes and
        // filters BEFORE claiming a channel slot, so `read()` below is a
        // single await point (Input::read cancellation-safety contract).
        tokio::spawn(async move {
            Self::handle_websocket_messages(
                reader,
                sender_clone,
                codec,
                input_name,
                cancellation_token,
            )
            .await;
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        // Check if we're still connected
        {
            let writer_arc = Arc::clone(&self.writer);
            if writer_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        let cancellation_token = self.cancellation_token.clone();

        tokio::select! {
            // Deliveries arrive pre-decoded from the reader task, so this
            // recv is the single await point — a dropped read loses nothing.
            result = self.receiver.recv_async() => {
                match result {
                    Ok(Delivery::Data(batch, ack)) => Ok((batch, ack)),
                    Ok(Delivery::Err(e)) => Err(e),
                    Err(_) => Err(Error::EOF),
                }
            },
            _ = cancellation_token.cancelled() => {
                Err(Error::EOF)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Send a shutdown signal
        let _ = self.cancellation_token.clone().cancel();

        // Close the WebSocket connection
        let writer_arc = Arc::clone(&self.writer);
        let mut writer_guard = writer_arc.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            // Try to send a close frame, but don't wait for the result
            let _ = writer.close().await;
        }

        Ok(())
    }
}

impl WebSocketInput {
    async fn handle_websocket_messages(
        mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        sender: Sender<Delivery>,
        codec: Option<Arc<dyn Codec>>,
        input_name: Option<String>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                result = reader.next() => {
                    match result {
                        Some(Ok(message)) => {
                            // Control frames never reach the channel — read()
                            // must not recurse to skip them (each recursion
                            // was another cancellation window).
                            let payload = match message {
                                Message::Text(text) => Vec::from(text.as_bytes()),
                                Message::Binary(binary) => Vec::from(binary),
                                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                                    continue;
                                }
                                Message::Close(_) => {
                                    if let Err(e) = sender.send_async(Delivery::Err(Error::Disconnection)).await {
                                        error!("Failed to send disconnection notification: {}", e);
                                    }
                                    break;
                                }
                            };
                            let delivery = crate::input::codec_helper::decode_delivery(
                                &payload,
                                &codec,
                                input_name.clone(),
                                Arc::new(NoopAck),
                            )
                            .await;
                            if let Err(e) = sender.send_async(delivery).await {
                                error!("Failed to forward WebSocket message: {}", e);
                            }
                        },
                        Some(Err(e)) => {
                            // Log the error and notify about disconnection
                            error!("WebSocket read error: {}", e);
                            if let Err(e) = sender.send_async(Delivery::Err(Error::Disconnection)).await {
                                error!("Failed to send error notification: {}", e);
                            }
                            break;
                        },
                        None => {
                            // Connection closed
                            if let Err(e) = sender.send_async(Delivery::Err(Error::Disconnection)).await {
                                error!("Failed to send disconnection notification: {}", e);
                            }
                            break;
                        }
                    }
                },
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }
}

pub(crate) struct WebSocketInputBuilder;
impl InputBuilder for WebSocketInputBuilder {
    fn build(
        &self,
        name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: WebSocketInputConfig = parse_config(config, "WebSocket input")?;
        Ok(Arc::new(WebSocketInput::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("websocket", Arc::new(WebSocketInputBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "websocket",
        "Connects to a WebSocket server and forwards each incoming message as a batch.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "WebSocket server URL (ws:// or wss://)."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Headers included in the WebSocket handshake."},
                "timeout": {"type": "integer", "minimum": 1, "description": "Connection timeout in seconds."}
            },
            "required": ["url"]
        }),
    ).with_example(serde_json::json!({
        "url": "ws://localhost:8080/stream"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::codec_helper::contract::{cancel_pending_read_then_expect_delivery, gate};
    use arkflow_core::input::Input;

    /// Spec "解码进行中的取消不丢消息" (end-to-end): a local WebSocket server
    /// pushes one message while a gate codec parks the reader task's
    /// decode; the engine-shaped probe drops the pending read exactly like
    /// a lost select! branch, and the delivery must still arrive exactly
    /// once on the next read. Pre-fix, the reader handed the raw message
    /// to `read()`, which claimed it and then lost it inside the decode
    /// await.
    #[tokio::test]
    async fn read_survives_cancellation_during_codec_decode() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            use futures_util::SinkExt;
            let (stream, _) = listener.accept().await.unwrap();
            let (mut write, _read) = tokio_tungstenite::accept_async(stream)
                .await
                .unwrap()
                .split();
            write.send(Message::Text("payload".into())).await.unwrap();
            // Keep the server side alive until the client is done.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        });

        let (codec, gate_handle) = gate();
        let input = WebSocketInput::new(
            None,
            WebSocketInputConfig {
                url: format!("ws://{addr}"),
                headers: None,
                timeout: Some(5),
            },
            Some(codec),
        )
        .unwrap();
        input.connect().await.unwrap();

        let input: std::sync::Arc<dyn Input> = std::sync::Arc::new(input);
        let (batch, _ack) = cancel_pending_read_then_expect_delivery(input, &gate_handle)
            .await
            .expect("delivery must survive a cancelled read");
        assert_eq!(batch.len(), 1);

        server.abort();
    }

    /// Spec "websocket 握手携带配置头": a local server captures the handshake
    /// request and must observe the configured HTTP headers.
    #[tokio::test]
    #[allow(clippy::result_large_err)] // the tungstenite callback's error type is fixed by the trait
    async fn handshake_carries_configured_headers() {
        use futures_util::SinkExt;
        use std::sync::mpsc;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let callback = move |req: &tokio_tungstenite::tungstenite::http::Request<()>,
                                 resp: tokio_tungstenite::tungstenite::http::Response<()>|
                  -> Result<
                tokio_tungstenite::tungstenite::http::Response<()>,
                tokio_tungstenite::tungstenite::http::Response<Option<String>>,
            > {
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let _ = tx.send(auth);
                Ok(resp)
            };
            let (mut write, _read) = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap()
                .split();
            write.send(Message::Text("hi".into())).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        });

        let mut headers = std::collections::HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer tok".to_string());
        let input = WebSocketInput::new(
            None,
            WebSocketInputConfig {
                url: format!("ws://{addr}"),
                headers: Some(headers),
                timeout: Some(5),
            },
            None,
        )
        .unwrap();
        input.connect().await.unwrap();

        let observed = rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("server must observe the handshake");
        assert_eq!(observed.as_deref(), Some("Bearer tok"));

        server.abort();
    }
}
