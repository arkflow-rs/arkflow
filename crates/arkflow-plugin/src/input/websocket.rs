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

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
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
enum WebSocketMsg {
    Message(Message),
    Err(Error),
}

/// WebSocket input component
pub struct WebSocketInput {
    #[allow(unused)]
    input_name: Option<String>,
    config: WebSocketInputConfig,
    sender: Sender<WebSocketMsg>,
    receiver: Receiver<WebSocketMsg>,
    writer: Arc<Mutex<Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    cancellation_token: CancellationToken,
}

impl WebSocketInput {
    /// Create a new WebSocket input component
    pub fn new(name: Option<&String>, config: WebSocketInputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<WebSocketMsg>(1000);
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.cloned(),
            config,
            sender,
            receiver,
            writer: Arc::new(Mutex::new(None)),
            cancellation_token,
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

        // Set up connection timeout if specified
        let connect_future = connect_async(url.to_string());
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

        // Spawn a task to handle incoming WebSocket messages
        tokio::spawn(async move {
            Self::handle_websocket_messages(reader, sender_clone, cancellation_token).await;
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        // Check if we're still connected
        {
            let writer_arc = Arc::clone(&self.writer);
            if writer_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        let cancellation_token = self.cancellation_token.clone();

        tokio::select! {
            result = self.receiver.recv_async() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            WebSocketMsg::Message(message) => {
                                let payload = match message {
                                    Message::Text(text) => Vec::from(text.as_bytes()),
                                    Message::Binary(binary) => Vec::from(binary),
                                    Message::Ping(_) | Message::Pong(_) => {
                                        // Skip control messages and wait for the next data message
                                        return self.read().await;
                                    }
                                    Message::Close(_) => {
                                        return Err(Error::Disconnection);
                                    }
                                    Message::Frame(_) => {
                                        // Skip raw frame messages
                                        return self.read().await;
                                    }
                                };

                                let mut msg = MessageBatch::new_binary(vec![payload])?;
                                msg.set_input_name(self.input_name.clone());

                                Ok((msg, Arc::new(NoopAck)))
                            },
                            WebSocketMsg::Err(e) => {
                                Err(e)
                            }
                        }
                    },
                    Err(_) => {
                        Err(Error::EOF)
                    }
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
        sender: Sender<WebSocketMsg>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                result = reader.next() => {
                    match result {
                        Some(Ok(message)) => {
                            // Forward the message to the channel
                            if let Err(e) = sender.send_async(WebSocketMsg::Message(message)).await {
                                error!("Failed to forward WebSocket message: {}", e);
                            }
                        },
                        Some(Err(e)) => {
                            // Log the error and notify about disconnection
                            error!("WebSocket read error: {}", e);
                            if let Err(e) = sender.send_async(WebSocketMsg::Err(Error::Disconnection)).await {
                                error!("Failed to send error notification: {}", e);
                            }
                            break;
                        },
                        None => {
                            // Connection closed
                            if let Err(e) = sender.send_async(WebSocketMsg::Err(Error::Disconnection)).await {
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
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "WebSocket input configuration is missing".to_string(),
            ));
        }

        let config: WebSocketInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(WebSocketInput::new(name, config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("websocket", Arc::new(WebSocketInputBuilder))
}
