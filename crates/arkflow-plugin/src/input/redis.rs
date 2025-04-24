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

//! Redis input component
//!
//! Receive data from Redis pub/sub channels

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use redis::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Redis input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisInputConfig {
    /// Redis server URL
    url: String,
    #[serde(flatten)]
    _type: Type,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subscribe_type", rename_all = "snake_case")]
pub enum Subscribe {
    /// List of channels to subscribe to
    Channels { channels: Vec<String> },
    /// List of patterns to subscribe to
    Patterns { patterns: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "redis_type", rename_all = "snake_case")]
pub enum Type {
    Subscribe { subscribe: Subscribe },
    List { list: Vec<String> },
}
/// Redis input component
pub struct RedisInput {
    config: RedisInputConfig,
    client: Arc<Mutex<Option<Client>>>,
    sender: Sender<RedisMsg>,
    receiver: Receiver<RedisMsg>,
    cancellation_token: CancellationToken,
}

enum RedisMsg {
    Message(String, Vec<u8>),
    Err(Error),
}

impl RedisInput {
    /// Create a new Redis input component
    pub fn new(config: RedisInputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<RedisMsg>(1000);
        let cancellation_token = CancellationToken::new();

        if let None = redis::parse_redis_url(&config.url) {
            return Err(Error::Config(format!("Invalid Redis URL: {}", &config.url)));
        }
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            sender,
            receiver,
            cancellation_token,
        })
    }
}

#[async_trait]
impl Input for RedisInput {
    async fn connect(&self) -> Result<(), Error> {
        let client = Client::open(self.config.url.as_str())
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;

        let mut client_guard = self.client.lock().await;
        *client_guard = Some(client.clone());
        drop(client_guard);

        let conn = client
            .get_async_connection()
            .await
            .map_err(|e| Error::Connection(format!("Failed to get Redis connection: {}", e)))?;

        let sender_clone = Sender::clone(&self.sender);
        let cancellation_token = self.cancellation_token.clone();

        let config_type = self.config._type.clone();

        tokio::spawn(async move {
            let mut pubsub = conn.into_pubsub();
            match config_type {
                Type::Subscribe { subscribe } => {
                    match subscribe {
                        Subscribe::Channels { channels } => {
                            // Subscribe to channels
                            for channel in channels {
                                if let Err(e) = pubsub.subscribe(&channel).await {
                                    error!(
                                        "Failed to subscribe to Redis channel {}: {}",
                                        channel, e
                                    );
                                    if let Err(e) = sender_clone
                                        .send_async(RedisMsg::Err(Error::Disconnection))
                                        .await
                                    {
                                        error!("{}", e);
                                    }
                                    return;
                                }
                            }
                        }
                        Subscribe::Patterns { patterns } => {
                            // Subscribe to patterns
                            for pattern in patterns {
                                if let Err(e) = pubsub.psubscribe(&pattern).await {
                                    error!(
                                        "Failed to subscribe to Redis pattern {}: {}",
                                        pattern, e
                                    );
                                    if let Err(e) = sender_clone
                                        .send_async(RedisMsg::Err(Error::Disconnection))
                                        .await
                                    {
                                        error!("{}", e);
                                    }
                                    return;
                                }
                            }
                        }
                    }
                    let mut msg_stream = pubsub.on_message();
                    loop {
                        tokio::select! {
                            Some(msg_result) = msg_stream.next() => {
                                let channel: String = msg_result.get_channel_name().to_string();
                                let payload: Vec<u8> = msg_result.get_payload().unwrap_or_default();
                                if let Err(e) = sender_clone.send_async(RedisMsg::Message(channel, payload)).await {
                                    error!("{}", e);
                                }
                            }
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }
                    }
                }
                Type::List { ref list } => {
                    let conn_result = client.get_async_connection().await;
                    if let Err(e) = conn_result {
                        error!("Failed to get Redis connection for list: {}", e);
                        let _ = sender_clone
                            .send_async(RedisMsg::Err(Error::Disconnection))
                            .await;
                        return;
                    }

                    let mut conn = conn_result.unwrap();

                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                            result = async {
                                let blpop_result: redis::RedisResult<Option<(String, Vec<u8>)>> = redis::cmd("BLPOP")
                                    .arg(list.clone())
                                    .arg(1) // 1秒超时
                                    .query_async(&mut conn)
                                    .await;
                                blpop_result
                            } => {
                                match result {
                                    Ok(Some((list_name, payload))) => {
                                        if let Err(e) = sender_clone.send_async(RedisMsg::Message(list_name, payload)).await {
                                            error!("Failed to send Redis list message: {}", e);
                                        }
                                    }
                                    Ok(None) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        error!("Error retrieving from Redis list: {}", e);
                                        if let Err(e) = sender_clone.send_async(RedisMsg::Err(Error::Disconnection)).await {
                                            error!("{}", e);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            };
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        {
            let client_arc = Arc::clone(&self.client);
            if client_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        match self.receiver.recv_async().await {
            Ok(msg) => match msg {
                RedisMsg::Message(_channel, payload) => {
                    let msg = MessageBatch::new_binary(vec![payload]).map_err(|e| {
                        Error::Connection(format!("Failed to create message batch: {}", e))
                    })?;
                    Ok((msg, Arc::new(NoopAck)))
                }
                RedisMsg::Err(e) => Err(e),
            },
            Err(_) => Err(Error::EOF),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        Ok(())
    }
}

/// Redis input builder
pub struct RedisInputBuilder;

impl InputBuilder for RedisInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        let config: RedisInputConfig =
            serde_json::from_value(config.clone().unwrap_or_default())
                .map_err(|e| Error::Config(format!("Invalid Redis input config: {}", e)))?;
        Ok(Arc::new(RedisInput::new(config)?))
    }
}

/// Initialize Redis input component
pub fn init() -> Result<(), Error> {
    register_input_builder("redis", Arc::new(RedisInputBuilder))
}
