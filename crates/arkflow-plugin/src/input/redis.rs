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
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client, FromRedisValue, PushInfo, PushKind, RedisResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Redis input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisInputConfig {
    mode: ModeConfig,
    redis_type: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ModeConfig {
    Cluster { urls: Vec<String> },
    Single { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Subscribe {
    /// List of channels to subscribe to
    Channels { channels: Vec<String> },
    /// List of patterns to subscribe to
    Patterns { patterns: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Type {
    Subscribe { subscribe: Subscribe },
    List { list: Vec<String> },
}

/// Redis input component
struct RedisInput {
    input_name: Option<String>,
    config: RedisInputConfig,
    client: Arc<Mutex<Option<Cli>>>,
    sender: Sender<RedisMsg>,
    receiver: Receiver<RedisMsg>,
    cancellation_token: CancellationToken,
}

enum Cli {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

enum RedisMsg {
    Message(String, Vec<u8>),
    Err(Error),
}

impl RedisInput {
    /// Create a new Redis input component
    fn new(name: Option<&String>, config: RedisInputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<RedisMsg>(1000);
        let cancellation_token = CancellationToken::new();
        match &config.mode {
            ModeConfig::Cluster { urls, .. } => {
                for url in urls {
                    if let None = redis::parse_redis_url(&url) {
                        return Err(Error::Config(format!("Invalid Redis URL: {}", url)));
                    }
                }
            }
            ModeConfig::Single { url, .. } => {
                if let None = redis::parse_redis_url(&url) {
                    return Err(Error::Config(format!("Invalid Redis URL: {}", url)));
                }
            }
        };

        Ok(Self {
            input_name: name.cloned(),
            config,
            client: Arc::new(Mutex::new(None)),
            sender,
            receiver,
            cancellation_token,
        })
    }

    async fn cluster_connect(&self, urls: Vec<String>) -> Result<(), Error> {
        let mut cli_guard = self.client.lock().await;

        let cancellation_token = self.cancellation_token.clone();

        let config_type = self.config.redis_type.clone();

        let client_builder = ClusterClientBuilder::new(urls);

        let client_builder = match config_type {
            Type::Subscribe { .. } => {
                let sender_clone = Sender::clone(&self.sender);
                client_builder.push_sender(move |msg: PushInfo| {
                    match msg.kind {
                        PushKind::Message | PushKind::PMessage | PushKind::SMessage => {
                            if msg.data.len() < 2 {
                                return Ok(());
                            }
                            let mut iter = msg.data.into_iter();
                            let channel: String = match iter.next() {
                                Some(v) => FromRedisValue::from_owned_redis_value(v)?,
                                None => return Ok(()),
                            };
                            let message = match iter.next() {
                                Some(v) => FromRedisValue::from_owned_redis_value(v)?,
                                None => return Ok(()),
                            };

                            if let Err(e) = sender_clone.send(RedisMsg::Message(channel, message)) {
                                error!("{}", e);
                            }
                        }
                        _ => {}
                    };

                    Ok(()) as RedisResult<()>
                })
            }
            Type::List { .. } => client_builder,
        };

        let cluster_client = client_builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        let mut cluster_conn = cluster_client
            .get_async_connection()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        match config_type {
            Type::Subscribe { subscribe } => {
                match subscribe {
                    Subscribe::Channels { channels } => {
                        // Subscribe to channels
                        for channel in channels {
                            if let Err(e) = cluster_conn.subscribe(&channel).await {
                                error!("Failed to subscribe to Redis channel {}: {}", channel, e);
                                return Err(Error::Disconnection);
                            }
                        }
                    }
                    Subscribe::Patterns { patterns } => {
                        // Subscribe to patterns
                        for pattern in patterns {
                            if let Err(e) = cluster_conn.psubscribe(&pattern).await {
                                error!("Failed to subscribe to Redis pattern {}: {}", pattern, e);
                                return Err(Error::Disconnection);
                            }
                        }
                    }
                }
            }
            Type::List { list } => {
                let sender_clone = Sender::clone(&self.sender);
                let mut cluster_connection = cluster_conn.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                            result = async {
                                let blpop_result: RedisResult<Option<(String, Vec<u8>)>> = cluster_connection.blpop(&list, 1f64).await;
                                blpop_result
                            } => {
                                match result {
                                    Ok(Some((list_name, payload))) => {
                                        debug!("Received Redis list message from {},payload: {}", list_name,  String::from_utf8_lossy(&payload));
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
                });
            }
        }
        cli_guard.replace(Cli::Cluster(cluster_conn));
        Ok(())
    }

    async fn single_connect(&self, url: String) -> Result<(), Error> {
        let mut cli_guard = self.client.lock().await;
        let client = Client::open(url)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;
        let manager = ConnectionManager::new(client.clone())
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;

        let sender_clone = Sender::clone(&self.sender);
        let cancellation_token = self.cancellation_token.clone();

        let config_type = self.config.redis_type.clone();

        match config_type {
            Type::Subscribe { subscribe } => {
                let mut pubsub_conn = client.get_async_pubsub().await.map_err(|e| {
                    Error::Connection(format!("Failed to get Redis connection: {}", e))
                })?;

                match subscribe {
                    Subscribe::Channels { channels } => {
                        // Subscribe to channels
                        for channel in channels {
                            if let Err(e) = pubsub_conn.subscribe(&channel).await {
                                error!("Failed to subscribe to Redis channel {}: {}", channel, e);
                                return Err(Error::Disconnection);
                            }
                        }
                    }
                    Subscribe::Patterns { patterns } => {
                        // Subscribe to patterns
                        for pattern in patterns {
                            if let Err(e) = pubsub_conn.psubscribe(&pattern).await {
                                error!("Failed to subscribe to Redis pattern {}: {}", pattern, e);
                                return Err(Error::Disconnection);
                            }
                        }
                    }
                }
                tokio::spawn(async move {
                    let mut msg_stream = pubsub_conn.on_message();

                    loop {
                        tokio::select! {
                            Some(msg_result) = msg_stream.next() => {
                                let channel: String = msg_result.get_channel_name().to_string();
                                let Ok(payload )=   msg_result.get_payload() else {
                                       continue;
                                };
                                if let Err(e) = sender_clone.send_async(RedisMsg::Message(channel, payload)).await {
                                    error!("{}", e);
                                }
                            }
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }
                    }
                });
            }
            Type::List { ref list } => {
                let list = list.clone();
                let mut manager = manager.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                            result = async {
                                let blpop_result: RedisResult<Option<(String, Vec<u8>)>> = manager.blpop(&list, 1f64).await;
                                blpop_result
                            } => {
                                match result {
                                    Ok(Some((list_name, payload))) => {
                                        debug!("Received Redis list message from {},payload: {}", list_name,  String::from_utf8_lossy(&payload));
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
                });
            }
        };

        cli_guard.replace(Cli::Single(manager));

        Ok(())
    }
}

#[async_trait]
impl Input for RedisInput {
    async fn connect(&self) -> Result<(), Error> {
        match &self.config.mode {
            ModeConfig::Cluster { urls } => {
                self.cluster_connect(urls.iter().cloned().collect()).await
            }
            ModeConfig::Single { url } => self.single_connect(url.clone()).await,
        }
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        {
            let client_arc = Arc::clone(&self.client);
            if client_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        match self.receiver.recv_async().await {
            Ok(RedisMsg::Message(_channel, payload)) => {
                let mut msg = MessageBatch::new_binary(vec![payload]).map_err(|e| {
                    Error::Connection(format!("Failed to create message batch: {}", e))
                })?;
                msg.set_input_name(self.input_name.clone());

                Ok((Arc::new(msg), Arc::new(NoopAck)))
            }
            Ok(RedisMsg::Err(e)) => Err(e),
            Err(_) => Err(Error::EOF),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        if let Some(cli) = self.client.lock().await.take() {
            match cli {
                Cli::Single(mut c) => match self.config.redis_type {
                    Type::Subscribe { ref subscribe } => match subscribe {
                        Subscribe::Channels { channels } => {
                            match c.unsubscribe(channels).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to unsubscribe from Redis channel: {}", e);
                                }
                            };
                        }
                        Subscribe::Patterns { patterns } => {
                            match c.punsubscribe(patterns).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to unsubscribe from Redis pattern: {}", e);
                                }
                            };
                        }
                    },
                    _ => {}
                },
                Cli::Cluster(mut c) => match self.config.redis_type {
                    Type::Subscribe { ref subscribe } => match subscribe {
                        Subscribe::Channels { channels } => {
                            match c.unsubscribe(channels).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to unsubscribe from Redis channel: {}", e);
                                }
                            };
                        }
                        Subscribe::Patterns { patterns } => {
                            match c.punsubscribe(patterns).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to unsubscribe from Redis pattern: {}", e);
                                }
                            };
                        }
                    },
                    _ => {}
                },
            }
        }
        Ok(())
    }
}

/// Redis input builder
pub struct RedisInputBuilder;

impl InputBuilder for RedisInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: RedisInputConfig =
            serde_json::from_value(config.clone().unwrap_or_default())
                .map_err(|e| Error::Config(format!("Invalid Redis input config: {}", e)))?;
        Ok(Arc::new(RedisInput::new(name, config)?))
    }
}

/// Initialize Redis input component
pub fn init() -> Result<(), Error> {
    register_input_builder("redis", Arc::new(RedisInputBuilder))
}
