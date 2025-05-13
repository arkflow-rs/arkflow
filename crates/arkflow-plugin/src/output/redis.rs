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
use crate::expr::{EvaluateExpr, Expr};
use arkflow_core::output::Output;
use arkflow_core::{Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisOutputConfig {
    url: String,
    redis_type: Type,
    /// Value field to use for message payload
    value_field: Option<String>,
    /// Whether to use cluster mode
    cluster: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Type {
    Publish { channel: Expr<String> },
    List { key: Expr<String> },
    Hashes { key: Expr<String> },
    Strings { key: Expr<String> },
}

struct RedisOutput {
    config: RedisOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    cluster_client: Arc<Mutex<Option<ClusterClient>>>,
    connection_manager: Arc<Mutex<Option<ConnectionManager>>>,
    cancellation_token: CancellationToken,
}

impl RedisOutput {
    /// Create a new Redis input component
    pub fn new(config: RedisOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        if let None = redis::parse_redis_url(&config.url) {
            return Err(Error::Config(format!("Invalid Redis URL: {}", &config.url)));
        }

        let cluster_client = if config.cluster.unwrap_or(false) {
            Some(ClusterClient::new(vec![config.url.clone()])?)
        } else {
            None
        };

        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            cluster_client: Arc::new(Mutex::new(cluster_client)),
            connection_manager: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }
}

#[async_trait]
impl Output for RedisOutput {
    async fn connect(&self) -> Result<(), Error> {
        if self.config.cluster.unwrap_or(false) {
            let cluster_client = self.cluster_client.lock().await;
            let manager =
                ConnectionManager::new(cluster_client.as_ref().unwrap().get_connection()?).await?;
            let mut manager_guard = self.connection_manager.lock().await;
            *manager_guard = Some(manager);
        } else {
            let client = Client::open(self.config.url.as_str()).map_err(|e| {
                Error::Connection(format!("Failed to connect to Redis server: {}", e))
            })?;
            let mut client_guard = self.client.lock().await;
            *client_guard = Some(client.clone());
        }
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let value_field = &self
            .config
            .value_field
            .as_deref()
            .unwrap_or(DEFAULT_BINARY_VALUE_FIELD);
        let data = msg.to_binary(value_field)?;

        if self.config.cluster.unwrap_or(false) {
            let manager = self.connection_manager.lock().await;
            let mut conn = manager.as_ref().unwrap().clone();

            match &self.config.redis_type {
                Type::Publish { channel } => {
                    let key_result = channel.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate channel expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(channel) = key_result.get(i) {
                            conn.publish(channel, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to publish message: {}", e))
                            })?;
                        }
                    }
                }
                Type::List { key } => {
                    let key_result = key.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate key expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(key) = key_result.get(i) {
                            conn.rpush(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to push to list: {}", e))
                            })?;
                        }
                    }
                }
                Type::Hashes { key } => {
                    let Some(key) = key.evaluate_expr(&msg) else {
                        return Err(Error::Process(
                            "Failed to evaluate key expression".to_string(),
                        ));
                    };

                    for payload in data {
                        let payload_str = String::from_utf8(payload)?;
                        let fields: Vec<&str> = payload_str.split(',').collect();

                        for hash_key in key {
                            for field in &fields {
                                let parts: Vec<&str> = field.splitn(2, '=').collect();
                                if parts.len() == 2 {
                                    conn.hset(hash_key, parts[0], parts[1]).await.map_err(|e| {
                                        Error::Process(format!("Failed to set hash field: {}", e))
                                    })?;
                                }
                            }
                        }
                    }
                }
                Type::Strings { key } => {
                    let key_result = key.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate key expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(key) = key_result.get(i) {
                            conn.set(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to set string value: {}", e))
                            })?;
                        }
                    }
                }
            }
        } else {
            let client = self.client.lock().await;
            let mut conn = client
                .as_ref()
                .unwrap()
                .get_async_connection()
                .await
                .map_err(|e| Error::Connection(format!("Failed to get Redis connection: {}", e)))?;

            match &self.config.redis_type {
                Type::Publish { channel } => {
                    let key_result = channel.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate channel expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(channel) = key_result.get(i) {
                            conn.publish(channel, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to publish message: {}", e))
                            })?;
                        }
                    }
                }
                Type::List { key } => {
                    let key_result = key.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate key expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(key) = key_result.get(i) {
                            conn.rpush(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to push to list: {}", e))
                            })?;
                        }
                    }
                }
                Type::Hashes { key } => {
                    for payload in data {
                        let payload_str = String::from_utf8(payload)?;
                        let fields: Vec<&str> = payload_str.split(',').collect();

                        for hash_key in key {
                            for field in &fields {
                                let parts: Vec<&str> = field.splitn(2, '=').collect();
                                if parts.len() == 2 {
                                    conn.hset(hash_key, parts[0], parts[1]).await.map_err(|e| {
                                        Error::Process(format!("Failed to set hash field: {}", e))
                                    })?;
                                }
                            }
                        }
                    }
                }
                Type::Strings { key } => {
                    let key_result = key.evaluate_expr(&msg).map_err(|e| {
                        Error::Process(format!("Failed to evaluate key expression: {}", e))
                    })?;

                    for (i, payload) in data.iter().enumerate() {
                        if let Some(key) = key_result.get(i) {
                            conn.set(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to set string value: {}", e))
                            })?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}
