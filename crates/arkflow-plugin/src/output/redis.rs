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
use arkflow_core::output::{Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisOutputConfig {
    mode: Mode,
    redis_type: Type,
    /// Value field to use for message payload
    value_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Mode {
    Cluster { urls: Vec<String> },
    Single { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Type {
    Publish {
        channel: Expr<String>,
    },
    List {
        key: Expr<String>,
    },
    Hashes {
        key: Expr<String>,
        field: Expr<String>,
    },
    Strings {
        key: Expr<String>,
    },
}

struct RedisOutput {
    config: RedisOutputConfig,
    client: Arc<Mutex<Option<Cli>>>,
    connection_manager: Arc<Mutex<Option<ConnectionManager>>>,
    cancellation_token: CancellationToken,
}

#[derive(Clone)]
enum Cli {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisOutput {
    /// Create a new Redis input component
    pub fn new(config: RedisOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connection_manager: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }

    async fn single_connect(&self, url: String) -> Result<(), Error> {
        let client = Client::open(url)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;
        let client = client
            .get_connection_manager()
            .await
            .map_err(|e| Error::Connection(format!("Failed to get connection manager: {}", e)))?;

        let mut client_guard = self.client.lock().await;
        client_guard.replace(Cli::Single(client));
        Ok(())
    }

    async fn cluster_connect(&self, urls: Vec<String>) -> Result<(), Error> {
        let client = ClusterClient::new(urls)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        let client = client.get_async_connection().await.map_err(|e| {
            Error::Connection(format!(
                "Failed to get connection from Redis cluster: {}",
                e
            ))
        })?;

        let mut client_guard = self.client.lock().await;
        client_guard.replace(Cli::Cluster(client));
        Ok(())
    }
}

#[async_trait]
impl Output for RedisOutput {
    async fn connect(&self) -> Result<(), Error> {
        match self.config.mode {
            Mode::Cluster { ref urls } => {
                self.cluster_connect(urls.clone()).await?;
            }
            Mode::Single { ref url } => {
                self.single_connect(url.clone()).await?;
            }
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
        let client_lock = self.client.lock().await;
        let Some(cli) = client_lock.as_ref() else {
            return Err(Error::Process(
                "Failed to get connection from Redis".to_string(),
            ));
        };

        match &self.config.redis_type {
            Type::Publish { channel } => {
                let key_result = channel.evaluate_expr(&msg).map_err(|e| {
                    Error::Process(format!("Failed to evaluate channel expression: {}", e))
                })?;

                for (i, payload) in data.iter().enumerate() {
                    if let Some(channel) = key_result.get(i) {
                        let cli = cli.clone();
                        match cli {
                            Cli::Single(mut c) => {
                                c.publish::<_, _, ()>(channel, payload).await.map_err(|e| {
                                    Error::Process(format!("Failed to publish message: {}", e))
                                })?;
                            }
                            Cli::Cluster(mut c) => {
                                c.publish::<_, _, ()>(channel, payload).await.map_err(|e| {
                                    Error::Process(format!("Failed to publish message: {}", e))
                                })?;
                            }
                        };
                    }
                }
            }
            Type::List { key } => {
                let key_result = key.evaluate_expr(&msg).map_err(|e| {
                    Error::Process(format!("Failed to evaluate key expression: {}", e))
                })?;

                for (i, payload) in data.iter().enumerate() {
                    if let Some(key) = key_result.get(i) {
                        let cli = cli.clone();
                        match cli {
                            Cli::Single(mut c) => {
                                c.rpush::<_, _, ()>(key, payload).await.map_err(|e| {
                                    Error::Process(format!("Failed to push to list: {}", e))
                                })?;
                            }
                            Cli::Cluster(mut c) => {
                                c.rpush::<_, _, ()>(key, payload).await.map_err(|e| {
                                    Error::Process(format!("Failed to push to list: {}", e))
                                })?;
                            }
                        };
                    }
                }
            }
            Type::Hashes { key, field } => {
                let key_result = key.evaluate_expr(&msg).map_err(|e| {
                    Error::Process(format!("Failed to evaluate key expression: {}", e))
                })?;

                let field_result = field.evaluate_expr(&msg).map_err(|e| {
                    Error::Process(format!("Failed to evaluate field expression: {}", e))
                })?;

                for (x, payload) in data.into_iter().enumerate() {
                    let Some(key) = key_result.get(x) else {
                        continue;
                    };
                    let Some(field) = field_result.get(x) else {
                        continue;
                    };

                    let cli = cli.clone();
                    match cli {
                        Cli::Single(mut c) => {
                            c.hset::<_, _, _, ()>(key, field, payload)
                                .await
                                .map_err(|e| {
                                    Error::Process(format!("Failed to set hash field: {}", e))
                                })?;
                        }
                        Cli::Cluster(mut c) => {
                            c.hset::<_, _, _, ()>(key, field, payload)
                                .await
                                .map_err(|e| {
                                    Error::Process(format!("Failed to set hash field: {}", e))
                                })?;
                        }
                    };
                }
            }
            Type::Strings { key } => {
                let key_result = key.evaluate_expr(&msg).map_err(|e| {
                    Error::Process(format!("Failed to evaluate key expression: {}", e))
                })?;
                for (x, payload) in data.into_iter().enumerate() {
                    let Some(key) = key_result.get(x) else {
                        continue;
                    };

                    let cli = cli.clone();
                    match cli {
                        Cli::Single(mut c) => {
                            c.set::<_, _, ()>(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to set string: {}", e))
                            })?;
                        }
                        Cli::Cluster(mut c) => {
                            c.set::<_, _, ()>(key, payload).await.map_err(|e| {
                                Error::Process(format!("Failed to set string: {}", e))
                            })?;
                        }
                    };
                }
            }
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();

        let mut client_guard = self.client.lock().await;
        *client_guard = None;
        // The connection manager will be dropped automatically when it goes out of scope
        let mut conn_manager_guard = self.connection_manager.lock().await;
        *conn_manager_guard = None;
        Ok(())
    }
}

struct RedisOutputBuilder;

impl OutputBuilder for RedisOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Redis output configuration is missing".to_string(),
            ));
        }
        let config: RedisOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(RedisOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    arkflow_core::output::register_output_builder("redis", Arc::new(RedisOutputBuilder))
}
