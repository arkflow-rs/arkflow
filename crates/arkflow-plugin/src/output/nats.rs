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

//! NATS output component
//!
//! Send data to a NATS subject

use crate::expr::Expr;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource, DEFAULT_BINARY_VALUE_FIELD};
use async_nats::jetstream::Context;
use async_nats::{Client, ConnectOptions};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// NATS output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NatsOutputConfig {
    /// NATS server URL
    url: String,
    /// NATS mode
    mode: Mode,
    /// Authentication credentials (optional)
    auth: Option<NatsAuth>,
    /// Value field to use for message payload
    value_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Mode {
    /// Regular NATS mode
    Regular {
        /// NATS subject to publish to
        subject: Expr<String>,
    },
    /// JetStream mode
    JetStream {
        /// NATS subject to publish to
        subject: Expr<String>,
    },
}

/// NATS authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NatsAuth {
    /// Username (optional)
    pub username: Option<String>,
    /// Password (optional)
    pub password: Option<String>,
    /// Token (optional)
    pub token: Option<String>,
}

/// NATS output component
struct NatsOutput {
    config: NatsOutputConfig,
    client: Arc<RwLock<Option<Client>>>,
    js_stream: Arc<RwLock<Option<Context>>>,
}

impl NatsOutput {
    /// Create a new NATS output component
    fn new(config: NatsOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            js_stream: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Output for NatsOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Configure connection options
        let mut options = ConnectOptions::new();

        // Apply authentication if provided
        if let Some(auth) = &self.config.auth {
            if let (Some(username), Some(password)) = (&auth.username, &auth.password) {
                options = options.user_and_password(username.clone(), password.clone());
            } else if let Some(token) = &auth.token {
                options = options.token(token.clone());
            }
        }

        // Connect to NATS server
        let client = options
            .connect(&self.config.url)
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to NATS server: {}", e)))?;

        // Store client
        let mut client_guard = self.client.write().await;
        *client_guard = Some(client.clone());

        // Setup JetStream if configured
        match &self.config.mode {
            Mode::Regular { .. } => {}
            Mode::JetStream { .. } => {
                let jetstream = async_nats::jetstream::new(client);
                // Store stream reference
                let mut stream_guard = self.js_stream.write().await;
                *stream_guard = Some(jetstream);
            }
        };

        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("NATS client not connected".to_string()))?;

        // Get value field
        let value_field = self
            .config
            .value_field
            .as_deref()
            .unwrap_or(DEFAULT_BINARY_VALUE_FIELD);

        // Get message payloads
        let payloads = msg.to_binary(value_field)?;
        if payloads.is_empty() {
            return Ok(());
        }

        // Clone payloads to avoid lifetime issues
        let owned_payloads: Vec<Vec<u8>> = payloads.into_iter().map(|p| p.to_vec()).collect();
        // Get subject
        let subject = match &self.config.mode {
            Mode::Regular { subject } | Mode::JetStream { subject } => {
                subject.evaluate_expr(&msg).await?
            }
        };

        // Publish messages
        for (i, payload) in owned_payloads.into_iter().enumerate() {
            let Some(subject_str) = subject.get(i) else {
                continue;
            };

            match &self.config.mode {
                Mode::Regular { .. } => {
                    // Publish to regular NATS
                    if let Err(e) = client
                        .publish(subject_str.to_string(), payload.into())
                        .await
                    {
                        error!("Failed to publish message to NATS: {}", e);
                        return Err(Error::Process(format!(
                            "Failed to publish message to NATS: {}",
                            e
                        )));
                    }
                }
                Mode::JetStream { .. } => {
                    // Prepare JetStream context once (if configured)
                    let js_stream_guard = self.js_stream.read().await;
                    let jet_ctx = js_stream_guard.as_ref();
                    if let Some(jetstream) = &jet_ctx {
                        // Publish to JetStream
                        if let Err(e) = jetstream
                            .publish(subject_str.to_string(), payload.into())
                            .await
                        {
                            error!("Failed to publish message to JetStream: {}", e);
                            return Err(Error::Process(format!(
                                "Failed to publish message to JetStream: {}",
                                e
                            )));
                        }
                    }
                }
            };
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Close JetStream stream if active
        let mut js_stream_guard = self.js_stream.write().await;
        *js_stream_guard = None;

        // Close NATS client
        let mut client_guard = self.client.write().await;
        if let Some(client) = client_guard.take() {
            // Client will be dropped automatically
            if let Err(e) = client.flush().await {
                error!("Error flushing NATS client: {}", e);
            }
        }

        Ok(())
    }
}

struct NatsOutputBuilder;

impl OutputBuilder for NatsOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "NATS output configuration is missing".to_string(),
            ));
        }
        let config: NatsOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(NatsOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("nats", Arc::new(NatsOutputBuilder))
}
