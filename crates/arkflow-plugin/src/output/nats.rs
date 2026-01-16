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
use arkflow_core::{
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatch, MessageBatchRef, Resource, DEFAULT_BINARY_VALUE_FIELD,
};
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

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn test_nats_output_config_regular_mode() {
        let config_json = serde_json::json!({
            "url": "nats://localhost:4222",
            "mode": {
                "type": "regular",
                "subject": {
                    "type": "expr",
                    "expr": "test.subject"
                }
            },
            "auth": null,
            "value_field": null
        });

        let config: NatsOutputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.url, "nats://localhost:4222");
        assert!(config.auth.is_none());
        assert!(config.value_field.is_none());
        match config.mode {
            Mode::Regular { subject } => match subject {
                crate::expr::Expr::Expr { expr } => assert_eq!(expr, "test.subject"),
                _ => panic!("Expected Expr type"),
            },
            _ => panic!("Expected Regular mode"),
        }
    }

    #[test]
    fn test_nats_output_config_jetstream_mode() {
        let config_json = serde_json::json!({
            "url": "nats://localhost:4222",
            "mode": {
                "type": "jet_stream",
                "subject": {
                    "type": "value",
                    "value": "my.subject"
                }
            }
        });

        let config: NatsOutputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.url, "nats://localhost:4222");
        match config.mode {
            Mode::JetStream { subject } => match subject {
                crate::expr::Expr::Value { value } => assert_eq!(value, "my.subject"),
                _ => panic!("Expected Value type"),
            },
            _ => panic!("Expected JetStream mode"),
        }
    }

    #[test]
    fn test_nats_output_config_with_auth() {
        let config_json = serde_json::json!({
            "url": "nats://localhost:4222",
            "mode": {
                "type": "regular",
                "subject": {
                    "type": "value",
                    "value": "test"
                }
            },
            "auth": {
                "username": "user",
                "password": "pass",
                "token": null
            },
            "value_field": "data"
        });

        let config: NatsOutputConfig = serde_json::from_value(config_json).unwrap();
        assert!(config.auth.is_some());
        let auth = config.auth.unwrap();
        assert_eq!(auth.username, Some("user".to_string()));
        assert_eq!(auth.password, Some("pass".to_string()));
        assert!(auth.token.is_none());
        assert_eq!(config.value_field, Some("data".to_string()));
    }

    #[test]
    fn test_nats_output_builder_without_config() {
        let builder = NatsOutputBuilder;
        let result = builder.build(None, &None, &create_test_resource());
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_nats_output_new() {
        let config = NatsOutputConfig {
            url: "nats://localhost:4222".to_string(),
            mode: Mode::Regular {
                subject: crate::expr::Expr::Value {
                    value: "test".to_string(),
                },
            },
            auth: None,
            value_field: None,
        };

        let output = NatsOutput::new(config);
        assert!(output.is_ok());
    }
}
