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

//! NATS input component
//!
//! Receive data from a NATS subject

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::stream::Stream;
use async_nats::{Client, ConnectOptions};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// NATS input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsInputConfig {
    /// NATS server URL
    pub url: String,
    /// NATS subject to subscribe to
    pub subject: String,
    /// NATS queue group (optional)
    pub queue_group: Option<String>,
    /// JetStream configuration (optional)
    pub jetstream: Option<JetStreamConfig>,
    /// Authentication credentials (optional)
    pub auth: Option<NatsAuth>,
}

/// JetStream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JetStreamConfig {
    /// Stream name
    pub stream: String,
    /// Consumer name
    pub consumer: String,
    /// Durable name (optional)
    pub durable: Option<String>,
}

/// NATS authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsAuth {
    /// Username (optional)
    pub username: Option<String>,
    /// Password (optional)
    pub password: Option<String>,
    /// Token (optional)
    pub token: Option<String>,
}

/// NATS input component
pub struct NatsInput {
    config: NatsInputConfig,
    client: Arc<RwLock<Option<Client>>>,
    js_consumer: Arc<RwLock<Option<PullConsumer>>>,
    js_stream: Arc<RwLock<Option<Stream>>>,
}

impl NatsInput {
    /// Create a new NATS input component
    pub fn new(config: NatsInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            js_consumer: Arc::new(RwLock::new(None)),
            js_stream: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Input for NatsInput {
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
        if let Some(js_config) = &self.config.jetstream {
            let jetstream = async_nats::jetstream::new(client);

            // Get or create stream
            let stream = jetstream
                .get_stream(&js_config.stream)
                .await
                .map_err(|e| Error::Connection(format!("Failed to get JetStream: {}", e)))?;

            // Store stream reference
            let mut stream_guard = self.js_stream.write().await;
            *stream_guard = Some(stream.clone());

            // Get or create consumer
            let consumer_config = async_nats::jetstream::consumer::pull::Config {
                durable_name: js_config.durable.clone(),
                name: Some(js_config.consumer.clone()),
                ..Default::default()
            };

            let consumer = stream
                .get_or_create_consumer(&js_config.consumer, consumer_config)
                .await
                .map_err(|e| Error::Connection(format!("Failed to create consumer: {}", e)))?;

            // Store consumer reference
            let mut consumer_guard = self.js_consumer.write().await;
            *consumer_guard = Some(consumer);
        }

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or(Error::Connection("NATS client not connected".to_string()))?;

        // Handle JetStream or regular subscription
        if let Some(_) = &self.config.jetstream {
            // Read from JetStream
            let consumer_guard = self.js_consumer.read().await;
            let consumer = consumer_guard.as_ref().ok_or(Error::Connection(
                "JetStream consumer not initialized".to_string(),
            ))?;

            // Fetch messages with timeout
            let mut messages = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                consumer.fetch().max_messages(1).messages(),
            )
            .await
            .map_err(|_| Error::Process("Timeout while fetching messages".to_string()))?
            .map_err(|e| Error::Process(format!("Failed to fetch messages: {}", e)))?;

            if let Some(message) = messages.next().await {
                let message =
                    message.map_err(|e| Error::Process(format!("Failed to get message: {}", e)))?;
                let payload = message.payload.to_vec();

                // Acknowledge the message with error handling
                message.ack().await.map_err(|e| {
                    tracing::warn!("Failed to acknowledge JetStream message: {}", e);
                    Error::Process(format!("Failed to acknowledge message: {}", e))
                })?;

                let msg_batch = MessageBatch::new_binary(vec![payload])?;
                return Ok((msg_batch, Arc::new(NoopAck)));
            }

            return Err(Error::Process("No messages available".to_string()));
        } else {
            // Handle regular NATS subscription
            let subject = self.config.subject.clone();
            let mut subscription = if let Some(queue_group) = &self.config.queue_group {
                client.queue_subscribe(subject, queue_group.clone()).await
            } else {
                client.subscribe(subject).await
            }
            .map_err(|e| Error::Process(format!("Failed to subscribe to NATS subject: {}", e)))?;

            // Get next message with timeout
            let message =
                tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next())
                    .await
                    .map_err(|_| Error::Process("Timeout while waiting for message".to_string()))?
                    .ok_or(Error::Process("No messages available".to_string()))?;

            let payload = message.payload.to_vec();
            let msg_batch = MessageBatch::new_binary(vec![payload])?;
            return Ok((msg_batch, Arc::new(NoopAck)));
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Close JetStream consumer if active
        let mut js_consumer_guard = self.js_consumer.write().await;
        *js_consumer_guard = None;

        // Close JetStream stream if active
        let mut js_stream_guard = self.js_stream.write().await;
        *js_stream_guard = None;

        // Close NATS client
        let mut client_guard = self.client.write().await;
        if let Some(_client) = client_guard.take() {
            // Client will be dropped automatically
        }

        Ok(())
    }
}

pub(crate) struct NatsInputBuilder;
impl InputBuilder for NatsInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "NATS input configuration is missing".to_string(),
            ));
        }
        let config: NatsInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(NatsInput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("nats", Arc::new(NatsInputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_serialization() {
        let config = NatsInputConfig {
            url: "nats://localhost:4222".to_string(),
            subject: "test.subject".to_string(),
            queue_group: Some("test-group".to_string()),
            jetstream: Some(JetStreamConfig {
                stream: "test-stream".to_string(),
                consumer: "test-consumer".to_string(),
                durable: Some("test-durable".to_string()),
            }),
            auth: Some(NatsAuth {
                username: Some("user".to_string()),
                password: Some("pass".to_string()),
                token: None,
            }),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NatsInputConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.url, config.url);
        assert_eq!(deserialized.subject, config.subject);
        assert_eq!(deserialized.queue_group, config.queue_group);
    }
}
