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

//! Pulsar output component
//!
//! Send data to a Pulsar topic

use crate::expr::Expr;
use crate::pulsar::{
    PulsarAuth, PulsarClient, PulsarClientUtils, PulsarConfigValidator, PulsarProducer,
};
use arkflow_core::{
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatch, MessageBatchRef, Resource, DEFAULT_BINARY_VALUE_FIELD,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// Pulsar output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarOutputConfig {
    /// Pulsar service URL
    pub service_url: String,
    /// Topic to publish to
    pub topic: Expr<String>,
    /// Authentication (optional)
    pub auth: Option<PulsarAuth>,
    /// Value field to use for message payload
    pub value_field: Option<String>,
}

/// Pulsar output component
pub struct PulsarOutput {
    config: PulsarOutputConfig,
    client: Arc<RwLock<Option<PulsarClient>>>,
    producer: Arc<RwLock<Option<PulsarProducer>>>,
}

impl PulsarOutput {
    /// Create a new Pulsar output component
    fn new(config: PulsarOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            producer: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Output for PulsarOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Validate configuration before connecting
        PulsarConfigValidator::validate_service_url(&self.config.service_url)?;

        if let Some(ref auth) = self.config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        // Use shared client builder with authentication
        let builder =
            PulsarClientUtils::create_client_builder(&self.config.service_url, &self.config.auth)?;

        // Connect to Pulsar
        let client = builder
            .build()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Pulsar: {}", e)))?;

        // Store client
        let mut client_guard = self.client.write().await;
        *client_guard = Some(client.clone());

        // Create single producer
        let producer = client
            .producer()
            .build()
            .await
            .map_err(|e| Error::Connection(format!("Failed to create producer: {}", e)))?;

        let mut producer_guard = self.producer.write().await;
        *producer_guard = Some(producer);

        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        let _client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("Pulsar client not connected".to_string()))?;

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

        // Get topics for each message
        let topics_result = self.config.topic.evaluate_expr(&msg).await?;
        let topics: Vec<String> = match topics_result {
            crate::expr::EvaluateResult::Scalar(topic) => vec![topic],
            crate::expr::EvaluateResult::Vec(topics_vec) => topics_vec,
        };

        // Use single producer
        let mut producer_guard = self.producer.write().await;
        let producer = producer_guard
            .as_mut()
            .ok_or_else(|| Error::Connection("Pulsar producer not initialized".to_string()))?;

        self.send_messages_individually(producer, &topics, &owned_payloads)
            .await
    }

    async fn close(&self) -> Result<(), Error> {
        // Close producer if active
        let mut producer_guard = self.producer.write().await;
        *producer_guard = None;

        // Close Pulsar client
        let mut client_guard = self.client.write().await;
        if let Some(_client) = client_guard.take() {
            // Client will be dropped automatically
        }

        Ok(())
    }
}

impl PulsarOutput {
    async fn send_messages_individually(
        &self,
        producer: &mut PulsarProducer,
        _topics: &[String],
        payloads: &[Vec<u8>],
    ) -> Result<(), Error> {
        for (index, payload) in payloads.iter().enumerate() {
            // Send message to Pulsar without retry
            match producer.send_non_blocking(payload.clone()).await {
                Ok(_) => {
                    tracing::debug!("Successfully sent message {} to Pulsar", index);
                }
                Err(e) => {
                    error!("Failed to send message {} to Pulsar: {}", index, e);
                    return Err(Error::Process(format!(
                        "Failed to send message to Pulsar: {}",
                        e
                    )));
                }
            }
        }

        Ok(())
    }
}

pub struct PulsarOutputBuilder;

impl OutputBuilder for PulsarOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Pulsar output configuration is missing".to_string(),
            ));
        }
        let config: PulsarOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        // Validate configuration during build
        PulsarConfigValidator::validate_service_url(&config.service_url)?;

        // Note: We can't fully validate the topic here since it's an expression
        // that needs to be evaluated at runtime with a MessageBatch

        if let Some(ref auth) = config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        Ok(Arc::new(PulsarOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("pulsar", Arc::new(PulsarOutputBuilder))
}
