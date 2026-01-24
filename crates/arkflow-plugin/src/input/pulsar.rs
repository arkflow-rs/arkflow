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

//! Pulsar input component
//!
//! Receive data from a Pulsar topic

use crate::pulsar::{
    PulsarAuth, PulsarClientUtils, PulsarConfigValidator, RetryConfig, SubscriptionType,
};
use arkflow_core::codec::Codec;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures::StreamExt;
use pulsar::{Pulsar, SubType, TokioExecutor};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// Pulsar input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarInputConfig {
    /// Pulsar service URL
    pub service_url: String,
    /// Topic to subscribe to
    pub topic: String,
    /// Subscription name
    pub subscription_name: String,
    /// Subscription type (optional, defaults to Exclusive)
    pub subscription_type: Option<SubscriptionType>,
    /// Authentication (optional)
    pub auth: Option<PulsarAuth>,
    /// Retry configuration (optional)
    pub retry_config: Option<RetryConfig>,
}

/// Pulsar message type for async processing
enum PulsarMsg {
    Message(pulsar::consumer::Message<Vec<u8>>),
    Err(Error),
}

/// Pulsar input component
pub struct PulsarInput {
    input_name: Option<String>,
    config: PulsarInputConfig,
    client: Arc<RwLock<Option<Pulsar<TokioExecutor>>>>,
    consumer:
        Arc<RwLock<Option<Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>>>>,
    sender: Sender<PulsarMsg>,
    receiver: Receiver<PulsarMsg>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

impl PulsarInput {
    /// Create a new Pulsar input component
    pub fn new(
        name: Option<&String>,
        config: PulsarInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let (sender, receiver) = flume::bounded::<PulsarMsg>(1000);
        Ok(Self {
            input_name: name.cloned(),
            config,
            client: Arc::new(RwLock::new(None)),
            consumer: Arc::new(RwLock::new(None)),
            sender,
            receiver,
            cancellation_token,
            codec,
        })
    }
}

#[async_trait]
impl Input for PulsarInput {
    async fn connect(&self) -> Result<(), Error> {
        // Validate configuration before connecting
        PulsarConfigValidator::validate_service_url(&self.config.service_url)?;
        PulsarConfigValidator::validate_topic(&self.config.topic)?;
        PulsarConfigValidator::validate_subscription_name(&self.config.subscription_name)?;

        if let Some(ref auth) = self.config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        if let Some(ref retry_config) = self.config.retry_config {
            PulsarConfigValidator::validate_retry_config(retry_config)?;
        }

        let _retry_config = self.config.retry_config.clone().unwrap_or_default();

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

        // Configure consumer
        let subscription_type = self.config.subscription_type.clone().unwrap_or_default();

        let _consumer_builder = client
            .consumer()
            .with_topic(&self.config.topic)
            .with_subscription(&self.config.subscription_name)
            .with_subscription_type(match subscription_type {
                SubscriptionType::Exclusive => SubType::Exclusive,
                SubscriptionType::Shared => SubType::Shared,
                SubscriptionType::Failover => SubType::Failover,
                SubscriptionType::KeyShared => SubType::KeyShared,
            });

        // Create consumer
        let consumer = client
            .consumer()
            .with_topic(&self.config.topic)
            .with_subscription(&self.config.subscription_name)
            .with_subscription_type(match subscription_type {
                SubscriptionType::Exclusive => SubType::Exclusive,
                SubscriptionType::Shared => SubType::Shared,
                SubscriptionType::Failover => SubType::Failover,
                SubscriptionType::KeyShared => SubType::KeyShared,
            })
            .build()
            .await
            .map_err(|e| Error::Connection(format!("Failed to create consumer: {}", e)))?;

        // Store consumer
        let consumer_arc = Arc::new(Mutex::new(consumer));
        let mut consumer_guard = self.consumer.write().await;
        *consumer_guard = Some(consumer_arc.clone());

        // Clone sender for async tasks
        let sender_clone = self.sender.clone();
        let cancellation_token_clone = self.cancellation_token.clone();

        // Start background task for message processing
        tokio::spawn(async move {
            let consumer = consumer_arc;
            loop {
                tokio::select! {
                    _ = cancellation_token_clone.cancelled() => {
                        break;
                    }
                    result = async {
                        let mut consumer = consumer.lock().await;
                        consumer.next().await
                    } => {
                        match result {
                            Some(Ok(message)) => {
                                if let Err(e) = sender_clone.send_async(PulsarMsg::Message(message)).await {
                                    error!("Failed to send message to channel: {}", e);
                                }
                            }
                            Some(Err(e)) => {
                                warn!("Failed to receive Pulsar message: {}", e);
                                if let Err(e) = sender_clone.send_async(PulsarMsg::Err(Error::Disconnection)).await {
                                    error!("Failed to send error to channel: {}", e);
                                }
                                // Break the loop to allow reconnection to create a new consumer task
                                break;
                            }
                            None => {
                                // Stream ended
                                if let Err(e) = sender_clone.send_async(PulsarMsg::Err(Error::EOF)).await {
                                    error!("Failed to send EOF to channel: {}", e);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        if client_guard.is_none() {
            return Err(Error::Connection("Pulsar client not connected".to_string()));
        }

        // Get cancellation token for potential cancellation
        let cancellation_token = self.cancellation_token.clone();

        // Use tokio::select to handle both message receiving and cancellation
        tokio::select! {
            result = self.receiver.recv_async() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            PulsarMsg::Message(mut message) => {
                                let payload = std::mem::take(&mut message.payload.data);

                                // Apply codec if configured
                                let mut msg_batch = crate::input::codec_helper::apply_codec_to_payload(
                                    &payload,
                                    &self.codec,
                                )?;
                                msg_batch.set_input_name(self.input_name.clone());

                                // Get consumer reference for acknowledgment
                                let consumer_guard = self.consumer.read().await;
                                let consumer = consumer_guard.as_ref().cloned()
                                    .ok_or_else(|| Error::Connection("Pulsar consumer not available".to_string()))?;

                                let ack = PulsarAck::new(message, consumer);
                                Ok((Arc::new(msg_batch), Arc::new(ack) as Arc<dyn Ack>))
                            },
                            PulsarMsg::Err(e) => {
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
        // Send cancellation signal
        self.cancellation_token.cancel();

        // Close consumer if active
        let mut consumer_guard = self.consumer.write().await;
        *consumer_guard = None;

        // Close Pulsar client
        let mut client_guard = self.client.write().await;
        if let Some(_client) = client_guard.take() {
            // Client will be dropped automatically
        }

        Ok(())
    }
}

pub struct PulsarInputBuilder;

impl InputBuilder for PulsarInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Pulsar input configuration is missing".to_string(),
            ));
        }
        let config: PulsarInputConfig = serde_json::from_value(config.clone().unwrap())?;

        // Validate configuration during build
        PulsarConfigValidator::validate_service_url(&config.service_url)?;
        PulsarConfigValidator::validate_topic(&config.topic)?;
        PulsarConfigValidator::validate_subscription_name(&config.subscription_name)?;

        if let Some(ref auth) = config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        if let Some(ref retry_config) = config.retry_config {
            PulsarConfigValidator::validate_retry_config(retry_config)?;
        }

        Ok(Arc::new(PulsarInput::new(name, config, codec)?))
    }
}

/// Pulsar message acknowledgment
pub struct PulsarAck {
    // Store the full message for acknowledgment
    message: Option<pulsar::consumer::Message<Vec<u8>>>,
    // Reference to consumer for acknowledgment
    consumer: Option<Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>>,
}

impl PulsarAck {
    pub fn new(
        message: pulsar::consumer::Message<Vec<u8>>,
        consumer: Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>,
    ) -> Self {
        Self {
            message: Some(message),
            consumer: Some(consumer),
        }
    }
}

#[async_trait]
impl Ack for PulsarAck {
    async fn ack(&self) {
        if let (Some(consumer), Some(message)) = (&self.consumer, &self.message) {
            let mut consumer_guard = consumer.lock().await;
            if let Err(e) = consumer_guard.ack(message).await {
                error!("Failed to acknowledge Pulsar message: {}", e);
            } else {
                tracing::debug!("Successfully acknowledged Pulsar message");
            }
        }
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("pulsar", Arc::new(PulsarInputBuilder))
}
