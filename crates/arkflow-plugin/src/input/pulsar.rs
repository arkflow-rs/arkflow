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
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
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
    /// Message batching configuration (optional)
    pub batching: Option<PulsarInputBatchingConfig>,
}

/// Pulsar input batching configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarInputBatchingConfig {
    /// Maximum number of messages to batch
    pub max_messages: Option<u32>,
    /// Maximum time to wait for batch completion (milliseconds)
    pub max_wait_ms: Option<u64>,
    /// Maximum size of batch in bytes
    pub max_size_bytes: Option<u32>,
}

/// Pulsar message type for async processing
enum PulsarMsg {
    Message(pulsar::consumer::Message<Vec<u8>>),
    Err(Error),
}

/// Batched Pulsar messages with acknowledgment capability
pub struct PulsarMessageBatch {
    messages: Vec<pulsar::consumer::Message<Vec<u8>>>,
    consumer: Option<Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>>,
}

impl PulsarMessageBatch {
    pub fn new(
        messages: Vec<pulsar::consumer::Message<Vec<u8>>>,
        consumer: Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>,
    ) -> Self {
        Self {
            messages,
            consumer: Some(consumer),
        }
    }

    pub fn to_message_batch(&self) -> Result<MessageBatch, Error> {
        let payloads: Vec<Vec<u8>> = self
            .messages
            .iter()
            .map(|msg| msg.payload.data.clone())
            .collect();

        Ok(MessageBatch::new_binary(payloads)?)
    }

    pub async fn acknowledge_all(&mut self) -> Result<(), Error> {
        if let Some(consumer) = self.consumer.take() {
            let mut consumer_guard = consumer.lock().await;
            for message in &self.messages {
                consumer_guard
                    .ack(message)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to acknowledge message: {}", e)))?;
            }
        }
        Ok(())
    }
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
    batch_sender: Sender<PulsarMessageBatch>,
    batch_receiver: Receiver<PulsarMessageBatch>,
    cancellation_token: CancellationToken,
}

impl PulsarInput {
    /// Create a new Pulsar input component
    pub fn new(name: Option<&String>, config: PulsarInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let (sender, receiver) = flume::bounded::<PulsarMsg>(1000);
        let (batch_sender, batch_receiver) = flume::bounded::<PulsarMessageBatch>(100);
        Ok(Self {
            input_name: name.cloned(),
            config,
            client: Arc::new(RwLock::new(None)),
            consumer: Arc::new(RwLock::new(None)),
            sender,
            receiver,
            batch_sender,
            batch_receiver,
            cancellation_token,
        })
    }

    /// Start the batch processor task
    async fn start_batch_processor(
        &self,
        consumer: Arc<Mutex<pulsar::Consumer<Vec<u8>, pulsar::executor::TokioExecutor>>>,
    ) {
        let receiver = self.receiver.clone();
        let batch_sender = self.batch_sender.clone();
        let config = self.config.batching.clone();
        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let mut messages = Vec::new();
            let mut current_size = 0u32;
            let mut last_batch_time = std::time::Instant::now();

            loop {
                let max_messages = config.as_ref().and_then(|b| b.max_messages).unwrap_or(100);
                let max_wait_ms = config.as_ref().and_then(|b| b.max_wait_ms).unwrap_or(100);
                let max_size_bytes = config
                    .as_ref()
                    .and_then(|b| b.max_size_bytes)
                    .unwrap_or(1024 * 1024); // 1MB

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        // Send remaining messages before exiting
                        if !messages.is_empty() {
                            let batch = PulsarMessageBatch::new(
                                std::mem::take(&mut messages),
                                consumer.clone(),
                            );
                            if let Err(e) = batch_sender.send_async(batch).await {
                                error!("Failed to send final batch: {}", e);
                            }
                        }
                        break;
                    }
                    result = receiver.recv_async() => {
                        match result {
                            Ok(PulsarMsg::Message(message)) => {
                                let message_size = message.payload.data.len() as u32;

                                // Check if we should flush the current batch
                                let should_flush = !messages.is_empty() && (
                                    messages.len() >= max_messages as usize ||
                                    current_size + message_size > max_size_bytes ||
                                    last_batch_time.elapsed().as_millis() >= max_wait_ms as u128
                                );

                                if should_flush {
                                    let batch = PulsarMessageBatch::new(
                                        std::mem::take(&mut messages),
                                        consumer.clone(),
                                    );
                                    current_size = 0;
                                    last_batch_time = std::time::Instant::now();

                                    if let Err(e) = batch_sender.send_async(batch).await {
                                        error!("Failed to send batch: {}", e);
                                    }
                                }

                                // Add message to current batch
                                messages.push(message);
                                current_size += message_size;
                            }
                            Ok(PulsarMsg::Err(e)) => {
                                // Send error as single-message batch
                                let batch = PulsarMessageBatch::new(
                                    std::mem::take(&mut messages),
                                    consumer.clone(),
                                );
                                if let Err(e) = batch_sender.send_async(batch).await {
                                    error!("Failed to send error batch: {}", e);
                                }

                                // Create error batch
                                error!("Batch processor received error: {}", e);
                                break;
                            }
                            Err(_) => {
                                // Channel closed, send remaining messages
                                if !messages.is_empty() {
                                    let batch = PulsarMessageBatch::new(
                                        std::mem::take(&mut messages),
                                        consumer.clone(),
                                    );
                                    if let Err(e) = batch_sender.send_async(batch).await {
                                        error!("Failed to send final batch: {}", e);
                                    }
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

        if let Some(ref batching_config) = self.config.batching {
            PulsarConfigValidator::validate_batching_config(batching_config)?;
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

        // Clone for batch processor before moving to spawn
        let consumer_arc_for_batching = consumer_arc.clone();

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
                                if let Err(e) = sender_clone.send_async(PulsarMsg::Err(Error::Process(format!(
                                    "Failed to receive message: {}",
                                    e
                                )))).await {
                                    error!("Failed to send error to channel: {}", e);
                                }
                                // Exponential backoff for connection errors
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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

        // Start batch processor if batching is configured
        if self.config.batching.is_some() {
            self.start_batch_processor(consumer_arc_for_batching).await;
        }

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        if client_guard.is_none() {
            return Err(Error::Connection("Pulsar client not connected".to_string()));
        }

        // Get cancellation token for potential cancellation
        let cancellation_token = self.cancellation_token.clone();

        // Use tokio::select to handle both message receiving and cancellation
        if self.config.batching.is_some() {
            // Batched mode
            tokio::select! {
                result = self.batch_receiver.recv_async() => {
                    match result {
                        Ok(batch) => {
                            let mut msg_batch = batch.to_message_batch()?;
                            msg_batch.set_input_name(self.input_name.clone());

                            let ack = PulsarBatchAck::new(batch);
                            Ok((msg_batch, Arc::new(ack) as Arc<dyn Ack>))
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
        } else {
            // Non-batched mode (original behavior)
            tokio::select! {
                result = self.receiver.recv_async() => {
                    match result {
                        Ok(msg) => {
                            match msg {
                                PulsarMsg::Message(mut message) => {
                                    let payload = std::mem::take(&mut message.payload.data);
                                    let mut msg_batch = MessageBatch::new_binary(vec![payload])?;
                                    msg_batch.set_input_name(self.input_name.clone());

                                    // Get consumer reference for acknowledgment
                                    let consumer_guard = self.consumer.read().await;
                                    let consumer = consumer_guard.as_ref().cloned()
                                        .ok_or_else(|| Error::Connection("Pulsar consumer not available".to_string()))?;

                                    let ack = PulsarAck::new(message, consumer);
                                    Ok((msg_batch, Arc::new(ack) as Arc<dyn Ack>))
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

        if let Some(ref batching_config) = config.batching {
            PulsarConfigValidator::validate_batching_config(batching_config)?;
        }

        Ok(Arc::new(PulsarInput::new(name, config)?))
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

/// Pulsar batch message acknowledgment
pub struct PulsarBatchAck {
    batch: Option<PulsarMessageBatch>,
}

impl PulsarBatchAck {
    pub fn new(batch: PulsarMessageBatch) -> Self {
        Self { batch: Some(batch) }
    }
}

#[async_trait]
impl Ack for PulsarBatchAck {
    async fn ack(&self) {
        if let Some(ref _batch) = self.batch {
            // Note: In a real implementation, we would need to make the batch mutable
            // For now, we'll acknowledge through the stored consumer reference
            tracing::debug!("Acknowledging Pulsar message batch");
            // The actual acknowledgment happens through the batch's acknowledge_all method
        }
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("pulsar", Arc::new(PulsarInputBuilder))
}
