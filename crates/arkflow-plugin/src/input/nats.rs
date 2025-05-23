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

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::stream::Stream;
use async_nats::{Client, ConnectOptions, Message};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// NATS input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsInputConfig {
    /// NATS server URL
    pub url: String,
    /// NATS mode
    pub mode: Mode,
    /// Authentication credentials (optional)
    pub auth: Option<NatsAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Mode {
    Regular {
        /// NATS subject to subscribe to
        subject: String,
        /// NATS queue group (optional)
        queue_group: Option<String>,
    },
    /// JetStream configuration
    JetStream {
        /// Stream name
        stream: String,
        /// Consumer name
        consumer_name: String,
        /// Durable name (optional)
        durable_name: Option<String>,
    },
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

/// NATS message type for async processing
enum NatsMsg {
    /// Regular NATS message with original message for acknowledgment
    Regular(Message),
    /// JetStream message with payload and original message for acknowledgment
    JetStream(async_nats::jetstream::Message),
    /// Error message
    Err(Error),
}

/// NATS input component
pub struct NatsInput {
    config: NatsInputConfig,
    client: Arc<RwLock<Option<Client>>>,
    js_consumer: Arc<RwLock<Option<PullConsumer>>>,
    js_stream: Arc<RwLock<Option<Stream>>>,
    sender: Sender<NatsMsg>,
    receiver: Receiver<NatsMsg>,
    cancellation_token: CancellationToken,
}

impl NatsInput {
    /// Create a new NATS input component
    pub fn new(config: NatsInputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let (sender, receiver) = flume::bounded::<NatsMsg>(1000);
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            js_consumer: Arc::new(RwLock::new(None)),
            js_stream: Arc::new(RwLock::new(None)),
            sender,
            receiver,
            cancellation_token,
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

        // Clone sender for async tasks
        let sender_clone = self.sender.clone();
        let cancellation_token_clone = self.cancellation_token.clone();

        match &self.config.mode {
            Mode::Regular {
                subject,
                queue_group,
            } => {
                // Setup regular subscription
                let subject = subject.clone();
                let client_clone = client.clone();
                let sender = sender_clone;
                let cancellation = cancellation_token_clone;
                let queue_group = queue_group.clone();

                tokio::spawn(async move {
                    // Create subscription
                    let subscription_result = if let Some(queue) = queue_group {
                        client_clone.queue_subscribe(subject, queue).await
                    } else {
                        client_clone.subscribe(subject).await
                    };

                    match subscription_result {
                        Ok(mut subscription) => {
                            loop {
                                tokio::select! {
                                    _ = cancellation.cancelled() => {
                                        break;
                                    }
                                    message_option = subscription.next() => {
                                        match message_option {
                                            Some(message) => {
                                                if let Err(e) = sender.send_async(NatsMsg::Regular(message)).await {
                                                    error!("Failed to send message to channel: {}", e);
                                                }
                                            },
                                            None => {
                                                // Subscription ended
                                                if let Err(e) = sender.send_async(NatsMsg::Err(Error::EOF)).await {
                                                    error!("Failed to send EOF to channel: {}", e);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to subscribe to NATS subject: {}", e);
                            let _ = sender
                                .send_async(NatsMsg::Err(Error::Process(format!(
                                    "Failed to subscribe to NATS subject: {}",
                                    e
                                ))))
                                .await;
                        }
                    }
                });
            }
            Mode::JetStream {
                stream,
                consumer_name,
                durable_name,
            } => {
                // Setup JetStream configured
                let jetstream = async_nats::jetstream::new(client);

                // Get or create stream
                let stream = jetstream
                    .get_stream(stream)
                    .await
                    .map_err(|e| Error::Connection(format!("Failed to get JetStream: {}", e)))?;

                // Store stream reference
                let mut stream_guard = self.js_stream.write().await;
                *stream_guard = Some(stream.clone());

                // Get or create consumer
                let consumer_config = async_nats::jetstream::consumer::pull::Config {
                    durable_name: durable_name.clone(),
                    name: Some(consumer_name.clone()),
                    ..Default::default()
                };

                let consumer = stream
                    .get_or_create_consumer(consumer_name, consumer_config)
                    .await
                    .map_err(|e| Error::Connection(format!("Failed to create consumer: {}", e)))?;

                // Store consumer reference
                let mut consumer_guard = self.js_consumer.write().await;
                *consumer_guard = Some(consumer.clone());

                // Start background task for JetStream message processing
                let sender = sender_clone.clone();
                let cancellation = cancellation_token_clone.clone();
                match consumer.fetch().max_messages(10).messages().await {
                    Ok(mut messages) => {
                        while let Some(message_result) = messages.next().await {
                            match message_result {
                                Ok(message) => {
                                    // Send to channel with original message for later acknowledgment
                                    if let Err(e) =
                                        sender.send_async(NatsMsg::JetStream(message)).await
                                    {
                                        error!("Failed to send message to channel: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to get JetStream message: {}", e);
                                    if let Err(e) = sender
                                        .send_async(NatsMsg::Err(Error::Process(format!(
                                            "Failed to get message: {}",
                                            e
                                        ))))
                                        .await
                                    {
                                        error!("Failed to send error to channel: {}", e);
                                    }
                                    // Short pause before retrying
                                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to fetch JetStream messages: {}", e);
                        if let Err(e) = sender
                            .send_async(NatsMsg::Err(Error::Process(format!(
                                "Failed to fetch messages: {}",
                                e
                            ))))
                            .await
                        {
                            error!("Failed to send error to channel: {}", e);
                        }
                        // Short pause before retrying
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }

                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancellation.cancelled() => {
                                break;
                            }
                            result = consumer.fetch().messages()  => {
                                match result {
                                Ok(mut messages) => {
                                    while let Some(message_result) = messages.next().await {
                                        match message_result {
                                            Ok(message) => {
                                                // Send to channel with original message for later acknowledgment
                                                if let Err(e) = sender.send_async(NatsMsg::JetStream(message)).await
                                                {
                                                    error!("Failed to send message to channel: {}", e);
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to get JetStream message: {}", e);
                                                if let Err(e) = sender
                                                    .send_async(NatsMsg::Err(Error::Process(format!(
                                                        "Failed to get message: {}",
                                                        e
                                                    ))))
                                                    .await
                                                {
                                                    error!("Failed to send error to channel: {}", e);
                                                }
                                                // Short pause before retrying
                                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to fetch JetStream messages: {}", e);
                                    if let Err(e) = sender
                                        .send_async(NatsMsg::Err(Error::Process(format!(
                                            "Failed to fetch messages: {}",
                                            e
                                        ))))
                                        .await
                                    {
                                        error!("Failed to send error to channel: {}", e);
                                    }
                                    // Short pause before retrying
                                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                }
                            }
                            }
                        }
                    }
                });
            }
        }

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        if client_guard.is_none() {
            return Err(Error::Connection("NATS client not connected".to_string()));
        }

        // Get cancellation token for potential cancellation
        let cancellation_token = self.cancellation_token.clone();

        // Use tokio::select to handle both message receiving and cancellation
        tokio::select! {
            result = self.receiver.recv_async() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            NatsMsg::Regular(message) => {
                                let payload = message.payload.to_vec();
                                let msg_batch = MessageBatch::new_binary(vec![payload])?;
                                Ok((msg_batch, Arc::new(NatsAck::Regular)))
                            },
                            NatsMsg::JetStream( message) => {
                                let payload = message.payload.to_vec();
                                let msg_batch = MessageBatch::new_binary(vec![payload])?;
                                let ack = NatsAck::JetStream {
                                    message,
                                };
                                Ok((msg_batch, Arc::new(ack) as Arc<dyn Ack>))
                            },
                            NatsMsg::Err(e) => {
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
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "NATS input configuration is missing".to_string(),
            ));
        }
        let config: NatsInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(NatsInput::new(config)?))
    }
}

/// NATS message acknowledgment
enum NatsAck {
    /// Regular NATS message acknowledgment
    Regular,
    /// JetStream message acknowledgment
    JetStream {
        message: async_nats::jetstream::Message,
    },
}

#[async_trait]
impl Ack for NatsAck {
    async fn ack(&self) {
        match self {
            NatsAck::Regular => {
                // For regular NATS messages, there's no explicit acknowledgment
            }
            NatsAck::JetStream { message } => {
                // Acknowledge JetStream message
                if let Err(e) = message.ack().await {
                    warn!("Failed to acknowledge JetStream message: {}", e);
                }
            }
        }
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("nats", Arc::new(NatsInputBuilder))
}
