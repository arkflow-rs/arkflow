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

//! Amqp09 input component
//!
//! Receive data from Amqp09 broker

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Consumer,
    ExchangeKind,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Amqp09 input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Amqp09InputConfig {
    /// Amqp09 broker URL (e.g., "amqp://localhost:5672")
    url: String,
    /// Queue name to consume from
    queue: String,
    /// Exchange name (optional)
    exchange: Option<String>,
    /// Exchange type (optional, default: "direct")
    exchange_type: Option<String>,
    /// Routing key (optional)
    routing_key: Option<String>,
    /// Whether to declare the queue if it doesn't exist
    declare_queue: Option<bool>,
    /// Whether to declare the exchange if it doesn't exist
    declare_exchange: Option<bool>,
    /// Queue durability
    durable: Option<bool>,
    /// Auto-delete queue when no consumers
    auto_delete: Option<bool>,
    /// Exclusive queue
    exclusive: Option<bool>,
    /// Consumer tag
    consumer_tag: Option<String>,
    /// Auto-acknowledge messages
    auto_ack: Option<bool>,
    /// Prefetch count
    prefetch_count: Option<u16>,
}

/// Amqp09 input component
pub struct Amqp09Input {
    input_name: Option<String>,
    config: Amqp09InputConfig,
    connection: Arc<Mutex<Option<Connection>>>,
    channel: Arc<Mutex<Option<Channel>>>,
    consumer: Arc<Mutex<Option<Consumer>>>,
    sender: Sender<Amqp09Msg>,
    receiver: Receiver<Amqp09Msg>,
    cancellation_token: CancellationToken,
}

enum Amqp09Msg {
    Delivery(lapin::message::Delivery),
    Err(Error),
}

impl Amqp09Input {
    /// Create a new Amqp09 input component
    fn new(name: Option<&String>, config: Amqp09InputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<Amqp09Msg>(1000);
        let cancellation_token = CancellationToken::new();
        Ok(Self {
            input_name: name.cloned(),
            config,
            connection: Arc::new(Mutex::new(None)),
            channel: Arc::new(Mutex::new(None)),
            consumer: Arc::new(Mutex::new(None)),
            sender,
            receiver,
            cancellation_token,
        })
    }
}

#[async_trait]
impl Input for Amqp09Input {
    async fn connect(&self) -> Result<(), Error> {
        // Create connection
        let conn = Connection::connect(&self.config.url, ConnectionProperties::default())
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Amqp09: {}", e)))?;

        // Create channel
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| Error::Connection(format!("Failed to create channel: {}", e)))?;

        // Set prefetch count if specified
        if let Some(prefetch_count) = self.config.prefetch_count {
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await
                .map_err(|e| Error::Connection(format!("Failed to set QoS: {}", e)))?;
        }

        // Declare exchange if needed
        if let Some(exchange) = &self.config.exchange {
            if self.config.declare_exchange.unwrap_or(false) {
                let exchange_type = match self.config.exchange_type.as_deref() {
                    Some("direct") => ExchangeKind::Direct,
                    Some("fanout") => ExchangeKind::Fanout,
                    Some("topic") => ExchangeKind::Topic,
                    Some("headers") => ExchangeKind::Headers,
                    _ => ExchangeKind::Direct,
                };

                channel
                    .exchange_declare(
                        exchange,
                        exchange_type,
                        ExchangeDeclareOptions {
                            durable: self.config.durable.unwrap_or(true),
                            auto_delete: self.config.auto_delete.unwrap_or(false),
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| Error::Connection(format!("Failed to declare exchange: {}", e)))?;
            }
        }

        // Declare queue if needed
        if self.config.declare_queue.unwrap_or(false) {
            channel
                .queue_declare(
                    &self.config.queue,
                    QueueDeclareOptions {
                        durable: self.config.durable.unwrap_or(true),
                        exclusive: self.config.exclusive.unwrap_or(false),
                        auto_delete: self.config.auto_delete.unwrap_or(false),
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| Error::Connection(format!("Failed to declare queue: {}", e)))?;
        }

        // Bind queue to exchange if both are specified
        if let (Some(exchange), Some(routing_key)) =
            (&self.config.exchange, &self.config.routing_key)
        {
            channel
                .queue_bind(
                    &self.config.queue,
                    exchange,
                    routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| Error::Connection(format!("Failed to bind queue: {}", e)))?;
        }

        // Create consumer
        let consumer = channel
            .basic_consume(
                &self.config.queue,
                self.config.consumer_tag.as_deref().unwrap_or(""),
                BasicConsumeOptions {
                    no_ack: self.config.auto_ack.unwrap_or(false),
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Connection(format!("Failed to create consumer: {}", e)))?;

        // Store connection, channel, and consumer
        {
            let mut conn_guard = self.connection.lock().await;
            *conn_guard = Some(conn);
        }
        {
            let mut channel_guard = self.channel.lock().await;
            *channel_guard = Some(channel);
        }
        {
            let mut consumer_guard = self.consumer.lock().await;
            *consumer_guard = Some(consumer);
        }

        // Start consuming messages
        let consumer_arc = Arc::clone(&self.consumer);
        let sender_clone = self.sender.clone();
        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    result = async {
                        let mut consumer_guard = consumer_arc.lock().await;
                        if let Some(consumer) = &mut *consumer_guard {
                            consumer.next().await
                        } else {
                            None
                        }
                    } => {
                        match result {
                            Some(delivery_result) => {
                                match delivery_result {
                                    Ok(delivery) => {
                                        if let Err(_) = sender_clone.send_async(Amqp09Msg::Delivery(delivery)).await {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Amqp09 delivery error: {}", e);
                                        if let Err(_) = sender_clone.send_async(Amqp09Msg::Err(Error::Disconnection)).await {
                                            break;
                                        }
                                    }
                                }
                            }
                            None => {
                                // Consumer ended
                                if let Err(_) = sender_clone.send_async(Amqp09Msg::Err(Error::EOF)).await {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        {
            let connection_guard = self.connection.lock().await;
            if connection_guard.is_none() {
                return Err(Error::Disconnection);
            }
        }

        let cancellation_token = self.cancellation_token.clone();

        tokio::select! {
            result = self.receiver.recv_async() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            Amqp09Msg::Delivery(delivery) => {
                                let payload = delivery.data.to_vec();
                                let mut msg = MessageBatch::new_binary(vec![payload])?;
                                msg.set_input_name(self.input_name.clone());

                                Ok((msg, Arc::new(Amqp09Ack {
                                    channel: Arc::clone(&self.channel),
                                    delivery_tag: delivery.delivery_tag,
                                    auto_ack: self.config.auto_ack.unwrap_or(false),
                                })))
                            }
                            Amqp09Msg::Err(e) => Err(e),
                        }
                    }
                    Err(_) => Err(Error::EOF),
                }
            }
            _ = cancellation_token.cancelled() => {
                Err(Error::EOF)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();

        // Close consumer
        {
            let mut consumer_guard = self.consumer.lock().await;
            *consumer_guard = None;
        }

        // Close channel
        {
            let mut channel_guard = self.channel.lock().await;
            if let Some(channel) = channel_guard.take() {
                let _ = channel.close(200, "Normal shutdown").await;
            }
        }

        // Close connection
        {
            let mut conn_guard = self.connection.lock().await;
            if let Some(conn) = conn_guard.take() {
                let _ = conn.close(200, "Normal shutdown").await;
            }
        }

        Ok(())
    }
}

struct Amqp09Ack {
    channel: Arc<Mutex<Option<Channel>>>,
    delivery_tag: u64,
    auto_ack: bool,
}

#[async_trait]
impl Ack for Amqp09Ack {
    async fn ack(&self) {
        if !self.auto_ack {
            let channel_guard = self.channel.lock().await;
            if let Some(channel) = &*channel_guard {
                if let Err(e) = channel
                    .basic_ack(self.delivery_tag, BasicAckOptions::default())
                    .await
                {
                    error!("Failed to ack message: {}", e);
                }
            }
        }
    }
}

pub(crate) struct Amqp09InputBuilder;

impl InputBuilder for Amqp09InputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Amqp0.9 input configuration is missing".to_string(),
            ));
        }

        let config: Amqp09InputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(Amqp09Input::new(name, config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("amqp09", Arc::new(Amqp09InputBuilder))
}
