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

//! Amqp09 output component
//!
//! Send data to Amqp09 broker

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};

use crate::expr::Expr;
use async_trait::async_trait;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Amqp09 output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Amqp09OutputConfig {
    /// Amqp09 broker URL (e.g., "amqp://localhost:5672")
    url: String,
    /// Exchange name to publish to
    exchange: String,
    /// Exchange type (optional, default: "direct")
    exchange_type: Option<String>,
    /// Routing key for publishing
    routing_key: Expr<String>,
    /// Whether to declare the exchange if it doesn't exist
    declare_exchange: Option<bool>,
    /// Exchange durability
    durable: Option<bool>,
    /// Auto-delete exchange when no queues are bound
    auto_delete: Option<bool>,
    /// Message persistence
    persistent: Option<bool>,
    /// Publisher confirms
    confirm: Option<bool>,
    /// Message priority
    priority: Option<u8>,
    /// Message expiration (in milliseconds)
    expiration: Option<String>,
    /// Content type
    content_type: Option<String>,
    /// Content encoding
    content_encoding: Option<String>,
    /// Value field to extract from messages
    value_field: Option<String>,
}

/// Amqp09 output component
struct Amqp09Output {
    config: Amqp09OutputConfig,
    connection: Arc<Mutex<Option<Connection>>>,
    channel: Arc<Mutex<Option<Channel>>>,
    connected: AtomicBool,
}

impl Amqp09Output {
    /// Create a new Amqp09 output component
    fn new(config: Amqp09OutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            connection: Arc::new(Mutex::new(None)),
            channel: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Output for Amqp09Output {
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

        // Enable publisher confirms if requested
        if self.config.confirm.unwrap_or(false) {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| {
                    Error::Connection(format!("Failed to enable publisher confirms: {}", e))
                })?;
        }

        // Declare exchange if needed
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
                    &self.config.exchange,
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

        // Store connection and channel
        {
            let mut conn_guard = self.connection.lock().await;
            *conn_guard = Some(conn);
        }
        {
            let mut channel_guard = self.channel.lock().await;
            *channel_guard = Some(channel);
        }

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, batch: MessageBatch) -> Result<(), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Process("Amqp09 output is not connected".to_string()));
        }

        let channel_arc = self.channel.clone();
        let channel_guard = channel_arc.lock().await;
        let channel = channel_guard
            .as_ref()
            .ok_or_else(|| Error::Process("Amqp09 channel is not initialized".to_string()))?;

        let value_field = self.config.value_field.as_deref().unwrap_or("data");

        // Get the message content
        let payloads = match batch.to_binary(value_field) {
            Ok(v) => v.to_vec(),
            Err(e) => {
                return Err(e);
            }
        };

        let routing_keys = self.config.routing_key.evaluate_expr(&batch).await?;

        for (i, payload) in payloads.into_iter().enumerate() {
            let routing_key = routing_keys.get(i).map_or("", |v| v.as_str());

            channel
                .basic_publish(
                    &self.config.exchange,
                    routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default(),
                )
                .await
                .map_err(|e| Error::Process(format!("Failed to publish message: {}", e)))?
                .await
                .map_err(|e| Error::Process(format!("Failed to confirm message: {}", e)))?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
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

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

struct Amqp09OutputBuilder;

impl OutputBuilder for Amqp09OutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Amqp09 output configuration is missing".to_string(),
            ));
        }

        let config: Amqp09OutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(Amqp09Output::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("amqp09", Arc::new(Amqp09OutputBuilder))
}
