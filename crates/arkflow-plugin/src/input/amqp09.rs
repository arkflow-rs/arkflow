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

//! AMQP 0.9/0.9.1 (RabbitMQ) input component
//!
//! Receive messages from an AMQP 0.9/0.9.1 broker such as RabbitMQ.

use crate::input::codec_helper::Delivery;
use crate::DELIVERY_CHANNEL_CAPACITY;
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatchRef, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, Acker, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Supported AMQP 0.9/0.9.1 exchange types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Amqp09ExchangeType {
    #[default]
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl Amqp09ExchangeType {
    pub(crate) fn into_exchange_kind(self) -> ExchangeKind {
        match self {
            Self::Direct => ExchangeKind::Direct,
            Self::Fanout => ExchangeKind::Fanout,
            Self::Topic => ExchangeKind::Topic,
            Self::Headers => ExchangeKind::Headers,
        }
    }
}

/// AMQP 0.9/0.9.1 input configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Amqp09InputConfig {
    /// AMQP broker URL (e.g., "amqp://guest:guest@localhost:5672/%2f" or "amqps://...")
    pub url: String,
    /// Queue name to consume from
    pub queue: String,
    /// Exchange name (optional; when set, binds the queue to this exchange)
    pub exchange: Option<String>,
    /// Exchange type when declaring an exchange (optional, default: "direct")
    pub exchange_type: Option<Amqp09ExchangeType>,
    /// Routing key used to bind the queue to the exchange (optional, default: "")
    pub routing_key: Option<String>,
    /// Whether to declare the queue if it doesn't exist (default: false)
    pub declare_queue: Option<bool>,
    /// Whether to declare the exchange if it doesn't exist (default: false)
    pub declare_exchange: Option<bool>,
    /// Durability for declared exchange/queue (default: true)
    pub durable: Option<bool>,
    /// Auto-delete for declared exchange/queue (default: false)
    pub auto_delete: Option<bool>,
    /// Exclusive flag for declared queue (default: false)
    pub exclusive: Option<bool>,
    /// Consumer tag (optional; empty lets the broker assign one)
    pub consumer_tag: Option<String>,
    /// Auto-acknowledge messages on delivery (default: false)
    pub auto_ack: Option<bool>,
    /// Per-channel prefetch count (basic.qos)
    pub prefetch_count: Option<u16>,
}

struct Amqp09InputState {
    connection: Connection,
    channel: Channel,
    cancel_token: CancellationToken,
    consumer_task: JoinHandle<()>,
}

/// AMQP 0.9/0.9.1 input component.
// The channel carries finished `Delivery` values: the consumer task decodes
// and pairs `Amqp09Ack` BEFORE claiming a slot, so `read()` is a single await
// point (Input::read cancellation-safety contract).
pub struct Amqp09Input {
    input_name: Option<String>,
    config: Amqp09InputConfig,
    state: Arc<RwLock<Option<Amqp09InputState>>>,
    sender: Sender<Delivery>,
    receiver: Receiver<Delivery>,
    codec: Option<Arc<dyn Codec>>,
}

impl Amqp09Input {
    /// Create a new AMQP 0.9/0.9.1 input component.
    pub fn new(
        name: Option<&str>,
        config: Amqp09InputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        validate_input_config(&config)?;
        let (sender, receiver) = flume::bounded::<Delivery>(DELIVERY_CHANNEL_CAPACITY);
        Ok(Self {
            input_name: name.map(str::to_string),
            config,
            state: Arc::new(RwLock::new(None)),
            sender,
            receiver,
            codec,
        })
    }

    async fn shutdown_state(state: Amqp09InputState) {
        state.cancel_token.cancel();
        let _ = state.consumer_task.await;
        let _ = state.channel.close(200, "Normal shutdown".into()).await;
        let _ = state.connection.close(200, "Normal shutdown".into()).await;
    }
}

fn validate_input_config(config: &Amqp09InputConfig) -> Result<(), Error> {
    if config.url.trim().is_empty() {
        return Err(Error::Config("amqp09 input: `url` cannot be empty".into()));
    }
    if config.queue.trim().is_empty() {
        return Err(Error::Config(
            "amqp09 input: `queue` cannot be empty".into(),
        ));
    }
    if config.declare_exchange.unwrap_or(false) {
        let missing_exchange = config
            .exchange
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty);
        if missing_exchange {
            return Err(Error::Config(
                "amqp09 input: `declare_exchange` requires a non-empty `exchange` name".into(),
            ));
        }
    }
    Ok(())
}

#[async_trait]
impl Input for Amqp09Input {
    async fn connect(&self) -> Result<(), Error> {
        // Tear down any previous connection before reconnecting so a prior
        // consumer task cannot compete with the new one and a previously
        // cancelled token never leaks into the fresh session.
        let prev = {
            let mut guard = self.state.write().await;
            guard.take()
        };
        if let Some(prev_state) = prev {
            Self::shutdown_state(prev_state).await;
        }

        // Create connection (lapin 4 uses the Tokio runtime natively).
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
                let exchange_kind = self
                    .config
                    .exchange_type
                    .unwrap_or_default()
                    .into_exchange_kind();

                channel
                    .exchange_declare(
                        exchange.as_str().into(),
                        exchange_kind,
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
                    self.config.queue.as_str().into(),
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

        // Bind queue to exchange whenever an exchange is specified (routing_key
        // defaults to "" so fanout and default-key bindings work without
        // requiring an explicit routing_key).
        if let Some(exchange) = &self.config.exchange {
            if !exchange.is_empty() {
                let routing_key = self.config.routing_key.as_deref().unwrap_or("");
                channel
                    .queue_bind(
                        self.config.queue.as_str().into(),
                        exchange.as_str().into(),
                        routing_key.into(),
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| Error::Connection(format!("Failed to bind queue: {}", e)))?;
            }
        }

        let auto_ack = self.config.auto_ack.unwrap_or(false);

        // Create consumer
        let mut consumer = channel
            .basic_consume(
                self.config.queue.as_str().into(),
                self.config.consumer_tag.as_deref().unwrap_or("").into(),
                BasicConsumeOptions {
                    no_ack: auto_ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Connection(format!("Failed to create consumer: {}", e)))?;

        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();
        let sender_clone = self.sender.clone();
        let codec_clone = self.codec.clone();
        let input_name_clone = self.input_name.clone();

        let consumer_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_token.cancelled() => {
                        break;
                    }
                    item = consumer.next() => {
                        match item {
                            Some(Ok(delivery)) => {
                                let ack: Arc<dyn Ack> = if auto_ack {
                                    Arc::new(NoopAck)
                                } else {
                                    Arc::new(Amqp09Ack { acker: delivery.acker })
                                };
                                // Decode and pair the ack BEFORE claiming a channel slot
                                // (Input::read cancellation-safety contract).
                                let decoded = crate::input::codec_helper::decode_delivery(
                                    &delivery.data,
                                    &codec_clone,
                                    input_name_clone.clone(),
                                    ack,
                                )
                                .await;
                                tokio::select! {
                                    _ = task_token.cancelled() => break,
                                    res = sender_clone.send_async(decoded) => {
                                        if res.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                error!("Amqp09 delivery error: {}", e);
                                tokio::select! {
                                    _ = task_token.cancelled() => {}
                                    _ = sender_clone.send_async(Delivery::Err(Error::Disconnection)) => {}
                                }
                                break;
                            }
                            None => {
                                // An AMQP consumer stream ending on a live broker
                                // source is an abnormal termination (broker shutdown,
                                // channel error, server-side cancel), not bounded EOF:
                                // emit Disconnection so the engine reconnects instead
                                // of silently terminating the job.
                                if !task_token.is_cancelled() {
                                    tokio::select! {
                                        _ = task_token.cancelled() => {}
                                        _ = sender_clone.send_async(Delivery::Err(Error::Disconnection)) => {}
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });

        let mut guard = self.state.write().await;
        *guard = Some(Amqp09InputState {
            connection: conn,
            channel,
            cancel_token,
            consumer_task,
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let cancel_token = {
            let guard = self.state.read().await;
            match guard.as_ref() {
                Some(state) => state.cancel_token.clone(),
                None => {
                    return Err(Error::Connection("Amqp09 input is not connected".into()));
                }
            }
        };

        tokio::select! {
            // Deliveries arrive pre-decoded with their ack paired, so this
            // recv is the single await point — a dropped read loses nothing.
            result = self.receiver.recv_async() => {
                match result {
                    Ok(Delivery::Data(batch, ack)) => Ok((batch, ack)),
                    Ok(Delivery::Err(e)) => Err(e),
                    Err(_) => {
                        // contract-eof: teardown — the channel closes only on
                        // shutdown; abnormal stream ends arrive as
                        // Delivery::Err(Error::Disconnection).
                        Err(Error::EOF)
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                // contract-eof: teardown
                Err(Error::EOF)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let state = {
            let mut guard = self.state.write().await;
            guard.take()
        };
        if let Some(state) = state {
            Self::shutdown_state(state).await;
        }
        Ok(())
    }
}

struct Amqp09Ack {
    acker: Acker,
}

#[async_trait]
impl Ack for Amqp09Ack {
    async fn ack(&self) -> Result<(), Error> {
        self.acker
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| Error::Process(format!("Failed to ack Amqp09 message: {}", e)))
    }
}

pub(crate) struct Amqp09InputBuilder;

impl InputBuilder for Amqp09InputBuilder {
    fn build(
        &self,
        name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: Amqp09InputConfig = parse_config(config, "AMQP 0.9 input")?;
        Ok(Arc::new(Amqp09Input::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("amqp09", Arc::new(Amqp09InputBuilder))?;
    register_input_metadata(
        ComponentMetadata::with_schema(
            "amqp09",
            "Consumes messages from an AMQP 0.9/0.9.1 broker (such as RabbitMQ).",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "AMQP 0.9/0.9.1 broker URL (e.g. amqp://guest:guest@localhost:5672/%2f or amqps://...)."
                    },
                    "queue": {
                        "type": "string",
                        "description": "Queue name to consume messages from."
                    },
                    "exchange": {
                        "type": "string",
                        "description": "Optional exchange name to declare and/or bind the queue to."
                    },
                    "exchange_type": {
                        "type": "string",
                        "enum": ["direct", "fanout", "topic", "headers"],
                        "description": "Exchange type used when declare_exchange is true (defaults to direct)."
                    },
                    "routing_key": {
                        "type": "string",
                        "description": "Routing key used when binding the queue to the exchange (defaults to empty string)."
                    },
                    "declare_queue": {
                        "type": "boolean",
                        "description": "Declare the queue if it does not already exist (defaults to false)."
                    },
                    "declare_exchange": {
                        "type": "boolean",
                        "description": "Declare the exchange if it does not already exist (defaults to false)."
                    },
                    "durable": {
                        "type": "boolean",
                        "description": "Mark declared exchange/queue as durable (defaults to true)."
                    },
                    "auto_delete": {
                        "type": "boolean",
                        "description": "Auto-delete declared exchange/queue when unused (defaults to false)."
                    },
                    "exclusive": {
                        "type": "boolean",
                        "description": "Declare the queue as exclusive to this connection (defaults to false)."
                    },
                    "consumer_tag": {
                        "type": "string",
                        "description": "Consumer tag identifying this consumer (defaults to broker-generated)."
                    },
                    "auto_ack": {
                        "type": "boolean",
                        "description": "Automatically acknowledge deliveries on receipt (defaults to false)."
                    },
                    "prefetch_count": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 65535,
                        "description": "Per-channel QoS prefetch count."
                    }
                },
                "required": ["url", "queue"]
            }),
        )
        .with_example(serde_json::json!({
            "url": "amqp://guest:guest@localhost:5672/%2f",
            "queue": "events",
            "declare_queue": true
        })),
    )
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
    fn test_config_deserialization_minimal() {
        let json = serde_json::json!({
            "url": "amqp://guest:guest@localhost:5672/%2f",
            "queue": "orders"
        });
        let cfg: Amqp09InputConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.url, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(cfg.queue, "orders");
        assert!(cfg.exchange.is_none());
        assert!(cfg.exchange_type.is_none());
        assert!(cfg.prefetch_count.is_none());
    }

    #[test]
    fn test_config_deserialization_full() {
        let json = serde_json::json!({
            "url": "amqps://user:pass@broker:5671/vhost",
            "queue": "orders.q",
            "exchange": "orders.ex",
            "exchange_type": "topic",
            "routing_key": "orders.#",
            "declare_queue": true,
            "declare_exchange": true,
            "durable": true,
            "auto_delete": false,
            "exclusive": false,
            "consumer_tag": "arkflow-1",
            "auto_ack": false,
            "prefetch_count": 64
        });
        let cfg: Amqp09InputConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.exchange_type, Some(Amqp09ExchangeType::Topic));
        assert_eq!(cfg.prefetch_count, Some(64));
        assert_eq!(cfg.consumer_tag.as_deref(), Some("arkflow-1"));
    }

    #[test]
    fn test_config_rejects_unknown_exchange_type() {
        let json = serde_json::json!({
            "url": "amqp://localhost:5672",
            "queue": "q",
            "exchange_type": "x-unknown"
        });
        let err = serde_json::from_value::<Amqp09InputConfig>(json).unwrap_err();
        assert!(err.to_string().contains("unknown variant"), "{err}");
    }

    #[test]
    fn test_builder_validation_errors() {
        let builder = Amqp09InputBuilder;
        let resource = create_test_resource();

        // Missing config
        assert!(matches!(
            builder.build(None, &None, None, &resource),
            Err(Error::Config(_))
        ));

        // Empty url
        let empty_url = Some(serde_json::json!({ "url": "  ", "queue": "q" }));
        assert!(matches!(
            builder.build(None, &empty_url, None, &resource),
            Err(Error::Config(_))
        ));

        // Empty queue
        let empty_queue = Some(serde_json::json!({ "url": "amqp://localhost:5672", "queue": "" }));
        assert!(matches!(
            builder.build(None, &empty_queue, None, &resource),
            Err(Error::Config(_))
        ));

        // declare_exchange without exchange
        let missing_exchange = Some(serde_json::json!({
            "url": "amqp://localhost:5672",
            "queue": "q",
            "declare_exchange": true
        }));
        assert!(matches!(
            builder.build(None, &missing_exchange, None, &resource),
            Err(Error::Config(_))
        ));
    }

    #[tokio::test]
    async fn test_read_when_not_connected_returns_connection_error() {
        let cfg = Amqp09InputConfig {
            url: "amqp://localhost:5672".into(),
            queue: "q".into(),
            exchange: None,
            exchange_type: None,
            routing_key: None,
            declare_queue: None,
            declare_exchange: None,
            durable: None,
            auto_delete: None,
            exclusive: None,
            consumer_tag: None,
            auto_ack: None,
            prefetch_count: None,
        };
        let input = Amqp09Input::new(Some("amqp_in"), cfg, None).unwrap();
        let err = input.read().await.err().expect("expected connection error");
        assert!(matches!(err, Error::Connection(_)), "got {err:?}");
    }
}
