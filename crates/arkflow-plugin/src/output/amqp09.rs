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

//! AMQP 0.9/0.9.1 (RabbitMQ) output component
//!
//! Publish messages to an AMQP 0.9/0.9.1 exchange.

use crate::expr::{EvaluateResult, Expr};
use crate::input::amqp09::Amqp09ExchangeType;
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};

use async_trait::async_trait;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ErrorKind,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// AMQP 0.9/0.9.1 output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Amqp09OutputConfig {
    /// AMQP broker URL (e.g., "amqp://guest:guest@localhost:5672/%2f" or "amqps://...")
    pub url: String,
    /// Exchange name to publish to (empty string "" targets the AMQP default exchange)
    pub exchange: String,
    /// Exchange type when declaring the exchange (optional, default: "direct")
    pub exchange_type: Option<Amqp09ExchangeType>,
    /// Routing key for publishing (static value or SQL expression)
    pub routing_key: Expr<String>,
    /// Whether to declare the exchange if it doesn't exist (default: false)
    pub declare_exchange: Option<bool>,
    /// Exchange durability (default: true)
    pub durable: Option<bool>,
    /// Auto-delete exchange when no queues are bound (default: false)
    pub auto_delete: Option<bool>,
    /// Message persistence (delivery_mode 2 if true, 1 if false)
    pub persistent: Option<bool>,
    /// Enable publisher confirms and fail on broker NACK (default: false)
    pub confirm: Option<bool>,
    /// Message priority (0-255; RabbitMQ supports 0-9 by default)
    pub priority: Option<u8>,
    /// Message expiration / TTL in milliseconds
    pub expiration: Option<u64>,
    /// MIME content type (e.g., "application/json")
    pub content_type: Option<String>,
    /// MIME content encoding (e.g., "utf-8")
    pub content_encoding: Option<String>,
    /// Record field to extract as message payload; when unset, codec encoding applies
    pub value_field: Option<String>,
}

struct Amqp09OutputState {
    connection: Connection,
    channel: Channel,
}

/// AMQP 0.9/0.9.1 output component.
pub struct Amqp09Output {
    config: Amqp09OutputConfig,
    state: Arc<RwLock<Option<Amqp09OutputState>>>,
    codec: Option<Arc<dyn Codec>>,
}

impl Amqp09Output {
    /// Create a new AMQP 0.9/0.9.1 output component.
    pub fn new(config: Amqp09OutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        validate_output_config(&config)?;
        Ok(Self {
            config,
            state: Arc::new(RwLock::new(None)),
            codec,
        })
    }

    async fn shutdown_state(state: Amqp09OutputState) {
        let _ = state.channel.close(200, "Normal shutdown".into()).await;
        let _ = state.connection.close(200, "Normal shutdown".into()).await;
    }
}

fn validate_output_config(config: &Amqp09OutputConfig) -> Result<(), Error> {
    if config.url.trim().is_empty() {
        return Err(Error::Config("amqp09 output: `url` cannot be empty".into()));
    }
    if config.declare_exchange.unwrap_or(false) && config.exchange.trim().is_empty() {
        return Err(Error::Config(
            "amqp09 output: `declare_exchange` cannot be true for the default (empty) exchange"
                .into(),
        ));
    }
    Ok(())
}

/// Build AMQP message properties from the configured defaults.
fn build_basic_properties(config: &Amqp09OutputConfig) -> BasicProperties {
    let mut props = BasicProperties::default();
    if let Some(persistent) = config.persistent {
        props = props.with_delivery_mode(if persistent { 2 } else { 1 });
    }
    if let Some(priority) = config.priority {
        props = props.with_priority(priority);
    }
    if let Some(expiration_ms) = config.expiration {
        props = props.with_expiration(expiration_ms.to_string().into());
    }
    if let Some(ref content_type) = config.content_type {
        props = props.with_content_type(content_type.as_str().into());
    }
    if let Some(ref content_encoding) = config.content_encoding {
        props = props.with_content_encoding(content_encoding.as_str().into());
    }
    props
}

/// Per-row destination alignment guard: a per-row routing-key vector shorter
/// than the payload count would silently publish unmatched rows to an empty
/// routing key or drop them. Scalar routing keys apply to every row.
fn check_routing_key_alignment(
    routing_keys: &EvaluateResult<String>,
    payload_count: usize,
) -> Result<(), Error> {
    match routing_keys {
        EvaluateResult::Scalar(_) => Ok(()),
        EvaluateResult::Vec(keys) if keys.len() >= payload_count => Ok(()),
        EvaluateResult::Vec(keys) => Err(Error::Process(format!(
            "amqp09 output: per-row routing_key count ({}) is fewer than the payload count ({}); \
             publishing would silently misroute or drop unmatched rows",
            keys.len(),
            payload_count
        ))),
    }
}

/// Classify a `lapin::Error` on the publish/confirm path per
/// `connector-recovery-contract`: transport/connection/channel-state loss maps
/// to `Error::Disconnection`, while broker protocol or local frame rejections
/// map to deterministic `Error::Process`.
fn classify_lapin_error(err: lapin::Error) -> Error {
    if err.is_io_error()
        || matches!(
            err.kind(),
            ErrorKind::InvalidConnectionState(_)
                | ErrorKind::InvalidChannelState(_, _)
                | ErrorKind::ChannelsLimitReached
                | ErrorKind::MissingHeartbeatError
        )
    {
        error!("Amqp09 transport/connection error during publish: {}", err);
        Error::Disconnection
    } else {
        Error::Process(format!("Amqp09 publish failed: {}", err))
    }
}

#[async_trait]
impl Output for Amqp09Output {
    async fn connect(&self) -> Result<(), Error> {
        // Tear down any previous connection before reconnecting.
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
            let exchange_kind = self
                .config
                .exchange_type
                .unwrap_or_default()
                .into_exchange_kind();

            channel
                .exchange_declare(
                    self.config.exchange.as_str().into(),
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

        let mut guard = self.state.write().await;
        *guard = Some(Amqp09OutputState {
            connection: conn,
            channel,
        });

        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        // Clone the lightweight channel handle and release the lock before any
        // async publish or expression evaluation.
        let channel = {
            let guard = self.state.read().await;
            guard
                .as_ref()
                .map(|s| s.channel.clone())
                .ok_or_else(|| Error::Connection("Amqp09 output is not connected".into()))?
        };

        // Payload selection: explicit `value_field` extracts that column per
        // row, otherwise codec encoding applies.
        let owned_payloads: Vec<Vec<u8>> = if let Some(field) = &self.config.value_field {
            crate::output::payload::field_payloads("amqp09", &msg, field)?
        } else {
            crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)
                .await?
                .into_iter()
                .map(|p| p.to_vec())
                .collect()
        };
        if owned_payloads.is_empty() {
            return Ok(());
        }
        // Per-row routing keys are indexed by payload, so a skipped NULL row
        // would shift later payloads onto earlier routing keys.
        crate::output::codec_helper::ensure_row_aligned(&owned_payloads, msg.num_rows(), "amqp09")?;

        let routing_keys = self.config.routing_key.evaluate_expr(&msg).await?;
        check_routing_key_alignment(&routing_keys, owned_payloads.len())?;

        let props = build_basic_properties(&self.config);
        let confirm_enabled = self.config.confirm.unwrap_or(false);

        for (i, payload) in owned_payloads.into_iter().enumerate() {
            let Some(routing_key) = routing_keys.get(i) else {
                return Err(Error::Process(format!(
                    "amqp09 output: per-row routing_key {i} is missing (routing_key vector shorter than payload count)"
                )));
            };

            let publish_confirm = channel
                .basic_publish(
                    self.config.exchange.as_str().into(),
                    routing_key.as_str().into(),
                    BasicPublishOptions::default(),
                    &payload,
                    props.clone(),
                )
                .await
                .map_err(classify_lapin_error)?;

            // Always await `PublisherConfirm` so lapin never registers a
            // dropped promise in `returned_messages`: when confirms are off it
            // resolves immediately to `Confirmation::NotRequested` with no
            // network wait; when confirms are on it awaits the broker Ack/Nack.
            let confirmation = publish_confirm.await.map_err(classify_lapin_error)?;
            if confirm_enabled && confirmation.is_nack() {
                return Err(Error::Process(format!(
                    "amqp09 output: broker nacked published message (exchange='{}', routing_key='{}')",
                    self.config.exchange, routing_key
                )));
            }
        }

        Ok(())
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

pub(crate) struct Amqp09OutputBuilder;

impl OutputBuilder for Amqp09OutputBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: Amqp09OutputConfig = parse_config(config, "AMQP 0.9 output")?;
        Ok(Arc::new(Amqp09Output::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("amqp09", Arc::new(Amqp09OutputBuilder))?;
    register_output_metadata(
        ComponentMetadata::with_schema(
            "amqp09",
            "Publishes messages to an AMQP 0.9/0.9.1 exchange (such as RabbitMQ).",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "AMQP 0.9/0.9.1 broker URL (e.g. amqp://guest:guest@localhost:5672/%2f or amqps://...)."
                    },
                    "exchange": {
                        "type": "string",
                        "description": "Exchange name to publish to (empty string targets the AMQP default exchange)."
                    },
                    "exchange_type": {
                        "type": "string",
                        "enum": ["direct", "fanout", "topic", "headers"],
                        "description": "Exchange type used when declare_exchange is true (defaults to direct)."
                    },
                    "routing_key": {
                        "description": "Routing key for publishing, either a static value or a SQL expression evaluated per row.",
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"const": "value"},
                                    "value": {"type": "string"}
                                },
                                "required": ["type", "value"],
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"const": "expr"},
                                    "expr": {"type": "string"}
                                },
                                "required": ["type", "expr"],
                                "additionalProperties": false
                            }
                        ]
                    },
                    "declare_exchange": {
                        "type": "boolean",
                        "description": "Declare the exchange if it does not already exist (defaults to false)."
                    },
                    "durable": {
                        "type": "boolean",
                        "description": "Mark the declared exchange as durable (defaults to true)."
                    },
                    "auto_delete": {
                        "type": "boolean",
                        "description": "Auto-delete the declared exchange when unused (defaults to false)."
                    },
                    "persistent": {
                        "type": "boolean",
                        "description": "Set AMQP delivery_mode to persistent (2) when true or transient (1) when false."
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Enable AMQP publisher confirms and fail writes on broker NACK (defaults to false)."
                    },
                    "priority": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 255,
                        "description": "AMQP message priority (0-255)."
                    },
                    "expiration": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Per-message TTL in milliseconds."
                    },
                    "content_type": {
                        "type": "string",
                        "description": "MIME content type header (e.g. application/json)."
                    },
                    "content_encoding": {
                        "type": "string",
                        "description": "MIME content encoding header (e.g. utf-8)."
                    },
                    "value_field": {
                        "type": "string",
                        "description": "Binary or string record column used as the message payload; when unset, codec encoding applies."
                    }
                },
                "required": ["url", "exchange", "routing_key"]
            }),
        )
        .with_example(serde_json::json!({
            "url": "amqp://guest:guest@localhost:5672/%2f",
            "exchange": "events",
            "routing_key": {"type": "value", "value": "events.created"},
            "declare_exchange": true,
            "confirm": true
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
    fn test_config_deserialization_and_basic_properties() {
        let json = serde_json::json!({
            "url": "amqp://guest:guest@localhost:5672/%2f",
            "exchange": "orders",
            "exchange_type": "topic",
            "routing_key": {
                "type": "value",
                "value": "orders.created"
            },
            "declare_exchange": true,
            "durable": true,
            "auto_delete": false,
            "persistent": true,
            "confirm": true,
            "priority": 5,
            "expiration": 60000,
            "content_type": "application/json",
            "content_encoding": "utf-8",
            "value_field": "payload"
        });

        let cfg: Amqp09OutputConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.exchange_type, Some(Amqp09ExchangeType::Topic));
        assert_eq!(cfg.expiration, Some(60000));

        let props = build_basic_properties(&cfg);
        assert_eq!(props.delivery_mode(), &Some(2));
        assert_eq!(props.priority(), &Some(5));
        assert_eq!(
            props.expiration().as_ref().map(|s| s.as_str()),
            Some("60000")
        );
        assert_eq!(
            props.content_type().as_ref().map(|s| s.as_str()),
            Some("application/json")
        );
        assert_eq!(
            props.content_encoding().as_ref().map(|s| s.as_str()),
            Some("utf-8")
        );
    }

    #[test]
    fn test_transient_delivery_mode_property() {
        let cfg = Amqp09OutputConfig {
            url: "amqp://localhost:5672".into(),
            exchange: "".into(),
            exchange_type: None,
            routing_key: Expr::Value { value: "q".into() },
            declare_exchange: None,
            durable: None,
            auto_delete: None,
            persistent: Some(false),
            confirm: None,
            priority: None,
            expiration: None,
            content_type: None,
            content_encoding: None,
            value_field: None,
        };
        let props = build_basic_properties(&cfg);
        assert_eq!(props.delivery_mode(), &Some(1));
        assert!(props.priority().is_none());
        assert!(props.expiration().is_none());
    }

    #[test]
    fn test_builder_validation_errors() {
        let builder = Amqp09OutputBuilder;
        let resource = create_test_resource();

        // Missing config
        assert!(matches!(
            builder.build(None, &None, None, &resource),
            Err(Error::Config(_))
        ));

        // Empty URL
        let empty_url = Some(serde_json::json!({
            "url": "",
            "exchange": "ex",
            "routing_key": {"type": "value", "value": "rk"}
        }));
        assert!(matches!(
            builder.build(None, &empty_url, None, &resource),
            Err(Error::Config(_))
        ));

        // declare_exchange=true on empty default exchange is invalid AMQP
        let declare_default = Some(serde_json::json!({
            "url": "amqp://localhost:5672",
            "exchange": "",
            "routing_key": {"type": "value", "value": "q"},
            "declare_exchange": true
        }));
        assert!(matches!(
            builder.build(None, &declare_default, None, &resource),
            Err(Error::Config(_))
        ));

        // Publishing to empty default exchange with declare_exchange=false is valid
        let publish_default = Some(serde_json::json!({
            "url": "amqp://localhost:5672",
            "exchange": "",
            "routing_key": {"type": "value", "value": "q"},
            "declare_exchange": false
        }));
        assert!(builder
            .build(None, &publish_default, None, &resource)
            .is_ok());
    }

    #[test]
    fn test_routing_key_alignment_rejects_short_vector() {
        let keys = EvaluateResult::Vec(vec!["k1".into(), "k2".into()]);
        let err = check_routing_key_alignment(&keys, 3).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("routing_key count (2)"), "{msg}");
        assert!(msg.contains("payload count (3)"), "{msg}");
    }

    #[test]
    fn test_routing_key_alignment_accepts_aligned_and_scalar() {
        let keys = EvaluateResult::Vec(vec!["k1".into(), "k2".into()]);
        check_routing_key_alignment(&keys, 2).unwrap();
        let scalar = EvaluateResult::Scalar("k".into());
        check_routing_key_alignment(&scalar, 5).unwrap();
    }

    #[tokio::test]
    async fn test_write_when_not_connected_returns_connection_error() {
        let cfg = Amqp09OutputConfig {
            url: "amqp://localhost:5672".into(),
            exchange: "".into(),
            exchange_type: None,
            routing_key: Expr::Value { value: "q".into() },
            declare_exchange: None,
            durable: None,
            auto_delete: None,
            persistent: None,
            confirm: None,
            priority: None,
            expiration: None,
            content_type: None,
            content_encoding: None,
            value_field: None,
        };
        let output = Amqp09Output::new(cfg, None).unwrap();
        let batch = Arc::new(arkflow_core::MessageBatch::new_binary(vec![b"hi".to_vec()]).unwrap());
        let err = output.write(batch).await.unwrap_err();
        assert!(matches!(err, Error::Connection(_)), "got {err:?}");
    }
}
