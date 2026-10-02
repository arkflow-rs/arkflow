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

//! Redis input component
//!
//! Receive data from Redis pub/sub channels

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use crate::input::codec_helper::Delivery;
use arkflow_core::{Error, MessageBatchRef, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client, FromRedisValue, PushInfo, PushKind, RedisResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Redis input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisInputConfig {
    mode: ModeConfig,
    redis_type: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ModeConfig {
    Cluster { urls: Vec<String> },
    Single { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Subscribe {
    /// List of channels to subscribe to
    Channels { channels: Vec<String> },
    /// List of patterns to subscribe to
    Patterns { patterns: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Type {
    Subscribe { subscribe: Subscribe },
    List { list: Vec<String> },
}

/// Redis input component
struct RedisInput {
    input_name: Option<String>,
    config: RedisInputConfig,
    client: Arc<Mutex<Option<Cli>>>,
    sender: Sender<Delivery>,
    receiver: Receiver<Delivery>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

enum Cli {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

// The channel carries finished `Delivery` values: producers decode and
// pair the (noop) acknowledgement before claiming a slot, so `read()` is a
// single await point and a dropped read future loses nothing
// (Input::read cancellation-safety contract).
/// Check if a Redis error is temporary (should retry) or permanent (should not retry)
///
/// Temporary errors include: connection issues, timeouts, I/O errors
/// Permanent errors include: authentication failures, permission errors, invalid configuration
fn is_temporary_redis_error<E: std::fmt::Display>(err: &E) -> bool {
    let err_str = err.to_string().to_lowercase();

    // Permanent errors - these should not trigger infinite reconnection
    if err_str.contains("noauth")
        || err_str.contains("wrongpass")
        || err_str.contains("noperm")
        || err_str.contains("not allowed")
        || err_str.contains("unknown command")
        || err_str.contains("invalid")
    {
        return false;
    }

    // Most other errors (connection issues, timeouts, etc.) are temporary
    true
}

/// Forwards cluster pub/sub pushes into the input channel. redis 1.x takes
/// push delivery through the `AsyncPushSender` trait instead of a closure;
/// `SendError` carries no payload (it only signals connection loss), so a
/// push message that fails to parse is logged and skipped rather than
/// propagated.
struct ClusterPushForwarder {
    sender: Sender<Delivery>,
    codec: Option<Arc<dyn Codec>>,
    input_name: Option<String>,
}

impl redis::aio::AsyncPushSender for ClusterPushForwarder {
    fn send(&self, msg: PushInfo) -> Result<(), redis::aio::SendError> {
        match msg.kind {
            PushKind::Message | PushKind::PMessage | PushKind::SMessage => {
                if msg.data.len() < 2 {
                    return Ok(());
                }
                let mut iter = msg.data.into_iter();
                let _channel = match iter.next() {
                    Some(v) => match String::from_redis_value(v) {
                        Ok(channel) => channel,
                        Err(error) => {
                            error!("redis cluster push channel failed to parse: {}", error);
                            return Ok(());
                        }
                    },
                    None => return Ok(()),
                };
                let message: Vec<u8> = match iter.next() {
                    Some(v) => match Vec::from_redis_value(v) {
                        Ok(message) => message,
                        Err(error) => {
                            error!("redis cluster push message failed to parse: {}", error);
                            return Ok(());
                        }
                    },
                    None => return Ok(()),
                };

                // The push callback is sync; decode off it so the
                // delivery entering the channel is already
                // finished (cancellation-safe read contract).
                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => {
                        let sender_cb = Sender::clone(&self.sender);
                        let codec_cb = self.codec.clone();
                        let input_name_cb = self.input_name.clone();
                        handle.spawn(async move {
                            let delivery = crate::input::codec_helper::decode_delivery(
                                &message,
                                &codec_cb,
                                input_name_cb,
                                Arc::new(NoopAck),
                            )
                            .await;
                            if let Err(e) = sender_cb.send_async(delivery).await {
                                error!("{}", e);
                            }
                        });
                    }
                    Err(e) => error!("no async runtime for redis push decode: {}", e),
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl RedisInput {
    /// Create a new Redis input component
    fn new(
        name: Option<&String>,
        config: RedisInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<Delivery>(1000);
        let cancellation_token = CancellationToken::new();
        match &config.mode {
            ModeConfig::Cluster { urls, .. } => {
                for url in urls {
                    if redis::parse_redis_url(url).is_none() {
                        return Err(Error::Config(format!("Invalid Redis URL: {}", url)));
                    }
                }
            }
            ModeConfig::Single { url, .. } => {
                if redis::parse_redis_url(url).is_none() {
                    return Err(Error::Config(format!("Invalid Redis URL: {}", url)));
                }
            }
        };

        Ok(Self {
            input_name: name.cloned(),
            config,
            client: Arc::new(Mutex::new(None)),
            sender,
            receiver,
            cancellation_token,
            codec,
        })
    }

    async fn cluster_connect(&self, urls: Vec<String>) -> Result<(), Error> {
        let mut cli_guard = self.client.lock().await;

        let cancellation_token = self.cancellation_token.clone();

        let config_type = self.config.redis_type.clone();

        let client_builder = ClusterClientBuilder::new(urls);

        let client_builder = match config_type {
            // Cluster pub/sub delivery requires RESP3 in redis 1.x: push
            // messages arrive as RESP3 push frames, and SUBSCRIBE on a
            // RESP2 cluster connection is rejected outright
            // ("RESP3 is required for this command").
            Type::Subscribe { .. } => client_builder
                .use_protocol(redis::ProtocolVersion::RESP3)
                .push_sender(ClusterPushForwarder {
                    sender: Sender::clone(&self.sender),
                    codec: self.codec.clone(),
                    input_name: self.input_name.clone(),
                }),
            Type::List { .. } => client_builder,
        };

        let cluster_client = client_builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        let mut cluster_conn = cluster_client
            .get_async_connection()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        match config_type {
            Type::Subscribe { subscribe } => {
                match subscribe {
                    Subscribe::Channels { channels } => {
                        // Subscribe to channels
                        for channel in channels {
                            if let Err(e) = cluster_conn.subscribe(&channel).await {
                                error!("Failed to subscribe to Redis channel {}: {}", channel, e);
                                if is_temporary_redis_error(&e) {
                                    return Err(Error::Disconnection);
                                } else {
                                    return Err(Error::Connection(format!(
                                        "Redis subscription failed for channel '{}': {}",
                                        channel, e
                                    )));
                                }
                            }
                        }
                    }
                    Subscribe::Patterns { patterns } => {
                        // Subscribe to patterns
                        for pattern in patterns {
                            if let Err(e) = cluster_conn.psubscribe(&pattern).await {
                                error!("Failed to subscribe to Redis pattern {}: {}", pattern, e);
                                if is_temporary_redis_error(&e) {
                                    return Err(Error::Disconnection);
                                } else {
                                    return Err(Error::Connection(format!(
                                        "Redis subscription failed for pattern '{}': {}",
                                        pattern, e
                                    )));
                                }
                            }
                        }
                    }
                }
            }
            Type::List { list } => {
                let sender_clone = Sender::clone(&self.sender);
                let codec_clone = self.codec.clone();
                let input_name_clone = self.input_name.clone();
                let mut cluster_connection = cluster_conn.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                            result = async {
                                let blpop_result: RedisResult<Option<(String, Vec<u8>)>> = cluster_connection.blpop(&list, 1f64).await;
                                blpop_result
                            } => {
                                match result {
                                    Ok(Some((list_name, payload))) => {
                                        debug!("Received Redis list message from {},payload: {}", list_name,  String::from_utf8_lossy(&payload));
                                        let delivery = crate::input::codec_helper::decode_delivery(
                                            &payload,
                                            &codec_clone,
                                            input_name_clone.clone(),
                                            Arc::new(NoopAck),
                                        )
                                        .await;
                                        if let Err(e) = sender_clone.send_async(delivery).await {
                                            error!("Failed to send Redis list message: {}", e);
                                        }
                                    }
                                    Ok(None) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        error!("Error retrieving from Redis list: {}", e);
                                        if let Err(e) = sender_clone.send_async(Delivery::Err(Error::Disconnection)).await {
                                            error!("{}", e);
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
        cli_guard.replace(Cli::Cluster(cluster_conn));
        Ok(())
    }

    async fn single_connect(&self, url: String) -> Result<(), Error> {
        let mut cli_guard = self.client.lock().await;
        let client = Client::open(url)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;
        let manager = ConnectionManager::new(client.clone())
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;

        let sender_clone = Sender::clone(&self.sender);
        let cancellation_token = self.cancellation_token.clone();

        let config_type = self.config.redis_type.clone();

        match config_type {
            Type::Subscribe { subscribe } => {
                let mut pubsub_conn = client.get_async_pubsub().await.map_err(|e| {
                    Error::Connection(format!("Failed to get Redis connection: {}", e))
                })?;

                match subscribe {
                    Subscribe::Channels { channels } => {
                        // Subscribe to channels
                        for channel in channels {
                            if let Err(e) = pubsub_conn.subscribe(&channel).await {
                                error!("Failed to subscribe to Redis channel {}: {}", channel, e);
                                if is_temporary_redis_error(&e) {
                                    return Err(Error::Disconnection);
                                } else {
                                    return Err(Error::Connection(format!(
                                        "Redis subscription failed for channel '{}': {}",
                                        channel, e
                                    )));
                                }
                            }
                        }
                    }
                    Subscribe::Patterns { patterns } => {
                        // Subscribe to patterns
                        for pattern in patterns {
                            if let Err(e) = pubsub_conn.psubscribe(&pattern).await {
                                error!("Failed to subscribe to Redis pattern {}: {}", pattern, e);
                                if is_temporary_redis_error(&e) {
                                    return Err(Error::Disconnection);
                                } else {
                                    return Err(Error::Connection(format!(
                                        "Redis subscription failed for pattern '{}': {}",
                                        pattern, e
                                    )));
                                }
                            }
                        }
                    }
                }
                let codec_clone = self.codec.clone();
                let input_name_clone = self.input_name.clone();
                tokio::spawn(async move {
                    let mut msg_stream = pubsub_conn.on_message();

                    loop {
                        tokio::select! {
                            Some(msg_result) = msg_stream.next() => {
                                let _channel: String = msg_result.get_channel_name().to_string();
                                let Ok(payload) = msg_result.get_payload::<Vec<u8>>() else {
                                       continue;
                                };
                                let delivery = crate::input::codec_helper::decode_delivery(
                                    &payload,
                                    &codec_clone,
                                    input_name_clone.clone(),
                                    Arc::new(NoopAck),
                                )
                                .await;
                                if let Err(e) = sender_clone.send_async(delivery).await {
                                    error!("{}", e);
                                }
                            }
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }
                    }
                });
            }
            Type::List { ref list } => {
                let list = list.clone();
                let mut manager = manager.clone();
                let sender_clone = Sender::clone(&self.sender);
                let codec_clone = self.codec.clone();
                let input_name_clone = self.input_name.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                            result = async {
                                let blpop_result: RedisResult<Option<(String, Vec<u8>)>> = manager.blpop(&list, 1f64).await;
                                blpop_result
                            } => {
                                match result {
                                    Ok(Some((list_name, payload))) => {
                                        debug!("Received Redis list message from {},payload: {}", list_name,  String::from_utf8_lossy(&payload));
                                        let delivery = crate::input::codec_helper::decode_delivery(
                                            &payload,
                                            &codec_clone,
                                            input_name_clone.clone(),
                                            Arc::new(NoopAck),
                                        )
                                        .await;
                                        if let Err(e) = sender_clone.send_async(delivery).await {
                                            error!("Failed to send Redis list message: {}", e);
                                        }
                                    }
                                    Ok(None) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        error!("Error retrieving from Redis list: {}", e);
                                        if let Err(e) = sender_clone.send_async(Delivery::Err(Error::Disconnection)).await {
                                            error!("{}", e);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });
            }
        };

        cli_guard.replace(Cli::Single(manager));

        Ok(())
    }
}

#[async_trait]
impl Input for RedisInput {
    async fn connect(&self) -> Result<(), Error> {
        match &self.config.mode {
            ModeConfig::Cluster { urls } => self.cluster_connect(urls.to_vec()).await,
            ModeConfig::Single { url } => self.single_connect(url.clone()).await,
        }
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        {
            let client_arc = Arc::clone(&self.client);
            if client_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        // Producers decode and pair the ack before claiming a slot, so this
        // recv is the single await point — a dropped read loses nothing.
        match self.receiver.recv_async().await {
            Ok(Delivery::Data(batch, ack)) => Ok((batch, ack)),
            Ok(Delivery::Err(e)) => Err(e),
            Err(_) => Err(Error::EOF),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        if let Some(cli) = self.client.lock().await.take() {
            match cli {
                Cli::Single(mut c) => {
                    if let Type::Subscribe { ref subscribe } = self.config.redis_type {
                        match subscribe {
                            Subscribe::Channels { channels } => {
                                match c.unsubscribe(channels).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Failed to unsubscribe from Redis channel: {}", e);
                                    }
                                };
                            }
                            Subscribe::Patterns { patterns } => {
                                match c.punsubscribe(patterns).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Failed to unsubscribe from Redis pattern: {}", e);
                                    }
                                };
                            }
                        }
                    }
                }
                Cli::Cluster(mut c) => {
                    if let Type::Subscribe { ref subscribe } = self.config.redis_type {
                        match subscribe {
                            Subscribe::Channels { channels } => {
                                match c.unsubscribe(channels).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Failed to unsubscribe from Redis channel: {}", e);
                                    }
                                };
                            }
                            Subscribe::Patterns { patterns } => {
                                match c.punsubscribe(patterns).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Failed to unsubscribe from Redis pattern: {}", e);
                                    }
                                };
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Redis input builder
pub struct RedisInputBuilder;

impl InputBuilder for RedisInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: RedisInputConfig =
            serde_json::from_value(config.clone().unwrap_or_default())
                .map_err(|e| Error::Config(format!("Invalid Redis input config: {}", e)))?;
        Ok(Arc::new(RedisInput::new(name, config, codec)?))
    }
}

/// Initialize Redis input component
pub fn init() -> Result<(), Error> {
    register_input_builder("redis", Arc::new(RedisInputBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "redis",
        "Reads from Redis: list blocking pops, pub/sub subscriptions, or stream consumer groups.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "mode": {
                    "type": "object",
                    "description": "Connection mode.",
                    "oneOf": [
                        {"properties": {"type": {"const": "cluster"}, "urls": {"type": "array", "items": {"type": "string"}}}, "required": ["type", "urls"]},
                        {"properties": {"type": {"const": "single"}, "url": {"type": "string"}}, "required": ["type", "url"]}
                    ]
                },
                "redis_type": {
                    "type": "object",
                    "description": "Data structure to consume from.",
                    "oneOf": [
                        {"properties": {"type": {"const": "subscribe"}, "subscribe": {"type": "object", "oneOf": [
                            {"properties": {"type": {"const": "channels"}, "channels": {"type": "array", "items": {"type": "string"}}}, "required": ["type", "channels"]},
                            {"properties": {"type": {"const": "patterns"}, "patterns": {"type": "array", "items": {"type": "string"}}}, "required": ["type", "patterns"]}
                        ]}}, "required": ["type", "subscribe"]},
                        {"properties": {"type": {"const": "list"}, "list": {"type": "array", "items": {"type": "string"}}}, "required": ["type", "list"]}
                    ]
                }
            },
            "required": ["mode", "redis_type"]
        }),
    ).with_example(serde_json::json!({
        "mode": {"type": "single", "url": "redis://localhost:6379"},
        "redis_type": {"type": "list", "list": ["events"]}
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::aio::AsyncPushSender;
    use redis::Value;

    #[tokio::test]
    async fn cluster_push_forwarder_delivers_message_payload() {
        let (sender, receiver) = flume::unbounded();
        let forwarder = ClusterPushForwarder {
            sender,
            codec: None,
            input_name: Some("redis-in".into()),
        };

        assert!(
            forwarder
                .send(PushInfo {
                    kind: PushKind::Message,
                    data: vec![
                        Value::BulkString(b"events".to_vec()),
                        Value::BulkString(b"payload".to_vec()),
                    ],
                })
                .is_ok(),
            "push handling never signals connection loss"
        );

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv_async())
            .await
            .expect("delivery arrives")
            .expect("channel stays open");
        let Delivery::Data(batch, _ack) = delivery else {
            panic!("no-codec push decode cannot fail");
        };
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get_input_name(), Some("redis-in".to_string()));
    }

    #[test]
    fn cluster_push_forwarder_skips_malformed_push_data() {
        // A push message with fewer than two parts, or parts that fail to
        // parse (Nil is not string-convertible), is skipped without
        // signalling connection loss: SendError must only mean "connection
        // gone", so a malformed push must never drop the cluster connection.
        let (sender, receiver) = flume::unbounded();
        let forwarder = ClusterPushForwarder {
            sender,
            codec: None,
            input_name: None,
        };

        let malformed = [
            PushInfo {
                kind: PushKind::Message,
                data: vec![Value::BulkString(b"events".to_vec())],
            },
            PushInfo {
                kind: PushKind::Message,
                data: vec![Value::Nil, Value::BulkString(b"payload".to_vec())],
            },
            PushInfo {
                kind: PushKind::Message,
                data: vec![Value::BulkString(b"events".to_vec()), Value::Nil],
            },
        ];
        for push in malformed {
            assert!(
                forwarder.send(push).is_ok(),
                "malformed push is skipped, not a connection error"
            );
        }
        assert!(receiver.try_recv().is_err(), "nothing is delivered");
    }

    #[test]
    fn cluster_push_forwarder_ignores_non_message_kinds() {
        let (sender, receiver) = flume::unbounded();
        let forwarder = ClusterPushForwarder {
            sender,
            codec: None,
            input_name: None,
        };

        for kind in [PushKind::PUnsubscribe, PushKind::SUnsubscribe] {
            assert!(
                forwarder
                    .send(PushInfo {
                        kind,
                        data: vec![
                            Value::BulkString(b"events".to_vec()),
                            Value::BulkString(b"ignored".to_vec()),
                        ],
                    })
                    .is_ok(),
                "non-message kinds are a no-op"
            );
        }
        assert!(receiver.try_recv().is_err(), "nothing is delivered");
    }

    #[test]
    fn temporary_error_classification() {
        // Permanent: auth/permission/unknown-command/invalid text.
        for text in [
            "NOAUTH Authentication required",
            "WRONGPASS invalid username-password pair",
            "NOPERM this user has no permissions",
            "User is not allowed",
            "unknown command 'BLPOPP'",
            "invalid port specifier",
        ] {
            assert!(
                !is_temporary_redis_error(&text),
                "should be permanent: {text}"
            );
        }
        // Temporary: connection/timeout/io wording falls through.
        for text in [
            "connection reset by peer",
            "IO timeout while reading",
            "cluster is down",
        ] {
            assert!(
                is_temporary_redis_error(&text),
                "should be temporary: {text}"
            );
        }
    }

    fn builder() -> RedisInputBuilder {
        RedisInputBuilder
    }

    fn config(value: serde_json::Value) -> Option<serde_json::Value> {
        Some(value)
    }

    #[test]
    fn builder_rejects_malformed_config() {
        let Err(err) = builder().build(
            None,
            &config(serde_json::json!({"unexpected": true})),
            None,
            &test_resource(),
        ) else {
            panic!("malformed config must be rejected");
        };
        assert!(err.to_string().contains("Invalid Redis input config"));
    }

    #[test]
    fn builder_rejects_invalid_single_url() {
        let Err(err) = builder().build(
            None,
            &config(serde_json::json!({
                "mode": {"type": "single", "url": "not-a-redis-url"},
                "redis_type": {"type": "list", "list": ["k"]}
            })),
            None,
            &test_resource(),
        ) else {
            panic!("invalid single url must be rejected");
        };
        assert!(err.to_string().contains("Invalid Redis URL"));
    }

    #[test]
    fn builder_rejects_invalid_cluster_url() {
        let Err(err) = builder().build(
            None,
            &config(serde_json::json!({
                "mode": {"type": "cluster", "urls": ["redis://ok:6379", "bad-url"]},
                "redis_type": {"type": "list", "list": ["k"]}
            })),
            None,
            &test_resource(),
        ) else {
            panic!("invalid cluster url must be rejected");
        };
        assert!(err.to_string().contains("Invalid Redis URL"));
    }

    #[test]
    fn read_before_connect_reports_disconnection() {
        let input = RedisInput::new(
            None,
            serde_json::from_value(serde_json::json!({
                "mode": {"type": "single", "url": "redis://127.0.0.1:6379"},
                "redis_type": {"type": "list", "list": ["k"]}
            }))
            .unwrap(),
            None,
        )
        .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(matches!(rt.block_on(input.read()), Err(Error::Disconnection)));
    }

    #[test]
    fn close_before_connect_is_a_clean_noop() {
        let input = RedisInput::new(
            None,
            serde_json::from_value(serde_json::json!({
                "mode": {"type": "single", "url": "redis://127.0.0.1:6379"},
                "redis_type": {"type": "subscribe", "subscribe": {"type": "channels", "channels": ["c"]}}
            }))
            .unwrap(),
            None,
        )
        .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(input.close()).unwrap();
        // After close, read still reports the disconnected state.
        assert!(matches!(rt.block_on(input.read()), Err(Error::Disconnection)));
    }

    fn test_resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    // ===== Offline connection-error paths (no Redis server required) =====

    fn input_from(value: serde_json::Value) -> RedisInput {
        RedisInput::new(
            None,
            serde_json::from_value(value).unwrap(),
            None,
        )
        .unwrap()
    }

    /// The builder accepts both well-formed mode shapes (validation of the
    /// URL *format* happens in `new`; reachability is only checked at
    /// `connect`).
    #[test]
    fn builder_accepts_well_formed_cluster_and_single_configs() {
        let ok = builder().build(
            None,
            &config(serde_json::json!({
                "mode": {"type": "cluster", "urls": ["redis://127.0.0.1:7001", "redis://127.0.0.1:7002"]},
                "redis_type": {"type": "subscribe", "subscribe": {"type": "patterns", "patterns": ["news.*"]}}
            })),
            None,
            &test_resource(),
        );
        assert!(ok.is_ok(), "well-formed cluster config must build");

        let ok = builder().build(
            None,
            &config(serde_json::json!({
                "mode": {"type": "single", "url": "redis://127.0.0.1:6379"},
                "redis_type": {"type": "list", "list": ["work"]}
            })),
            None,
            &test_resource(),
        );
        assert!(ok.is_ok(), "well-formed single config must build");
    }

    /// Cluster mode: an empty URL list fails at `ClusterClientBuilder`
    /// construction (offline — no connection attempted).
    #[tokio::test]
    async fn cluster_connect_with_no_urls_fails_at_client_build() {
        let input = input_from(serde_json::json!({
            "mode": {"type": "cluster", "urls": []},
            "redis_type": {"type": "list", "list": ["k"]}
        }));
        let err = input.connect().await.expect_err("no cluster nodes");
        assert!(
            err.to_string().contains("Failed to connect to Redis cluster"),
            "got: {err}"
        );
    }

    /// Cluster mode with a subscribable type still registers the push
    /// sender before attempting the connection; an unreachable seed node
    /// (port 1 → refused) surfaces as a connection error.
    #[tokio::test]
    async fn cluster_connect_with_unreachable_urls_fails_at_connection() {
        let input = input_from(serde_json::json!({
            "mode": {"type": "cluster", "urls": ["redis://127.0.0.1:1/", "redis://127.0.0.1:2/"]},
            "redis_type": {"type": "subscribe", "subscribe": {"type": "channels", "channels": ["c"]}}
        }));
        let err = match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            input.connect(),
        )
        .await
        {
            Ok(Err(e)) => e,
            Ok(Ok(())) => panic!("unreachable cluster seeds must not connect"),
            Err(_) => panic!("cluster connect must fail fast on refused ports"),
        };
        assert!(
            err.to_string().contains("Failed to connect to Redis cluster"),
            "got: {err}"
        );
        // A failed connect leaves the client slot empty: read reports the
        // disconnected state rather than parking on the channel.
        assert!(matches!(input.read().await, Err(Error::Disconnection)));
        // close() on the never-connected input stays a clean no-op.
        input.close().await.unwrap();
    }

    /// Single mode: a URL whose path is not a valid database number passes
    /// the builder's scheme-level validation but fails `Client::open`
    /// offline, instantly and deterministically (no server involved).
    #[tokio::test]
    async fn single_connect_with_invalid_db_fails_at_client_open() {
        let input = input_from(serde_json::json!({
            "mode": {"type": "single", "url": "redis://127.0.0.1:6379/not-a-number"},
            "redis_type": {"type": "subscribe", "subscribe": {"type": "channels", "channels": ["c"]}}
        }));
        let err = input.connect().await.expect_err("invalid db path");
        assert!(
            err.to_string().contains("Failed to connect to Redis server"),
            "got: {err}"
        );
        // The failed connect leaves the client slot empty.
        assert!(matches!(input.read().await, Err(Error::Disconnection)));
        input.close().await.unwrap();
    }
}
