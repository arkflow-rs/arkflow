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

//! MQTT output component
//!
//! Send the processed data to the MQTT broker

use crate::expr::Expr;
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    codec::Codec,
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatchRef, Resource,
};
use async_trait::async_trait;
use rumqttc::{AsyncClient, ClientError, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// MQTT output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MqttOutputConfig {
    /// MQTT broker address
    host: String,
    /// MQTT broker port
    port: u16,
    /// Client ID
    client_id: String,
    /// Username (optional)
    username: Option<String>,
    /// Password (optional)
    password: Option<String>,
    /// Published topics
    topic: Expr<String>,
    /// Quality of Service (0, 1, 2)
    qos: Option<u8>,
    /// TLS transport configuration
    #[serde(default)]
    tls: Option<crate::mqtt_tls::MqttTlsConfig>,
    /// Whether to use clean session
    clean_session: Option<bool>,
    /// Keep alive interval (seconds)
    keep_alive: Option<u64>,
    /// Whether to retain the message
    retain: Option<bool>,
    /// Value type
    value_field: Option<String>,
}

/// Bounded write-path reconnect: attempts with exponential backoff before
/// giving up and surfacing the failure to the output chain.
const RECONNECT_ATTEMPTS: u32 = 3;

/// Upper bound for the broker handshake (ConnAck) during connect. A fresh
/// rumqttc client opens no network connection until the eventloop polls,
/// so "created" must not be reported as "connected": without this gate the
/// write-path reconnect succeeds instantly against a dead broker and the
/// 1s/2s/4s backoff can never trigger in production.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// MQTT output component
struct MqttOutput<T: MqttClient> {
    config: MqttOutputConfig,
    client: Arc<Mutex<Option<T>>>,
    connected: Arc<AtomicBool>,
    /// Terminal state set by `close()`: the write path must not resurrect a
    /// closed output through its lazy reconnect.
    closed: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    codec: Option<Arc<dyn Codec>>,
}

impl<T: MqttClient> MqttOutput<T> {
    /// Create a new MQTT output component
    fn new(config: MqttOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            closed: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
            codec,
        })
    }

    /// Build a fresh client/eventloop pair, tearing down any previous
    /// generation first (abort the old eventloop task, best-effort
    /// disconnect of the old client) so repeated connects do not leak tasks
    /// or connections. Both locks are held across the swap to serialize
    /// concurrent connects into a single winner.
    async fn establish_connection(&self) -> Result<(), Error> {
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            mqtt_options.set_credentials(username, password);
        }
        if let Some(tls) = &self.config.tls {
            tls.apply(&mut mqtt_options).await?;
        }
        if let Some(keep_alive) = self.config.keep_alive {
            mqtt_options.set_keep_alive(std::time::Duration::from_secs(keep_alive));
        }
        if let Some(clean_session) = self.config.clean_session {
            mqtt_options.set_clean_session(clean_session);
        }

        let (client, mut eventloop) = T::create(mqtt_options, 10).await?;

        // A fresh client is not connected: the broker handshake happens on
        // the first eventloop poll. Wait for ConnAck (bounded) before the
        // connection is declared established — an unreachable broker fails
        // HERE, so `reconnect`'s backoff and exhaustion are real.
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, eventloop.poll()).await {
            Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)))) => {}
            Ok(Ok(other)) => {
                // rumqttc always yields ConnAck first; any other first
                // event means the protocol stream is not what we expect.
                return Err(Error::Connection(format!(
                    "MQTT broker sent an unexpected first event: {other:?}"
                )));
            }
            Ok(Err(e)) => {
                return Err(Error::Connection(format!(
                    "MQTT broker handshake failed: {e}"
                )));
            }
            Err(_) => {
                return Err(Error::Connection(format!(
                    "MQTT broker handshake timed out after {HANDSHAKE_TIMEOUT:?}"
                )));
            }
        }

        let mut client_guard = self.client.lock().await;
        let mut eventloop_handle_guard = self.eventloop_handle.lock().await;
        if let Some(old_handle) = eventloop_handle_guard.take() {
            old_handle.abort();
        }
        if let Some(old_client) = client_guard.take() {
            let _ = old_client.disconnect().await;
        }

        // The eventloop keeps the connection alive; when it exits the
        // connection is gone, so the flag must drop immediately (not wait
        // for close()).
        let connected = self.connected.clone();
        let eventloop_handle = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("MQTT output event loop error: {}", e);
                        connected.store(false, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        *client_guard = Some(client);
        *eventloop_handle_guard = Some(eventloop_handle);
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Bounded lazy reconnect used by the write path: 3 attempts with
    /// exponential backoff (1s, 2s, 4s), then the error is surfaced.
    async fn reconnect(&self) -> Result<(), Error> {
        let mut delay = std::time::Duration::from_secs(1);
        for attempt in 1..=RECONNECT_ATTEMPTS {
            match self.establish_connection().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt == RECONNECT_ATTEMPTS {
                        return Err(Error::Connection(format!(
                            "MQTT output reconnect failed after {} attempts: last error: {}",
                            RECONNECT_ATTEMPTS, e
                        )));
                    }
                    warn!(
                        "MQTT output reconnect attempt {}/{} failed: {}; retrying in {:?}",
                        attempt, RECONNECT_ATTEMPTS, e, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
        unreachable!("reconnect loop always returns inside the loop")
    }
}

/// Publish every payload; the first error aborts the loop.
async fn publish_all<T: MqttClient>(
    client: &T,
    topic: &crate::expr::EvaluateResult<String>,
    payloads: &[Vec<u8>],
    qos_level: QoS,
    retain: bool,
) -> Result<(), ClientError> {
    for (i, payload) in payloads.iter().enumerate() {
        if let Some(topic_str) = topic.get(i) {
            client
                .publish(topic_str.clone(), qos_level, retain, payload.clone())
                .await?;
        }
    }
    Ok(())
}

#[async_trait]
impl<T: MqttClient> Output for MqttOutput<T> {
    async fn connect(&self) -> Result<(), Error> {
        self.closed.store(false, Ordering::SeqCst);
        self.establish_connection().await
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Connection("The output is closed".to_string()));
        }
        if !self.connected.load(Ordering::SeqCst) {
            // Lazy reconnect: a dead eventloop is not fatal until the
            // bounded reconnect round exhausts.
            self.reconnect().await?;
        }

        // Payload selection: an explicit `value_field` takes the named
        // column's value per row, otherwise the codec encoding applies.
        let payloads: Vec<Vec<u8>> = if let Some(field) = &self.config.value_field {
            crate::output::payload::field_payloads("mqtt", &msg, field)?
        } else {
            crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)
                .await?
                .into_iter()
                .map(|p| p.to_vec())
                .collect()
        };
        if payloads.is_empty() {
            return Ok(());
        }

        let topic = self.config.topic.evaluate_expr(&msg).await?;

        // Determine the QoS level
        let qos_level = match self.config.qos {
            Some(0) => QoS::AtMostOnce,
            Some(1) => QoS::AtLeastOnce,
            Some(2) => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce, // The default is QoS 1
        };

        // Decide whether to keep the message
        let retain = self.config.retain.unwrap_or(false);

        let first_attempt = {
            let client_guard = self.client.lock().await;
            let client = client_guard.as_ref().ok_or_else(|| {
                Error::Connection("The MQTT client is not initialized".to_string())
            })?;
            publish_all(client, &topic, &payloads, qos_level, retain).await
        };

        if first_attempt.is_err() {
            // Publish failures are usually a dead eventloop: one bounded
            // reconnect round, then retry the whole payload list. Messages
            // published before the failure may be duplicated (at-least-once),
            // which is the output delivery contract.
            warn!("MQTT publish failed; attempting one bounded reconnect + retry");
            self.reconnect().await?;
            let client_guard = self.client.lock().await;
            let client = client_guard.as_ref().ok_or_else(|| {
                Error::Connection("The MQTT client is not initialized".to_string())
            })?;
            publish_all(client, &topic, &payloads, qos_level, retain)
                .await
                .map_err(|e| {
                    Error::Connection(format!("MQTT publishing failed after reconnect: {}", e))
                })?;
        } else {
            for payload in &payloads {
                info!("Send message: {}", &String::from_utf8_lossy(payload));
            }
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Stop the event loop processing thread
        let mut eventloop_handle_guard = self.eventloop_handle.lock().await;
        if let Some(handle) = eventloop_handle_guard.take() {
            handle.abort();
        }

        // Disconnect the MQTT connection
        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        if let Some(client) = &*client_guard {
            // Try to disconnect, but don't wait for the result
            let _ = client.disconnect().await;
        }

        self.connected.store(false, Ordering::SeqCst);
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct MqttOutputBuilder;
impl OutputBuilder for MqttOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: MqttOutputConfig = parse_config(config, "MqttOutput input")?;
        Ok(Arc::new(MqttOutput::<AsyncClient>::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("mqtt", Arc::new(MqttOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "mqtt",
        "Publishes messages to an MQTT broker topic.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "host": {"type": "string", "description": "MQTT broker hostname."},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535, "description": "MQTT broker port."},
                "client_id": {"type": "string", "description": "Client identifier."},
                "username": {"type": "string", "description": "Optional username."},
                "password": {"type": "string", "description": "Optional password."},
                "topic": {"oneOf": [ {"type": "object", "properties": {"type": {"const": "value"}, "value": {"type": "string"}}, "required": ["type", "value"], "additionalProperties": false}, {"type": "object", "properties": {"type": {"const": "expr"}, "expr": {"type": "string"}}, "required": ["type", "expr"], "additionalProperties": false}
                    ], "description": "Literal value or a SQL expression evaluated per batch."},
                "qos": {"type": "integer", "enum": [0, 1, 2], "default": 0, "description": "Quality of Service."},
                "clean_session": {"type": "boolean", "default": true},
                "keep_alive": {"type": "integer", "minimum": 1, "description": "Keep-alive interval in seconds."},
                "retain": {"type": "boolean", "default": false, "description": "Whether to retain the message on the broker."},
                "value_field": {"type": "string", "description": "Column whose per-row value becomes the message payload (binary or string column). When unset, the codec encoding of the batch applies."},
                "tls": {"type": "object", "description": "TLS transport configuration.", "properties": {
                    "enabled": {"type": "boolean", "default": true},
                    "ca": {"type": "string", "description": "CA certificate file path."},
                    "client_cert": {"type": "string", "description": "Client certificate file (mTLS)."},
                    "client_key": {"type": "string", "description": "Client private key file (mTLS)."}
                }}
            },
            "required": ["host", "port", "client_id", "topic"]
        }),
    ).with_example(serde_json::json!({
        "host": "localhost", "port": 1883, "client_id": "arkflow", "topic": {"type": "value", "value": "sensors/data"}
    })))
}

#[async_trait]
trait MqttClient: Send + Sync {
    async fn create(
        mqtt_options: MqttOptions,
        cap: usize,
    ) -> Result<(Self, rumqttc::EventLoop), Error>
    where
        Self: Sized;

    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send;

    // Add the disconnect method to the trait
    async fn disconnect(&self) -> Result<(), ClientError>;
}

#[async_trait]
impl MqttClient for AsyncClient {
    async fn create(
        mqtt_options: MqttOptions,
        cap: usize,
    ) -> Result<(Self, rumqttc::EventLoop), Error>
    where
        Self: Sized,
    {
        let (client, eventloop) = AsyncClient::new(mqtt_options, cap);
        Ok((client, eventloop))
    }

    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send,
    {
        AsyncClient::publish(self, topic, qos, retain, payload).await
    }

    async fn disconnect(&self) -> Result<(), ClientError> {
        AsyncClient::disconnect(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Serializes tests that touch the shared injection counters / connect.
    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn reset_injections() {
        MOCK_CREATE_FAILURES.store(0, Ordering::SeqCst);
        MOCK_PUBLISH_FAILURES.store(0, Ordering::SeqCst);
    }

    /// Remaining injected `create` failures (shared across mock instances).
    static MOCK_CREATE_FAILURES: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    /// Remaining injected `publish` failures.
    static MOCK_PUBLISH_FAILURES: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    /// How many mock clients were created (leak check).
    static MOCK_CREATES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    /// How many mock clients were disconnected (teardown check).
    static MOCK_DISCONNECTS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    /// Minimal fake broker: accepts TCP connections and immediately replies
    /// with a CONNACK (session-present=0, code=0), then holds the socket
    /// open. This makes the mock's eventloop pass the ConnAck gate in
    /// `establish_connection` like a real broker would.
    async fn spawn_fake_broker() -> Result<u16, std::io::Error> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let (mut rd, mut wr) = socket.into_split();
                    let _ = wr.write_all(&[0x20, 0x02, 0x00, 0x00]).await;
                    // keep the connection open until the client goes away
                    let mut buf = [0u8; 512];
                    loop {
                        match rd.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                });
            }
        });
        Ok(port)
    }

    // Mock MQTT client for testing
    struct MockMqttClient {
        connected: Arc<AtomicBool>,
        #[allow(clippy::type_complexity)]
        published_messages: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
        /// Kept alive so the request channel stays open and the eventloop
        /// (returned to `establish_connection`) keeps polling happily.
        #[allow(dead_code)]
        _real_client: AsyncClient,
    }

    impl MockMqttClient {
        fn new(real_client: AsyncClient) -> Self {
            MOCK_CREATES.fetch_add(1, Ordering::SeqCst);
            Self {
                connected: Arc::new(AtomicBool::new(true)),
                published_messages: Arc::new(Mutex::new(Vec::new())),
                _real_client: real_client,
            }
        }

        fn mock_client_error() -> ClientError {
            rumqttc::ClientError::Request(rumqttc::Request::Disconnect(rumqttc::Disconnect))
        }
    }

    #[async_trait]
    impl MqttClient for MockMqttClient {
        async fn create(
            _mqtt_options: MqttOptions,
            _cap: usize,
        ) -> Result<(Self, rumqttc::EventLoop), Error> {
            if MOCK_CREATE_FAILURES.load(Ordering::SeqCst) > 0 {
                MOCK_CREATE_FAILURES.fetch_sub(1, Ordering::SeqCst);
                return Err(Error::Connection("injected create failure".to_string()));
            }
            // A real eventloop against the fake broker: the first poll
            // yields the broker's CONNACK, so the handshake gate passes.
            let port = spawn_fake_broker()
                .await
                .map_err(|e| Error::Connection(format!("fake broker bind failed: {e}")))?;
            let (client, eventloop) =
                AsyncClient::new(MqttOptions::new("mock", "127.0.0.1", port), 10);
            Ok((Self::new(client), eventloop))
        }

        async fn publish<S, V>(
            &self,
            topic: S,
            _qos: QoS,
            _retain: bool,
            payload: V,
        ) -> Result<(), ClientError>
        where
            S: Into<String> + Send,
            V: Into<Vec<u8>> + Send,
        {
            if MOCK_PUBLISH_FAILURES.load(Ordering::SeqCst) > 0 {
                MOCK_PUBLISH_FAILURES.fetch_sub(1, Ordering::SeqCst);
                return Err(Self::mock_client_error());
            }
            let mut messages = self.published_messages.lock().await;
            messages.push((topic.into(), payload.into()));
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), ClientError> {
            MOCK_DISCONNECTS.fetch_add(1, Ordering::SeqCst);
            self.connected.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    fn test_config() -> MqttOutputConfig {
        MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: None,
            password: None,
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
            value_field: None,
        }
    }

    #[tokio::test]
    async fn test_write_reconnects_when_eventloop_dead() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        // A stale/dead connection (flag already false) must not fail the
        // write outright: the write path performs a bounded reconnect.
        let output = MqttOutput::<MockMqttClient>::new(test_config(), None).unwrap();
        output.connect().await.unwrap();
        output.connected.store(false, Ordering::SeqCst);

        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        output
            .write(msg)
            .await
            .expect("lazy reconnect must recover");

        let client = output.client.lock().await;
        let mock_client = client.as_ref().unwrap();
        let messages = mock_client.published_messages.lock().await;
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_write_publish_failure_reconnects_and_retries() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let output = MqttOutput::<MockMqttClient>::new(test_config(), None).unwrap();
        output.connect().await.unwrap();
        MOCK_PUBLISH_FAILURES.store(1, Ordering::SeqCst);

        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        output
            .write(msg)
            .await
            .expect("publish failure must trigger reconnect + retry");
        assert_eq!(
            MOCK_PUBLISH_FAILURES.load(Ordering::SeqCst),
            0,
            "injected failure must have been consumed"
        );

        let client = output.client.lock().await;
        let mock_client = client.as_ref().unwrap();
        let messages = mock_client.published_messages.lock().await;
        assert_eq!(messages.len(), 1, "retry must publish the message");
    }

    #[tokio::test]
    async fn test_reconnect_exhaustion_errors() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let output = MqttOutput::<MockMqttClient>::new(test_config(), None).unwrap();
        output.connect().await.unwrap();
        output.connected.store(false, Ordering::SeqCst);
        MOCK_CREATE_FAILURES.store(3, Ordering::SeqCst);

        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        let result = output.write(msg).await;
        assert!(result.is_err(), "exhausted reconnect must surface an error");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("reconnect failed"), "got: {msg}");
    }

    #[tokio::test]
    async fn test_second_connect_tears_down_previous_generation() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let creates_before = MOCK_CREATES.load(Ordering::SeqCst);
        let disconnects_before = MOCK_DISCONNECTS.load(Ordering::SeqCst);

        let output = MqttOutput::<MockMqttClient>::new(test_config(), None).unwrap();
        output.connect().await.unwrap();
        output.connect().await.unwrap();

        let creates = MOCK_CREATES.load(Ordering::SeqCst) - creates_before;
        let disconnects = MOCK_DISCONNECTS.load(Ordering::SeqCst) - disconnects_before;
        assert_eq!(creates, 2, "two clients were built");
        assert_eq!(
            disconnects, 1,
            "the previous client must be disconnected exactly once (no leak)"
        );
    }

    /// Test creating a new MQTT output component
    #[tokio::test]
    async fn test_mqtt_output_new() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: Some(1),
            clean_session: Some(true),
            keep_alive: Some(60),
            retain: Some(false),
            value_field: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config, None);
        assert!(output.is_ok());
    }

    /// Test MQTT output connection
    #[tokio::test]
    async fn test_mqtt_output_connect() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: None,
            password: None,
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
            value_field: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config, None).unwrap();
        assert!(output.connect().await.is_ok());
    }

    /// Test MQTT message publishing
    #[tokio::test]
    async fn test_mqtt_output_write() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: None,
            password: None,
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
            value_field: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config, None).unwrap();
        output.connect().await.unwrap();

        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        assert!(output.write(msg).await.is_ok());

        // Verify the message was published
        let client = output.client.lock().await;
        let mock_client = client.as_ref().unwrap();
        let messages = mock_client.published_messages.lock().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "test/topic");
        assert_eq!(messages[0].1, b"test message");
    }

    /// Test MQTT output disconnection
    #[tokio::test]
    async fn test_mqtt_output_close() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: None,
            password: None,
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
            value_field: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config, None).unwrap();
        output.connect().await.unwrap();
        assert!(output.close().await.is_ok());

        // Verify the client is disconnected
        let client = output.client.lock().await;
        let mock_client = client.as_ref().unwrap();
        assert!(!mock_client.connected.load(Ordering::SeqCst));
    }

    /// Test error handling when writing to disconnected client
    #[tokio::test]
    async fn test_mqtt_output_write_disconnected() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            tls: None,
            username: None,
            password: None,
            topic: Expr::Value {
                value: "test/topic".to_string(),
            },
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
            value_field: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config, None).unwrap();
        output.connect().await.unwrap();
        output.close().await.unwrap();

        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        assert!(output.write(msg).await.is_err());
    }

    /// Spec: connector-recovery-contract — a fresh rumqttc client opens no
    /// connection until the eventloop polls, so connect() must be gated on
    /// the broker's ConnAck: an unreachable broker fails the handshake
    /// instead of reporting "connected". This exercises the REAL
    /// AsyncClient path (port 1 has no listener).
    #[tokio::test]
    async fn test_connect_fails_loudly_when_broker_unreachable() {
        let _guard = TEST_LOCK.lock().await;
        reset_injections();
        let mut config = test_config();
        config.host = "127.0.0.1".to_string();
        config.port = 1;

        let output = MqttOutput::<AsyncClient>::new(config, None).unwrap();
        let err = match output.connect().await {
            Ok(_) => panic!("connect must fail against a closed port"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("handshake"),
            "error must name the handshake stage, got: {msg}"
        );
        assert!(
            !output.connected.load(Ordering::SeqCst),
            "connected must stay false after a failed handshake"
        );
    }
}
