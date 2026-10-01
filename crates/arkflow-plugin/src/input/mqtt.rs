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

//! MQTT input component
//!
//! Receive data from the MQTT broker

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use crate::input::codec_helper::Delivery;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;
/// MQTT input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttInputConfig {
    /// MQTT broker address
    pub host: String,
    /// MQTT broker port
    pub port: u16,
    /// Client ID
    pub client_id: String,
    /// Username (optional)
    pub username: Option<String>,
    /// Password (optional)
    pub password: Option<String>,
    /// List of topics to subscribe to
    pub topics: Vec<String>,
    /// Quality of Service (0, 1, 2)
    pub qos: Option<u8>,
    /// Whether to use clean session
    pub clean_session: Option<bool>,
    /// Keep alive interval (in seconds)
    pub keep_alive: Option<u64>,
    /// TLS transport configuration
    #[serde(default)]
    pub tls: Option<crate::mqtt_tls::MqttTlsConfig>,
}

/// MQTT input component
pub struct MqttInput {
    input_name: Option<String>,
    config: MqttInputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    sender: Sender<Delivery>,
    receiver: Receiver<Delivery>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

// The channel carries finished `Delivery` values: the eventloop task
// decodes and pairs the MqttAck BEFORE claiming a slot (manual acks are
// settled only when the engine acknowledges), so `read()` is a single
// await point — a dropped read future loses nothing
// (Input::read cancellation-safety contract).
impl MqttInput {
    /// Create a new MQTT input component
    pub fn new(
        name: Option<&String>,
        config: MqttInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<Delivery>(1000);
        let cancellation_token = CancellationToken::new();
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
}

#[async_trait]
impl Input for MqttInput {
    async fn connect(&self) -> Result<(), Error> {
        // Create MQTT options
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);
        mqtt_options.set_manual_acks(true);
        // Set the authentication information
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            mqtt_options.set_credentials(username, password);
        }
        if let Some(tls) = &self.config.tls {
            tls.apply(&mut mqtt_options).await?;
        }

        // Set the keep-alive time
        if let Some(keep_alive) = self.config.keep_alive {
            mqtt_options.set_keep_alive(std::time::Duration::from_secs(keep_alive));
        }

        // Set up a clean session
        if let Some(clean_session) = self.config.clean_session {
            mqtt_options.set_clean_session(clean_session);
        }

        // Create an MQTT client
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        // Subscribe to topics
        let qos_level = match self.config.qos {
            Some(0) => QoS::AtMostOnce,
            Some(1) => QoS::AtLeastOnce,
            Some(2) => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce, // Default is QoS 1
        };

        for topic in &self.config.topics {
            client.subscribe(topic, qos_level).await.map_err(|e| {
                Error::Connection(format!(
                    "Unable to subscribe to MQTT topics {}: {}",
                    topic, e
                ))
            })?;
        }

        let client_arc = Arc::new(&self.client);
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        let sender_clone = Sender::clone(&self.sender);
        let codec_clone = self.codec.clone();
        let input_name_clone = self.input_name.clone();
        let client_for_ack = Arc::clone(&self.client);

        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = eventloop.poll() => {
                        match result {
                            Ok(event) => {
                                if let Event::Incoming(Packet::Publish(publish)) = event {
                                    // Decode and pair the manual ack BEFORE
                                    // claiming a channel slot: with
                                    // set_manual_acks(true) the broker does
                                    // not redeliver, so a read dropped
                                    // between claim and ack construction
                                    // would lose the message forever.
                                    let payload = publish.payload.to_vec();
                                    let delivery = crate::input::codec_helper::decode_delivery(
                                        &payload,
                                        &codec_clone,
                                        input_name_clone.clone(),
                                        Arc::new(MqttAck {
                                            client: Arc::clone(&client_for_ack),
                                            publish,
                                        }),
                                    )
                                    .await;
                                    match sender_clone.send_async(delivery).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("{}",e)
                                        }
                                    };
                                }
                            }
                            Err(e) => {
                               // Log the error and notify the main loop to trigger reconnection
                                error!("MQTT event loop error: {}", e);
                                match sender_clone.send_async(Delivery::Err(Error::Disconnection)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("{}",e)
                                        }
                                };
                                // Break the loop to allow reconnection to create a new event loop task
                                break;
                            }
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        {
            let client_arc = Arc::clone(&self.client);
            if client_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }
        let cancellation_token = self.cancellation_token.clone();

        tokio::select! {
            // Deliveries arrive pre-decoded with their ack paired, so this
            // recv is the single await point — a dropped read loses nothing.
            result = self.receiver.recv_async() =>{
                match result {
                    Ok(Delivery::Data(batch, ack)) => Ok((batch, ack)),
                    Ok(Delivery::Err(e)) => Err(e),
                    Err(_) => {
                        Err(Error::EOF)
                    }
                }
            },
            _ = cancellation_token.cancelled()=>{
                Err(Error::EOF)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Send a shutdown signal
        let _ = self.cancellation_token.clone().cancel();

        // Disconnect the MQTT connection
        let client_arc = Arc::clone(&self.client);
        let client_guard = client_arc.lock().await;
        if let Some(client) = &*client_guard {
            // Try to disconnect, but don't wait for the result
            let _ = client.disconnect().await;
        }

        Ok(())
    }
}

struct MqttAck {
    client: Arc<Mutex<Option<AsyncClient>>>,
    publish: Publish,
}
#[async_trait]
impl Ack for MqttAck {
    async fn ack(&self) -> Result<(), Error> {
        let mutex_guard = self.client.lock().await;
        if let Some(client) = &*mutex_guard {
            client
                .ack(&self.publish)
                .await
                .map_err(|e| Error::Process(format!("Failed to ack MQTT message: {}", e)))?;
        }
        Ok(())
    }
}

pub(crate) struct MqttInputBuilder;
impl InputBuilder for MqttInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: MqttInputConfig = parse_config(config, "MQTT input")?;
        Ok(Arc::new(MqttInput::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("mqtt", Arc::new(MqttInputBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "mqtt",
        "Subscribes to an MQTT broker and forwards messages from the configured topics.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "host": {"type": "string", "description": "MQTT broker hostname."},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535, "description": "MQTT broker port."},
                "client_id": {"type": "string", "description": "Unique client identifier."},
                "username": {"type": "string", "description": "Optional username."},
                "password": {"type": "string", "description": "Optional password."},
                "topics": {"type": "array", "items": {"type": "string"}, "description": "Topics to subscribe to (MQTT wildcards supported)."},
                "qos": {"type": "integer", "enum": [0, 1, 2], "default": 0, "description": "Quality of Service level."},
                "clean_session": {"type": "boolean", "default": true, "description": "Whether to use a clean session."},
                "keep_alive": {"type": "integer", "minimum": 1, "description": "Keep-alive interval in seconds."},
                "tls": {"type": "object", "description": "TLS transport configuration.", "properties": {
                    "enabled": {"type": "boolean", "default": true},
                    "ca": {"type": "string", "description": "CA certificate file path."},
                    "client_cert": {"type": "string", "description": "Client certificate file (mTLS)."},
                    "client_key": {"type": "string", "description": "Client private key file (mTLS)."}
                }}
            },
            "required": ["host", "port", "client_id", "topics"]
        }),
    ).with_example(serde_json::json!({
        "host": "localhost",
        "port": 1883,
        "client_id": "arkflow",
        "topics": ["sensors/#"]
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::time::Duration;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn config(qos: Option<u8>) -> MqttInputConfig {
        serde_json::from_value(serde_json::json!({
            "host": "127.0.0.1",
            "port": 1883,
            "client_id": "test-client",
            "topics": ["sensors/#"],
            "qos": qos,
        }))
        .unwrap()
    }

    #[test]
    fn builder_rejects_missing_required_fields() {
        let bad = Some(serde_json::json!({
            "host": "localhost",
            "port": 1883,
            "client_id": "c"
            // topics missing
        }));
        assert!(matches!(
            MqttInputBuilder.build(None, &bad, None, &test_resource()),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn builder_accepts_full_config() {
        let config = Some(serde_json::json!({
            "host": "localhost",
            "port": 1883,
            "client_id": "c",
            "topics": ["a", "b/#"],
            "qos": 2,
            "username": "u",
            "password": "p",
            "clean_session": false,
            "keep_alive": 30
        }));
        assert!(
            MqttInputBuilder
                .build(None, &config, None, &test_resource())
                .is_ok()
        );
    }

    #[test]
    fn qos_variants_deserialize() {
        assert_eq!(config(Some(0)).qos, Some(0));
        assert_eq!(config(Some(2)).qos, Some(2));
        assert!(config(Some(9)).qos.is_some(), "out-of-range qos still parses");
        assert!(config(None).qos.is_none());
        assert!(config(None).tls.is_none(), "tls defaults to none");
    }

    #[tokio::test]
    async fn read_before_connect_reports_disconnection() {
        let input = MqttInput::new(None, config(None), None).unwrap();
        assert!(matches!(input.read().await, Err(Error::Disconnection)));
    }

    #[tokio::test]
    async fn close_before_connect_is_ok() {
        let input = MqttInput::new(None, config(None), None).unwrap();
        input.close().await.expect("close before connect");
    }

    #[tokio::test]
    async fn dead_broker_surfaces_disconnection_on_read() {
        // rumqttc queues the subscribe without awaiting the broker, so
        // connect returns Ok; the eventloop error must then surface as a
        // Disconnection from read(), not hang.
        let input = MqttInput::new(
            None,
            MqttInputConfig {
                port: 1,
                ..config(Some(2))
            },
            None,
        )
        .unwrap();
        input.connect().await.expect("connect queues subscriptions");
        let outcome = tokio::time::timeout(Duration::from_secs(15), input.read())
            .await
            .expect("read must resolve, not hang");
        let error = match outcome {
            Err(error) => error,
            Ok(_) => panic!("read on a dead broker must fail, not deliver"),
        };
        assert!(
            matches!(error, Error::Disconnection),
            "unexpected error: {error}"
        );
    }
}
