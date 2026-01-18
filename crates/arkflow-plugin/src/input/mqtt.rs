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
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};

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
}

/// MQTT input component
pub struct MqttInput {
    input_name: Option<String>,
    config: MqttInputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    sender: Sender<MqttMsg>,
    receiver: Receiver<MqttMsg>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

enum MqttMsg {
    Publish(Publish),
    Err(Error),
}

impl MqttInput {
    /// Create a new MQTT input component
    pub fn new(
        name: Option<&String>,
        config: MqttInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<MqttMsg>(1000);
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

        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = eventloop.poll() => {
                        match result {
                            Ok(event) => {
                                if let Event::Incoming(Packet::Publish(publish)) = event {
                                    // Add messages to the queue
                                    match sender_clone.send_async(MqttMsg::Publish(publish)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("{}",e)
                                        }
                                    };
                                }
                            }
                            Err(e) => {
                               // Log the error and wait a short time before continuing
                                error!("MQTT event loop error: {}", e);
                                match sender_clone.send_async(MqttMsg::Err(Error::Disconnection)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("{}",e)
                                        }
                                };
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
            result = self.receiver.recv_async() =>{
                match result {
                    Ok(msg) => {
                        match msg{
                            MqttMsg::Publish(publish) => {
                                let payload = publish.payload.to_vec();

                                // Apply codec if configured
                                let mut msg = crate::input::codec_helper::apply_codec_to_payload(
                                    &payload,
                                    &self.codec,
                                )?;
                                msg.set_input_name(self.input_name.clone());

                                Ok((Arc::new(msg), Arc::new(MqttAck {
                                    client: Arc::clone(&self.client),
                                    publish,
                                })))
                            },
                            MqttMsg::Err(e) => {
                                  Err(e)
                            }
                        }
                    }
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
    async fn ack(&self) {
        let mutex_guard = self.client.lock().await;
        if let Some(client) = &*mutex_guard {
            if let Err(e) = client.ack(&self.publish).await {
                error!("{}", e);
            }
        }
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
        if config.is_none() {
            return Err(Error::Config(
                "MQTT input configuration is missing".to_string(),
            ));
        }

        let config: MqttInputConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(MqttInput::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("mqtt", Arc::new(MqttInputBuilder))
}
