//! MQTT input component
//!
//! Receive data from the MQTT broker

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
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
    config: MqttInputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    sender: Arc<Sender<MqttMsg>>,
    receiver: Arc<Receiver<MqttMsg>>,
    close_tx: broadcast::Sender<()>,
}

enum MqttMsg {
    Publish(Publish),
    Err(Error),
}

impl MqttInput {
    /// Create a new MQTT input component
    pub fn new(config: MqttInputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<MqttMsg>(1000);
        let (close_tx, _) = broadcast::channel(1);
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            sender: Arc::new(sender),
            receiver: Arc::new(receiver),
            close_tx,
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

        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        let sender_arc = self.sender.clone();
        let mut rx = self.close_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = eventloop.poll() => {
                        match result {
                            Ok(event) => {
                                if let Event::Incoming(Packet::Publish(publish)) = event {
                                    // Add messages to the queue
                                    match sender_arc.send_async(MqttMsg::Publish(publish)).await {
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
                                match sender_arc.send_async(MqttMsg::Err(Error::Disconnection)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("{}",e)
                                        }
                                };
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        {
            let client_arc = self.client.clone();
            if client_arc.lock().await.is_none() {
                return Err(Error::Disconnection);
            }
        }

        let mut close_rx = self.close_tx.subscribe();
        tokio::select! {
            result = self.receiver.recv_async() =>{
                match result {
                    Ok(msg) => {
                        match msg{
                            MqttMsg::Publish(publish) => {
                                 let payload = publish.payload.to_vec();
                            let msg = MessageBatch::new_binary(vec![payload])?;
                            Ok((msg, Arc::new(MqttAck {
                                client: self.client.clone(),
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
            _ = close_rx.recv()=>{
                Err(Error::EOF)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Send a shutdown signal
        let _ = self.close_tx.send(());

        // Disconnect the MQTT connection
        let client_arc = self.client.clone();
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
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "MQTT input configuration is missing".to_string(),
            ));
        }

        let config: MqttInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MqttInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("mqtt", Arc::new(MqttInputBuilder));
}
