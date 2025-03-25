//! MQTT output component
//!
//! Send the processed data to the MQTT broker

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use rumqttc::{AsyncClient, ClientError, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// MQTT output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttOutputConfig {
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
    /// Published topics
    pub topic: String,
    /// Quality of Service (0, 1, 2)
    pub qos: Option<u8>,
    /// Whether to use clean session
    pub clean_session: Option<bool>,
    /// Keep alive interval (seconds)
    pub keep_alive: Option<u64>,
    /// Whether to retain the message
    pub retain: Option<bool>,
}

/// MQTT output component
struct MqttOutput<T: MqttClient> {
    config: MqttOutputConfig,
    client: Arc<Mutex<Option<T>>>,
    connected: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl<T: MqttClient> MqttOutput<T> {
    /// Create a new MQTT output component
    pub fn new(config: MqttOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl<T: MqttClient> Output for MqttOutput<T> {
    async fn connect(&self) -> Result<(), Error> {
        // Create MQTT options
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);

        // Set the authentication information
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            mqtt_options.set_credentials(username, password);
        }

        // Set the keep-alive time
        if let Some(keep_alive) = self.config.keep_alive {
            mqtt_options.set_keep_alive(std::time::Duration::from_secs(keep_alive));
        }

        // Set up a purge session
        if let Some(clean_session) = self.config.clean_session {
            mqtt_options.set_clean_session(clean_session);
        }

        // Create an MQTT client
        let (client, mut eventloop) = T::create(mqtt_options, 10).await?;
        // Save the client
        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        // Start an event loop processing thread (keep the connection active)
        let eventloop_handle = tokio::spawn(async move {
            while let Ok(_) = eventloop.poll().await {
                // Just keep the event loop running and don't need to process the event
            }
        });

        // Holds the event loop processing thread handle
        let eventloop_handle_arc = self.eventloop_handle.clone();
        let mut eventloop_handle_guard = eventloop_handle_arc.lock().await;
        *eventloop_handle_guard = Some(eventloop_handle);

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("The MQTT client is not initialized".to_string()))?;

        // Get the message content
        let payloads = match msg.as_string() {
            Ok(v) => v.to_vec(),
            Err(e) => {
                return Err(e);
            }
        };

        for payload in payloads {
            info!(
                "Send message: {}",
                &String::from_utf8_lossy((&payload).as_ref())
            );

            // Determine the QoS level
            let qos_level = match self.config.qos {
                Some(0) => QoS::AtMostOnce,
                Some(1) => QoS::AtLeastOnce,
                Some(2) => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce, // The default is QoS 1
            };

            // Decide whether to keep the message
            let retain = self.config.retain.unwrap_or(false);

            // Post a message
            client
                .publish(&self.config.topic, qos_level, retain, payload)
                .await
                .map_err(|e| Error::Process(format!("MQTT publishing failed: {}", e)))?;
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
        Ok(())
    }
}

pub(crate) struct MqttOutputBuilder;
impl OutputBuilder for MqttOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "HTTP output configuration is missing".to_string(),
            ));
        }
        let config: MqttOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MqttOutput::<AsyncClient>::new(config)?))
    }
}

pub fn init() {
    register_output_builder("mqtt", Arc::new(MqttOutputBuilder));
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
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Mock MQTT client for testing
    struct MockMqttClient {
        connected: Arc<AtomicBool>,
        published_messages: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
    }

    impl MockMqttClient {
        fn new() -> Self {
            Self {
                connected: Arc::new(AtomicBool::new(true)),
                published_messages: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl MqttClient for MockMqttClient {
        async fn create(
            _mqtt_options: MqttOptions,
            _cap: usize,
        ) -> Result<(Self, rumqttc::EventLoop), Error> {
            // Create a new EventLoop directly without using new() method
            let (_, eventloop) = AsyncClient::new(MqttOptions::new("", "", 0), 10);
            Ok((Self::new(), eventloop))
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
            let mut messages = self.published_messages.lock().await;
            messages.push((topic.into(), payload.into()));
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), ClientError> {
            self.connected.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Test creating a new MQTT output component
    #[tokio::test]
    async fn test_mqtt_output_new() {
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            topic: "test/topic".to_string(),
            qos: Some(1),
            clean_session: Some(true),
            keep_alive: Some(60),
            retain: Some(false),
        };

        let output = MqttOutput::<MockMqttClient>::new(config);
        assert!(output.is_ok());
    }

    /// Test MQTT output connection
    #[tokio::test]
    async fn test_mqtt_output_connect() {
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            username: None,
            password: None,
            topic: "test/topic".to_string(),
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config).unwrap();
        assert!(output.connect().await.is_ok());
    }

    /// Test MQTT message publishing
    #[tokio::test]
    async fn test_mqtt_output_write() {
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            username: None,
            password: None,
            topic: "test/topic".to_string(),
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config).unwrap();
        output.connect().await.unwrap();

        let msg = MessageBatch::from_string("test message");
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
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            username: None,
            password: None,
            topic: "test/topic".to_string(),
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config).unwrap();
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
        let config = MqttOutputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test_client".to_string(),
            username: None,
            password: None,
            topic: "test/topic".to_string(),
            qos: None,
            clean_session: None,
            keep_alive: None,
            retain: None,
        };

        let output = MqttOutput::<MockMqttClient>::new(config).unwrap();
        output.connect().await.unwrap();
        output.close().await.unwrap();

        let msg = MessageBatch::from_string("test message");
        assert!(output.write(msg).await.is_err());
    }
}
