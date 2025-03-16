//! Kafka output component
//!
//! Send the processed data to the Kafka topic

use serde::{Deserialize, Serialize};

use crate::output::{register_output_builder, OutputBuilder};
use crate::{output::Output, MessageBatch};
use crate::{Content, Error};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::message::ToBytes;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
        }
    }
}

/// Kafka output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOutputConfig {
    /// List of Kafka server addresses
    pub brokers: Vec<String>,
    /// Target topic
    pub topic: String,
    /// Partition key (optional)
    pub key: Option<String>,
    /// Client ID
    pub client_id: Option<String>,
    /// Compression type
    pub compression: Option<CompressionType>,
    /// Acknowledgment level (0=no acknowledgment, 1=leader acknowledgment, all=all replica acknowledgments)
    pub acks: Option<String>,
}

/// Kafka output component
struct KafkaOutput<T> {
    config: KafkaOutputConfig,
    producer: Arc<RwLock<Option<T>>>,
}

impl<T: KafkaClient> KafkaOutput<T> {
    /// Create a new Kafka output component
    pub fn new(config: KafkaOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            producer: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl<T: KafkaClient> Output for KafkaOutput<T> {
    async fn connect(&self) -> Result<(), Error> {
        let mut client_config = ClientConfig::new();

        // Configure the Kafka server address
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // Set the client ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // Set the compression type
        if let Some(compression) = &self.config.compression {
            client_config.set("compression.type", compression.to_string().to_lowercase());
        }

        // Set the confirmation level
        if let Some(acks) = &self.config.acks {
            client_config.set("acks", acks);
        }

        // Create a producer
        let producer = T::create(&client_config)
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Save the producer instance
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;
        *producer_guard = Some(producer);

        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let producer_arc = self.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        let payloads = msg.as_string()?;
        if payloads.is_empty() {
            return Ok(());
        }

        match &msg.content {
            Content::Arrow(_) => {
                return Err(Error::Processing(
                    "The arrow format is not supported".to_string(),
                ))
            }
            Content::Binary(v) => {
                for x in v {
                    // Create record
                    let mut record = FutureRecord::to(&self.config.topic).payload(&x);

                    // Set partition key if available
                    if let Some(key) = &self.config.key {
                        record = record.key(key);
                    }

                    // Get the producer and send the message
                    producer
                        .send(record, Duration::from_secs(5))
                        .await
                        .map_err(|(e, _)| {
                            Error::Processing(format!("Failed to send a Kafka message: {}", e))
                        })?;
                }
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Get the producer and close
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;

        if let Some(producer) = producer_guard.take() {
            // Wait for all messages to be sent
            producer.flush(Duration::from_secs(30)).map_err(|e| {
                Error::Connection(format!(
                    "Failed to refresh the message when the Kafka producer is disabled: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

pub(crate) struct KafkaOutputBuilder;
impl OutputBuilder for KafkaOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "HTTP output configuration is missing".to_string(),
            ));
        }
        let config: KafkaOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(KafkaOutput::<FutureProducer>::new(config)?))
    }
}

pub fn init() {
    register_output_builder("kafka", Arc::new(KafkaOutputBuilder));
}
#[async_trait]
trait KafkaClient: Send + Sync {
    fn create(config: &ClientConfig) -> KafkaResult<Self>
    where
        Self: Sized;

    async fn send<K, P, T>(
        &self,
        record: FutureRecord<'_, K, P>,
        queue_timeout: T,
    ) -> OwnedDeliveryResult
    where
        K: ToBytes + ?Sized + Sync,
        P: ToBytes + ?Sized + Sync,
        T: Into<Timeout> + Sync + Send;

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;
}
#[async_trait]
impl KafkaClient for FutureProducer {
    fn create(config: &ClientConfig) -> KafkaResult<Self> {
        config.create()
    }
    async fn send<K, P, T>(
        &self,
        record: FutureRecord<'_, K, P>,
        queue_timeout: T,
    ) -> OwnedDeliveryResult
    where
        K: ToBytes + ?Sized + Sync,
        P: ToBytes + ?Sized + Sync,
        T: Into<Timeout> + Sync + Send,
    {
        FutureProducer::send(self, record, queue_timeout).await
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        Producer::flush(self, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use rdkafka::Timestamp;
    use tokio::sync::Mutex;

    // Mock Kafka client for testing
    struct MockKafkaClient {
        // Track if client is connected
        connected: Arc<AtomicBool>,
        // Store sent messages for verification
        sent_messages: Arc<Mutex<Vec<(String, Vec<u8>, Option<String>)>>>,
        // Flag to simulate errors
        should_fail: Arc<AtomicBool>,
    }

    impl MockKafkaClient {
        fn new() -> Self {
            Self {
                connected: Arc::new(AtomicBool::new(true)),
                sent_messages: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(AtomicBool::new(false)),
            }
        }

        fn with_failure() -> Self {
            let client = Self::new();
            client.should_fail.store(true, Ordering::SeqCst);
            client
        }
    }

    #[async_trait]
    impl KafkaClient for MockKafkaClient {
        fn create(config: &ClientConfig) -> KafkaResult<Self> {
            // Simulate connection failure if bootstrap.servers is empty
            if config.get("bootstrap.servers").unwrap_or("") == "" {
                return Err(rdkafka::error::KafkaError::ClientCreation(
                    "Failed to create client".to_string(),
                ));
            }
            Ok(Self::new())
        }

        async fn send<K, P, T>(
            &self,
            record: FutureRecord<'_, K, P>,
            _queue_timeout: T,
        ) -> OwnedDeliveryResult
        where
            K: ToBytes + ?Sized + Sync,
            P: ToBytes + ?Sized + Sync,
            T: Into<Timeout> + Sync + Send,
        {
            // Check if we should simulate a failure
            if self.should_fail.load(Ordering::SeqCst) {
                let err = rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::QueueFull);
                // Create OwnedMessage instead of Vec<u8> for the error return
                let payload = rdkafka::message::OwnedMessage::new(
                    Some(record.payload.unwrap().to_bytes().to_vec()),
                    None,
                    record.topic.to_string(),
                     Timestamp::NotAvailable,
                    0,
                    0,
                    None,
                );
                return Err((err, payload));
            }

            // Store the message for later verification
            let mut messages = self.sent_messages.lock().await;
            messages.push((
                record.topic.to_string(),
                record.payload.unwrap().to_bytes().to_vec(),
                record.key.map(|k| String::from_utf8_lossy(k.to_bytes()).to_string()),
            ));

            // Return a successful delivery
            // Convert RDKafkaRespErr to i32 for the success case
            Ok((rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as i32, 0))
        }

        fn flush<T: Into<Timeout>>(&self, _timeout: T) -> KafkaResult<()> {
            // Immediately return error if should_fail is true, preventing any blocking
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(rdkafka::error::KafkaError::Flush(
                    rdkafka::types::RDKafkaErrorCode::QueueFull,
                ));
            }
            Ok(())
        }
    }

    /// Test creating a new Kafka output component
    #[tokio::test]
    async fn test_kafka_output_new() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create a new Kafka output component
        let output = KafkaOutput::<MockKafkaClient>::new(config);
        assert!(output.is_ok(), "Failed to create Kafka output component");
    }

    /// Test connecting to Kafka
    #[tokio::test]
    async fn test_kafka_output_connect() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        let result = output.connect().await;
        assert!(result.is_ok(), "Failed to connect to Kafka");

        // Verify producer is initialized
        let producer_guard = output.producer.read().await;
        assert!(producer_guard.is_some(), "Kafka producer not initialized");
    }

    /// Test connection failure
    #[tokio::test]
    async fn test_kafka_output_connect_failure() {
        // Create a configuration with empty brokers to trigger failure
        let config = KafkaOutputConfig {
            brokers: vec![],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and try to connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        let result = output.connect().await;
        assert!(result.is_err(), "Connection should fail with empty brokers");
    }

    /// Test writing messages to Kafka
    #[tokio::test]
    async fn test_kafka_output_write() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        output.connect().await.unwrap();

        // Create a test message
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;
        assert!(result.is_ok(), "Failed to write message to Kafka");

        // Verify the message was sent
        let producer_guard = output.producer.read().await;
        let producer = producer_guard.as_ref().unwrap();
        let messages = producer.sent_messages.lock().await;
        assert_eq!(messages.len(), 1, "Message not sent to Kafka");
        assert_eq!(messages[0].0, "test-topic", "Wrong topic");
        assert_eq!(messages[0].1, b"test message", "Wrong message content");
        assert_eq!(messages[0].2, None, "Key should be None");
    }

    /// Test writing messages with a partition key
    #[tokio::test]
    async fn test_kafka_output_write_with_key() {
        // Create a configuration with a partition key
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: Some("test-key".to_string()),
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        output.connect().await.unwrap();

        // Create a test message
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;
        assert!(result.is_ok(), "Failed to write message to Kafka");

        // Verify the message was sent with the key
        let producer_guard = output.producer.read().await;
        let producer = producer_guard.as_ref().unwrap();
        let messages = producer.sent_messages.lock().await;
        assert_eq!(messages.len(), 1, "Message not sent to Kafka");
        assert_eq!(messages[0].2, Some("test-key".to_string()), "Wrong key");
    }

    /// Test writing to Kafka without connecting first
    #[tokio::test]
    async fn test_kafka_output_write_without_connect() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create Kafka output without connecting
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;

        // Should return connection error
        assert!(result.is_err(), "Write should fail when not connected");
        match result {
            Err(Error::Connection(_)) => {} // Expected error
            _ => panic!("Expected Connection error"),
        }
    }

    /// Test writing with send failure
    #[tokio::test]
    async fn test_kafka_output_write_failure() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        output.connect().await.unwrap();

        // Set the producer to fail
        let producer_guard = output.producer.read().await;
        let producer = producer_guard.as_ref().unwrap();
        producer.should_fail.store(true, Ordering::SeqCst);

        // Create a test message
        let msg = MessageBatch::from_string("test message");
        let result = output.write(&msg).await;
        assert!(result.is_err(), "Write should fail with producer error");
    }

    /// Test closing Kafka connection
    #[tokio::test]
    async fn test_kafka_output_close() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        output.connect().await.unwrap();

        // Close the connection
        let result = output.close().await;
        assert!(result.is_ok(), "Failed to close Kafka connection");

        // Verify producer is cleared
        let producer_guard = output.producer.read().await;
        assert!(producer_guard.is_none(), "Kafka producer not cleared");
    }

    /// Test closing with flush failure
    #[tokio::test]
    async fn test_kafka_output_close_failure() {
        // Create a basic configuration
        let config = KafkaOutputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            key: None,
            client_id: None,
            compression: None,
            acks: None,
        };

        // Create and connect the Kafka output
        let output = KafkaOutput::<MockKafkaClient>::new(config).unwrap();
        output.connect().await.unwrap();

        // Set the producer to fail before acquiring the write lock
        {
            let producer_guard = output.producer.read().await;
            let producer = producer_guard.as_ref().unwrap();
            producer.should_fail.store(true, Ordering::SeqCst);
        }

        // Close the connection
        let result = output.close().await;
        assert!(result.is_err(), "Close should fail with flush error");
    }
}
