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
