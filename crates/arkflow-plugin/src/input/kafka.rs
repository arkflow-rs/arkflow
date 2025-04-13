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

//! Kafka input component
//!
//! Receive data from a Kafka topic

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Kafka input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaInputConfig {
    /// List of Kafka server addresses
    pub brokers: Vec<String>,
    /// Subscribed to a topics
    pub topics: Vec<String>,
    /// Consumer group ID
    pub consumer_group: String,
    /// Client ID (optional)
    pub client_id: Option<String>,
    /// Start with the most news
    pub start_from_latest: bool,
}

/// Kafka input component
pub struct KafkaInput {
    config: KafkaInputConfig,
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
}

impl KafkaInput {
    /// Create a new Kafka input component
    pub fn new(config: KafkaInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            consumer: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Input for KafkaInput {
    async fn connect(&self) -> Result<(), Error> {
        let mut client_config = ClientConfig::new();

        // Configure the Kafka server address
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // Set the consumer group ID
        client_config.set("group.id", &self.config.consumer_group);

        // Set the client ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // Set the offset reset policy
        if self.config.start_from_latest {
            client_config.set("auto.offset.reset", "latest");
        } else {
            client_config.set("auto.offset.reset", "earliest");
        }

        // Create consumers
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("Unable to create a Kafka consumer: {}", e)))?;

        // Subscribe to a topic
        let x: Vec<&str> = self
            .config
            .topics
            .iter()
            .map(|topic| topic.as_str())
            .collect();
        consumer.subscribe(&x).map_err(|e| {
            Error::Connection(format!("You cannot subscribe to a Kafka topic: {}", e))
        })?;

        // Update consumer and connection status
        let consumer_arc = self.consumer.clone();
        let mut consumer_guard = consumer_arc.write().await;
        *consumer_guard = Some(consumer);

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let consumer_arc = self.consumer.clone();
        let consumer_guard = consumer_arc.read().await;
        if consumer_guard.is_none() {
            return Err(Error::Connection("The input is not connected".to_string()));
        }
        let consumer = consumer_guard.as_ref().unwrap();

        match consumer.recv().await {
            Ok(kafka_message) => {
                // Create internal message from Kafka message
                let payload = kafka_message.payload().ok_or_else(|| {
                    Error::Process("The Kafka message has no content".to_string())
                })?;

                let mut binary_data = Vec::new();
                binary_data.push(payload.to_vec());
                let msg_batch = MessageBatch::new_binary(binary_data)?;

                // Create acknowledgment object
                let topic = kafka_message.topic().to_string();
                let partition = kafka_message.partition();
                let offset = kafka_message.offset();

                let ack = KafkaAck {
                    consumer: self.consumer.clone(),
                    topic,
                    partition,
                    offset,
                };

                Ok((msg_batch, Arc::new(ack)))
            }
            Err(e) => Err(Error::Connection(format!(
                "Error receiving Kafka message: {}",
                e
            ))),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut consumer_guard = self.consumer.write().await;
        if let Some(consumer) = consumer_guard.take() {
            if let Err(e) = consumer.unassign() {
                tracing::warn!("Error unassigning Kafka consumer: {}", e);
            }
        }
        Ok(())
    }
}

/// Kafka message acknowledgment
pub struct KafkaAck {
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
    topic: String,
    partition: i32,
    offset: i64,
}

#[async_trait]
impl Ack for KafkaAck {
    async fn ack(&self) {
        // Commit offsets
        let consumer_mutex_guard = self.consumer.read().await;
        if let Some(v) = &*consumer_mutex_guard {
            if let Err(e) = v.store_offset(&self.topic, self.partition, self.offset) {
                tracing::error!("Error committing Kafka offset: {}", e);
            }
        }
    }
}

pub(crate) struct KafkaInputBuilder;
impl InputBuilder for KafkaInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Kafka input configuration is missing".to_string(),
            ));
        }
        let config: KafkaInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(KafkaInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("kafka", Arc::new(KafkaInputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kafka_input_new() {
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: Some("test-client".to_string()),
            start_from_latest: false,
        };

        let input = KafkaInput::new(config);
        assert!(input.is_ok());
        let input = input.unwrap();
        assert_eq!(input.config.brokers, vec!["localhost:9092".to_string()]);
        assert_eq!(input.config.topics, vec!["test-topic".to_string()]);
        assert_eq!(input.config.consumer_group, "test-group".to_string());
        assert_eq!(input.config.client_id, Some("test-client".to_string()));
        assert_eq!(input.config.start_from_latest, false);
    }

    #[tokio::test]
    async fn test_kafka_input_read_not_connected() {
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
        };

        let input = KafkaInput::new(config).unwrap();
        // Try to read in unconnected state, should return error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(msg)) => {
                assert_eq!(msg, "The input is not connected");
            }
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_kafka_ack() {
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
        };

        let input = KafkaInput::new(config).unwrap();
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
        };

        // Test acknowledgment, should have no effect since there is no actual consumer
        ack.ack().await;
        // This test mainly verifies that the ack method does not panic
    }
}
