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

//! Kafka output component
//!
//! Send the processed data to the Kafka topic

use serde::{Deserialize, Serialize};

use arkflow_core::{
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatch, MessageBatchRef, Resource, DEFAULT_BINARY_VALUE_FIELD,
};

use crate::expr::{EvaluateResult, Expr};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka_sys::RDKafkaErrorCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
struct KafkaOutputConfig {
    /// List of Kafka server addresses
    brokers: Vec<String>,
    /// Target topic
    topic: Expr<String>,
    /// Partition key (optional)
    key: Option<Expr<String>>,
    /// Client ID
    client_id: Option<String>,
    /// Compression type
    compression: Option<CompressionType>,
    /// Acknowledgment level (0=no acknowledgment, 1=leader acknowledgment, all=all replica acknowledgments)
    acks: Option<String>,
    /// Value type
    value_field: Option<String>,
}

/// Kafka output component
struct KafkaOutput {
    config: KafkaOutputConfig,
    inner_kafka_output: Arc<InnerKafkaOutput>,
    cancellation_token: CancellationToken,
}

struct InnerKafkaOutput {
    producer: Arc<RwLock<Option<FutureProducer>>>,
    send_futures: Arc<Mutex<Vec<DeliveryFuture>>>,
}

impl KafkaOutput {
    /// Create a new Kafka output component
    pub fn new(config: KafkaOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let inner_kafka_output = Arc::new(InnerKafkaOutput {
            producer: Arc::new(RwLock::new(None)),
            send_futures: Arc::new(Mutex::new(vec![])),
        });

        let output_p = Arc::clone(&inner_kafka_output);
        let cancellation_token_clone = CancellationToken::clone(&cancellation_token);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = time::sleep(Duration::from_secs(1)) => {
                        output_p.flush().await;
                        debug!("Kafka output flushed");
                    },
                    _ = cancellation_token_clone.cancelled()=>{
                        break;
                    }
                }
            }
        });

        Ok(Self {
            config,
            inner_kafka_output,
            cancellation_token,
        })
    }
}

impl InnerKafkaOutput {
    async fn flush(&self) {
        let mut send_futures = self.send_futures.lock().await;
        for future in send_futures.drain(..) {
            match future.await {
                Ok(Ok(_)) => {} // Success
                Ok(Err((e, _))) => {
                    error!("Kafka producer shut down: {:?}", e);
                }
                Err(e) => {
                    error!("Future error during Kafka shutdown: {:?}", e);
                }
            }
        }
    }
}

#[async_trait]
impl Output for KafkaOutput {
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

        // Set the confirmation level (default to "all" for reliability)
        if let Some(acks) = &self.config.acks {
            client_config.set("acks", acks);
        }

        // Create a producer
        let producer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Save the producer instance
        let producer_arc = self.inner_kafka_output.producer.clone();
        let mut producer_guard = producer_arc.write().await;
        *producer_guard = Some(producer);

        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        let producer_arc = self.inner_kafka_output.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        let value_field = self
            .config
            .value_field
            .as_deref()
            .unwrap_or(DEFAULT_BINARY_VALUE_FIELD);
        let payloads = msg.to_binary(value_field)?;
        if payloads.is_empty() {
            return Ok(());
        }

        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        // Prepare all records for sending
        for (i, x) in payloads.into_iter().enumerate() {
            // Create record
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x),
                EvaluateResult::Vec(v) => FutureRecord::to(&*v[i]).payload(x),
            };

            // Add key if available
            match &key {
                Some(EvaluateResult::Scalar(s)) => record = record.key(s),
                Some(EvaluateResult::Vec(v)) if i < v.len() => {
                    record = record.key(&v[i]);
                }
                _ => {}
            }

            // Send the record
            debug!("send payload:{}", String::from_utf8_lossy(x));

            loop {
                match producer.send_result(record) {
                    Ok(future) => {
                        self.inner_kafka_output
                            .send_futures
                            .lock()
                            .await
                            .push(future);
                        debug!("Kafka record sent");
                        break;
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), f)) => {
                        record = f;
                    }
                    Err((e, _)) => {
                        return Err(Error::Connection(format!("Failed to write to Kafka: {e}")));
                    }
                };

                // back off and retry
                tokio::time::sleep(Duration::from_millis(50)).await;
                debug!("Kafka queue full, retrying...");
            }
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        // Get the producer and close
        let producer_arc = self.inner_kafka_output.producer.clone();
        let mut producer_guard = producer_arc.write().await;

        if let Some(producer) = producer_guard.take() {
            producer.poll(Timeout::After(Duration::ZERO));
            for future in self.inner_kafka_output.send_futures.lock().await.drain(..) {
                match future.await {
                    Ok(Ok(_)) => {} // Success
                    Ok(Err((e, _))) => {
                        error!("Kafka producer shut down: {:?}", e);
                    }
                    Err(e) => {
                        error!("Future error during Kafka shutdown: {:?}", e);
                    }
                }
            }

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
impl KafkaOutput {
    async fn get_topic(&self, msg: &MessageBatch) -> Result<EvaluateResult<String>, Error> {
        self.config.topic.evaluate_expr(msg).await
    }

    async fn get_key(&self, msg: &MessageBatch) -> Result<Option<EvaluateResult<String>>, Error> {
        let Some(v) = &self.config.key else {
            return Ok(None);
        };

        Ok(Some(v.evaluate_expr(msg).await?))
    }
}

pub(crate) struct KafkaOutputBuilder;
impl OutputBuilder for KafkaOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Kafka output configuration is missing".to_string(),
            ));
        }

        // Parse the configuration
        let config: KafkaOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(KafkaOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("kafka", Arc::new(KafkaOutputBuilder))
}
