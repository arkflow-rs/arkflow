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
    codec::Codec,
    output::{register_output_builder, Output, OutputBuilder},
    transaction::TransactionId,
    Error, MessageBatch, MessageBatchRef, Resource,
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
    /// Transactional ID for exactly-once semantics (optional)
    transactional_id: Option<String>,
    /// Transaction timeout (default 30s)
    #[serde(default = "default_transaction_timeout")]
    transaction_timeout: u64,
}

fn default_transaction_timeout() -> u64 {
    30
}

/// Kafka output component
struct KafkaOutput {
    config: KafkaOutputConfig,
    inner_kafka_output: Arc<InnerKafkaOutput>,
    cancellation_token: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
}

struct InnerKafkaOutput {
    producer: Arc<RwLock<Option<FutureProducer>>>,
    send_futures: Arc<Mutex<Vec<DeliveryFuture>>>,
    /// Current transaction ID (if in transactional mode)
    current_transaction_id: Arc<Mutex<Option<TransactionId>>>,
    /// Whether transactional mode is enabled
    transactional: Arc<std::sync::atomic::AtomicBool>,
}

impl KafkaOutput {
    /// Create a new Kafka output component
    pub fn new(config: KafkaOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let transactional = config.transactional_id.is_some();
        let inner_kafka_output = Arc::new(InnerKafkaOutput {
            producer: Arc::new(RwLock::new(None)),
            send_futures: Arc::new(Mutex::new(vec![])),
            current_transaction_id: Arc::new(Mutex::new(None)),
            transactional: Arc::new(std::sync::atomic::AtomicBool::new(transactional)),
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
            codec,
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
        client_config.set("bootstrap.servers", self.config.brokers.join(","));

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

        // Configure transactional settings
        if let Some(ref transactional_id) = self.config.transactional_id {
            client_config.set("transactional.id", transactional_id);
            client_config.set(
                "transaction.timeout.ms",
                format!("{}", self.config.transaction_timeout * 1000),
            );
            // Enable idempotence for transactions
            client_config.set("enable.idempotence", "true");
        }

        // Create a producer
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Initialize transactions if transactional
        if self.config.transactional_id.is_some() {
            producer
                .init_transactions(Duration::from_secs(self.config.transaction_timeout))
                .map_err(|e| {
                    Error::Connection(format!("Failed to initialize Kafka transactions: {}", e))
                })?;
            debug!("Kafka transactions initialized");
        }

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

        // Apply codec encoding if configured
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)?;
        if payloads.is_empty() {
            return Ok(());
        }

        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        // Prepare all records for sending
        for (i, x) in payloads.into_iter().enumerate() {
            // Create record
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x.as_slice()),
                EvaluateResult::Vec(v) => FutureRecord::to(&v[i]).payload(x.as_slice()),
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
            debug!("send payload:{}", String::from_utf8_lossy(&x));

            // Retry with exponential backoff
            const MAX_RETRIES: u32 = 10;
            const BASE_BACKOFF_MS: u64 = 50;
            let mut retries = 0;

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
                        retries += 1;

                        if retries >= MAX_RETRIES {
                            return Err(Error::Connection(format!(
                                "Kafka queue full after {} retries",
                                MAX_RETRIES
                            )));
                        }

                        // Exponential backoff with jitter
                        let backoff_ms = BASE_BACKOFF_MS * (1 << retries.min(6));
                        let jitter = (fastrand::u64(0..backoff_ms / 4)) as u64;
                        let total_backoff = backoff_ms + jitter;

                        debug!(
                            "Kafka queue full, retrying {} after {}ms...",
                            retries, total_backoff
                        );
                        tokio::time::sleep(Duration::from_millis(total_backoff)).await;
                    }
                    Err((e, _)) => {
                        return Err(Error::Connection(format!("Failed to write to Kafka: {e}")));
                    }
                };
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

    async fn write_idempotent(
        &self,
        msg: MessageBatchRef,
        idempotency_key: &str,
    ) -> Result<(), Error> {
        let producer_arc = self.inner_kafka_output.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        // Apply codec encoding if configured
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)?;
        if payloads.is_empty() {
            return Ok(());
        }

        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        // Prepare all records for sending
        for (i, x) in payloads.into_iter().enumerate() {
            // Create record
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x.as_slice()),
                EvaluateResult::Vec(v) => FutureRecord::to(&v[i]).payload(x.as_slice()),
            };

            // Add key if available
            match &key {
                Some(EvaluateResult::Scalar(s)) => record = record.key(s),
                Some(EvaluateResult::Vec(v)) if i < v.len() => {
                    record = record.key(&v[i]);
                }
                _ => {}
            }

            // Add idempotency key as a header
            record = record.headers(rdkafka::message::OwnedHeaders::new().insert(
                rdkafka::message::Header {
                    key: "idempotency-key",
                    value: Some(idempotency_key),
                },
            ));

            // Send the record
            debug!(
                "send payload with idempotency key {}: {}",
                idempotency_key,
                String::from_utf8_lossy(&x)
            );

            // Retry with exponential backoff
            const MAX_RETRIES: u32 = 10;
            const BASE_BACKOFF_MS: u64 = 50;
            let mut retries = 0;

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
                        retries += 1;

                        if retries >= MAX_RETRIES {
                            return Err(Error::Connection(format!(
                                "Kafka queue full after {} retries",
                                MAX_RETRIES
                            )));
                        }

                        // Exponential backoff with jitter
                        let backoff_ms = BASE_BACKOFF_MS * (1 << retries.min(6));
                        let jitter = (fastrand::u64(0..backoff_ms / 4)) as u64;
                        let total_backoff = backoff_ms + jitter;

                        debug!(
                            "Kafka queue full, retrying {} after {}ms...",
                            retries, total_backoff
                        );
                        tokio::time::sleep(Duration::from_millis(total_backoff)).await;
                    }
                    Err((e, _)) => {
                        return Err(Error::Connection(format!("Failed to write to Kafka: {e}")));
                    }
                };
            }
        }

        Ok(())
    }

    async fn begin_transaction(&self) -> Result<TransactionId, Error> {
        // Check if transactional mode is enabled
        if !self
            .inner_kafka_output
            .transactional
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(Error::Process(
                "Kafka output is not configured for transactions. Set 'transactional_id' in config.".to_string(),
            ));
        }

        let producer_arc = self.inner_kafka_output.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        // Generate a new transaction ID using UUID for better uniqueness
        // Combine UUID timestamp and random bits for collision-free IDs
        let uuid = uuid::Uuid::new_v4();
        let tx_id = {
            // Use a combination of UUID and timestamp for maximum uniqueness
            let uuid_u128 = uuid.as_u128();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| Error::Process(format!("Failed to get timestamp: {}", e)))?
                .as_nanos() as u64;

            // XOR the high and low parts of UUID with timestamp
            ((uuid_u128 >> 64) as u64) ^ ((uuid_u128 & 0xFFFFFFFFFFFFFFFF) as u64) ^ timestamp
        };

        // Begin the transaction
        producer
            .begin_transaction()
            .map_err(|e| Error::Connection(format!("Failed to begin Kafka transaction: {}", e)))?;

        // Store the transaction ID
        let mut current_tx = self.inner_kafka_output.current_transaction_id.lock().await;
        *current_tx = Some(tx_id);

        debug!("Kafka transaction {} started", tx_id);
        Ok(tx_id)
    }

    async fn prepare_transaction(&self, _id: TransactionId) -> Result<(), Error> {
        // Kafka uses single-phase commit, so prepare is a no-op
        // The transaction is prepared implicitly when we call commit_transaction
        debug!("Kafka transaction prepare (no-op for single-phase commit)");
        Ok(())
    }

    async fn commit_transaction(&self, id: TransactionId) -> Result<(), Error> {
        let producer_arc = self.inner_kafka_output.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        // Verify the transaction ID matches
        let current_tx = self.inner_kafka_output.current_transaction_id.lock().await;
        if *current_tx != Some(id) {
            return Err(Error::Process(format!(
                "Transaction ID mismatch: expected {:?}, got {}",
                *current_tx, id
            )));
        }
        drop(current_tx);

        // Commit the transaction
        producer
            .commit_transaction(Duration::from_secs(self.config.transaction_timeout))
            .map_err(|e| Error::Connection(format!("Failed to commit Kafka transaction: {}", e)))?;

        // Clear the transaction ID
        let mut current_tx = self.inner_kafka_output.current_transaction_id.lock().await;
        *current_tx = None;

        debug!("Kafka transaction {} committed", id);
        Ok(())
    }

    async fn rollback_transaction(&self, id: TransactionId) -> Result<(), Error> {
        let producer_arc = self.inner_kafka_output.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        // Verify the transaction ID matches
        let current_tx = self.inner_kafka_output.current_transaction_id.lock().await;
        if *current_tx != Some(id) {
            return Err(Error::Process(format!(
                "Transaction ID mismatch: expected {:?}, got {}",
                *current_tx, id
            )));
        }
        drop(current_tx);

        // Abort the transaction
        producer
            .abort_transaction(Duration::from_secs(self.config.transaction_timeout))
            .map_err(|e| Error::Connection(format!("Failed to abort Kafka transaction: {}", e)))?;

        // Clear the transaction ID
        let mut current_tx = self.inner_kafka_output.current_transaction_id.lock().await;
        *current_tx = None;

        debug!("Kafka transaction {} rolled back", id);
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
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Kafka output configuration is missing".to_string(),
            ));
        }

        // Parse the configuration
        let config: KafkaOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(KafkaOutput::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("kafka", Arc::new(KafkaOutputBuilder))
}
