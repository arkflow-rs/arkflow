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
    component::{register_output_metadata, ComponentMetadata},
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
    /// Enable exactly-once transactional production (L2). Default false.
    exactly_once: Option<bool>,
    /// Transactional id (required when exactly_once is true). Must be stable
    /// across restarts so the broker can fence prior producer epochs.
    transactional_id: Option<String>,
}

/// Map a Kafka transaction error to an `Error`, logging which of rdkafka's
/// three transactional states it is in. Failures return `Err` so the stream
/// withholds the ack and replays the whole batch (which re-begins a fresh
/// transaction); the broker fences zombie producers via the stable
/// transactional.id on restart.
fn map_kafka_txn_error(e: KafkaError, ctx: &str) -> Error {
    if let KafkaError::Transaction(rd) = &e {
        if rd.is_fatal() {
            error!("Kafka {} fatal (producer must be discarded): {:?}", ctx, e);
        } else if rd.txn_requires_abort() {
            error!("Kafka {} requires abort (will replay): {:?}", ctx, e);
        } else if rd.is_retriable() {
            error!("Kafka {} retriable (will replay): {:?}", ctx, e);
        }
    }
    Error::Connection(format!("Kafka {} failed: {}", ctx, e))
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
}

impl KafkaOutput {
    /// Create a new Kafka output component
    pub fn new(config: KafkaOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
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

        let exactly_once = self.config.exactly_once.unwrap_or(false);

        // Configure the transactional producer when exactly_once is enabled.
        // Idempotence is implied by transactional.id but set explicitly.
        if exactly_once {
            client_config.set(
                "transactional.id",
                self.config.transactional_id.as_ref().expect(
                    "transactional_id presence is validated by the builder when exactly_once is on",
                ),
            );
            client_config.set("enable.idempotence", "true");
        }

        // Create a producer
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Initialize transactions once (blocking broker round-trip).
        if exactly_once {
            let p = producer.clone();
            tokio::task::spawn_blocking(move || {
                p.init_transactions(Timeout::After(Duration::from_secs(60)))
            })
            .await
            .map_err(|e| Error::Connection(format!("init_transactions task join failed: {}", e)))?
            .map_err(|e| Error::Connection(format!("Kafka init_transactions failed: {}", e)))?;
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
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec).await?;
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
                EvaluateResult::Vec(v) => FutureRecord::to(&*v[i]).payload(x.as_slice()),
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

    async fn write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        if !self.config.exactly_once.unwrap_or(false) {
            // Non-transactional path: default per-message behavior
            // (continue-on-error), inlined to avoid a Default-trait dance.
            let mut err = None;
            for msg in msgs {
                if let Err(e) = self.write(msg.clone()).await {
                    err = Some(e);
                }
            }
            return match err {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }
        self.write_batch_transactional(msgs).await
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
    /// Transactional write: begin → send all → commit. On any failure, abort
    /// (best-effort) and return Err so the stream withholds the ack and
    /// replays the whole batch — which re-begins a fresh transaction. Zombie
    /// producers from a crashed run are fenced by the broker via the stable
    /// transactional.id on restart.
    async fn write_batch_transactional(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        let producer_guard = self.inner_kafka_output.producer.read().await;
        let producer = match producer_guard.as_ref() {
            Some(p) => p,
            None => {
                return Err(Error::Connection(
                    "The Kafka producer is not initialized".to_string(),
                ));
            }
        };

        if let Err(e) = producer.begin_transaction() {
            return Err(map_kafka_txn_error(e, "begin_transaction"));
        }

        let mut failed: Option<Error> = None;
        for msg in msgs {
            if let Err(e) = self.send_in_transaction(producer, msg.clone()).await {
                error!("Kafka transactional send failed: {}", e);
                failed = Some(e);
                break;
            }
        }

        if let Some(e) = failed {
            // Best-effort abort; the broker fences zombies on restart anyway.
            let p = producer.clone();
            drop(producer_guard);
            if let Err(ab) = tokio::task::spawn_blocking(move || {
                p.abort_transaction(Timeout::After(Duration::from_secs(30)))
            })
            .await
            {
                error!("Kafka abort_transaction task join failed: {}", ab);
            }
            return Err(e);
        }

        // Commit (blocking broker round-trip → spawn_blocking).
        let p = producer.clone();
        drop(producer_guard);
        match tokio::task::spawn_blocking(move || {
            p.commit_transaction(Timeout::After(Duration::from_secs(30)))
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(map_kafka_txn_error(e, "commit_transaction")),
            Err(e) => Err(Error::Connection(format!(
                "commit_transaction task join failed: {}",
                e
            ))),
        }
    }

    /// Send one message's records into the current transaction. Does not
    /// collect delivery futures — commit_transaction flushes the queue.
    async fn send_in_transaction(
        &self,
        producer: &FutureProducer,
        msg: MessageBatchRef,
    ) -> Result<(), Error> {
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec).await?;
        if payloads.is_empty() {
            return Ok(());
        }
        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        for (i, x) in payloads.into_iter().enumerate() {
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x.as_slice()),
                EvaluateResult::Vec(v) => FutureRecord::to(&*v[i]).payload(x.as_slice()),
            };
            match &key {
                Some(EvaluateResult::Scalar(s)) => record = record.key(s),
                Some(EvaluateResult::Vec(v)) if i < v.len() => {
                    record = record.key(&v[i]);
                }
                _ => {}
            }

            loop {
                match producer.send_result(record) {
                    Ok(_future) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), f)) => {
                        record = f;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err((e, _)) => {
                        return Err(Error::Connection(format!(
                            "Failed to write to Kafka transaction: {e}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

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

        // D5: exactly_once requires a non-empty transactional_id (spec:
        // "Explicit stable transactional identity").
        if config.exactly_once.unwrap_or(false) {
            match &config.transactional_id {
                Some(id) if !id.trim().is_empty() => {}
                _ => {
                    return Err(Error::Config(
                        "Kafka output: transactional_id is required and must be \
                         non-empty when exactly_once is true"
                            .into(),
                    ));
                }
            }
        }

        Ok(Arc::new(KafkaOutput::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("kafka", Arc::new(KafkaOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "kafka",
        "Produces messages to Apache Kafka. Supports key-based partitioning and compression.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "brokers": {"type": "array", "items": {"type": "string"}, "description": "List of Kafka broker addresses."},
                "topic": {"type": "string", "description": "Destination topic (supports {field} placeholders)."},
                "key": {"type": "string", "description": "Field used as the message key for partitioning."},
                "client_id": {"type": "string", "description": "Optional client identifier."},
                "compression": {"type": "string", "enum": ["none", "gzip", "snappy", "lz4", "zstd"], "description": "Compression algorithm."},
                "acks": {"type": "string", "enum": ["0", "1", "all"], "description": "Acknowledgment level."},
                "value_field": {"type": "string", "description": "Record field used as the message payload."},
                "exactly_once": {"type": "boolean", "default": false, "description": "Enable exactly-once transactional production (L2)."},
                "transactional_id": {"type": "string", "description": "Transactional id (required when exactly_once is true); must be stable across restarts for zombie fencing."}
            },
            "required": ["brokers", "topic"]
        }),
    ).with_example(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topic": "events"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: RefCell::new(vec![]),
        }
    }

    /// Spec "Explicit stable transactional identity": the builder rejects
    /// `exactly_once: true` without a non-empty `transactional_id`.
    #[test]
    fn rejects_exactly_once_without_transactional_id() {
        let config = serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"},
            "exactly_once": true
        });
        let err = match KafkaOutputBuilder.build(None, &Some(config), None, &resource()) {
            Ok(_) => {
                panic!("expected build to fail when exactly_once is set without a transactional_id")
            }
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("transactional_id"),
            "expected transactional_id in error, got: {msg}"
        );
    }

    /// `exactly_once: true` with a `transactional_id` builds successfully
    /// (the producer itself is only created at `connect` time).
    #[tokio::test]
    async fn accepts_exactly_once_with_transactional_id() {
        let config = serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"},
            "exactly_once": true,
            "transactional_id": "my-tx-id"
        });
        let _output = KafkaOutputBuilder
            .build(None, &Some(config), None, &resource())
            .expect("build should succeed; producer is created at connect");
    }
}
