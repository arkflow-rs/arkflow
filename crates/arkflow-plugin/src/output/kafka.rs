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
    Error, MessageBatch, MessageBatchRef, Resource,
};

use crate::expr::{EvaluateResult, Expr};
use crate::kafka_security::KafkaSecurityConfig;
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
    /// L3 exactly-once: name the consumer group of a Kafka input in this
    /// process whose source offsets ride this output's producer
    /// transactions (`send_offsets_to_transaction`). Requires
    /// `exactly_once`; the input must declare `transactional_offsets` with
    /// the same group. Offsets are derived from each batch's
    /// `__meta_partition`/`__meta_offset` columns, so only batches sourced
    /// from a Kafka input carry committable positions.
    offset_commit_group: Option<String>,
    /// SASL authentication and TLS settings (optional; absent means
    /// plaintext, exactly as before this field existed)
    security: Option<KafkaSecurityConfig>,
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
    /// Build the rdkafka `ClientConfig` from the output configuration.
    ///
    /// Extracted from `connect()` (mirroring the input side) so the
    /// transactional and security properties are unit-testable without a
    /// broker.
    fn build_client_config(config: &KafkaOutputConfig) -> Result<ClientConfig, Error> {
        let mut client_config = ClientConfig::new();

        // Configure the Kafka server address
        client_config.set("bootstrap.servers", config.brokers.join(","));

        // Set the client ID
        if let Some(client_id) = &config.client_id {
            client_config.set("client.id", client_id);
        }

        // Set the compression type
        if let Some(compression) = &config.compression {
            client_config.set("compression.type", compression.to_string().to_lowercase());
        }

        // Set the confirmation level (default to "all" for reliability)
        if let Some(acks) = &config.acks {
            client_config.set("acks", acks);
        }

        let exactly_once = config.exactly_once.unwrap_or(false);

        // Configure the transactional producer when exactly_once is enabled.
        // Idempotence is implied by transactional.id but set explicitly.
        if exactly_once {
            client_config.set(
                "transactional.id",
                config.transactional_id.as_ref().expect(
                    "transactional_id presence is validated by the builder when exactly_once is on",
                ),
            );
            client_config.set("enable.idempotence", "true");
        }

        if let Some(security) = &config.security {
            security.apply(&mut client_config)?;
        }

        Ok(client_config)
    }

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
        let client_config = Self::build_client_config(&self.config)?;

        // Create a producer
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Initialize transactions once (blocking broker round-trip).
        if self.config.exactly_once.unwrap_or(false) {
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

        // Payload selection: an explicit `value_field` takes the named
        // column's value per row, otherwise the codec encoding applies.
        let payloads: Vec<Vec<u8>> = if let Some(field) = &self.config.value_field {
            crate::output::payload::field_payloads("kafka", &msg, field)?
        } else {
            crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)
                .await?
                .into_iter()
                .map(|p| p.to_vec())
                .collect()
        };
        if payloads.is_empty() {
            return Ok(());
        }

        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        // Prepare all records for sending
        let payloads_len = payloads.len();
        for (i, x) in payloads.into_iter().enumerate() {
            // Create record. The per-row topic must exist for every row:
            // index panicking here would take the whole stream down, so a
            // short result (a broken row-alignment invariant) is a named
            // error instead.
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x.as_slice()),
                EvaluateResult::Vec(v) => match v.get(i) {
                    Some(t) => FutureRecord::to(t).payload(x.as_slice()),
                    None => {
                        return Err(Error::Process(format!(
                            "Kafka topic expression produced {} values for {} rows (row {i} has no topic)",
                            v.len(),
                            payloads_len,
                        )))
                    }
                },
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

        // L3: fold the covered source offsets into the transaction before
        // committing. Offsets come from the batches' source metadata
        // columns (partition, consumed offset); commit positions are the
        // exclusive next offsets, matching librdkafka's convention.
        if let Some(group) = self.config.offset_commit_group.as_deref() {
            let group_topic = crate::kafka_txn::single_topic(group).ok_or_else(|| {
                Error::Config(format!(
                    "Kafka offset commit group '{group}' does not declare exactly one topic; \
                     transactional offset commits require a single-topic Kafka input"
                ))
            })?;
            let (offsets, covered) =
                transactional_offsets_for_batches(msgs, Some(group_topic.as_str()))?;
            if covered {
                let metadata = crate::kafka_txn::group_metadata(group)
                    .await
                    .ok_or_else(|| {
                        Error::Config(format!(
                        "Kafka offset commit group '{group}' has no live input in this process; \
                         the paired Kafka input must declare transactional_offsets"
                    ))
                    })?;
                let p = producer.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    p.send_offsets_to_transaction(
                        &offsets,
                        &metadata,
                        Timeout::After(Duration::from_secs(30)),
                    )
                })
                .await
                {
                    return Err(Error::Connection(format!(
                        "Kafka send_offsets_to_transaction task join failed: {}",
                        e
                    )));
                }
            }
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
        let payloads: Vec<Vec<u8>> = if let Some(field) = &self.config.value_field {
            crate::output::payload::field_payloads("kafka", &msg, field)?
        } else {
            crate::output::codec_helper::apply_codec_encode(&msg, &self.codec)
                .await?
                .into_iter()
                .map(|p| p.to_vec())
                .collect()
        };
        if payloads.is_empty() {
            return Ok(());
        }
        let topic = self.get_topic(&msg).await?;
        let key = self.get_key(&msg).await?;

        let payloads_len = payloads.len();
        for (i, x) in payloads.into_iter().enumerate() {
            // Same row-alignment guard as the non-transactional path: a
            // short topic result must be a named error, never an index
            // panic inside a transaction.
            let mut record = match &topic {
                EvaluateResult::Scalar(s) => FutureRecord::to(s).payload(x.as_slice()),
                EvaluateResult::Vec(v) => match v.get(i) {
                    Some(t) => FutureRecord::to(t).payload(x.as_slice()),
                    None => {
                        return Err(Error::Process(format!(
                            "Kafka topic expression produced {} values for {} rows (row {i} has no topic)",
                            v.len(),
                            payloads_len,
                        )))
                    }
                },
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
        // L3: offset_commit_group rides on a transactional producer, so it
        // is meaningless (and silently non-functional) without exactly_once.
        if config.offset_commit_group.is_some() && !config.exactly_once.unwrap_or(false) {
            return Err(Error::Config(
                "Kafka output: offset_commit_group requires exactly_once (source offsets \
                 commit inside the producer transaction)"
                    .into(),
            ));
        }

        // Fail before any stream starts on an inconsistent security block
        // (spec: 构建期校验与错误语义) — `--validate` reaches this path.
        if let Some(security) = &config.security {
            security.validate()?;
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
                "topic": {"oneOf": [ {"type": "object", "properties": {"type": {"const": "value"}, "value": {"type": "string"}}, "required": ["type", "value"], "additionalProperties": false}, {"type": "object", "properties": {"type": {"const": "expr"}, "expr": {"type": "string"}}, "required": ["type", "expr"], "additionalProperties": false}
                    ], "description": "Literal value or a SQL expression evaluated per batch."},
                "key": {"oneOf": [ {"type": "object", "properties": {"type": {"const": "value"}, "value": {"type": "string"}}, "required": ["type", "value"], "additionalProperties": false}, {"type": "object", "properties": {"type": {"const": "expr"}, "expr": {"type": "string"}}, "required": ["type", "expr"], "additionalProperties": false}
                    ], "description": "Literal value or a SQL expression evaluated per batch."},
                "client_id": {"type": "string", "description": "Optional client identifier."},
                "compression": {"type": "string", "enum": ["none", "gzip", "snappy", "lz4", "zstd"], "description": "Compression algorithm."},
                "acks": {"type": "string", "enum": ["0", "1", "all"], "description": "Acknowledgment level."},
                "value_field": {"type": "string", "description": "Record field used as the message payload."},
                "exactly_once": {"type": "boolean", "default": false, "description": "Enable exactly-once transactional production (L2)."},
                "transactional_id": {"type": "string", "description": "Transactional id (required when exactly_once is true); must be stable across restarts for zombie fencing."},
                "offset_commit_group": {"type": "string", "description": "L3 exactly-once: consumer group of a paired Kafka input (with transactional_offsets: true) whose source offsets commit inside this output's producer transactions. Requires exactly_once."},
                "security": crate::kafka_security::json_schema()
            },
            "required": ["brokers", "topic"]
        }),
    ).with_example(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topic": {"type": "value", "value": "events"}
    })))
}

/// Derive the transactional offset commit set from the batches' source
/// metadata columns. Returns the topic-partition list (exclusive next
/// offsets) and whether any committable position existed; batches without
/// Kafka source metadata contribute nothing (L3 applies to Kafka→Kafka
/// flows). Rows that carry Kafka position metadata but a source topic other
/// than the group's topic (a fan-in graph merging a second Kafka input into
/// the same batch) fail the write: folding their offsets into the group
/// topic would transactionally skip records the group never consumed.
fn transactional_offsets_for_batches(
    msgs: &[MessageBatchRef],
    group_topic: Option<&str>,
) -> Result<(rdkafka::TopicPartitionList, bool), Error> {
    use arkflow_core::meta_columns;
    let mut offsets = rdkafka::TopicPartitionList::new();
    let mut covered = false;
    for msg in msgs {
        let schema = msg.record_batch().schema();
        let (Some(partition_col), Some(offset_col)) = (
            schema.index_of(meta_columns::PARTITION).ok(),
            schema.index_of(meta_columns::OFFSET).ok(),
        ) else {
            continue;
        };
        let partitions = msg
            .record_batch()
            .column(partition_col)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .ok_or_else(|| {
                Error::Process(format!(
                    "column '{}' must be a UInt32 column for transactional offsets",
                    meta_columns::PARTITION
                ))
            })?;
        let offsets_col = msg
            .record_batch()
            .column(offset_col)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                Error::Process(format!(
                    "column '{}' must be a UInt64 column for transactional offsets",
                    meta_columns::OFFSET
                ))
            })?;
        // Per-row source topics, when the rows carry extended metadata: the
        // Kafka input records each message's topic under the "topic" key of
        // `__meta_ext`.
        let row_topics = msg
            .record_batch()
            .column_by_name(meta_columns::EXT)
            .and_then(|column| {
                column
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::MapArray>()
            });
        use datafusion::arrow::array::Array as _;
        for row in 0..msg.record_batch().num_rows() {
            if partitions.is_null(row) || offsets_col.is_null(row) {
                continue;
            }
            if let (Some(expected), Some(topics)) = (group_topic, row_topics) {
                if let Some(source_topic) = ext_map_entry(topics, row, "topic") {
                    if source_topic != expected {
                        return Err(Error::Process(format!(
                            "transactional offsets: batch row carries source topic \
                             '{source_topic}' but the offset commit group's input is \
                             subscribed to '{expected}'; committing foreign-topic offsets \
                             would skip records the group never consumed (fan-in graphs \
                             must route the second source through its own output)"
                        )));
                    }
                }
            }
            let partition = partitions.value(row) as i32;
            let next_offset = i64::try_from(offsets_col.value(row).saturating_add(1))
                .map_err(|_| Error::Process("Kafka offset overflow".into()))?;
            // Rows validated above all belong to the group's single topic
            // (or carry no row-level topic metadata at all, in which case
            // L3 keeps routing partitions through the group topic).
            let topic = group_topic.unwrap_or_default();
            // Keep the max next-offset per partition (rows arrive ordered,
            // but a merged batch may interleave).
            let updated = match offsets.find_partition(topic, partition) {
                Some(element) => match element.offset() {
                    rdkafka::Offset::Offset(existing) => next_offset.max(existing),
                    _ => next_offset,
                },
                None => next_offset,
            };
            if offsets
                .add_partition_offset(topic, partition, rdkafka::Offset::Offset(updated))
                .is_err()
            {
                return Err(Error::Process(format!(
                    "invalid transactional offset {updated} for topic '{topic}' partition {partition}"
                )));
            }
            covered = true;
        }
    }
    Ok((offsets, covered))
}

/// Read `key` from the row's `__meta_ext` map entries, if the row carries
/// one. Mirrors the kernel's per-row topic attribution on
/// `crate::arkflow_core`'s `batch_topic`.
fn ext_map_entry(
    map: &datafusion::arrow::array::MapArray,
    row: usize,
    key: &str,
) -> Option<String> {
    let entries = map.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()?;
    let offsets = map.offsets();
    let start = offsets.get(row).copied()? as usize;
    let end = offsets.get(row + 1).copied()? as usize;
    (start..end)
        .find_map(|index| (keys.value(index) == key).then(|| values.value(index).to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    fn base_output_config() -> serde_json::Value {
        serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topic": {"type": "value", "value": "events"},
        })
    }

    #[test]
    fn compression_display_and_config_knobs_round_trip() {
        assert_eq!(CompressionType::None.to_string(), "none");
        assert_eq!(CompressionType::Gzip.to_string(), "gzip");
        assert_eq!(CompressionType::Snappy.to_string(), "snappy");
        assert_eq!(CompressionType::Lz4.to_string(), "lz4");

        let mut value = base_output_config();
        value["client_id"] = serde_json::json!("producer-1");
        value["compression"] = serde_json::json!("snappy");
        value["acks"] = serde_json::json!("all");
        let config = serde_json::from_value::<KafkaOutputConfig>(value).unwrap();
        let client_config = KafkaOutput::build_client_config(&config).unwrap();
        let get = |key: &str| client_config.get(key).map(str::to_string);
        assert_eq!(get("bootstrap.servers").as_deref(), Some("127.0.0.1:9092"));
        assert_eq!(get("client.id").as_deref(), Some("producer-1"));
        assert_eq!(get("compression.type").as_deref(), Some("snappy"));
        assert_eq!(get("acks").as_deref(), Some("all"));

        // Defaults: no optional knob is set on the bare config.
        let config = serde_json::from_value::<KafkaOutputConfig>(base_output_config()).unwrap();
        let client_config = KafkaOutput::build_client_config(&config).unwrap();
        assert!(client_config.get("client.id").is_none());
        assert!(client_config.get("compression.type").is_none());
        assert!(client_config.get("acks").is_none());
    }

    #[test]
    fn transactional_settings_map_onto_the_client_config() {
        let mut value = base_output_config();
        value["exactly_once"] = serde_json::json!(true);
        value["transactional_id"] = serde_json::json!("txn-1");
        let config = serde_json::from_value::<KafkaOutputConfig>(value).unwrap();
        let client_config = KafkaOutput::build_client_config(&config).unwrap();
        assert_eq!(
            client_config.get("transactional.id"),
            Some("txn-1")
        );
        assert_eq!(
            client_config.get("enable.idempotence"),
            Some("true")
        );
    }



    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: RefCell::new(vec![]),
        }
    }

    /// Build a batch carrying L3 position metadata (`__meta_partition` /
    /// `__meta_offset`) plus an optional per-row `__meta_ext` map whose
    /// "topic" key attributes each row to its source topic — the shape the
    /// Kafka input produces.
    fn l3_meta_batch(
        partitions: Vec<u32>,
        offsets: Vec<u64>,
        topics: Option<Vec<&str>>,
    ) -> MessageBatchRef {
        use arkflow_core::meta_columns;
        use datafusion::arrow::array::{
            ArrayRef, MapArray, StringArray, StructArray, UInt32Array, UInt64Array,
        };
        use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let mut fields = vec![
            Field::new(meta_columns::PARTITION, DataType::UInt32, false),
            Field::new(meta_columns::OFFSET, DataType::UInt64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(partitions)),
            Arc::new(UInt64Array::from(offsets)),
        ];
        if let Some(topics) = topics {
            use datafusion::arrow::array::Array as _;
            let keys = StringArray::from(vec!["topic"; topics.len()]);
            let values = StringArray::from(topics.clone());
            let entries = StructArray::try_new(
                datafusion::arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]),
                vec![Arc::new(keys), Arc::new(values)],
                None,
            )
            .expect("entries struct");
            let index: Vec<i32> = (0..=topics.len() as i32).collect();
            let map = MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(ScalarBuffer::from(index)),
                entries,
                None,
                false,
            )
            .expect("ext map");
            fields.push(Field::new(meta_columns::EXT, map.data_type().clone(), true));
            columns.push(Arc::new(map));
        }
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )
        .expect("meta batch");
        Arc::new(MessageBatch::new_arrow(batch))
    }

    /// Spec "混入异源 topic 的行显式失败": a fan-in graph merging a second
    /// Kafka input's rows into the same write batch must fail the write
    /// instead of folding foreign-topic offsets into the group topic.
    #[test]
    fn l3_rejects_rows_from_a_foreign_topic() {
        let batch = l3_meta_batch(
            vec![0, 1],
            vec![10, 20],
            Some(vec!["orders", "clickstream"]),
        );
        let err = match transactional_offsets_for_batches(&[batch], Some("orders")) {
            Ok(_) => panic!("foreign-topic rows must fail the write"),
            Err(e) => e,
        };
        let message = err.to_string();
        assert!(
            message.contains("clickstream"),
            "error names the conflict: {message}"
        );
        assert!(
            message.contains("orders"),
            "error names the group topic: {message}"
        );
    }

    /// Rows attributed to the group topic fold into the offset list as
    /// before.
    #[test]
    fn l3_accepts_rows_matching_group_topic() {
        let batch = l3_meta_batch(vec![0, 1], vec![10, 20], Some(vec!["orders", "orders"]));
        let (offsets, covered) =
            transactional_offsets_for_batches(&[batch], Some("orders")).expect("accepted");
        assert!(covered);
        assert_eq!(offsets.count(), 2, "one entry per partition");
        for element in offsets.elements() {
            assert_eq!(element.topic(), "orders");
        }
    }

    /// Batches without the ext metadata column keep the legacy attribution
    /// (partitions route through the group topic).
    #[test]
    fn l3_without_ext_topic_keeps_group_topic_attribution() {
        let batch = l3_meta_batch(vec![3], vec![7], None);
        let (offsets, covered) =
            transactional_offsets_for_batches(&[batch], Some("orders")).expect("accepted");
        assert!(covered);
        assert_eq!(offsets.count(), 1);
        assert_eq!(offsets.elements()[0].topic(), "orders");
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

    fn output_config(json: serde_json::Value) -> KafkaOutputConfig {
        serde_json::from_value(json).expect("valid output config")
    }

    /// Regression (spec: 未配置 security 时保持 plaintext): with no
    /// `security` block the client config must not carry any security/sasl/ssl
    /// property — behaviour is identical to before the field existed.
    #[test]
    fn test_kafka_output_without_security_sets_no_security_properties() {
        let config = output_config(serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"}
        }));
        let client_config = KafkaOutput::build_client_config(&config).unwrap();
        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("localhost:9092")
        );
        for key in client_config.config_map().keys() {
            assert!(
                !key.starts_with("security.")
                    && !key.starts_with("sasl.")
                    && !key.starts_with("ssl."),
                "unexpected security property without a security block: {key}"
            );
        }
    }

    /// Spec: 统一安全配置块 + SASL/TLS 装配 — the same security shape as the
    /// input assembles the same librdkafka properties on the output side.
    #[test]
    fn test_kafka_output_assembles_sasl_ssl_properties() {
        let ca_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
        let config = output_config(serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"},
            "security": {
                "protocol": "sasl_ssl",
                "sasl": {"mechanism": "scram-sha-256", "username": "alice", "password": "secret"},
                "tls": {"ca": ca_pem, "insecure_skip_verify": false}
            }
        }));
        let client_config = KafkaOutput::build_client_config(&config).unwrap();
        assert_eq!(client_config.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(client_config.get("sasl.mechanisms"), Some("SCRAM-SHA-256"));
        assert_eq!(client_config.get("sasl.username"), Some("alice"));
        assert_eq!(client_config.get("sasl.password"), Some("secret"));
        assert_eq!(client_config.get("ssl.ca.pem"), Some(ca_pem));
    }

    /// Spec: 构建期校验与错误语义 — the builder rejects an inconsistent
    /// security block before any component is constructed (offline).
    #[test]
    fn test_kafka_output_builder_rejects_inconsistent_security() {
        let config = serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"},
            "security": {"sasl": {"mechanism": "scram-sha-512", "username": "u"}}
        });
        let err = match KafkaOutputBuilder.build(None, &Some(config), None, &resource()) {
            Ok(_) => panic!("scram without a password must fail at build"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("security.sasl.password"),
            "expected the error to name security.sasl.password, got: {err}"
        );
    }

    /// Spec: expr-row-routing — a topic expression that evaluates to NULL
    /// for some row must fail the batch with the expression and row named.
    /// Before the row-alignment fix the null was silently dropped, the
    /// result vector ran short, and the per-row topic lookup panicked on
    /// the index.
    #[tokio::test]
    async fn test_topic_expression_null_fails_loudly_not_panic() {
        let config = output_config(serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "expr", "expr": "device_topic"}
        }));
        let output = KafkaOutput::new(config, None).unwrap();

        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let rb = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "device_topic",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some("devices/a"),
                None,
                Some("devices/c"),
            ])) as ArrayRef],
        )
        .unwrap();
        let msg = MessageBatch::new_arrow(rb);

        let err = match output.get_topic(&msg).await {
            Ok(_) => panic!("a null topic cell must fail the evaluation"),
            Err(e) => e,
        };
        let text = format!("{err}");
        assert!(
            text.contains("device_topic") && text.contains("row 1"),
            "error must name the expression and the null row, got: {text}"
        );
    }

    // ===== Offline coverage: transaction error mapping, producer lifecycle,
    // and the transactional-offset derivation edge cases. =====

    /// `map_kafka_txn_error` wraps non-transactional errors without any
    /// state classification.
    #[test]
    fn map_kafka_txn_error_wraps_plain_errors() {
        let err = map_kafka_txn_error(KafkaError::Canceled, "begin_transaction");
        assert!(matches!(err, Error::Connection(ref t) if t.contains("begin_transaction failed")));
        let err = map_kafka_txn_error(
            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
            "commit_transaction",
        );
        assert!(
            matches!(err, Error::Connection(ref t) if t.contains("commit_transaction failed")),
            "got: {err}"
        );
    }

    /// Real native `RDKafkaError`s (the only constructible shape — the type
    /// has no public constructor) produced offline by calling transactional
    /// methods on an uninitialized transactional producer with a zero
    /// timeout. Every classification combination the broker-less client can
    /// emit must map to `Error::Connection` naming the failed stage.
    #[tokio::test]
    async fn map_kafka_txn_error_classifies_native_transaction_errors() {
        use rdkafka::producer::Producer;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("transactional.id", "cov-txn-id")
            .create()
            .expect("producer creation is offline");

        // begin_transaction on an uninitialized producer: a native
        // Transaction error with every flag false.
        let begin = tokio::task::spawn_blocking(move || {
            let err = producer
                .begin_transaction()
                .expect_err("uninitialized producer cannot begin");
            let KafkaError::Transaction(rd) = &err else {
                panic!("begin must yield a Transaction error, got: {err}");
            };
            let flags = (rd.is_fatal(), rd.txn_requires_abort(), rd.is_retriable());
            let code = rd.code();
            (err, flags, code)
        })
        .await
        .unwrap();
        let (err, flags, code) = begin;
        assert_eq!(
            flags,
            (false, false, false),
            "the INIT-state error carries no classification flags (code {code:?})"
        );
        let mapped = map_kafka_txn_error(err, "begin_transaction");
        assert!(
            matches!(mapped, Error::Connection(ref t) if t.contains("begin_transaction failed")),
            "got: {mapped}"
        );

        // Zero-timeout init/commit/abort surface native Transaction errors
        // of other kinds; each must map to a Connection error, and the
        // retriable flag (timeouts are retriable) exercises the log branch.
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("transactional.id", "cov-txn-id")
            .create()
            .unwrap();
        let zero = Timeout::After(Duration::ZERO);
        let results = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            for (ctx, result) in [
                (
                    "init_transactions",
                    producer.clone().init_transactions(zero),
                ),
                ("commit_transaction", producer.clone().commit_transaction(zero)),
                ("abort_transaction", producer.abort_transaction(zero)),
            ] {
                let flags = match &result {
                    Err(KafkaError::Transaction(rd)) => {
                        (rd.is_fatal(), rd.txn_requires_abort(), rd.is_retriable())
                    }
                    other => panic!("{ctx} must yield a Transaction error, got: {other:?}"),
                };
                let err = match result {
                    Err(e) => e,
                    Ok(()) => panic!("{ctx} must yield a Transaction error"),
                };
                out.push((ctx, err, flags));
            }
            out
        })
        .await
        .unwrap();
        let mut saw_retriable = false;
        for (ctx, err, flags) in results {
            let mapped = map_kafka_txn_error(err, ctx);
            assert!(
                matches!(mapped, Error::Connection(ref t) if t.contains(&format!("{ctx} failed"))),
                "{ctx}: got {mapped}"
            );
            saw_retriable |= flags.2;
        }
        assert!(
            saw_retriable,
            "the zero-timeout paths must include a retriable native error"
        );
    }

    /// A codec emitting MORE payloads than the topic expression's rows makes
    /// the row-alignment guard fail loudly instead of panicking on the index.
    struct ThreePayloadsCodec;

    #[async_trait]
    impl arkflow_core::codec::Encoder for ThreePayloadsCodec {
        async fn encode(&self, _batch: MessageBatch) -> Result<Vec<arkflow_core::Bytes>, Error> {
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        }
    }

    #[async_trait]
    impl arkflow_core::codec::Decoder for ThreePayloadsCodec {
        async fn decode(&self, _b: Vec<arkflow_core::Bytes>) -> Result<MessageBatch, Error> {
            Err(Error::Process("decode not used in this test".into()))
        }
    }

    fn utf8_batch(columns: &[(&str, Vec<Option<&str>>)]) -> MessageBatchRef {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let fields = columns
            .iter()
            .map(|(name, _)| Field::new(*name, datafusion::arrow::datatypes::DataType::Utf8, true))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|(_, values)| {
                Arc::new(StringArray::from(
                    values.iter().map(|v| v.map(|s| s.to_string())).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect::<Vec<_>>();
        let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            arrays,
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(rb))
    }

    /// A binary-column batch. `column` defaults to the binary codec's
    /// implicit `__value__`; the value_field path names it explicitly.
    fn binary_field_batch(rows: usize) -> MessageBatchRef {
        binary_batch_in(arkflow_core::DEFAULT_BINARY_VALUE_FIELD, rows)
    }

    fn binary_batch_in(column: &str, rows: usize) -> MessageBatchRef {
        use datafusion::arrow::array::{ArrayRef, BinaryArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let values = (0..rows)
            .map(|i| Some(format!("row-{i}").into_bytes()))
            .collect::<Vec<_>>();
        let values = values.iter().map(|v| v.as_deref()).collect::<Vec<_>>();
        let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                column,
                datafusion::arrow::datatypes::DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from_opt_vec(values)) as ArrayRef],
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(rb))
    }

    /// connect() creates the producer offline (librdkafka connects lazily),
    /// and close() on a freshly connected producer drains an empty queue.
    #[tokio::test]
    async fn connect_creates_producer_and_close_drains_it() {
        let output = KafkaOutputBuilder
            .build(
                None,
                &Some(serde_json::json!({
                    "brokers": ["localhost:9092"],
                    "topic": {"type": "value", "value": "t"}
                })),
                None,
                &resource(),
            )
            .unwrap();
        output.connect().await.expect("producer creation is offline");
        output.close().await.expect("empty flush terminates");
        // A second close (no producer) is a no-op.
        output.close().await.unwrap();
    }

    /// write() before connect names the missing producer.
    #[tokio::test]
    async fn write_without_connect_errors() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"}
            })),
            None,
        )
        .unwrap();
        let err = output.write(binary_field_batch(1)).await.unwrap_err();
        assert!(
            err.to_string().contains("not initialized"),
            "got: {err}"
        );
    }

    /// The non-transactional write path, offline: records are enqueued
    /// through `send_result` (delivery reports resolve lazily) for both the
    /// codec path and the `value_field` path, with scalar and per-row keys.
    #[tokio::test]
    async fn write_enqueues_records_for_codec_and_value_field_paths() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "events"},
                "key": {"type": "expr", "expr": "k"}
            })),
            None,
        )
        .unwrap();
        output.connect().await.unwrap();

        // Codec path (no value_field): binary `__value__` rows with a
        // per-row Utf8 key column.
        use datafusion::arrow::array::{ArrayRef, BinaryArray, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", datafusion::arrow::datatypes::DataType::Utf8, true),
                Field::new(
                    arkflow_core::DEFAULT_BINARY_VALUE_FIELD,
                    datafusion::arrow::datatypes::DataType::Binary,
                    true,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("k1"), Some("k2")])) as ArrayRef,
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"v1" as &[u8]),
                    Some(b"v2"),
                ])),
            ],
        )
        .unwrap();
        output
            .write(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .expect("codec path enqueues");

        // value_field path: one payload per row of the named column.
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "events"},
                "key": {"type": "value", "value": "static-key"},
                "value_field": "payload"
            })),
            None,
        )
        .unwrap();
        output.connect().await.unwrap();
        output
            .write(binary_batch_in("payload", 2))
            .await
            .expect("value_field path enqueues");

        // An empty batch short-circuits before any record is built.
        output
            .write(binary_batch_in("payload", 0))
            .await
            .expect("empty batch is a no-op");
        // Do not close() here: unresolved delivery futures would block.
    }

    /// A codec producing more payloads than topic rows trips the
    /// row-alignment guard (previously an index panic).
    #[tokio::test]
    async fn write_fails_loudly_when_topics_run_short() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "expr", "expr": "device_topic"}
            })),
            Some(Arc::new(ThreePayloadsCodec) as Arc<dyn Codec>),
        )
        .unwrap();
        output.connect().await.unwrap();
        let batch = utf8_batch(&[("device_topic", vec![Some("a"), Some("b")])]);
        let err = output.write(batch).await.unwrap_err();
        assert!(
            err.to_string().contains("has no topic"),
            "got: {err}"
        );
    }

    /// write_batch's non-transactional path aggregates per-message failures
    /// (continue-on-error) and returns the last error.
    #[tokio::test]
    async fn write_batch_non_transactional_aggregates_errors() {
        let good = output_config(serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"}
        }));
        let output = KafkaOutput::new(good, None).unwrap();
        output.connect().await.unwrap();
        // All-good batch.
        output
            .write_batch(&[binary_field_batch(1)])
            .await
            .expect("single good message");

        // A message whose value_field column is absent fails; the sibling
        // still goes through.
        let bad = output_config(serde_json::json!({
            "brokers": ["localhost:9092"],
            "topic": {"type": "value", "value": "t"},
            "value_field": "missing_column"
        }));
        let output = KafkaOutput::new(bad, None).unwrap();
        output.connect().await.unwrap();
        let err = output
            .write_batch(&[binary_field_batch(1), binary_field_batch(1)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("value_field"),
            "got: {err}"
        );
    }

    /// The transactional path fails fast offline: begin_transaction on a
    /// producer whose transactions were never initialized maps to a
    /// Connection error naming the stage. The producer is installed
    /// directly (connect()'s init_transactions would block on a broker).
    #[tokio::test]
    async fn write_batch_transactional_fails_fast_before_init() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"},
                "exactly_once": true,
                "transactional_id": "cov-txn"
            })),
            None,
        )
        .unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("transactional.id", "cov-txn")
            .set("message.timeout.ms", "15000")
            .create()
            .unwrap();
        *output.inner_kafka_output.producer.write().await = Some(producer);

        let err = output
            .write_batch(&[binary_field_batch(1)])
            .await
            .expect_err("uninitialized transactions cannot begin");
        assert!(
            err.to_string().contains("begin_transaction failed"),
            "got: {err}"
        );
        // A transactional write before any producer is installed names the
        // missing producer instead.
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"},
                "exactly_once": true,
                "transactional_id": "cov-txn"
            })),
            None,
        )
        .unwrap();
        let err = output
            .write_batch(&[binary_field_batch(1)])
            .await
            .expect_err("no producer");
        assert!(
            err.to_string().contains("not initialized"),
            "got: {err}"
        );
    }

    /// The periodic flush loop runs while the output lives and stops on
    /// close (virtual time auto-advances through the 1s tick).
    #[tokio::test(start_paused = true)]
    async fn periodic_flush_loop_runs_and_stops_on_close() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"}
            })),
            None,
        )
        .unwrap();
        // Two+ flush ticks fire (the queue is empty; flush drains nothing).
        tokio::time::sleep(Duration::from_millis(2500)).await;
        output.close().await.expect("close without producer");
        // After cancellation the loop has exited; another virtual second
        // proves no further work happens (and the test terminates).
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    /// Builder guards: missing config and offset_commit_group without
    /// exactly_once.
    #[test]
    fn builder_rejects_missing_and_inconsistent_configs() {
        let err = KafkaOutputBuilder
            .build(None, &None, None, &resource())
            .err()
            .expect("missing config rejected");
        assert!(err.to_string().contains("configuration is missing"));

        let err = KafkaOutputBuilder
            .build(
                None,
                &Some(serde_json::json!({
                    "brokers": ["localhost:9092"],
                    "topic": {"type": "value", "value": "t"},
                    "offset_commit_group": "g"
                })),
                None,
                &resource(),
            )
            .err()
            .expect("offset_commit_group requires exactly_once");
        assert!(err.to_string().contains("offset_commit_group requires exactly_once"));
    }

    /// Batches without position metadata contribute nothing (covered=false).
    #[test]
    fn l3_plain_batches_contribute_nothing() {
        let batch = utf8_batch(&[("v", vec![Some("x"), Some("y")])]);
        let (offsets, covered) =
            transactional_offsets_for_batches(&[batch], Some("orders")).unwrap();
        assert!(!covered);
        assert_eq!(offsets.count(), 0);
    }

    /// The metadata columns must be UInt32/UInt64 — anything else is a
    /// named error, not a silent skip.
    #[test]
    fn l3_rejects_wrongly_typed_position_columns() {
        use datafusion::arrow::array::{Int64Array, UInt64Array};
        use arkflow_core::meta_columns;
        use datafusion::arrow::datatypes::{Field, Schema};
        let batch = {
            let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new(meta_columns::PARTITION, datafusion::arrow::datatypes::DataType::Int64, false),
                    Field::new(meta_columns::OFFSET, datafusion::arrow::datatypes::DataType::UInt64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![0])),
                    Arc::new(UInt64Array::from(vec![1u64])),
                ],
            )
            .unwrap();
            Arc::new(MessageBatch::new_arrow(rb))
        };
        let err = transactional_offsets_for_batches(&[batch], Some("orders")).unwrap_err();
        assert!(
            err.to_string().contains(meta_columns::PARTITION),
            "got: {err}"
        );

        let batch = {
            let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new(meta_columns::PARTITION, datafusion::arrow::datatypes::DataType::UInt32, false),
                    Field::new(meta_columns::OFFSET, datafusion::arrow::datatypes::DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(datafusion::arrow::array::UInt32Array::from(vec![0u32])),
                    Arc::new(datafusion::arrow::array::StringArray::from(vec!["1"])),
                ],
            )
            .unwrap();
            Arc::new(MessageBatch::new_arrow(rb))
        };
        let err = transactional_offsets_for_batches(&[batch], Some("orders")).unwrap_err();
        assert!(
            err.to_string().contains(meta_columns::OFFSET),
            "got: {err}"
        );
    }

    /// Null position cells are skipped (they carry no committable position).
    #[test]
    fn l3_skips_rows_with_null_positions() {
        use arkflow_core::meta_columns;
        use datafusion::arrow::array::{UInt32Array, UInt64Array};
        use datafusion::arrow::datatypes::{Field, Schema};
        let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(meta_columns::PARTITION, datafusion::arrow::datatypes::DataType::UInt32, true),
                Field::new(meta_columns::OFFSET, datafusion::arrow::datatypes::DataType::UInt64, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![Some(0u32), None])),
                Arc::new(UInt64Array::from(vec![Some(10u64), Some(11u64)])),
            ],
        )
        .unwrap();
        let batch = Arc::new(MessageBatch::new_arrow(rb));
        let (offsets, covered) =
            transactional_offsets_for_batches(&[batch], Some("orders")).unwrap();
        assert!(covered, "the one positioned row still commits");
        assert_eq!(offsets.count(), 1);
        assert_eq!(offsets.elements()[0].offset(), rdkafka::Offset::Offset(11));
    }

    /// An offset at u64::MAX overflows the i64 commit position.
    #[test]
    fn l3_rejects_positions_that_overflow_i64() {
        use arkflow_core::meta_columns;
        use datafusion::arrow::array::{UInt32Array, UInt64Array};
        use datafusion::arrow::datatypes::{Field, Schema};
        let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(meta_columns::PARTITION, datafusion::arrow::datatypes::DataType::UInt32, false),
                Field::new(meta_columns::OFFSET, datafusion::arrow::datatypes::DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();
        let batch = Arc::new(MessageBatch::new_arrow(rb));
        let err = transactional_offsets_for_batches(&[batch], Some("orders")).unwrap_err();
        assert!(
            err.to_string().contains("overflow"),
            "got: {err}"
        );
    }

    /// Duplicate rows for one partition fold to the max next-offset; a
    /// partition that does not fit i32 lands on rdkafka's "all partitions"
    /// sentinel (-1) rather than an invalid list entry.
    #[test]
    fn l3_folds_duplicate_partitions_and_handles_oversized_partitions() {
        use arkflow_core::meta_columns;
        use datafusion::arrow::array::{UInt32Array, UInt64Array};
        use datafusion::arrow::datatypes::{Field, Schema};

        let build = |partitions: Vec<u32>, offsets: Vec<u64>| {
            let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new(meta_columns::PARTITION, datafusion::arrow::datatypes::DataType::UInt32, false),
                    Field::new(meta_columns::OFFSET, datafusion::arrow::datatypes::DataType::UInt64, false),
                ])),
                vec![
                    Arc::new(UInt32Array::from(partitions)),
                    Arc::new(UInt64Array::from(offsets)),
                ],
            )
            .unwrap();
            Arc::new(MessageBatch::new_arrow(rb))
        };

        // Two rows on partition 2: the max next-offset must be present.
        // (rdkafka's add appends one element per call, so the list carries
        // both the initial 11 and the folded 31 — observed behavior.)
        let (offsets, covered) = transactional_offsets_for_batches(
            &[build(vec![2, 2], vec![10, 30])],
            Some("orders"),
        )
        .unwrap();
        assert!(covered);
        let committed: Vec<rdkafka::Offset> =
            offsets.elements().iter().map(|e| e.offset()).collect();
        assert!(
            committed.contains(&rdkafka::Offset::Offset(31)),
            "the max next-offset per partition is kept: {committed:?}"
        );

        // u32::MAX as a partition becomes i32 -1 — rdkafka's sentinel for
        // "all partitions" — and is accepted as-is.
        let (offsets, covered) = transactional_offsets_for_batches(
            &[build(vec![u32::MAX], vec![1])],
            Some("orders"),
        )
        .unwrap();
        assert!(covered);
        assert_eq!(offsets.elements()[0].partition(), -1);

        // Without a group topic the rows fold into the empty topic name.
        let (offsets, covered) =
            transactional_offsets_for_batches(&[build(vec![0], vec![5])], None).unwrap();
        assert!(covered);
        assert_eq!(offsets.elements()[0].topic(), "");
    }

    /// close() drains queued delivery futures: with a short message timeout
    /// the broker-less producer resolves them as delivery failures (the
    /// `Ok(Err(..))` arm), then the final flush completes over an empty
    /// queue.
    #[tokio::test]
    async fn close_drains_delivery_futures_that_failed_on_timeout() {
        let output = KafkaOutput::new(
            output_config(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"}
            })),
            None,
        )
        .unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "1000")
            .create()
            .unwrap();
        *output.inner_kafka_output.producer.write().await = Some(producer);

        // Two records → two delivery futures in the queue.
        output
            .write(binary_field_batch(2))
            .await
            .expect("records enqueue");
        // Let the message timeout resolve the futures as failures.
        tokio::time::sleep(Duration::from_millis(1800)).await;
        output.close().await.expect("drain and flush complete");
    }

    /// An inconsistent TLS material block fails producer creation at
    /// `connect` time, offline (the SSL context is built eagerly).
    #[tokio::test]
    async fn connect_maps_producer_creation_failures() {
        let output = match KafkaOutputBuilder.build(
            None,
            &Some(serde_json::json!({
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "t"},
                "security": {"protocol": "ssl", "tls": {"ca": "definitely-not-a-pem"}}
            })),
            None,
            &resource(),
        ) {
            Ok(output) => output,
            Err(e) => panic!("the security block is consistent, build must pass: {e}"),
        };
        match output.connect().await {
            Err(e) => assert!(
                e.to_string().contains("cannot be created"),
                "got: {e}"
            ),
            Ok(()) => panic!("a garbage CA PEM must fail producer creation"),
        }
    }

    /// flush() reports a delivery future cancelled by the producer's drop
    /// (the `Err` arm) without panicking.
    #[tokio::test]
    async fn flush_reports_delivery_futures_cancelled_by_producer_drop() {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "60000")
            .create()
            .unwrap();
        let payload = vec![1u8];
        let future = producer
            .send_result(FutureRecord::<String, Vec<u8>>::to("t").payload(&payload))
            .expect("enqueue");
        let inner = InnerKafkaOutput {
            producer: Arc::new(RwLock::new(None)),
            send_futures: Arc::new(Mutex::new(vec![future])),
        };
        // Dropping the producer cancels its outstanding delivery callbacks.
        drop(producer);
        tokio::time::timeout(Duration::from_secs(10), inner.flush())
            .await
            .expect("a cancelled future resolves promptly")
            // The outcome is logged either way; flush itself never errors.
    }
}
