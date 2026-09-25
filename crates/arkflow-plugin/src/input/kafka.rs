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

use arkflow_core::checkpoint::SourcePosition;
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::event_time::EventTimePartition;
use arkflow_core::executor::commit::{AckAdvance, CommitFrontier};
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{metadata, Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;

use crate::kafka_security::KafkaSecurityConfig;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers as KafkaHeaders, Message as KafkaMessage, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;

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
    /// Fetch min bytes
    pub fetch_min_bytes: Option<u32>,
    /// Fetch max bytes
    pub fetch_max_bytes: Option<u32>,
    /// Fetch max partition bytes
    pub fetch_max_partition_bytes: Option<u32>,
    /// Fetch wait max milliseconds
    pub fetch_wait_max_ms: Option<u64>,
    /// SASL authentication and TLS settings (optional; absent means
    /// plaintext, exactly as before this field existed)
    pub security: Option<KafkaSecurityConfig>,
    /// L3 exactly-once: delegate broker offset commits to a transactional
    /// Kafka output's `send_offsets_to_transaction` (the output declares
    /// `offset_commit_group` naming this consumer group). The input still
    /// advances its in-memory frontier and publishes its consumer-group
    /// metadata for the output's transactions, but suppresses its own
    /// `store_offset` calls so the broker's committed position only ever
    /// advances inside an output transaction.
    #[serde(default)]
    pub transactional_offsets: bool,
}

/// Kafka input component
pub struct KafkaInput {
    input_name: Option<String>,
    config: KafkaInputConfig,
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
    assigned_partition: Arc<RwLock<Option<u32>>>,
    /// Acknowledged-position frontier: per topic-partition contiguous
    /// next-offsets. `current_positions` (and therefore every checkpoint)
    /// exposes only the contiguous acknowledged run — a maximum observed
    /// offset would silently skip unacknowledged records in a gap.
    frontier: Arc<CommitFrontier>,
    /// Serialize frontier advancement and broker `store_offset` calls. A
    /// concurrent fan-out ack must not issue an older broker write after a
    /// newer one and regress the committed offset.
    ack_lock: Arc<tokio::sync::Mutex<()>>,
    /// Wakes acknowledgements waiting for an earlier offset to close the
    /// contiguous frontier gap.
    ack_notify: Arc<Notify>,
    /// Cancels frontier waiters before the consumer is torn down.
    close: CancellationToken,
    codec: Option<Arc<dyn Codec>>,
    /// L3 bridge slot: when `transactional_offsets` is enabled, the live
    /// consumer-group metadata lands here for transactional outputs to
    /// commit offsets inside their producer transactions.
    txn_metadata: Option<crate::kafka_txn::SharedMetadata>,
}

impl KafkaInput {
    fn validate_checkpoint_offset(offset: u64, low: i64, high: i64) -> Result<i64, Error> {
        let offset = i64::try_from(offset)
            .map_err(|_| Error::Config("Kafka checkpoint offset exceeds i64".into()))?;
        if offset < low || offset > high {
            return Err(Error::Process(format!(
                "Kafka checkpoint offset {offset} is outside broker range {low}..={high}"
            )));
        }
        Ok(offset)
    }

    /// Create a new Kafka input component
    pub fn new(
        name: Option<&String>,
        config: KafkaInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let txn_metadata = if config.transactional_offsets {
            Some(crate::kafka_txn::register_group(
                &config.consumer_group,
                config.topics.clone(),
            ))
        } else {
            None
        };
        Ok(Self {
            input_name: name.cloned(),
            config,
            consumer: Arc::new(RwLock::new(None)),
            assigned_partition: Arc::new(RwLock::new(None)),
            frontier: Arc::new(CommitFrontier::new()),
            ack_lock: Arc::new(tokio::sync::Mutex::new(())),
            ack_notify: Arc::new(Notify::new()),
            close: CancellationToken::new(),
            codec,
            txn_metadata,
        })
    }

    fn retryable_receive_error(error: &KafkaError) -> bool {
        if matches!(error, KafkaError::Canceled) {
            return true;
        }
        let Some(code) = (match error {
            KafkaError::MessageConsumption(code)
            | KafkaError::ConsumerQueueClose(code)
            | KafkaError::Global(code) => Some(*code),
            _ => None,
        }) else {
            return false;
        };
        matches!(
            code,
            RDKafkaErrorCode::TimedOutQueue
                | RDKafkaErrorCode::Retry
                | RDKafkaErrorCode::UnknownBroker
                | RDKafkaErrorCode::AssignmentLost
                | RDKafkaErrorCode::BrokerDestroy
                | RDKafkaErrorCode::DestroyBroker
                | RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::Resolve
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::OperationTimedOut
                | RDKafkaErrorCode::WaitingForCoordinator
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::RequestTimedOut
                | RDKafkaErrorCode::BrokerNotAvailable
                | RDKafkaErrorCode::ReplicaNotAvailable
                | RDKafkaErrorCode::NetworkException
                | RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::CoordinatorNotAvailable
                | RDKafkaErrorCode::NotCoordinator
                | RDKafkaErrorCode::RebalanceInProgress
                | RDKafkaErrorCode::ReassignmentInProgress
                | RDKafkaErrorCode::FetchSessionIdNotFound
                | RDKafkaErrorCode::InvalidFetchSessionEpoch
                | RDKafkaErrorCode::FencedLeaderEpoch
                | RDKafkaErrorCode::UnknownLeaderEpoch
                | RDKafkaErrorCode::StaleBrokerEpoch
                | RDKafkaErrorCode::OffsetNotAvailable
        )
    }

    /// Merge checkpoint positions into the COMPLETE configured assignment for
    /// one explicitly-assigned partition. Configured topics whose partitions
    /// the checkpoint omitted keep their configured starting behavior — a
    /// subset checkpoint must never unassign a configured partition.
    fn merged_restore_assignment(
        topics: &[String],
        assigned_partition: u32,
        positions: &[SourcePosition],
        start_from_latest: bool,
    ) -> TopicPartitionList {
        let mut assignment = TopicPartitionList::new();
        for topic in topics {
            let restored = positions.iter().find(|position| {
                position.topic.as_deref() == Some(topic.as_str())
                    && position.partition == assigned_partition
            });
            let offset = match restored {
                Some(position) => Offset::Offset(position.offset as i64),
                None if start_from_latest => Offset::End,
                None => Offset::Beginning,
            };
            let _ = assignment.add_partition_offset(topic, assigned_partition as i32, offset);
        }
        assignment
    }

    /// The checkpoint positions applicable to this reader: in
    /// explicit-partition mode only its own partition, in subscription mode
    /// every configured topic's matching partitions.
    fn applicable_positions(
        &self,
        positions: &[SourcePosition],
    ) -> Result<Vec<SourcePosition>, Error> {
        let assigned = *self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?;
        Ok(positions
            .iter()
            .filter(|position| {
                self.config
                    .topics
                    .iter()
                    .any(|topic| position.topic.as_deref() == Some(topic.as_str()))
                    && assigned.is_none_or(|partition| position.partition == partition)
            })
            .cloned()
            .collect())
    }
    /// Convert Kafka timestamps to SystemTime
    fn convert_kafka_timestamp(millis_since_epoch: i64) -> Option<SystemTime> {
        if millis_since_epoch < 0 {
            return None;
        }

        let millis_u64 = u64::try_from(millis_since_epoch).ok()?;
        let duration = std::time::Duration::from_millis(millis_u64);
        SystemTime::UNIX_EPOCH.checked_add(duration)
    }

    /// Build the rdkafka `ClientConfig` from the input configuration.
    ///
    /// Extracted from `connect()` so the crash-safety settings (notably
    /// `enable.auto.offset.store=false`) and the security properties are
    /// unit-testable without a broker.
    fn build_client_config(&self) -> Result<ClientConfig, Error> {
        let mut client_config = ClientConfig::new();

        // Configure the Kafka server address
        client_config.set("bootstrap.servers", self.config.brokers.join(","));

        // Set the consumer group ID
        client_config.set("group.id", &self.config.consumer_group);

        // Set the client ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // Set the fetch min bytes
        if let Some(fetch_min_bytes) = self.config.fetch_min_bytes {
            client_config.set("fetch.min.bytes", fetch_min_bytes.to_string());
        }
        // Set the fetch max bytes
        if let Some(fetch_max_bytes) = self.config.fetch_max_bytes {
            client_config.set("fetch.max.bytes", fetch_max_bytes.to_string());
        }
        // Set the fetch max partition bytes
        if let Some(fetch_max_partition_bytes) = self.config.fetch_max_partition_bytes {
            client_config.set(
                "max.partition.fetch.bytes",
                fetch_max_partition_bytes.to_string(),
            );
        }
        // Set the fetch max wait
        if let Some(fetch_wait_max_ms) = self.config.fetch_wait_max_ms {
            client_config.set("fetch.wait.max.ms", fetch_wait_max_ms.to_string());
        }

        // Set the offset reset policy
        if self.config.start_from_latest {
            client_config.set("auto.offset.reset", "latest");
        } else {
            client_config.set("auto.offset.reset", "earliest");
        }

        // Disable automatic offset storage so offsets are NOT advanced when a
        // message is merely delivered to the application. With the default
        // (`enable.auto.offset.store=true`) every `recv()` would store its
        // offset, and the periodic auto-commit would then commit it to the
        // broker before the downstream output has confirmed the write — a
        // crash in between loses the message.
        //
        // Disabling it makes `store_offset()` (called in `KafkaAck::ack()`,
        // which only fires after a successful `output.write()`) the sole way
        // to advance the offset, giving at-least-once delivery across crashes.
        // The periodic auto-commit (`enable.auto.commit=true`, the default)
        // still runs, but it only commits offsets that have been explicitly
        // stored — i.e. only acked messages.
        client_config.set("enable.auto.offset.store", "false");

        if let Some(security) = &self.config.security {
            security.apply(&mut client_config)?;
        }

        Ok(client_config)
    }
}

#[async_trait]
impl Input for KafkaInput {
    async fn connect(&self) -> Result<(), Error> {
        let client_config = self.build_client_config()?;

        // Create consumers
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("Unable to create a Kafka consumer: {}", e)))?;

        if let Some(partition) = *self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?
        {
            // Build the explicit assignment from the CONTIGUOUS ACKNOWLEDGED
            // frontier. On the first connect the frontier is empty and the
            // configured start applies (End/Beginning mirrors the
            // `auto.offset.reset` policy). On a reconnect — the only path
            // that re-enters `connect()` while a cursor exists — the
            // acknowledged frontier becomes explicit offsets: without them
            // librdkafka would apply `auto.offset.reset`, which either skips
            // every record produced during the outage (`latest`) or replays
            // the whole retained log (`earliest`).
            let assignment = Self::merged_restore_assignment(
                &self.config.topics,
                partition,
                &self.frontier.contiguous_positions(),
                self.config.start_from_latest,
            );
            consumer.assign(&assignment).map_err(|e| {
                Error::Connection(format!("You cannot assign Kafka partitions: {}", e))
            })?;
        } else {
            // Subscribe to all partitions for the legacy single-reader path.
            let x: Vec<&str> = self
                .config
                .topics
                .iter()
                .map(|topic| topic.as_str())
                .collect();
            consumer.subscribe(&x).map_err(|e| {
                Error::Connection(format!("You cannot subscribe to a Kafka topic: {}", e))
            })?;
        }

        // Update consumer and connection status
        let consumer_arc = self.consumer.clone();
        let mut consumer_guard = consumer_arc.write().await;
        *consumer_guard = Some(consumer);

        // L3 bridge: publish the live group metadata for transactional
        // outputs. Group metadata is only meaningful once the consumer has
        // joined the group, which `create` + subscribe/assign initiates; the
        // broker accepts the metadata carried in the transaction regardless
        // of join timing on the same client instance.
        if self.config.transactional_offsets {
            let slot = self
                .txn_metadata
                .clone()
                .expect("transactional_offsets implies a registered slot");
            if let Some(consumer) = consumer_guard.as_ref() {
                match consumer.group_metadata() {
                    Some(metadata) => {
                        *slot.write().await = Some(std::sync::Arc::new(metadata));
                    }
                    None => {
                        return Err(Error::Connection(
                            "Kafka consumer group metadata unavailable for transactional offsets"
                                .into(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let consumer_arc = self.consumer.clone();
        let consumer_guard = consumer_arc.read().await;
        if consumer_guard.is_none() {
            return Err(Error::Connection("The input is not connected".to_string()));
        }
        let consumer = consumer_guard.as_ref().unwrap();

        loop {
            match consumer.recv().await {
                Ok(kafka_message) => {
                    // Compacted topics deliver deletion markers with a null
                    // payload. They are ordinary Kafka data: settle them here.
                    // Treating them as a fatal error re-delivers the same
                    // tombstone after every restart and crashes the stream in
                    // a loop; skipping without an acknowledgement would replay
                    // it forever because the acknowledged frontier never moves
                    // past it.
                    //
                    // The settlement is handed to its own task rather than
                    // awaited inline: an acknowledgement can wait for a
                    // partition (re)assignment, and `read` is this source's only
                    // path for records AND injected control events, so blocking
                    // it would stall checkpoints and watermarks. A settlement
                    // failure still surfaces as the frontier failure fence that
                    // the next acknowledged delivery reports, which is the same
                    // retry contract a forwarded delivery gets.
                    let Some(payload) = kafka_message.payload() else {
                        let close_for_retry = self.close.clone();
                        let ack = KafkaAck {
                            consumer: self.consumer.clone(),
                            frontier: self.frontier.clone(),
                            ack_lock: self.ack_lock.clone(),
                            ack_notify: self.ack_notify.clone(),
                            close: self.close.clone(),
                            topic: kafka_message.topic().to_string(),
                            partition: kafka_message.partition(),
                            offset: kafka_message.offset(),
                            transactional_offsets: self.config.transactional_offsets,
                        };
                        self.frontier.anchor_delivery(&SourcePosition {
                            topic: Some(kafka_message.topic().to_string()),
                            partition: kafka_message.partition() as u32,
                            offset: kafka_message.offset() as u64,
                        });
                        tokio::spawn(async move {
                            // Retry inside the task: a settlement that leaves
                            // the frontier short of this offset blocks every
                            // later acknowledgement of the partition behind a
                            // gap that can no longer close, and no redelivery
                            // retries it (the tombstone is not forwarded).
                            for attempt in 0..4 {
                                if let Ok(()) = ack.ack().await {
                                    return;
                                }
                                if close_for_retry.is_cancelled() {
                                    return;
                                }
                                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1)))
                                    .await;
                            }
                            tracing::warn!(
                                "Kafka tombstone settlement failed after retries; the frontier fence reports it to the next acknowledgement"
                            );
                        });
                        continue;
                    };

                    // Apply codec if configured
                    let mut msg_batch =
                        crate::input::codec_helper::apply_codec_to_payload(payload, &self.codec)
                            .await?;
                    msg_batch.set_input_name(self.input_name.clone());

                    // Convert to RecordBatch to add metadata
                    let mut record_batch: datafusion::arrow::record_batch::RecordBatch =
                        msg_batch.into();

                    // Add core metadata
                    record_batch = metadata::with_source(record_batch, "kafka")?;

                    let partition = kafka_message.partition();
                    record_batch = metadata::with_partition(record_batch, partition as u32)?;

                    let offset = kafka_message.offset();
                    record_batch = metadata::with_offset(record_batch, offset as u64)?;

                    // Add key if present
                    if let Some(key) = kafka_message.key() {
                        record_batch = metadata::with_key(record_batch, key)?;
                    }

                    // Add timestamp if available
                    let kafka_timestamp = kafka_message.timestamp();
                    if let Timestamp::CreateTime(millis_since_epoch) = kafka_timestamp {
                        if let Some(timestamp) = Self::convert_kafka_timestamp(millis_since_epoch) {
                            record_batch = metadata::with_timestamp(record_batch, timestamp)?;
                        }
                    }
                    // Add ingest time
                    let ingest_time = SystemTime::now();
                    record_batch = metadata::with_ingest_time(record_batch, ingest_time)?;

                    // Add extended metadata (topic, headers)
                    let topic = kafka_message.topic().to_string();

                    // Anchor the partition's frontier at this delivery: an
                    // out-of-order FIRST acknowledgement (fan-out completing a
                    // later branch first) cannot then claim the earlier records
                    // of this delivery were acknowledged.
                    self.frontier.anchor_delivery(&SourcePosition {
                        topic: Some(topic.clone()),
                        partition: kafka_message.partition() as u32,
                        offset: kafka_message.offset() as u64,
                    });

                    let mut ext_metadata = HashMap::new();
                    ext_metadata.insert("topic".to_string(), topic);

                    // Extract Kafka message headers into extended metadata.
                    // Each header key becomes `header_<key>` in the metadata
                    // map, preserving the original key inside the value's key
                    // namespace for downstream routing/filtering.
                    if let Some(headers) = kafka_message.headers() {
                        for (key, value) in header_metadata(headers) {
                            ext_metadata.insert(key, value);
                        }
                    }

                    record_batch = metadata::with_ext_metadata(record_batch, &ext_metadata)?;

                    // Convert back to MessageBatch
                    let mut msg_batch = MessageBatch::new_arrow(record_batch);
                    msg_batch.set_input_name(self.input_name.clone());

                    // Create acknowledgment object
                    let ack = KafkaAck {
                        consumer: self.consumer.clone(),
                        frontier: self.frontier.clone(),
                        ack_lock: self.ack_lock.clone(),
                        ack_notify: self.ack_notify.clone(),
                        close: self.close.clone(),
                        topic: kafka_message.topic().to_string(),
                        partition,
                        offset,
                            transactional_offsets: self.config.transactional_offsets,
                        };

                    return Ok((Arc::new(msg_batch), Arc::new(ack)));
                }
                Err(e) if Self::retryable_receive_error(&e) => return Err(Error::Disconnection),
                Err(e) => {
                    return Err(Error::Connection(format!(
                        "Error receiving Kafka message: {}",
                        e
                    )))
                }
            }
        }
    }

    async fn current_positions(&self) -> Result<Vec<SourcePosition>, Error> {
        // Only the contiguous acknowledged frontier: a gap (fan-out
        // acknowledgement still pending) holds the position back so a
        // checkpoint never skips unacknowledged records.
        Ok(self.frontier.contiguous_positions())
    }

    async fn watermark_partitions(&self) -> Result<Vec<EventTimePartition>, Error> {
        let assigned = *self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?;
        if let Some(partition) = assigned {
            return Ok(self
                .config
                .topics
                .iter()
                .map(|topic| EventTimePartition::new(Some(topic.clone()), partition))
                .collect());
        }

        let consumer_guard = self.consumer.read().await;
        let Some(consumer) = consumer_guard.as_ref() else {
            return Err(Error::Connection("The input is not connected".into()));
        };
        let assignment = consumer
            .assignment()
            .map_err(|error| Error::Connection(format!("read Kafka assignment: {error}")))?;
        Ok(assignment
            .elements()
            .into_iter()
            .filter_map(|element| {
                (element.partition() >= 0).then(|| {
                    EventTimePartition::new(
                        Some(element.topic().to_owned()),
                        element.partition() as u32,
                    )
                })
            })
            .collect())
    }

    async fn ack_for_position(
        &self,
        position: &SourcePosition,
    ) -> Result<Option<Arc<dyn Ack>>, Error> {
        if position.offset == 0 {
            return Ok(None);
        }
        if !self
            .config
            .topics
            .iter()
            .any(|topic| position.topic.as_deref() == Some(topic.as_str()))
        {
            return Ok(None);
        }
        if self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?
            .is_some_and(|partition| partition != position.partition)
        {
            return Ok(None);
        }
        let topic = position
            .topic
            .clone()
            .ok_or_else(|| Error::Process("Kafka source position is missing its topic".into()))?;
        // WAL replay bypasses `read()`, so there was no opportunity to anchor
        // the first delivered record in the in-memory frontier.  Anchor at
        // the record offset (the checkpoint position is exclusive); otherwise
        // an out-of-order replay acknowledgement can be mistaken for the
        // first contiguous offset and skip an earlier replayed record.
        self.frontier.anchor_delivery(&SourcePosition {
            topic: Some(topic.clone()),
            partition: position.partition,
            offset: position.offset.saturating_sub(1),
        });
        let offset = i64::try_from(position.offset.saturating_sub(1))
            .map_err(|_| Error::Process("Kafka source position exceeds i64".into()))?;
        Ok(Some(Arc::new(KafkaAck {
            consumer: self.consumer.clone(),
            frontier: self.frontier.clone(),
            ack_lock: self.ack_lock.clone(),
            ack_notify: self.ack_notify.clone(),
            close: self.close.clone(),
            topic,
            partition: position.partition as i32,
            offset,
                            transactional_offsets: self.config.transactional_offsets,
                        })))
    }

    async fn restore_positions(&self, positions: &[SourcePosition]) -> Result<(), Error> {
        let assigned_partition = *self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?;
        let applicable = self.applicable_positions(positions)?;
        let consumer_guard = self.consumer.read().await;
        let Some(consumer) = consumer_guard.as_ref() else {
            return Err(Error::Process(
                "cannot restore Kafka positions before connect".into(),
            ));
        };
        // Validate every restored offset against the broker's watermarks so a
        // stale or out-of-range checkpoint fails the recovery here, keeping
        // the previous valid checkpoint selected.
        for position in &applicable {
            let (low, high) = consumer
                .fetch_watermarks(
                    position.topic.as_deref().unwrap_or_default(),
                    position.partition as i32,
                    Duration::from_secs(10),
                )
                .map_err(|error| {
                    Error::Process(format!(
                        "fetch Kafka watermarks for {}-{}: {error}",
                        position.topic.as_deref().unwrap_or_default(),
                        position.partition
                    ))
                })?;
            Self::validate_checkpoint_offset(position.offset, low, high)?;
        }
        match assigned_partition {
            Some(partition) => {
                // Explicit-partition mode: the checkpoint is merged into the
                // complete configured assignment. Omitted configured
                // partitions keep their configured start — a subset
                // checkpoint never replaces the assignment with a subset.
                let assignment = Self::merged_restore_assignment(
                    &self.config.topics,
                    partition,
                    positions,
                    self.config.start_from_latest,
                );
                consumer
                    .assign(&assignment)
                    .map_err(|error| Error::Process(format!("restore Kafka positions: {error}")))?;
            }
            None => {
                // Subscription mode: keep the full subscription; seek each
                // checkpointed partition to its offset. A partition the group
                // has not assigned yet rejects the seek right after
                // (re)connect — wait (bounded) for the assignment instead of
                // burning the retry budget while the rebalance is still in
                // flight, then re-check before every seek.
                for position in &applicable {
                    let offset =
                        Self::validate_checkpoint_offset(position.offset, i64::MIN, i64::MAX)?;
                    let topic = position.topic.as_deref().unwrap_or_default();
                    let partition = position.partition as i32;
                    let deadline = tokio::time::Instant::now() + KAFKA_ASSIGNMENT_WAIT;
                    let mut attempt = 0_u32;
                    loop {
                        if !KafkaAck::partition_assigned(consumer, topic, partition) {
                            if tokio::time::Instant::now() >= deadline {
                                return Err(Error::Process(format!(
                                    "restore Kafka position for {topic}-{partition}: partition was not assigned within {}s",
                                    KAFKA_ASSIGNMENT_WAIT.as_secs()
                                )));
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                        match consumer.seek(
                            topic,
                            partition,
                            Offset::Offset(offset),
                            Duration::from_secs(5),
                        ) {
                            Ok(()) => break,
                            Err(error) if tokio::time::Instant::now() < deadline => {
                                attempt += 1;
                                tracing::warn!(
                                    %error, topic, partition,
                                    "Kafka restore seek failed; waiting for group assignment"
                                );
                                tokio::time::sleep(
                                    Duration::from_millis(200 * attempt as u64)
                                        .min(Duration::from_secs(1)),
                                )
                                .await;
                            }
                            Err(error) => {
                                return Err(Error::Process(format!(
                                    "restore Kafka position for {topic}-{partition}: {error}"
                                )));
                            }
                        }
                    }
                }
            }
        }
        // Seed the in-memory acknowledged frontier: a checkpoint taken
        // immediately after restore (before any new acknowledgement) still
        // reports the restored cursor instead of an empty position set.
        self.frontier.seed(&applicable);
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        self.ack_notify.notify_waiters();
        let mut consumer_guard = self.consumer.write().await;
        if let Some(consumer) = consumer_guard.take() {
            if let Err(e) = consumer.unassign() {
                tracing::warn!("Error unassigning Kafka consumer: {}", e);
            }
        }
        Ok(())
    }

    fn assign_partition(&self, partition: u32) -> Result<(), Error> {
        let mut assigned = self
            .assigned_partition
            .try_write()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?;
        *assigned = Some(partition);
        Ok(())
    }

    fn supports_partitioning(&self) -> bool {
        true
    }
}

/// Kafka message acknowledgment
pub struct KafkaAck {
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
    frontier: Arc<CommitFrontier>,
    ack_lock: Arc<tokio::sync::Mutex<()>>,
    ack_notify: Arc<Notify>,
    close: CancellationToken,
    topic: String,
    partition: i32,
    offset: i64,
    /// L3: the broker offset commit rides the transactional output's
    /// producer transaction instead of this consumer's `store_offset`.
    transactional_offsets: bool,
}

/// How long an in-flight acknowledgement waits for the consumer to (re)gain
/// its partition assignment before settling locally without a broker offset
/// store. Covers a subscription-mode group (re)join after a reconnect or a
/// rebalance.
const KAFKA_ASSIGNMENT_WAIT: Duration = Duration::from_secs(60);

impl KafkaAck {
    /// Whether the consumer currently owns `partition` of `topic`.
    fn partition_assigned(consumer: &StreamConsumer, topic: &str, partition: i32) -> bool {
        consumer
            .assignment()
            .map(|assignment| {
                assignment
                    .elements()
                    .iter()
                    .any(|element| element.topic() == topic && element.partition() == partition)
            })
            .unwrap_or(false)
    }
}

#[async_trait]
impl Ack for KafkaAck {
    async fn ack(&self) -> Result<(), Error> {
        let position = SourcePosition {
            topic: Some(self.topic.clone()),
            partition: self.partition.max(0) as u32,
            offset: u64::try_from(self.offset.saturating_add(1))
                .map_err(|_| Error::Process("Kafka offset overflow".into()))?,
        };
        loop {
            // Register the notification before inspecting the frontier. If
            // the gap-closing ack completes between these two operations, the
            // Notify permit is retained and this wait still wakes.
            let notified = self.ack_notify.notified();
            // A reconnect or rebalance can leave this in-flight acknowledgement
            // racing the partition (re)assignment. `store_offset` for an
            // unassigned partition fails and the failure fence would fail the
            // stream, so wait for the assignment — but NEVER inside the lock
            // scope below: that lock serializes every acknowledgement of this
            // input, and the consumer read guard blocks the `connect` that
            // installs the consumer whose assignment this wait is watching for.
            // Holding either one across the wait wedges unrelated partitions
            // (and the checkpoint drain behind them) until it expires.
            // Poll the assignment with SHORT read-lock acquisitions instead of
            // waiting while holding the guard: waiting under the read lock (up
            // to the 60s bound) would block the `connect()` write lock that
            // installs the very consumer whose assignment this wait is
            // watching for, and wedge unrelated partitions behind the consumer
            // lock. Each iteration releases the lock, so a reconnect
            // interleaves immediately and the wait follows the CURRENT
            // consumer.
            let wait_deadline = tokio::time::Instant::now() + KAFKA_ASSIGNMENT_WAIT;
            let assigned = loop {
                let assigned_now = {
                    let consumer_guard = self.consumer.read().await;
                    match consumer_guard.as_ref() {
                        Some(consumer) => {
                            Self::partition_assigned(consumer, &self.topic, self.partition)
                        }
                        // No consumer at all: the acknowledgement retries
                        // below with an explicit error instead of waiting.
                        None => true,
                    }
                };
                if assigned_now {
                    break true;
                }
                if self.close.is_cancelled() || tokio::time::Instant::now() >= wait_deadline {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            if !assigned {
                tracing::warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    offset = position.offset,
                    "Kafka partition is no longer assigned; acknowledging without a broker offset store (the record may be delivered again)"
                );
            }
            let result = {
                // Keep frontier advancement and broker-side store_offset in
                // one order, but never hold this lock while waiting for an
                // earlier offset. Otherwise two out-of-order acks can
                // deadlock each other.
                let _ack_guard = self.ack_lock.lock().await;
                let consumer_mutex_guard = self.consumer.read().await;
                let Some(consumer) = consumer_mutex_guard.as_ref() else {
                    return Err(Error::Connection(
                        "Kafka consumer is not connected; acknowledgement is retryable".into(),
                    ));
                };
                let partition = self.partition.max(0) as u32;
                if let Some(failure) = self.frontier.failure(Some(&self.topic), partition) {
                    if failure.next_offset != position.offset {
                        return Err(Error::Process(format!(
                            "Kafka acknowledgement is blocked by an earlier source failure at next offset {}: {}",
                            failure.next_offset, failure.error
                        )));
                    }
                    // This is the failed delivery retrying its own source
                    // commit.  Clear only the matching fence; later
                    // deliveries must continue to observe it.
                    self.frontier
                        .clear_failure_if(Some(&self.topic), partition, position.offset);
                }
                let snapshot = self
                    .frontier
                    .snapshot_partition(Some(&self.topic), partition);
                let next_offset = match self.frontier.acknowledge(&position) {
                    AckAdvance::Pending { .. } => None,
                    AckAdvance::Advanced { next_offset } => Some(next_offset),
                    // A retry after a durable store failure: the frontier
                    // already advanced, so the broker store only needs to
                    // catch up to the current contiguous next offset.
                    AckAdvance::AlreadyCovered => Some(
                        self.frontier
                            .next_offset_of(Some(&self.topic), self.partition.max(0) as u32)
                            .unwrap_or(position.offset),
                    ),
                };

                match next_offset {
                    None => Ok::<Option<u64>, Error>(None),
                    Some(next_offset) => {
                        // librdkafka stores the next offset to consume, not
                        // the offset of the last message.  `next_offset` is
                        // already the exclusive contiguous frontier.
                        if self.transactional_offsets {
                            // L3: the transactional output commits the
                            // covered offsets inside its producer
                            // transaction; a local store here could advance
                            // the group past a transaction that still rolls
                            // back.
                            self.ack_notify.notify_waiters();
                            return Ok(());
                        }
                        let store_offset_value = i64::try_from(next_offset)
                            .map_err(|_| Error::Process("Kafka offset overflow".into()))?;
                        // The assignment was checked (and waited for) before
                        // the lock scope, so this critical section never spans
                        // a rebalance.
                        if assigned {
                            if let Err(error) = consumer.store_offset(
                                &self.topic,
                                self.partition,
                                store_offset_value,
                            ) {
                                let restored = self.frontier.restore_partition_if_current(
                                    Some(&self.topic),
                                    partition,
                                    next_offset,
                                    snapshot,
                                );
                                let message = if restored {
                                    format!("Failed to store Kafka offset: {error}")
                                } else {
                                    format!(
                                        "Failed to store Kafka offset: {error}; frontier changed during compensation"
                                    )
                                };
                                self.frontier.record_failure(
                                    Some(&self.topic),
                                    partition,
                                    position.offset,
                                    message.clone(),
                                );
                                // A later offset may be waiting for this
                                // frontier.  Wake it so it observes the recorded
                                // failure instead of waiting forever for a gap
                                // that can no longer close.
                                self.ack_notify.notify_waiters();
                                return Err(Error::Process(message));
                            }
                        }
                        Ok::<Option<u64>, Error>(Some(next_offset))
                    }
                }
            };
            match result {
                Ok(Some(_)) => {
                    // A successful store wakes any later branch waiting on
                    // this newly closed gap, while this caller's durable cut
                    // is now complete.
                    self.ack_notify.notify_waiters();
                    return Ok(());
                }
                Ok(None) => {
                    tokio::select! {
                        _ = notified => {}
                        _ = self.close.cancelled() => {
                            return Err(Error::Process(
                                "Kafka acknowledgement cancelled while waiting for an earlier offset".into(),
                            ));
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn undo(&self) -> Result<(), Error> {
        let position = SourcePosition {
            topic: Some(self.topic.clone()),
            partition: self.partition.max(0) as u32,
            offset: u64::try_from(self.offset.saturating_add(1))
                .map_err(|_| Error::Process("Kafka offset overflow".into()))?,
        };
        let _ack_guard = self.ack_lock.lock().await;
        let consumer_guard = self.consumer.read().await;
        let Some(consumer) = consumer_guard.as_ref() else {
            return Err(Error::Connection(
                "Kafka consumer is not connected; acknowledgement compensation is retryable".into(),
            ));
        };
        let current = self
            .frontier
            .next_offset_of(Some(&self.topic), self.partition.max(0) as u32)
            .unwrap_or_default();
        if current < position.offset {
            return Ok(());
        }
        if current > position.offset {
            return Err(Error::Process(
                "cannot compensate Kafka acknowledgement behind a later offset".into(),
            ));
        }
        // `store_offset` also takes the exclusive next offset.  Restoring a
        // message at offset N therefore stores N, so the broker can redeliver
        // that message after compensation.
        let restored_broker_offset = position.offset.saturating_sub(1);
        consumer
            .store_offset(
                &self.topic,
                self.partition,
                i64::try_from(restored_broker_offset)
                    .map_err(|_| Error::Process("Kafka offset overflow".into()))?,
            )
            .map_err(|error| Error::Process(format!("restore Kafka offset: {error}")))?;
        if !self.frontier.rewind_position(
            Some(&self.topic),
            self.partition.max(0) as u32,
            position.offset,
        ) {
            return Err(Error::Process(
                "Kafka acknowledgement frontier changed during compensation".into(),
            ));
        }
        self.ack_notify.notify_waiters();
        Ok(())
    }
}

pub(crate) struct KafkaInputBuilder;

impl InputBuilder for KafkaInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let kafka_config: KafkaInputConfig = parse_config(config, "Kafka input")?;
        // Fail before any stream starts on an inconsistent security block
        // (spec: 构建期校验与错误语义) — `--validate` reaches this path.
        if let Some(security) = &kafka_config.security {
            security.validate()?;
        }
        Ok(Arc::new(KafkaInput::new(name, kafka_config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("kafka", Arc::new(KafkaInputBuilder))?;
    register_input_metadata(ComponentMetadata::with_schema(
        "kafka",
        "Consumes messages from Apache Kafka topics with a consumer group.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "brokers": {"type": "array", "items": {"type": "string"}, "description": "List of Kafka broker addresses."},
                "topics": {"type": "array", "items": {"type": "string"}, "description": "Topics to subscribe to."},
                "consumer_group": {"type": "string", "description": "Consumer group ID for offset coordination."},
                "client_id": {"type": "string", "description": "Optional client identifier."},
                "start_from_latest": {"type": "boolean", "default": false, "description": "When true, ignore committed offsets and start from the latest message."},
                "fetch_min_bytes": {"type": "integer", "minimum": 0, "description": "Minimum bytes before the broker responds to a fetch request."},
                "fetch_max_bytes": {"type": "integer", "minimum": 0, "description": "Maximum bytes for a fetch request."},
                "fetch_max_partition_bytes": {"type": "integer", "minimum": 0, "description": "Maximum bytes per partition in a fetch request."},
                "fetch_wait_max_ms": {"type": "integer", "minimum": 0, "description": "Maximum time to wait for fetch data in milliseconds."},
                "security": crate::kafka_security::json_schema()
            },
            "required": ["brokers", "topics", "consumer_group"]
        }),
    ).with_example(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topics": ["events"],
        "consumer_group": "arkflow"
    })))
}

/// Maps Kafka record headers into `__meta_`-prefixed metadata entries:
/// each header becomes `header_<key>`. Duplicate keys get a positional
/// suffix (`header_<key>_2`, `_3`, …) instead of silently overwriting each
/// other, and binary values are lossy-decoded rather than dropped.
fn header_metadata(headers: &impl KafkaHeaders) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = Vec::with_capacity(headers.count());
    let mut key_counts: HashMap<String, usize> = HashMap::new();
    for i in 0..headers.count() {
        let header = headers.get(i);
        let value = header
            .value
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default();
        let count = key_counts
            .entry(header.key.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(0);
        let key = if *count == 0 {
            format!("header_{}", header.key)
        } else {
            format!("header_{}_{}", header.key, *count + 1)
        };
        entries.push((key, value));
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Header metadata mapping: duplicate keys keep every value (positional
    /// suffix) and binary values are lossy-decoded instead of dropped.
    #[test]
    fn header_metadata_handles_duplicates_and_binary() {
        use rdkafka::message::OwnedHeaders;

        let headers = OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "trace",
                value: Some(&b"abc"[..]),
            })
            .insert(rdkafka::message::Header {
                key: "trace",
                value: Some(&[0xffu8, 0x00][..]),
            })
            .insert(rdkafka::message::Header {
                key: "trace",
                value: Some(&b""[..]),
            });

        let entries = super::header_metadata(&headers);
        assert_eq!(
            entries,
            vec![
                ("header_trace".to_string(), "abc".to_string()),
                ("header_trace_2".to_string(), "\u{fffd}\u{0}".to_string()),
                ("header_trace_3".to_string(), String::new()),
            ]
        );
    }


    /// Regression: `wait_for_assignment` used to run inside the per-input
    /// acknowledgement lock and its consumer read guard, so one partition
    /// waiting out a rebalance blocked every sibling acknowledgement — and the
    /// consumer read guard blocked the `connect` that installs the
    /// reassignment it was waiting for. The wait must poll with SHORT guard
    /// acquisitions (guard dropped before sleeping) and stay outside the
    /// acknowledgement lock scope.
    #[test]
    fn assignment_wait_starts_before_the_acknowledgement_lock() {
        let source = include_str!("kafka.rs");
        let ack_start = source
            .find("impl Ack for KafkaAck")
            .expect("the Kafka acknowledgement exists");
        // Bound the scanned body at the test module: the assertion literals
        // below would otherwise match their own text embedded by
        // `include_str!`.
        let tests_start = source
            .find("#[cfg(test)]")
            .expect("the test module exists");
        let ack_body = &source[ack_start..tests_start];
        // The old held-guard wait is gone from the acknowledgement path.
        assert!(
            !ack_body.contains("Some(consumer) => self.wait_for_assignment(consumer).await"),
            "the assignment wait must not run while holding the consumer read guard"
        );
        let wait_at = ack_body
            .find("let wait_deadline")
            .expect("the assignment wait loop exists");
        // The read guard is scoped inside one polling iteration.
        let guard_at = ack_body[wait_at..]
            .find("let consumer_guard = self.consumer.read().await")
            .expect("the polling loop takes the consumer read guard");
        let iteration_scope_end = ack_body[wait_at..]
            .find("if assigned_now {")
            .expect("the polling iteration closes before using the verdict");
        assert!(
            guard_at < iteration_scope_end,
            "the read guard must be released before the assignment verdict is used"
        );
        let sleep_at = ack_body[wait_at..]
            .find("tokio::time::sleep")
            .expect("the polling loop sleeps between iterations");
        assert!(
            iteration_scope_end < sleep_at,
            "the consumer read guard must be dropped before the wait sleeps"
        );
        // The whole wait still precedes the acknowledgement lock scope.
        let lock_at = ack_body
            .find("let _ack_guard = self.ack_lock.lock().await;")
            .expect("the acknowledgement lock exists");
        assert!(
            wait_at < lock_at,
            "the assignment wait must be outside the acknowledgement lock scope"
        );
    }

    #[tokio::test]
    async fn test_kafka_input_new() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: Some("test-client".to_string()),
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };

        let input = KafkaInput::new(None, config, None);
        assert!(input.is_ok());
        let input = input.unwrap();
        assert_eq!(input.config.brokers, vec!["localhost:9092".to_string()]);
        assert_eq!(input.config.topics, vec!["test-topic".to_string()]);
        assert_eq!(input.config.consumer_group, "test-group".to_string());
        assert_eq!(input.config.client_id, Some("test-client".to_string()));
        assert!(!input.config.start_from_latest);
        assert!(input.codec.is_none()); // Verify codec is None
    }

    #[tokio::test]
    async fn test_kafka_input_read_not_connected() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };

        let input = KafkaInput::new(None, config, None).unwrap();
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
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };

        let input = KafkaInput::new(None, config, None).unwrap();
        assert!(input.current_positions().await.unwrap().is_empty());
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
                            transactional_offsets: false,
                        };

        // Acknowledging without a live consumer must fail; treating this as
        // success would advance the in-memory frontier while no broker offset
        // was stored.
        assert!(matches!(
            ack.ack().await,
            Err(Error::Connection(message)) if message.contains("not connected")
        ));
        let positions = input.current_positions().await.unwrap();
        assert!(positions.is_empty());
    }

    /// Task 3.3: out-of-order acknowledgements expose only the contiguous
    /// frontier — an acknowledged offset beyond a gap does not advance the
    /// checkpoint position past the unacknowledged records.
    #[tokio::test]
    async fn out_of_order_acknowledgements_wait_for_the_gap() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let frontier = input.frontier.clone();
        // Deliveries 5, 6, 7 (the frontier anchors at the first delivery).
        frontier.anchor_delivery(&SourcePosition {
            topic: Some("test-topic".into()),
            partition: 0,
            offset: 5,
        });
        // The connector-level acknowledgement requires a live broker. The
        // frontier itself remains unit-testable without one.
        assert_eq!(
            frontier.acknowledge(&SourcePosition {
                topic: Some("test-topic".into()),
                partition: 0,
                offset: 8,
            }),
            AckAdvance::Pending { gap: 6 }
        );
        assert_eq!(
            frontier.acknowledge(&SourcePosition {
                topic: Some("test-topic".into()),
                partition: 0,
                offset: 6,
            }),
            AckAdvance::Advanced { next_offset: 6 }
        );
        assert_eq!(
            frontier.acknowledge(&SourcePosition {
                topic: Some("test-topic".into()),
                partition: 0,
                offset: 7,
            }),
            AckAdvance::Advanced { next_offset: 8 }
        );
        assert_eq!(input.current_positions().await.unwrap()[0].offset, 8);
    }

    /// Task 3.3: restored positions seed the in-memory frontier, so a
    /// checkpoint immediately after restore reports the restored cursor.
    #[tokio::test]
    async fn restored_positions_seed_the_checkpoint_cursor() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        // Seed directly (restore_positions itself needs a broker for
        // watermark validation); this is exactly what it does on success.
        input.frontier.seed(&[SourcePosition {
            topic: Some("test-topic".into()),
            partition: 3,
            offset: 42,
        }]);
        let positions = input.current_positions().await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].partition, 3);
        assert_eq!(positions[0].offset, 42);
        // A new acknowledgement cannot commit without a live consumer.
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: "test-topic".to_string(),
            partition: 3,
            offset: 42,
                            transactional_offsets: false,
                        };
        assert!(ack.ack().await.is_err());
        assert_eq!(input.current_positions().await.unwrap()[0].offset, 42);
    }

    /// A reconnect must rebuild the explicit assignment from the contiguous
    /// acknowledged frontier instead of letting `auto.offset.reset` skip the
    /// outage window (`latest`) or replay the whole retained log
    /// (`earliest`).
    #[tokio::test]
    async fn reconnect_assignment_uses_the_acknowledged_frontier() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        input
            .assign_partition(3)
            .expect("explicit partition assignment");
        // Acknowledged progress before the disconnection.
        input.frontier.seed(&[SourcePosition {
            topic: Some("test-topic".into()),
            partition: 3,
            offset: 42,
        }]);
        input.connect().await.unwrap();
        let consumer_guard = input.consumer.read().await;
        let consumer = consumer_guard.as_ref().expect("connected consumer");
        let assignment = consumer.assignment().expect("assignment readable");
        let element = assignment
            .find_partition("test-topic", 3)
            .expect("configured partition assigned");
        assert!(
            matches!(element.offset(), Offset::Offset(42)),
            "the reconnect assignment must resume at the acknowledged frontier, got {:?}",
            element.offset()
        );
    }

    /// The first connect keeps the configured start: with an empty frontier
    /// and `start_from_latest`, the explicit assignment starts at the end —
    /// the same semantics the previous `auto.offset.reset` path produced.
    #[tokio::test]
    async fn first_connect_keeps_configured_start_semantics() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        input
            .assign_partition(0)
            .expect("explicit partition assignment");
        input.connect().await.unwrap();
        let consumer_guard = input.consumer.read().await;
        let consumer = consumer_guard.as_ref().expect("connected consumer");
        let assignment = consumer.assignment().expect("assignment readable");
        let element = assignment
            .find_partition("test-topic", 0)
            .expect("configured partition assigned");
        assert!(
            matches!(element.offset(), Offset::End),
            "an empty frontier must keep the configured start, got {:?}",
            element.offset()
        );
    }

    /// Task 3.2: restoring a subset of configured partitions merges the
    /// checkpoint into the complete assignment instead of replacing it —
    /// omitted partitions keep their configured start.
    #[test]
    fn merged_restore_assignment_retains_omitted_partitions() {
        let topics = vec!["orders".to_string(), "payments".to_string()];
        let assignment = KafkaInput::merged_restore_assignment(
            &topics,
            2,
            &[
                SourcePosition {
                    topic: Some("orders".into()),
                    partition: 2,
                    offset: 101,
                },
                // A different task's partition must be ignored.
                SourcePosition {
                    topic: Some("orders".into()),
                    partition: 5,
                    offset: 900,
                },
            ],
            false,
        );
        assert_eq!(
            assignment.count(),
            2,
            "every configured topic stays assigned"
        );
        let elements: Vec<(String, i32, Offset)> = assignment
            .elements()
            .iter()
            .map(|element| {
                (
                    element.topic().to_string(),
                    element.partition(),
                    element.offset(),
                )
            })
            .collect();
        let orders = elements
            .iter()
            .find(|(topic, _, _)| topic == "orders")
            .unwrap();
        assert_eq!(orders.1, 2);
        assert_eq!(orders.2, Offset::Offset(101));
        let payments = elements
            .iter()
            .find(|(topic, _, _)| topic == "payments")
            .unwrap();
        assert_eq!(payments.2, Offset::Beginning);

        let latest = KafkaInput::merged_restore_assignment(&topics, 2, &[], true);
        assert!(latest
            .elements()
            .iter()
            .all(|element| element.offset() == Offset::End));
    }

    #[test]
    fn retryable_receive_errors_are_reconnectable() {
        for code in [
            RDKafkaErrorCode::TimedOutQueue,
            RDKafkaErrorCode::Retry,
            RDKafkaErrorCode::UnknownBroker,
            RDKafkaErrorCode::AssignmentLost,
            RDKafkaErrorCode::ReassignmentInProgress,
            RDKafkaErrorCode::InvalidFetchSessionEpoch,
            RDKafkaErrorCode::OffsetNotAvailable,
        ] {
            assert!(KafkaInput::retryable_receive_error(
                &KafkaError::MessageConsumption(code)
            ));
        }
        assert!(!KafkaInput::retryable_receive_error(
            &KafkaError::MessageConsumption(RDKafkaErrorCode::Authentication)
        ));
    }

    #[test]
    fn test_kafka_metadata_api_compatibility() {
        // Test that metadata API imports work correctly
        use arkflow_core::metadata;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Create a simple test batch
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "test",
            ]))],
        )
        .unwrap();

        // Test that metadata functions are callable
        let result = metadata::with_source(batch, "kafka");
        assert!(result.is_ok());

        let batch = result.unwrap();
        let result = metadata::with_partition(batch, 0);
        assert!(result.is_ok());

        let batch = result.unwrap();
        let result = metadata::with_offset(batch, 100);
        assert!(result.is_ok());

        let batch = result.unwrap();
        let result = metadata::with_key(batch, b"test-key");
        assert!(result.is_ok());

        let batch = result.unwrap();
        let result = metadata::with_timestamp(batch, std::time::SystemTime::now());
        assert!(result.is_ok());

        let batch = result.unwrap();
        let result = metadata::with_ingest_time(batch, std::time::SystemTime::now());
        assert!(result.is_ok());

        let batch = result.unwrap();
        use std::collections::HashMap;
        let mut ext_meta = HashMap::new();
        ext_meta.insert("topic".to_string(), "test-topic".to_string());
        let result = metadata::with_ext_metadata(batch, &ext_meta);
        assert!(result.is_ok());
    }

    #[test]
    fn rejects_checkpoint_offsets_outside_broker_range() {
        assert!(KafkaInput::validate_checkpoint_offset(9, 10, 20).is_err());
        assert!(KafkaInput::validate_checkpoint_offset(21, 10, 20).is_err());
        assert!(KafkaInput::validate_checkpoint_offset(u64::MAX, 0, i64::MAX).is_err());
        assert_eq!(
            KafkaInput::validate_checkpoint_offset(10, 10, 20).unwrap(),
            10
        );
        assert_eq!(
            KafkaInput::validate_checkpoint_offset(20, 10, 20).unwrap(),
            20
        );
    }

    #[test]
    fn classifies_broker_transport_errors_as_reconnectable() {
        assert!(KafkaInput::retryable_receive_error(&KafkaError::Global(
            RDKafkaErrorCode::AllBrokersDown,
        )));
        assert!(KafkaInput::retryable_receive_error(
            &KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut,)
        ));
        assert!(!KafkaInput::retryable_receive_error(&KafkaError::Global(
            RDKafkaErrorCode::InvalidArgument,
        )));
    }

    #[tokio::test]
    async fn closing_kafka_wakes_a_frontier_gap_waiter() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        input.frontier.anchor_delivery(&SourcePosition {
            topic: Some("test-topic".into()),
            partition: 0,
            offset: 0,
        });
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: "test-topic".into(),
            partition: 0,
            offset: 1,
                            transactional_offsets: false,
                        };
        let waiter = tokio::spawn(async move { ack.ack().await });
        tokio::task::yield_now().await;
        input.close().await.unwrap();
        let result = waiter.await.unwrap();
        assert!(
            matches!(result, Err(Error::Connection(message)) if message.contains("not connected"))
        );
    }

    #[test]
    fn test_kafka_disables_auto_offset_store_for_crash_safety() {
        // Phase 0 (add-input-durability): at-least-once crash-safety depends on
        // offsets being stored ONLY inside `KafkaAck::ack()` (which fires after
        // the downstream output confirms the write), never on `recv()`. Verify
        // the consumer config disables rdkafka's automatic offset store.
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let client_config = input.build_client_config().unwrap();
        assert_eq!(
            client_config.get("enable.auto.offset.store"),
            Some("false"),
            "auto offset store MUST be disabled so offsets advance only on ack (at-least-once)"
        );
    }

    /// Regression (spec: 未配置 security 时保持 plaintext): with no
    /// `security` block the client config must not carry any security/sasl/ssl
    /// property — behaviour is byte-identical to before the field existed.
    #[test]
    fn test_kafka_input_without_security_sets_no_security_properties() {
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let client_config = input.build_client_config().unwrap();
        for key in client_config.config_map().keys() {
            assert!(
                !key.starts_with("security.")
                    && !key.starts_with("sasl.")
                    && !key.starts_with("ssl."),
                "unexpected security property without a security block: {key}"
            );
        }
    }

    /// Spec: 统一安全配置块 + SASL/TLS 装配 — SCRAM over TLS with an inline
    /// PEM CA flows from the YAML-shaped config into librdkafka properties.
    #[test]
    fn test_kafka_input_assembles_sasl_ssl_properties() {
        let ca_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
        let config = KafkaInputConfig {
            transactional_offsets: false,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
            security: Some(crate::kafka_security::KafkaSecurityConfig {
                protocol: None,
                sasl: Some(crate::kafka_security::SaslConfig {
                    mechanism: crate::kafka_security::SaslMechanism::ScramSha256,
                    username: Some("alice".to_string()),
                    password: Some("secret".to_string()),
                }),
                tls: Some(crate::kafka_security::TlsConfig {
                    ca: Some(ca_pem.to_string()),
                    cert: None,
                    key: None,
                    key_password: None,
                    insecure_skip_verify: None,
                }),
            }),
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let client_config = input.build_client_config().unwrap();
        // sasl + tls with no explicit protocol infers sasl_ssl.
        assert_eq!(client_config.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(client_config.get("sasl.mechanisms"), Some("SCRAM-SHA-256"));
        assert_eq!(client_config.get("sasl.username"), Some("alice"));
        assert_eq!(client_config.get("sasl.password"), Some("secret"));
        assert_eq!(client_config.get("ssl.ca.pem"), Some(ca_pem));
    }

    /// Spec: 构建期校验与错误语义 — the builder rejects an inconsistent
    /// security block before any component is constructed (offline).
    #[test]
    fn test_kafka_input_builder_rejects_inconsistent_security() {
        let config = serde_json::json!({
            "brokers": ["localhost:9092"],
            "topics": ["t"],
            "consumer_group": "g",
            "start_from_latest": false,
            "security": {"protocol": "sasl_ssl"}
        });
        let err = match KafkaInputBuilder.build(
            None,
            &Some(config),
            None,
            &Resource {
                temporary: HashMap::new(),
                input_names: Default::default(),
            },
        ) {
            Ok(_) => {
                panic!("sasl_ssl without a sasl block must fail at build")
            }
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("security.sasl"),
            "expected the error to name security.sasl, got: {err}"
        );
    }

    /// Broker-gated integration test (Phase 0, task 1.3): proves a replayable
    /// source re-delivers an unacknowledged message after a simulated crash,
    /// because `enable.auto.offset.store=false` means the offset only advances
    /// inside `KafkaAck::ack()` (which never fires below).
    ///
    /// Skipped unless `ARKFLOW_KAFKA_BROKER` is set. Run with:
    /// `cargo test -p arkflow-plugin --lib input::kafka::tests::kafka_redelivers_unacked_after_restart -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn kafka_redelivers_unacked_after_restart() {
        use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
        use std::time::Duration;

        let broker = match std::env::var("ARKFLOW_KAFKA_BROKER") {
            Ok(b) => b,
            Err(_) => return, // no broker available — skip
        };
        let topic = std::env::var("ARKFLOW_KAFKA_TOPIC")
            .unwrap_or_else(|_| "arkflow_durability_test".to_string());
        let group = format!("arkflow-dur-test-{}", std::process::id());

        fn cfg(brokers: &str, topics: &str, group: &str) -> KafkaInputConfig {
            KafkaInputConfig {
                transactional_offsets: false,
                brokers: vec![brokers.to_string()],
                topics: vec![topics.to_string()],
                consumer_group: group.to_string(),
                client_id: None,
                start_from_latest: false,
                fetch_min_bytes: None,
                fetch_max_bytes: None,
                fetch_max_partition_bytes: None,
                fetch_wait_max_ms: None,
                security: None,
            }
        }

        // Produce one message.
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &broker)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("producer create");
        let payload_bytes = b"ping".to_vec();
        producer
            .send(
                FutureRecord::<String, Vec<u8>>::to(&topic).payload(&payload_bytes),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
        producer.flush(Duration::from_secs(10)).expect("flush");

        // First consumer: read the message, do NOT ack (simulate a crash before
        // the downstream output confirms). The offset is NOT stored.
        let input = KafkaInput::new(None, cfg(&broker, &topic, &group), None).unwrap();
        input.connect().await.unwrap();
        let (_msg, _ack) = input.read().await.expect("first read must succeed");
        drop(input); // crash without acking

        // Reconnect with the same group: the message must be re-delivered.
        let input2 = KafkaInput::new(None, cfg(&broker, &topic, &group), None).unwrap();
        input2.connect().await.unwrap();
        let (_msg2, _ack2) = input2
            .read()
            .await
            .expect("unacked message must be re-delivered after restart (no loss)");
    }
}