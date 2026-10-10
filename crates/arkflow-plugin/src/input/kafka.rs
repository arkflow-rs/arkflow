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
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, VecAck};
use arkflow_core::{metadata, Bytes, Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};

use crate::kafka_security::KafkaSecurityConfig;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers as KafkaHeaders, Message as KafkaMessage, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Maximum number of messages aggregated into one `read()` batch.
    /// Values below 1 are clamped to 1 (per-message batches).
    pub batch_max_rows: Option<u32>,
    /// Maximum accumulated payload bytes aggregated into one `read()` batch
    /// (the first message is always included). Values below 1 are clamped
    /// to 1.
    pub batch_max_bytes: Option<u64>,
}

/// Default row bound for one `read()` batch.
const DEFAULT_BATCH_MAX_ROWS: u32 = 1024;
/// Default payload-byte bound for one `read()` batch.
const DEFAULT_BATCH_MAX_BYTES: u64 = 8 * 1024 * 1024;

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
    /// Resolved batch bounds for `read()` aggregation (clamped to >= 1).
    batch_max_rows: usize,
    batch_max_bytes: u64,
    /// Set when a drain hit a retryable receive error: the drained batch is
    /// still returned (at-least-once), and the NEXT `read()` must surface
    /// `Error::Disconnection` at its blocking claim point once the queue has
    /// drained — already-buffered messages come out first.
    pending_reconnect: AtomicBool,
    /// L3 bridge slot: when `transactional_offsets` is enabled, the live
    /// consumer-group metadata lands here for transactional outputs to
    /// commit offsets inside their producer transactions.
    txn_metadata: Option<crate::kafka_txn::SharedMetadata>,
}

/// One Kafka message claimed from the consumer's queue, with everything the
/// batch assembly needs copied to owned storage (the `BorrowedMessage` does
/// not outlive the consumer read guard).
struct ClaimedRecord {
    payload: Bytes,
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    timestamp: Option<SystemTime>,
    /// Ordered extended-metadata entries (`topic` plus `header_<key>`).
    ext: Vec<(String, String)>,
}

/// A tombstone (null payload) claimed from the queue: it carries no data
/// row, so it is settled out-of-band instead of joining the batch.
struct TombstoneSite {
    topic: String,
    partition: i32,
    offset: i64,
}

/// The outcome of claiming one queue message.
enum Claim {
    Data(ClaimedRecord),
    Tombstone(TombstoneSite),
}

/// Why a drain stopped pulling more messages.
#[derive(Debug, PartialEq, Eq)]
enum DrainStop {
    /// A batch bound (`batch_max_rows`/`batch_max_bytes`) was reached.
    Complete,
    /// No buffered message was available without blocking.
    QueueEmpty,
    /// A retryable receive error surfaced; already-claimed records are kept
    /// and the next `read()` reports the disconnection through `recv()`.
    Reconnect,
}

/// A contiguous per-(topic, partition) run of claimed offsets inside one
/// `read()` batch. A single consumer's queue preserves per-partition offset
/// order, so the claimed offsets of a partition form a contiguous run.
struct AckSegment {
    topic: String,
    partition: i32,
    first: i64,
    last: i64,
}

impl KafkaInput {
    /// Convert a claimed queue message into owned storage. Tombstones (null
    /// payloads) become [`Claim::Tombstone`]; the caller settles them.
    fn claim_message(message: &impl KafkaMessage) -> Claim {
        let topic = message.topic().to_string();
        let partition = message.partition();
        let offset = message.offset();
        let Some(payload) = message.payload() else {
            return Claim::Tombstone(TombstoneSite {
                topic,
                partition,
                offset,
            });
        };
        let timestamp = if let Timestamp::CreateTime(millis_since_epoch) = message.timestamp() {
            Self::convert_kafka_timestamp(millis_since_epoch)
        } else {
            None
        };
        let mut ext = Vec::with_capacity(2);
        ext.push(("topic".to_string(), topic.clone()));
        if let Some(headers) = message.headers() {
            ext.extend(header_metadata(headers));
        }
        Claim::Data(ClaimedRecord {
            payload: payload.to_vec(),
            topic,
            partition,
            offset,
            key: message.key().map(<[u8]>::to_vec),
            timestamp,
            ext,
        })
    }

    /// Settle a tombstone out-of-band: anchor the frontier and hand the
    /// acknowledgement to its own task, exactly as the per-message path
    /// did. A settlement failure surfaces through the frontier failure
    /// fence; blocking `read` here would stall records and control events.
    fn settle_tombstone(&self, site: TombstoneSite) {
        let TombstoneSite {
            topic,
            partition,
            offset,
        } = site;
        let ack = self.ack_for_segment(&AckSegment {
            topic: topic.clone(),
            partition,
            first: offset,
            last: offset,
        });
        self.frontier.anchor_delivery(&SourcePosition {
            topic: Some(topic),
            partition: partition as u32,
            offset: offset as u64,
        });
        let close_for_retry = self.close.clone();
        // Retry inside the task: a settlement that leaves the frontier
        // short of this offset blocks every later acknowledgement of the
        // partition behind a gap that can no longer close, and no
        // redelivery retries it (the tombstone is not forwarded).
        tokio::spawn(async move {
            for attempt in 0..4 {
                if let Ok(()) = ack.ack().await {
                    return;
                }
                if close_for_retry.is_cancelled() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
            }
            tracing::warn!(
                "Kafka tombstone settlement failed after retries; the frontier fence reports it to the next acknowledgement"
            );
        });
    }

    /// Aggregate already-claimed `first` with further buffered messages.
    ///
    /// MUST stay a non-async function: the engine's source loop may drop the
    /// pending `read()` future at any select! branch, and the
    /// cancellation-safety contract forbids a suspension point between the
    /// first claim and the batch being returned. `next` returns `None` when
    /// no message is buffered (non-blocking probe) — a `None` probes nothing
    /// out of the queue.
    fn drain_records(
        first: ClaimedRecord,
        max_rows: usize,
        max_bytes: u64,
        mut next: impl FnMut() -> Option<Result<Claim, KafkaError>>,
        mut on_tombstone: impl FnMut(TombstoneSite),
    ) -> Result<(Vec<ClaimedRecord>, DrainStop), KafkaError> {
        let mut bytes = first.payload.len() as u64;
        let mut records = Vec::with_capacity(1);
        records.push(first);
        loop {
            if records.len() >= max_rows || bytes >= max_bytes {
                return Ok((records, DrainStop::Complete));
            }
            let Some(claimed) = next() else {
                return Ok((records, DrainStop::QueueEmpty));
            };
            match claimed {
                Ok(Claim::Data(record)) => {
                    bytes += record.payload.len() as u64;
                    records.push(record);
                }
                Ok(Claim::Tombstone(site)) => on_tombstone(site),
                Err(error) if Self::retryable_receive_error(&error) => {
                    return Ok((records, DrainStop::Reconnect));
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// First claim for a `read()` that follows a drain which saw a retryable
    /// receive error. Gives already-buffered messages one non-blocking chance
    /// (tombstones settle out of band) before the reconnect is surfaced;
    /// `Ok(None)` means nothing is buffered — surface the reconnect.
    fn pending_reconnect_first(
        mut next: impl FnMut() -> Option<Result<Claim, KafkaError>>,
        mut on_tombstone: impl FnMut(TombstoneSite),
    ) -> Result<Option<ClaimedRecord>, KafkaError> {
        loop {
            match next() {
                None => return Ok(None),
                Some(Ok(Claim::Data(record))) => return Ok(Some(record)),
                Some(Ok(Claim::Tombstone(site))) => on_tombstone(site),
                Some(Err(error)) => return Err(error),
            }
        }
    }

    /// Group claimed records into per-(topic, partition) contiguous segments,
    /// in first-seen order. One partition never appears in two segments of
    /// the same batch.
    fn group_ack_segments(records: &[ClaimedRecord]) -> Vec<AckSegment> {
        let mut order: Vec<(String, i32)> = Vec::new();
        let mut bounds: HashMap<(String, i32), (i64, i64)> = HashMap::new();
        for record in records {
            let key = (record.topic.clone(), record.partition);
            match bounds.get_mut(&key) {
                Some((first, last)) => {
                    *first = (*first).min(record.offset);
                    *last = (*last).max(record.offset);
                }
                None => {
                    order.push(key.clone());
                    bounds.insert(key, (record.offset, record.offset));
                }
            }
        }
        order
            .into_iter()
            .map(|(topic, partition)| {
                let (first, last) = bounds
                    .remove(&(topic.clone(), partition))
                    .expect("every ordered key has bounds");
                AckSegment {
                    topic,
                    partition,
                    first,
                    last,
                }
            })
            .collect()
    }

    fn ack_for_segment(&self, segment: &AckSegment) -> KafkaAck {
        KafkaAck {
            consumer: self.consumer.clone(),
            frontier: self.frontier.clone(),
            ack_lock: self.ack_lock.clone(),
            ack_notify: self.ack_notify.clone(),
            close: self.close.clone(),
            topic: segment.topic.clone(),
            partition: segment.partition,
            segment_start: segment.first,
            offset: segment.last,
            transactional_offsets: self.config.transactional_offsets,
        }
    }

    /// Decode the claimed payloads and attach per-row source metadata in one
    /// RecordBatch rebuild.
    ///
    /// Fast path: one codec call for the whole batch, used when it yields
    /// exactly one row per payload — the shape every shipped codec produces
    /// for one Kafka message per payload. Fallback: when the row count
    /// disagrees (a skip-mode codec dropped a payload, or a payload decoded
    /// to multiple rows), each payload is decoded individually so every row
    /// maps back to its own message's metadata.
    async fn decode_and_attach(
        &self,
        records: &[ClaimedRecord],
    ) -> Result<datafusion::arrow::record_batch::RecordBatch, Error> {
        let payloads: Vec<Bytes> = records
            .iter()
            .map(|record| record.payload.clone())
            .collect();
        let decoded =
            crate::input::codec_helper::apply_codec_to_payloads(payloads, &self.codec).await?;
        let decoded_batch: datafusion::arrow::record_batch::RecordBatch = decoded.into();
        // One ingest timestamp for the whole batch (batch granularity).
        let ingest_time = SystemTime::now();

        if decoded_batch.num_rows() == records.len() {
            let row_meta: Vec<metadata::RowSourceMetadata<'_>> = records
                .iter()
                .map(|record| metadata::RowSourceMetadata {
                    partition: record.partition as u32,
                    offset: record.offset as u64,
                    key: record.key.as_deref(),
                    timestamp: record.timestamp,
                    ext: &record.ext,
                })
                .collect();
            return metadata::attach_row_source_metadata(
                decoded_batch,
                "kafka",
                ingest_time,
                &row_meta,
            );
        }

        // Fallback: per-payload decode for an exact row↔payload mapping.
        let mut batches: Vec<datafusion::arrow::record_batch::RecordBatch> = Vec::new();
        let mut row_records: Vec<&ClaimedRecord> = Vec::new();
        for record in records {
            match crate::input::codec_helper::apply_codec_to_payload(&record.payload, &self.codec)
                .await
            {
                Ok(batch) => {
                    let rows = batch.len();
                    if rows == 0 {
                        // A skip-mode codec dropped this payload: no row, no
                        // metadata, no acknowledgement entry for it.
                        continue;
                    }
                    row_records.extend(std::iter::repeat_n(record, rows));
                    batches.push(batch.into());
                }
                Err(error) => return Err(error),
            }
        }
        let merged = if batches.is_empty() {
            // Every payload was skipped: an empty batch carrying the fast
            // path's schema (there are no rows to align metadata with).
            let schema = decoded_batch.schema();
            datafusion::arrow::record_batch::RecordBatch::new_empty(schema)
        } else {
            crate::component::batch_merge::normalize_and_concat(&batches)?
        };
        let row_meta: Vec<metadata::RowSourceMetadata<'_>> = row_records
            .iter()
            .map(|record| metadata::RowSourceMetadata {
                partition: record.partition as u32,
                offset: record.offset as u64,
                key: record.key.as_deref(),
                timestamp: record.timestamp,
                ext: &record.ext,
            })
            .collect();
        metadata::attach_row_source_metadata(merged, "kafka", ingest_time, &row_meta)
    }

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
        name: Option<&str>,
        config: KafkaInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let frontier = Arc::new(CommitFrontier::new());
        let txn_metadata = if config.transactional_offsets {
            // The registry entry carries the SAME frontier instance the
            // acknowledgements below advance: the paired transactional
            // output clamps its offset commits to the contiguous
            // acknowledged run it exposes, so unsettled records can never
            // be skipped by a transactional commit.
            Some(crate::kafka_txn::register_group(
                &config.consumer_group,
                config.topics.clone(),
                frontier.clone(),
            ))
        } else {
            None
        };
        let batch_max_rows = config
            .batch_max_rows
            .unwrap_or(DEFAULT_BATCH_MAX_ROWS)
            .max(1) as usize;
        let batch_max_bytes = config
            .batch_max_bytes
            .unwrap_or(DEFAULT_BATCH_MAX_BYTES)
            .max(1);
        if config.batch_max_rows.is_some_and(|rows| rows < 1)
            || config.batch_max_bytes.is_some_and(|bytes| bytes < 1)
        {
            tracing::debug!(
                batch_max_rows,
                batch_max_bytes,
                "Kafka input batch bounds below 1 are clamped to 1"
            );
        }
        Ok(Self {
            input_name: name.map(str::to_string),
            config,
            consumer: Arc::new(RwLock::new(None)),
            assigned_partition: Arc::new(RwLock::new(None)),
            frontier,
            ack_lock: Arc::new(tokio::sync::Mutex::new(())),
            ack_notify: Arc::new(Notify::new()),
            close: CancellationToken::new(),
            codec,
            batch_max_rows,
            batch_max_bytes,
            pending_reconnect: AtomicBool::new(false),
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

        // Cancellation safety: the blocking `recv()` below is the ONLY
        // suspension point before the batch is returned. Everything between
        // the first claim and the return runs synchronously — the drain
        // probes with non-blocking `now_or_never` (see `drain_records`) and
        // the codec decode of every shipped codec completes without ever
        // yielding. A `read()` future dropped before the claim loses
        // nothing; after the claim it cannot be dropped mid-assembly.
        let first = loop {
            // A previous drain saw a retryable receive error: before blocking
            // on `recv()` (which could wait out a dead connection), give
            // already-buffered messages one non-blocking chance, then surface
            // the reconnect through the existing Disconnection path. The flag
            // stays set while buffered messages flow out, so the reconnect
            // surfaces at the first claim that would otherwise block — the
            // "queue drained" point the batching contract specifies.
            if self.pending_reconnect.load(Ordering::Relaxed) {
                match Self::pending_reconnect_first(
                    || {
                        let mut stream = consumer.stream();
                        match stream.next().now_or_never() {
                            // Pending (or stream end): nothing buffered.
                            None | Some(None) => None,
                            Some(Some(outcome)) => {
                                Some(outcome.map(|message| Self::claim_message(&message)))
                            }
                        }
                    },
                    |site| self.settle_tombstone(site),
                ) {
                    Ok(Some(record)) => break record,
                    // Nothing buffered: the queue has drained — surface it.
                    Ok(None) => {
                        self.pending_reconnect.store(false, Ordering::Relaxed);
                        return Err(Error::Disconnection);
                    }
                    Err(e) => {
                        self.pending_reconnect.store(false, Ordering::Relaxed);
                        if Self::retryable_receive_error(&e) {
                            return Err(Error::Disconnection);
                        }
                        return Err(Error::Connection(format!(
                            "Error receiving Kafka message: {}",
                            e
                        )));
                    }
                }
            }
            match consumer.recv().await {
                Ok(kafka_message) => {
                    // A successful blocking receive proves the connection is
                    // producing again; a stale reconnect signal no longer
                    // applies.
                    self.pending_reconnect.store(false, Ordering::Relaxed);
                    match Self::claim_message(&kafka_message) {
                        Claim::Data(record) => break record,
                        // Compacted topics deliver deletion markers with a null
                        // payload. They are ordinary Kafka data: settle them here,
                        // never as a fatal error (a crash loop) and never as a
                        // silent skip (the frontier would replay it forever).
                        Claim::Tombstone(site) => self.settle_tombstone(site),
                    }
                }
                Err(e) if Self::retryable_receive_error(&e) => return Err(Error::Disconnection),
                Err(e) => {
                    return Err(Error::Connection(format!(
                        "Error receiving Kafka message: {}",
                        e
                    )))
                }
            }
        };

        let (records, stop) = Self::drain_records(
            first,
            self.batch_max_rows,
            self.batch_max_bytes,
            || {
                let mut stream = consumer.stream();
                match stream.next().now_or_never() {
                    // Pending: nothing buffered right now — the probe claimed
                    // nothing out of the consumer's queue.
                    None => None,
                    // Kafka streams never terminate; treat it as empty.
                    Some(None) => None,
                    Some(Some(outcome)) => {
                        Some(outcome.map(|message| Self::claim_message(&message)))
                    }
                }
            },
            |site| self.settle_tombstone(site),
        )
        .map_err(|e| Error::Connection(format!("Error receiving Kafka message: {}", e)))?;
        if matches!(stop, DrainStop::Reconnect) {
            // The drained batch is still returned below (at-least-once); the
            // next `read()` surfaces the reconnect at its blocking claim
            // point once the queue has drained.
            self.pending_reconnect.store(true, Ordering::Relaxed);
        }

        // Every claim is owned storage now: release the consumer read guard
        // before the decode and assembly work.
        drop(consumer_guard);

        let record_batch = self.decode_and_attach(&records).await?;

        // Anchor each segment's frontier at its first delivery: an
        // out-of-order FIRST acknowledgement (fan-out completing a later
        // branch first) cannot then claim the earlier records of this
        // delivery were acknowledged.
        let segments = Self::group_ack_segments(&records);
        for segment in &segments {
            self.frontier.anchor_delivery(&SourcePosition {
                topic: Some(segment.topic.clone()),
                partition: segment.partition as u32,
                offset: segment.first as u64,
            });
        }

        let mut msg_batch = MessageBatch::new_arrow(record_batch);
        msg_batch.set_input_name(self.input_name.clone());

        // One ack per (topic, partition) segment; partitions are
        // independent frontiers, so a VecAck composes them without
        // cross-partition gap waits.
        let ack: Arc<dyn Ack> = if segments.len() == 1 {
            Arc::new(self.ack_for_segment(&segments[0]))
        } else {
            Arc::new(VecAck(
                segments
                    .iter()
                    .map(|segment| Arc::new(self.ack_for_segment(segment)) as Arc<dyn Ack>)
                    .collect(),
            ))
        };

        Ok((Arc::new(msg_batch), ack))
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
        Ok(Some(Arc::new(self.ack_for_segment(&AckSegment {
            topic,
            partition: position.partition as i32,
            first: offset,
            last: offset,
        }))))
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
        if self.config.transactional_offsets {
            return Err(Error::Config(
                "Kafka transactional_offsets requires subscribe mode (group membership for \
                 send_offsets_to_transaction); a partition-assigned consumer never joins the \
                 consumer group. Use a single-reader (non-job) input for L3 flows"
                    .into(),
            ));
        }
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
    /// First offset of the contiguous delivery segment this ack settles
    /// (equal to `offset` for a single-message segment). Undo rewinds the
    /// whole segment to this offset — a batch is one delivery unit under
    /// at-least-once.
    segment_start: i64,
    /// Last offset of the segment; the ack's position is `offset + 1`.
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
                // Acknowledge every offset of the segment individually: the
                // frontier tracks single next-offsets in a pending set, so
                // one acknowledge(last+1) would leave a gap only the segment
                // itself can close. The segment's offsets are consecutive,
                // so only the first acknowledge can report Pending.
                let mut advanced: Option<u64> = None;
                let mut blocked_on_gap = false;
                for record_offset in self.segment_start..=self.offset {
                    debug_assert!(record_offset >= 0, "Kafka record offsets are non-negative");
                    let step = SourcePosition {
                        topic: Some(self.topic.clone()),
                        partition,
                        offset: (record_offset + 1) as u64,
                    };
                    match self.frontier.acknowledge(&step) {
                        AckAdvance::Advanced { next_offset } => advanced = Some(next_offset),
                        // A retry after a durable store failure or a
                        // tombstone settlement already covered this offset.
                        AckAdvance::AlreadyCovered => {}
                        AckAdvance::Pending { .. } => {
                            blocked_on_gap = true;
                            break;
                        }
                    }
                }
                let next_offset = if blocked_on_gap {
                    None
                } else {
                    Some(advanced.unwrap_or_else(|| {
                        // Every offset was already covered: the broker store
                        // only needs to catch up to the current contiguous
                        // next offset.
                        self.frontier
                            .next_offset_of(Some(&self.topic), self.partition.max(0) as u32)
                            .unwrap_or(position.offset)
                    }))
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
        if self.transactional_offsets {
            // L3 (spec: 配对输入的 undo 不触 store_offset): the group's
            // broker offset has exactly one writer — the paired output's
            // producer transaction. A local `store_offset` here could be
            // published by the periodic auto-commit and advance the group
            // past a transaction that still rolls back. The in-memory
            // frontier rewind below is what guarantees redelivery; it does
            // not need a live consumer.
        } else {
            let consumer_guard = self.consumer.read().await;
            let Some(consumer) = consumer_guard.as_ref() else {
                return Err(Error::Connection(
                    "Kafka consumer is not connected; acknowledgement compensation is retryable"
                        .into(),
                ));
            };
            // `store_offset` also takes the exclusive next offset: storing
            // the segment's first offset redelivers the WHOLE segment after
            // compensation — a batch is one delivery unit.
            consumer
                .store_offset(&self.topic, self.partition, self.segment_start)
                .map_err(|error| Error::Process(format!("restore Kafka offset: {error}")))?;
        }
        // Rewind the frontier one next-offset at a time down to the segment
        // start, each step guarded by the value it must observe.
        let mut expected = position.offset;
        while expected > self.segment_start.max(0) as u64 {
            if !self.frontier.rewind_position(
                Some(&self.topic),
                self.partition.max(0) as u32,
                expected,
            ) {
                return Err(Error::Process(
                    "Kafka acknowledgement frontier changed during compensation".into(),
                ));
            }
            expected -= 1;
        }
        self.ack_notify.notify_waiters();
        Ok(())
    }
}

pub(crate) struct KafkaInputBuilder;

impl InputBuilder for KafkaInputBuilder {
    fn build(
        &self,
        name: Option<&str>,
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
                "batch_max_rows": {"type": "integer", "minimum": 1, "default": 1024, "description": "Maximum number of messages aggregated into one read batch (clamped to 1 minimum; 1 restores per-message batches)."},
                "batch_max_bytes": {"type": "integer", "minimum": 1, "default": 8388608, "description": "Maximum accumulated payload bytes per read batch; the first message is always included (clamped to 1 minimum)."},
                "security": crate::kafka_security::json_schema()
            },
            "required": ["brokers", "topics", "consumer_group"]
        }),
    ).with_example(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topics": ["events"],
        "consumer_group": "arkflow",
        "start_from_latest": false
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

    fn config_from_json(value: serde_json::Value) -> KafkaInputConfig {
        serde_json::from_value(value).unwrap()
    }

    fn base_config() -> serde_json::Value {
        serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topics": ["orders"],
            "consumer_group": "group-a",
            "start_from_latest": false,
        })
    }

    fn input_with(value: serde_json::Value) -> KafkaInput {
        KafkaInput::new(None, config_from_json(value), None).unwrap()
    }

    #[test]
    fn client_config_carries_every_tuning_knob() {
        let mut value = base_config();
        value["client_id"] = serde_json::json!("client-1");
        value["fetch_min_bytes"] = serde_json::json!(10);
        value["fetch_max_bytes"] = serde_json::json!(20_000_000);
        value["fetch_max_partition_bytes"] = serde_json::json!(1_000_000);
        value["fetch_wait_max_ms"] = serde_json::json!(250);
        let input = input_with(value);
        let config = input.build_client_config().unwrap();
        let get = |key: &str| config.get(key).map(str::to_string);
        assert_eq!(get("bootstrap.servers").as_deref(), Some("127.0.0.1:9092"));
        assert_eq!(get("group.id").as_deref(), Some("group-a"));
        assert_eq!(get("client.id").as_deref(), Some("client-1"));
        assert_eq!(get("fetch.min.bytes").as_deref(), Some("10"));
        assert_eq!(get("fetch.max.bytes").as_deref(), Some("20000000"));
        assert_eq!(get("max.partition.fetch.bytes").as_deref(), Some("1000000"));
        assert_eq!(get("fetch.wait.max.ms").as_deref(), Some("250"));
        // Crash-safety invariant: offsets are committed explicitly only.
        assert_eq!(get("enable.auto.offset.store").as_deref(), Some("false"));
    }

    #[test]
    fn client_config_latest_offset_reset_and_defaults() {
        let mut value = base_config();
        value["start_from_latest"] = serde_json::json!(true);
        let input = input_with(value);
        let config = input.build_client_config().unwrap();
        assert_eq!(config.get("auto.offset.reset"), Some("latest"));

        let input = input_with(base_config());
        let config = input.build_client_config().unwrap();
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
        // Optional knobs stay unset.
        assert!(config.get("client.id").is_none());
        assert!(config.get("fetch.min.bytes").is_none());
    }

    #[test]
    fn kafka_timestamp_conversion_handles_bounds() {
        assert!(KafkaInput::convert_kafka_timestamp(-1).is_none());
        assert_eq!(
            KafkaInput::convert_kafka_timestamp(0),
            Some(SystemTime::UNIX_EPOCH)
        );
        assert!(KafkaInput::convert_kafka_timestamp(1_700_000_000_000).is_some());
        // Very large-but-positive values stay representable.
        assert!(KafkaInput::convert_kafka_timestamp(i64::MAX).is_some());
    }

    #[test]
    fn applicable_positions_filter_by_topic_and_assigned_partition() {
        let input = input_with(serde_json::json!({
            "brokers": ["b"],
            "topics": ["orders", "billing"],
            "consumer_group": "g",
            "start_from_latest": false,
        }));
        let positions = vec![
            arkflow_core::checkpoint::SourcePosition {
                topic: Some("orders".into()),
                partition: 0,
                offset: 1,
            },
            arkflow_core::checkpoint::SourcePosition {
                topic: Some("orders".into()),
                partition: 3,
                offset: 2,
            },
            arkflow_core::checkpoint::SourcePosition {
                topic: Some("other".into()),
                partition: 0,
                offset: 3,
            },
        ];

        // Subscription mode (no explicit assignment): topic filter only.
        let applicable = input.applicable_positions(&positions).unwrap();
        assert_eq!(applicable.len(), 2);

        // Explicit partition mode: only the assigned partition survives.
        *input.assigned_partition.try_write().expect("write guard") = Some(3);
        let applicable = input.applicable_positions(&positions).unwrap();
        assert_eq!(applicable.len(), 1);
        assert_eq!(applicable[0].partition, 3);
    }

    #[test]
    fn merged_restore_assignment_honors_positions_and_start_policy() {
        let restored = arkflow_core::checkpoint::SourcePosition {
            topic: Some("orders".into()),
            partition: 2,
            offset: 42,
        };
        // A checkpointed topic restores its offset; an uncheckpointed topic
        // follows the start_from_latest policy.
        let assignment = KafkaInput::merged_restore_assignment(
            &["orders".to_string(), "billing".to_string()],
            2,
            &[restored],
            false,
        );
        let elements = assignment.elements();
        assert_eq!(elements.len(), 2);
        let orders = elements
            .iter()
            .find(|e| e.topic() == "orders")
            .expect("orders assigned");
        assert_eq!(orders.offset(), rdkafka::Offset::Offset(42));
        let billing = elements
            .iter()
            .find(|e| e.topic() == "billing")
            .expect("billing assigned");
        assert_eq!(billing.offset(), rdkafka::Offset::Beginning);

        let assignment =
            KafkaInput::merged_restore_assignment(&["orders".to_string()], 2, &[], true);
        let elements = assignment.elements();
        assert_eq!(elements[0].offset(), rdkafka::Offset::End);
    }

    #[test]
    fn transactional_offsets_register_the_consumer_group() {
        let mut value = base_config();
        value["transactional_offsets"] = serde_json::json!(true);
        let input = input_with(value);
        assert!(
            input.txn_metadata.is_some(),
            "transactional offsets must register group metadata"
        );

        let input = input_with(base_config());
        assert!(input.txn_metadata.is_none());
    }

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
        let tests_start = source.find("#[cfg(test)]").expect("the test module exists");
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            segment_start: 100,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            segment_start: 42,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            segment_start: 1,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
            batch_max_rows: None,
            batch_max_bytes: None,
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
                batch_max_rows: None,
                batch_max_bytes: None,
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

    // ===== Offline coverage: subscription-mode connect, watermark and
    // ack-for-position guards, restore guards, and error classification. =====

    fn ack_of(input: &KafkaInput, topic: &str, partition: i32, offset: i64) -> KafkaAck {
        KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: topic.to_string(),
            partition,
            segment_start: offset,
            offset,
            transactional_offsets: false,
        }
    }

    /// Error classification: cancellation and queue-close variants are
    /// reconnectable; production errors and unknown shapes are not.
    #[test]
    fn retryable_receive_error_classification_is_complete() {
        // Canceled: reconnectable before the code extraction.
        assert!(KafkaInput::retryable_receive_error(&KafkaError::Canceled));
        // ConsumerQueueClose carries a code like the other two variants.
        assert!(KafkaInput::retryable_receive_error(
            &KafkaError::ConsumerQueueClose(RDKafkaErrorCode::AllBrokersDown,)
        ));
        assert!(!KafkaInput::retryable_receive_error(
            &KafkaError::ConsumerQueueClose(RDKafkaErrorCode::Authentication,)
        ));
        // An error shape without an embedded code is never retryable.
        assert!(!KafkaInput::retryable_receive_error(
            &KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull,)
        ));
        // Admin-op errors carry no consumer code either.
        assert!(!KafkaInput::retryable_receive_error(&KafkaError::AdminOp(
            RDKafkaErrorCode::Authentication,
        )));
    }

    /// Subscription-mode connect is broker-less (librdkafka joins the group
    /// in the background); watermark reads flow through the assignment.
    #[tokio::test]
    async fn connect_subscribe_mode_reads_watermarks_and_closes() {
        let input = input_with(base_config());

        // Before connect: watermark reports the missing consumer.
        let err = input.watermark_partitions().await.unwrap_err();
        assert!(err.to_string().contains("not connected"), "got: {err}");

        input
            .connect()
            .await
            .expect("subscribe-mode connect is offline");
        // The group assignment is still empty (no broker) — the partition
        // list comes back empty rather than erroring.
        let partitions = input.watermark_partitions().await.unwrap();
        assert!(partitions.is_empty());

        // The assignment probe agrees the partition is not ours yet.
        {
            let consumer_guard = input.consumer.read().await;
            let consumer = consumer_guard.as_ref().expect("connected consumer");
            assert!(!KafkaAck::partition_assigned(consumer, "orders", 0));
        }

        // close() unassigns the live consumer without an error.
        input.close().await.unwrap();
        // A read after close reports the missing consumer again.
        match input.read().await {
            Err(e) => assert!(e.to_string().contains("not connected")),
            Ok(_) => panic!("a closed input cannot read"),
        }
    }

    /// Explicit-partition watermarks map every configured topic to the one
    /// assigned partition, without touching the consumer at all.
    #[tokio::test]
    async fn watermark_partitions_with_an_explicit_assignment_maps_topics() {
        let input = input_with(serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topics": ["orders", "billing"],
            "consumer_group": "g",
            "start_from_latest": false,
        }));
        input.assign_partition(3).unwrap();
        let partitions = input.watermark_partitions().await.unwrap();
        let mut named: Vec<(Option<String>, u32)> = partitions
            .into_iter()
            .map(|p| (p.topic.clone(), p.partition))
            .collect();
        named.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            named,
            vec![
                (Some("billing".to_string()), 3),
                (Some("orders".to_string()), 3),
            ]
        );
    }

    /// `ack_for_position` guards: zero offsets, foreign topics and foreign
    /// partitions decline (None); matching positions hand back an ack; a
    /// missing topic or an out-of-i64 offset is a named error.
    #[tokio::test]
    async fn ack_for_position_guards_and_anchors() {
        let input = input_with(serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topics": ["orders"],
            "consumer_group": "g",
            "start_from_latest": false,
        }));
        // Zero offset: nothing to acknowledge.
        assert!(input
            .ack_for_position(&SourcePosition {
                topic: Some("orders".into()),
                partition: 0,
                offset: 0,
            })
            .await
            .unwrap()
            .is_none());
        // A topic this input is not subscribed to is not ours to ack.
        assert!(input
            .ack_for_position(&SourcePosition {
                topic: Some("other".into()),
                partition: 0,
                offset: 7,
            })
            .await
            .unwrap()
            .is_none());
        // Explicit-partition mode declines other partitions.
        input.assign_partition(1).unwrap();
        assert!(input
            .ack_for_position(&SourcePosition {
                topic: Some("orders".into()),
                partition: 5,
                offset: 7,
            })
            .await
            .unwrap()
            .is_none());
        // Matching topic+partition: an ack comes back anchored at the
        // record offset (checkpoint positions are exclusive).
        assert!(input
            .ack_for_position(&SourcePosition {
                topic: Some("orders".into()),
                partition: 1,
                offset: 9,
            })
            .await
            .unwrap()
            .is_some());
        // A position without a topic is filtered by the configured-topic
        // guard above (which requires Some(topic)); it declines instead of
        // reaching the missing-topic error.
        assert!(input
            .ack_for_position(&SourcePosition {
                topic: None,
                partition: 1,
                offset: 9,
            })
            .await
            .unwrap()
            .is_none());
        // An offset that does not fit i64 cannot become a record offset.
        let err = match input
            .ack_for_position(&SourcePosition {
                topic: Some("orders".into()),
                partition: 1,
                offset: u64::MAX,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("an out-of-i64 offset cannot yield an ack"),
        };
        assert!(err.to_string().contains("exceeds i64"), "got: {err}");
    }

    /// restore_positions before connect is a hard error; an empty
    /// checkpoint is a no-op in both assignment modes.
    #[tokio::test]
    async fn restore_positions_requires_connect_and_accepts_empty_checkpoints() {
        let input = input_with(base_config());
        let err = input.restore_positions(&[]).await.unwrap_err();
        assert!(err.to_string().contains("before connect"), "got: {err}");

        // Subscription mode: empty applicable set → no seeks, frontier seeded.
        input.connect().await.unwrap();
        input.restore_positions(&[]).await.unwrap();
        assert!(input.current_positions().await.unwrap().is_empty());
        input.close().await.unwrap();

        // Explicit-partition mode: the complete configured assignment is
        // re-applied even with an empty checkpoint.
        let input = input_with(base_config());
        input.assign_partition(0).unwrap();
        input.connect().await.unwrap();
        input.restore_positions(&[]).await.unwrap();
        input.close().await.unwrap();
    }

    /// A checkpoint whose watermarks cannot be fetched (broker-less connect)
    /// fails the restore — keeping the previous valid checkpoint selected.
    #[tokio::test]
    async fn restore_positions_surfaces_watermark_failures() {
        let input = input_with(base_config());
        input.connect().await.unwrap();
        let err = input
            .restore_positions(&[SourcePosition {
                topic: Some("orders".into()),
                partition: 0,
                offset: 5,
            }])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("fetch Kafka watermarks"),
            "got: {err}"
        );
        input.close().await.unwrap();
    }

    /// Partition assignment is rejected for L3 (transactional offsets)
    /// inputs; partitioning support is advertised.
    #[test]
    fn assign_partition_is_rejected_for_transactional_offsets() {
        let mut value = base_config();
        value["transactional_offsets"] = serde_json::json!(true);
        let input = input_with(value);
        let err = input.assign_partition(0).unwrap_err();
        assert!(
            err.to_string().contains("requires subscribe mode"),
            "got: {err}"
        );
        assert!(input.supports_partitioning());

        let plain = input_with(base_config());
        plain.assign_partition(7).unwrap();
        assert!(plain.supports_partitioning());
    }

    /// Compensation without a live consumer is an explicit retryable error
    /// (reaching the broker store requires the frontier to stand at the
    /// ack's next offset — the state undo actually compensates).
    #[tokio::test]
    async fn undo_without_a_consumer_is_retryable() {
        let input = input_with(base_config());
        input.frontier.seed(&[SourcePosition {
            topic: Some("orders".into()),
            partition: 0,
            offset: 42,
        }]);
        let ack = ack_of(&input, "orders", 0, 41);
        let err = ack.undo().await.unwrap_err();
        assert!(
            err.to_string().contains("compensation is retryable"),
            "got: {err}"
        );
        // An unacknowledged delivery (empty frontier) has nothing to
        // compensate: undo is a no-op success, not a consumer error.
        let input = input_with(base_config());
        let ack = ack_of(&input, "orders", 0, 41);
        ack.undo().await.expect("nothing to compensate");
    }

    /// Spec "配对输入的 undo 不触 store_offset": with
    /// `transactional_offsets` the undo compensates purely in memory — it
    /// never touches the consumer (there is none here) and still rewinds
    /// the frontier so the record is redelivered. The broker group offset
    /// only ever moves inside the paired output's transaction.
    #[tokio::test]
    async fn undo_with_transactional_offsets_only_rewinds_the_frontier() {
        let mut value = base_config();
        // A unique group keeps the process-global pairing registry clean.
        value["consumer_group"] = serde_json::json!(format!("undo-txn-{}", std::process::id()));
        value["transactional_offsets"] = serde_json::json!(true);
        let input = input_with(value);
        crate::kafka_txn::declare_offset_committer(&input.config.consumer_group);
        // The acknowledged frontier stands at the ack's next offset.
        input.frontier.seed(&[SourcePosition {
            topic: Some("orders".into()),
            partition: 0,
            offset: 42,
        }]);
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: "orders".to_string(),
            partition: 0,
            segment_start: 41,
            offset: 41,
            transactional_offsets: true,
        };
        // No consumer is connected: a store_offset path would fail here
        // with the retryable error — the L3 branch must succeed regardless.
        ack.undo()
            .await
            .expect("L3 undo compensates in memory only");
        let positions = input.current_positions().await.unwrap();
        assert_eq!(
            positions.len(),
            1,
            "the frontier still tracks the partition"
        );
        assert_eq!(
            positions[0].offset, 41,
            "the frontier rewound by one record"
        );
    }

    /// The frontier registered for an L3 input is the input's own instance
    /// — the one its acknowledgements advance (the output clamps against
    /// exactly this object).
    #[test]
    fn transactional_offsets_register_the_inputs_own_frontier() {
        let mut value = base_config();
        value["consumer_group"] = serde_json::json!(format!("frontier-reg-{}", std::process::id()));
        value["transactional_offsets"] = serde_json::json!(true);
        let input = input_with(value);
        crate::kafka_txn::declare_offset_committer(&input.config.consumer_group);
        let registered = crate::kafka_txn::group_frontier(&input.config.consumer_group)
            .expect("transactional input registers its frontier");
        assert!(
            Arc::ptr_eq(&registered, &input.frontier),
            "the registry must carry the frontier instance the acks advance"
        );
    }

    /// The builder rejects malformed configurations up front.
    #[test]
    fn builder_rejects_malformed_configs() {
        let missing = match KafkaInputBuilder.build(
            None,
            &None,
            None,
            &Resource {
                temporary: HashMap::new(),
                input_names: Default::default(),
            },
        ) {
            Err(e) => e,
            Ok(_) => panic!("a missing config must be rejected"),
        };
        assert!(
            missing.to_string().to_lowercase().contains("kafka input"),
            "got: {missing}"
        );

        let malformed = match KafkaInputBuilder.build(
            None,
            &Some(serde_json::json!({"unexpected": true})),
            None,
            &Resource {
                temporary: HashMap::new(),
                input_names: Default::default(),
            },
        ) {
            Err(e) => e,
            Ok(_) => panic!("a malformed config must be rejected"),
        };
        assert!(
            malformed.to_string().to_lowercase().contains("kafka input"),
            "got: {malformed}"
        );
    }

    /// The builder hands back a working input for a valid configuration.
    #[test]
    fn builder_accepts_a_valid_config() {
        let built = KafkaInputBuilder.build(
            None,
            &Some(base_config()),
            None,
            &Resource {
                temporary: HashMap::new(),
                input_names: Default::default(),
            },
        );
        assert!(built.is_ok(), "a valid config must build");
    }

    /// A consumer that librdkafka refuses to construct surfaces as a
    /// connection error at connect time (empty `group.id` is rejected by
    /// the client, offline).
    #[tokio::test]
    async fn connect_maps_consumer_creation_failures() {
        let input = input_with(serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topics": ["orders"],
            "consumer_group": "",
            "start_from_latest": false,
        }));
        match input.connect().await {
            Err(e) => assert!(
                e.to_string().contains("Unable to create a Kafka consumer"),
                "got: {e}"
            ),
            Ok(()) => panic!("an empty group id must fail consumer creation"),
        }
    }

    // ===== Batch read machinery (spec: kafka-input-batching) =====

    use std::collections::VecDeque;

    fn claimed_record(
        topic: &str,
        partition: i32,
        offset: i64,
        payload: &[u8],
        key: Option<&[u8]>,
    ) -> ClaimedRecord {
        ClaimedRecord {
            payload: payload.to_vec(),
            topic: topic.to_string(),
            partition,
            offset,
            key: key.map(<[u8]>::to_vec),
            timestamp: Some(SystemTime::UNIX_EPOCH),
            ext: vec![
                ("topic".to_string(), topic.to_string()),
                ("header_trace".to_string(), format!("v{offset}")),
            ],
        }
    }

    /// The drain aggregates buffered messages up to the row bound and stops
    /// with `Complete`; a single unclaimed queue is a one-record batch.
    #[test]
    fn drain_aggregates_up_to_the_row_bound() {
        let source = |offsets: &[i64]| {
            let mut pending: VecDeque<Result<Claim, KafkaError>> = offsets
                .iter()
                .map(|o| Ok(Claim::Data(claimed_record("t", 0, *o, b"x", None))))
                .collect();
            move || pending.pop_front()
        };

        let (records, stop) = KafkaInput::drain_records(
            claimed_record("t", 0, 1, b"x", None),
            3,
            u64::MAX,
            source(&[2, 3, 4, 5]),
            |_| {},
        )
        .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(stop, DrainStop::Complete);

        let (records, stop) = KafkaInput::drain_records(
            claimed_record("t", 0, 1, b"x", None),
            1024,
            u64::MAX,
            source(&[]),
            |_| {},
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(stop, DrainStop::QueueEmpty);
    }

    /// The byte bound stops the drain even when the row bound would allow
    /// more (payloads of 10 bytes, bound 25 → three records accumulate 30).
    #[test]
    fn drain_stops_at_the_byte_bound() {
        let mut pending: VecDeque<Result<Claim, KafkaError>> = (2..=6)
            .map(|o| Ok(Claim::Data(claimed_record("t", 0, o, &[0u8; 10], None))))
            .collect();
        let (records, stop) = KafkaInput::drain_records(
            claimed_record("t", 0, 1, &[0u8; 10], None),
            1024,
            25,
            move || pending.pop_front(),
            |_| {},
        )
        .unwrap();
        assert_eq!(records.len(), 3, "bytes 10+10+10=30 crosses the 25 bound");
        assert_eq!(stop, DrainStop::Complete);
    }

    /// Tombstones inside the drain are handed to the settlement callback and
    /// never join the data batch; a retryable receive error keeps every
    /// already-claimed record; a fatal error propagates.
    #[test]
    fn drain_settles_tombstones_and_classifies_errors() {
        let mut settled: Vec<i64> = Vec::new();
        let mut pending: VecDeque<Result<Claim, KafkaError>> = VecDeque::from(vec![
            Ok(Claim::Data(claimed_record("t", 0, 2, b"x", None))),
            Ok(Claim::Tombstone(TombstoneSite {
                topic: "t".into(),
                partition: 0,
                offset: 3,
            })),
            Ok(Claim::Data(claimed_record("t", 0, 4, b"x", None))),
        ]);
        let (records, stop) = KafkaInput::drain_records(
            claimed_record("t", 0, 1, b"x", None),
            1024,
            u64::MAX,
            move || pending.pop_front(),
            |site| settled.push(site.offset),
        )
        .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [1, 2, 4]
        );
        assert_eq!(stop, DrainStop::QueueEmpty);
        assert_eq!(settled, [3]);

        let mut retryable: VecDeque<Result<Claim, KafkaError>> = VecDeque::from(vec![
            Ok(Claim::Data(claimed_record("t", 0, 2, b"x", None))),
            Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::AllBrokersDown,
            )),
        ]);
        let (records, stop) = KafkaInput::drain_records(
            claimed_record("t", 0, 1, b"x", None),
            1024,
            u64::MAX,
            move || retryable.pop_front(),
            |_| {},
        )
        .unwrap();
        assert_eq!(records.len(), 2, "claimed records survive a reconnect");
        assert_eq!(stop, DrainStop::Reconnect);

        let mut fatal: VecDeque<Result<Claim, KafkaError>> = VecDeque::from(vec![Err(
            KafkaError::MessageConsumption(RDKafkaErrorCode::Authentication),
        )]);
        assert!(KafkaInput::drain_records(
            claimed_record("t", 0, 1, b"x", None),
            1024,
            u64::MAX,
            move || fatal.pop_front(),
            |_| {},
        )
        .is_err());
    }

    /// After a drain saw a retryable disconnect, the reconnect probe gives
    /// buffered messages one non-blocking chance first (tombstones settle out
    /// of band), surfaces `Err(retryable)` on another receive error, and
    /// reports `Ok(None)` — the "queue drained, surface the reconnect" point —
    /// once nothing is buffered.
    #[test]
    fn pending_reconnect_probe_yields_buffered_first_then_none() {
        let mut buffered: VecDeque<Result<Claim, KafkaError>> = VecDeque::from(vec![
            Ok(Claim::Tombstone(TombstoneSite {
                topic: "t".to_string(),
                partition: 0,
                offset: 2,
            })),
            Ok(Claim::Data(claimed_record("t", 0, 3, b"x", None))),
        ]);
        let settled = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let sink = settled.clone();
        let first = KafkaInput::pending_reconnect_first(
            move || buffered.pop_front(),
            move |site| sink.borrow_mut().push(site.offset),
        )
        .unwrap();
        assert_eq!(first.unwrap().offset, 3, "buffered data comes out first");
        assert_eq!(*settled.borrow(), [2], "tombstones settle out of band");

        let mut empty: VecDeque<Result<Claim, KafkaError>> = VecDeque::new();
        assert!(
            KafkaInput::pending_reconnect_first(move || empty.pop_front(), |_| {})
                .unwrap()
                .is_none(),
            "an empty queue is the reconnect surfacing point"
        );

        let mut errored: VecDeque<Result<Claim, KafkaError>> = VecDeque::from(vec![Err(
            KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown),
        )]);
        assert!(KafkaInput::pending_reconnect_first(move || errored.pop_front(), |_| {}).is_err());
    }

    /// Segments group per (topic, partition) in first-seen order with
    /// min/max offsets, so one partition never splits across two segments.
    #[test]
    fn segments_group_per_partition_in_first_seen_order() {
        let records = vec![
            claimed_record("t", 0, 5, b"x", None),
            claimed_record("t", 0, 6, b"x", None),
            claimed_record("t", 1, 9, b"x", None),
            claimed_record("u", 0, 100, b"x", None),
            claimed_record("t", 0, 7, b"x", None),
        ];
        let segments = KafkaInput::group_ack_segments(&records);
        let shape: Vec<(&str, i32, i64, i64)> = segments
            .iter()
            .map(|s| (s.topic.as_str(), s.partition, s.first, s.last))
            .collect();
        assert_eq!(shape, [("t", 0, 5, 7), ("t", 1, 9, 9), ("u", 0, 100, 100)]);
    }

    #[tokio::test]
    async fn decode_and_attach_aligns_fast_path_per_row() {
        let input = input_with(base_config());
        let records = vec![
            claimed_record("orders", 0, 10, b"a", Some(b"k1")),
            claimed_record("orders", 0, 11, b"b", None),
            claimed_record("orders", 1, 20, b"c", Some(b"k3")),
        ];
        let batch = input.decode_and_attach(&records).await.unwrap();
        assert_eq!(batch.num_rows(), 3);

        use datafusion::arrow::array::{
            Array as _, BinaryArray, MapArray, StringArray, UInt32Array, UInt64Array,
        };
        let offsets = batch
            .column_by_name("__meta_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(offsets.values(), &[10, 11, 20]);
        let partitions = batch
            .column_by_name("__meta_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(partitions.values(), &[0, 0, 1]);
        let keys = batch
            .column_by_name("__meta_key")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"k1");
        assert!(keys.is_null(1));
        assert_eq!(keys.value(2), b"k3");

        let ext = batch
            .column_by_name("__meta_ext")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        for (row, offset) in [10i64, 11, 20].iter().enumerate() {
            let entries = ext.value(row);
            let keys = entries
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = entries
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let map: Vec<(&str, &str)> = (0..entries.len())
                .map(|i| (keys.value(i), values.value(i)))
                .collect();
            assert_eq!(
                map,
                vec![("topic", "orders"), ("header_trace", &format!("v{offset}"))],
                "row {row}"
            );
        }
    }

    /// A codec that expands one payload into two rows trips the fast path's
    /// row-count check; the fallback re-decodes per payload so every row
    /// still carries its own message's offsets.
    struct RowDoublingCodec;

    #[async_trait]
    impl arkflow_core::codec::Encoder for RowDoublingCodec {
        async fn encode(&self, _messages: MessageBatch) -> Result<Vec<Bytes>, Error> {
            Err(Error::Process("test codec does not encode".into()))
        }
    }

    #[async_trait]
    impl arkflow_core::codec::Decoder for RowDoublingCodec {
        async fn decode(&self, payloads: Vec<Bytes>) -> Result<MessageBatch, Error> {
            let doubled: Vec<Bytes> = payloads
                .iter()
                .flat_map(|p| [p.clone(), p.clone()])
                .collect();
            MessageBatch::new_binary(doubled)
        }
    }

    /// A codec that drops payloads not starting with `b` (skip-mode shape):
    /// the batch decode yields fewer rows than payloads, and the per-payload
    /// fallback drops exactly the skipped payloads while keeping alignment.
    struct OddOnlyCodec;

    #[async_trait]
    impl arkflow_core::codec::Encoder for OddOnlyCodec {
        async fn encode(&self, _messages: MessageBatch) -> Result<Vec<Bytes>, Error> {
            Err(Error::Process("test codec does not encode".into()))
        }
    }

    #[async_trait]
    impl arkflow_core::codec::Decoder for OddOnlyCodec {
        async fn decode(&self, payloads: Vec<Bytes>) -> Result<MessageBatch, Error> {
            let kept: Vec<Bytes> = payloads
                .iter()
                .filter(|payload| payload.first() == Some(&b'b'))
                .cloned()
                .collect();
            MessageBatch::new_binary(kept)
        }
    }

    fn input_with_codec(codec: Arc<dyn Codec>) -> KafkaInput {
        KafkaInput::new(None, config_from_json(base_config()), Some(codec)).unwrap()
    }

    #[tokio::test]
    async fn decode_and_attach_fallback_maps_rows_to_their_payload() {
        let input = input_with_codec(Arc::new(RowDoublingCodec));
        let records = vec![
            claimed_record("orders", 0, 10, b"a", None),
            claimed_record("orders", 0, 11, b"b", None),
        ];
        let batch = input.decode_and_attach(&records).await.unwrap();
        assert_eq!(batch.num_rows(), 4);
        let offsets = batch
            .column_by_name("__meta_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(offsets.values(), &[10, 10, 11, 11]);
    }

    #[tokio::test]
    async fn decode_and_attach_fallback_skipped_payload_keeps_alignment() {
        let input = input_with_codec(Arc::new(OddOnlyCodec));
        let records = vec![
            claimed_record("orders", 0, 10, b"a", None),
            claimed_record("orders", 0, 11, b"b", None),
            claimed_record("orders", 0, 12, b"c", None),
        ];
        let batch = input.decode_and_attach(&records).await.unwrap();
        assert_eq!(batch.num_rows(), 1, "only the odd-indexed payload survives");
        let offsets = batch
            .column_by_name("__meta_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(offsets.values(), &[11]);
    }

    /// Cancellation safety (structural): the drain engine must stay a
    /// synchronous function — an `async fn` here could suspend between the
    /// first claim and the batch return, and the engine's select! would drop
    /// the claimed messages. The read loop must probe with `now_or_never`.
    #[test]
    fn drain_engine_is_synchronous_and_probes_non_blocking() {
        // Bound the scan at the test module: the assertion literals below
        // would otherwise match their own text embedded by `include_str!`.
        let source = include_str!("kafka.rs");
        let (head, _) = source
            .split_once("#[cfg(test)]")
            .expect("the test module exists");
        assert!(
            !head.contains("async fn drain_records"),
            "drain_records must stay non-async (cancellation-safety contract)"
        );
        assert!(head.contains("fn drain_records("));
        assert!(
            head.contains("stream.next().now_or_never()"),
            "the drain must probe the consumer's queue non-blockingly"
        );
    }

    /// L3 undo rewinds the frontier to the segment's FIRST offset — the
    /// whole batch is one delivery unit and must be redelivered together.
    #[tokio::test]
    async fn undo_rewinds_the_whole_segment_frontier() {
        let mut value = base_config();
        value["consumer_group"] = serde_json::json!(format!("segment-undo-{}", std::process::id()));
        value["transactional_offsets"] = serde_json::json!(true);
        let input = input_with(value);
        crate::kafka_txn::declare_offset_committer(&input.config.consumer_group);
        // The acknowledged frontier stands at the segment's next offset (8).
        input.frontier.seed(&[SourcePosition {
            topic: Some("orders".into()),
            partition: 0,
            offset: 8,
        }]);
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            ack_lock: input.ack_lock.clone(),
            ack_notify: input.ack_notify.clone(),
            close: input.close.clone(),
            topic: "orders".to_string(),
            partition: 0,
            segment_start: 5,
            offset: 7,
            transactional_offsets: true,
        };
        ack.undo().await.expect("L3 undo compensates in memory");
        let positions = input.current_positions().await.unwrap();
        assert_eq!(positions[0].offset, 5, "the whole segment replays");
    }

    /// Segment ack semantics against the frontier: anchoring at the segment
    /// start and acknowledging every offset advances the contiguous frontier
    /// exactly as per-message acknowledgements would.
    #[test]
    fn segment_acknowledgement_advances_like_per_message_acks() {
        let input = input_with(base_config());
        let frontier = &input.frontier;
        frontier.anchor_delivery(&SourcePosition {
            topic: Some("orders".into()),
            partition: 0,
            offset: 5,
        });
        for next_offset in [6u64, 7, 8] {
            assert_eq!(
                frontier.acknowledge(&SourcePosition {
                    topic: Some("orders".into()),
                    partition: 0,
                    offset: next_offset,
                }),
                AckAdvance::Advanced { next_offset }
            );
        }
        let positions = futures::executor::block_on(input.current_positions()).unwrap();
        assert_eq!(positions[0].offset, 8);
    }

    /// Batch bounds resolution: defaults, and sub-1 values clamp to 1.
    #[test]
    fn batch_bounds_default_and_clamp() {
        let plain = input_with(base_config());
        assert_eq!(plain.batch_max_rows, DEFAULT_BATCH_MAX_ROWS as usize);
        assert_eq!(plain.batch_max_bytes, DEFAULT_BATCH_MAX_BYTES);

        let mut value = base_config();
        value["batch_max_rows"] = serde_json::json!(0);
        value["batch_max_bytes"] = serde_json::json!(0);
        let clamped = input_with(value);
        assert_eq!(clamped.batch_max_rows, 1, "0 rows clamps to per-message");
        assert_eq!(clamped.batch_max_bytes, 1);

        let mut value = base_config();
        value["batch_max_rows"] = serde_json::json!(1);
        let single = input_with(value);
        assert_eq!(single.batch_max_rows, 1);
    }

    /// Ad-hoc release timing (NOT run by CI): batch assembly (one codec call
    /// + one metadata rebuild) vs the historical per-message path (7 chained
    /// `with_*` rebuilds per message). Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored kafka_batch_assembly_timing --nocapture`
    #[tokio::test]
    #[ignore]
    async fn kafka_batch_assembly_timing() {
        let total = 200_000usize;
        let batch_size = 1_000usize;
        let ingest = SystemTime::now();
        let records: Vec<ClaimedRecord> = (0..total)
            .map(|offset| {
                let mut record =
                    claimed_record("bench", 0, offset as i64, b"payload-bytes", Some(b"key"));
                record.timestamp = Some(SystemTime::UNIX_EPOCH);
                record
            })
            .collect();

        let time = |name: &str, f: &mut dyn FnMut() -> usize| {
            let mut best = std::time::Duration::MAX;
            let mut rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                rows = f();
                best = best.min(start.elapsed());
            }
            println!(
                "{name}: {} rows in {:?} ({:.0} rows/s)",
                rows,
                best,
                rows as f64 / best.as_secs_f64()
            );
        };

        let input = input_with(base_config());
        let mut batch_path = || {
            let mut rows = 0;
            for chunk in records.chunks(batch_size) {
                let batch =
                    futures::executor::block_on(input.decode_and_attach(chunk)).expect("assembly");
                rows += batch.num_rows();
            }
            rows
        };
        time("batch assembly (decode_and_attach)", &mut batch_path);

        let mut oracle_path = || {
            let mut rows = 0;
            for record in &records {
                let batch = MessageBatch::new_binary(vec![record.payload.clone()]).unwrap();
                let batch: datafusion::arrow::record_batch::RecordBatch = batch.into();
                let batch = metadata::with_source(batch, "kafka").unwrap();
                let batch = metadata::with_partition(batch, record.partition as u32).unwrap();
                let batch = metadata::with_offset(batch, record.offset as u64).unwrap();
                let batch = metadata::with_key(batch, &record.key.clone().unwrap()).unwrap();
                let batch = metadata::with_timestamp(batch, record.timestamp.unwrap()).unwrap();
                let batch = metadata::with_ingest_time(batch, ingest).unwrap();
                let mut ext = HashMap::new();
                for (key, value) in &record.ext {
                    ext.insert(key.clone(), value.clone());
                }
                metadata::with_ext_metadata(batch, &ext).unwrap();
                rows += 1;
            }
            rows
        };
        time("per-message metadata chain (oracle)", &mut oracle_path);
    }
}
