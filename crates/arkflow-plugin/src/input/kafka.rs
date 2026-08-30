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
use arkflow_core::executor::commit::{AckAdvance, CommitFrontier};
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{metadata, Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Message as KafkaMessage, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
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
    /// Fetch min bytes
    pub fetch_min_bytes: Option<u32>,
    /// Fetch max bytes
    pub fetch_max_bytes: Option<u32>,
    /// Fetch max partition bytes
    pub fetch_max_partition_bytes: Option<u32>,
    /// Fetch wait max milliseconds
    pub fetch_wait_max_ms: Option<u64>,
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
    codec: Option<Arc<dyn Codec>>,
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
        Ok(Self {
            input_name: name.cloned(),
            config,
            consumer: Arc::new(RwLock::new(None)),
            assigned_partition: Arc::new(RwLock::new(None)),
            frontier: Arc::new(CommitFrontier::new()),
            codec,
        })
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
    /// `enable.auto.offset.store=false`) are unit-testable without a broker.
    fn build_client_config(&self) -> ClientConfig {
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

        client_config
    }
}

#[async_trait]
impl Input for KafkaInput {
    async fn connect(&self) -> Result<(), Error> {
        let client_config = self.build_client_config();

        // Create consumers
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("Unable to create a Kafka consumer: {}", e)))?;

        if let Some(partition) = *self
            .assigned_partition
            .try_read()
            .map_err(|_| Error::Process("Kafka partition assignment lock is unavailable".into()))?
        {
            let mut assignment = TopicPartitionList::new();
            for topic in &self.config.topics {
                assignment.add_partition(topic, partition as i32);
            }
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

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let consumer_arc = self.consumer.clone();
        let consumer_guard = consumer_arc.read().await;
        if consumer_guard.is_none() {
            return Err(Error::Connection("The input is not connected".to_string()));
        }
        let consumer = consumer_guard.as_ref().unwrap();

        match consumer.recv().await {
            Ok(kafka_message) => {
                // Get payload from Kafka message
                let payload = kafka_message.payload().ok_or_else(|| {
                    Error::Process("The Kafka message has no content".to_string())
                })?;

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

                // Add headers if present
                // Note: rdkafka Headers API varies by version, skipping for now
                // TODO: Implement headers extraction based on rdkafka version

                record_batch = metadata::with_ext_metadata(record_batch, &ext_metadata)?;

                // Convert back to MessageBatch
                let mut msg_batch = MessageBatch::new_arrow(record_batch);
                msg_batch.set_input_name(self.input_name.clone());

                // Create acknowledgment object
                let ack = KafkaAck {
                    consumer: self.consumer.clone(),
                    frontier: self.frontier.clone(),
                    topic: kafka_message.topic().to_string(),
                    partition,
                    offset,
                };

                Ok((Arc::new(msg_batch), Arc::new(ack)))
            }
            Err(e) => Err(Error::Connection(format!(
                "Error receiving Kafka message: {}",
                e
            ))),
        }
    }

    async fn current_positions(&self) -> Result<Vec<SourcePosition>, Error> {
        // Only the contiguous acknowledged frontier: a gap (fan-out
        // acknowledgement still pending) holds the position back so a
        // checkpoint never skips unacknowledged records.
        Ok(self.frontier.contiguous_positions())
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
                // has not assigned yet may reject the seek right after
                // (re)connect — retry briefly for the assignment to arrive.
                for position in &applicable {
                    let offset =
                        Self::validate_checkpoint_offset(position.offset, i64::MIN, i64::MAX)?;
                    let topic = position.topic.as_deref().unwrap_or_default();
                    let partition = position.partition as i32;
                    let mut attempt = 0;
                    loop {
                        match consumer.seek(
                            topic,
                            partition,
                            Offset::Offset(offset),
                            Duration::from_secs(5),
                        ) {
                            Ok(()) => break,
                            Err(error) if attempt < 2 => {
                                attempt += 1;
                                tracing::warn!(
                                    %error, topic, partition,
                                    "Kafka restore seek failed; waiting for group assignment"
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
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
    topic: String,
    partition: i32,
    offset: i64,
}

#[async_trait]
impl Ack for KafkaAck {
    async fn ack(&self) -> Result<(), Error> {
        // Only called after the downstream output confirms the write. The
        // frontier records the acknowledgement; the durable store advances to
        // the CONTIGUOUS acknowledged next offset — a fan-out completing this
        // branch before an earlier one leaves the store waiting at the gap so
        // a crash never skips the unacknowledged records in between.
        let position = SourcePosition {
            topic: Some(self.topic.clone()),
            partition: self.partition.max(0) as u32,
            offset: u64::try_from(self.offset.saturating_add(1))
                .map_err(|_| Error::Process("Kafka offset overflow".into()))?,
        };
        let next_offset = match self.frontier.acknowledge(&position) {
            AckAdvance::Pending { .. } => return Ok(()),
            AckAdvance::Advanced { next_offset } => next_offset,
            // A retry after the durable store failed: the frontier already
            // advanced, only the store needs to catch up.
            AckAdvance::AlreadyCovered => self
                .frontier
                .next_offset_of(Some(&self.topic), self.partition.max(0) as u32)
                .unwrap_or(position.offset),
        };
        let store_offset_value = i64::try_from(next_offset.saturating_sub(1))
            .map_err(|_| Error::Process("Kafka offset overflow".into()))?;
        let consumer_mutex_guard = self.consumer.read().await;
        if let Some(consumer) = &*consumer_mutex_guard {
            consumer
                .store_offset(&self.topic, self.partition, store_offset_value)
                .map_err(|e| Error::Process(format!("Failed to store Kafka offset: {}", e)))?;
        }
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
                "fetch_wait_max_ms": {"type": "integer", "minimum": 0, "description": "Maximum time to wait for fetch data in milliseconds."}
            },
            "required": ["brokers", "topics", "consumer_group"]
        }),
    ).with_example(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topics": ["events"],
        "consumer_group": "arkflow"
    })))
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
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
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
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
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
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
        };

        let input = KafkaInput::new(None, config, None).unwrap();
        assert!(input.current_positions().await.unwrap().is_empty());
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
        };

        // Test acknowledgment, should have no effect since there is no actual consumer
        let _ = ack.ack().await;
        let positions = input.current_positions().await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].topic.as_deref(), Some("test-topic"));
        assert_eq!(positions[0].partition, 0);
        assert_eq!(positions[0].offset, 101);
    }

    /// Task 3.3: out-of-order acknowledgements expose only the contiguous
    /// frontier — an acknowledged offset beyond a gap does not advance the
    /// checkpoint position past the unacknowledged records.
    #[tokio::test]
    async fn out_of_order_acknowledgements_wait_for_the_gap() {
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: true,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let frontier = input.frontier.clone();
        // Deliveries 5, 6, 7 (the frontier anchors at the first delivery).
        frontier.anchor_delivery(&SourcePosition {
            topic: Some("test-topic".into()),
            partition: 0,
            offset: 5,
        });
        let ack_at = |offset: i64| KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset,
        };
        // The later branch of a fan-out completes first.
        ack_at(7).ack().await.unwrap();
        let positions = input.current_positions().await.unwrap();
        assert_eq!(positions[0].offset, 5, "the gap holds the frontier");
        ack_at(6).ack().await.unwrap();
        assert_eq!(input.current_positions().await.unwrap()[0].offset, 5);
        // Closing the gap drains the contiguous run to 8 at once.
        ack_at(5).ack().await.unwrap();
        assert_eq!(input.current_positions().await.unwrap()[0].offset, 8);
    }

    /// Task 3.3: restored positions seed the in-memory frontier, so a
    /// checkpoint immediately after restore reports the restored cursor.
    #[tokio::test]
    async fn restored_positions_seed_the_checkpoint_cursor() {
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
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
        // A new acknowledgement continues from the restored cursor.
        let ack = KafkaAck {
            consumer: input.consumer.clone(),
            frontier: input.frontier.clone(),
            topic: "test-topic".to_string(),
            partition: 3,
            offset: 42,
        };
        ack.ack().await.unwrap();
        assert_eq!(input.current_positions().await.unwrap()[0].offset, 43);
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
    fn test_kafka_disables_auto_offset_store_for_crash_safety() {
        // Phase 0 (add-input-durability): at-least-once crash-safety depends on
        // offsets being stored ONLY inside `KafkaAck::ack()` (which fires after
        // the downstream output confirms the write), never on `recv()`. Verify
        // the consumer config disables rdkafka's automatic offset store.
        let config = KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: None,
            start_from_latest: false,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_partition_bytes: None,
            fetch_wait_max_ms: None,
        };
        let input = KafkaInput::new(None, config, None).unwrap();
        let client_config = input.build_client_config();
        assert_eq!(
            client_config.get("enable.auto.offset.store"),
            Some("false"),
            "auto offset store MUST be disabled so offsets advance only on ack (at-least-once)"
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
                brokers: vec![brokers.to_string()],
                topics: vec![topics.to_string()],
                consumer_group: group.to_string(),
                client_id: None,
                start_from_latest: false,
                fetch_min_bytes: None,
                fetch_max_bytes: None,
                fetch_max_partition_bytes: None,
                fetch_wait_max_ms: None,
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
