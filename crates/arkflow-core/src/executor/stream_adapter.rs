//! Job component adapter that rebuilds compiled-stream components verbatim.
//!
//! The stream compiler (see `stream_compiler`) stashes the original
//! `InputConfig`/`OutputConfig` payloads inside the JobSpec source/sink
//! configs. This adapter reverses that mapping so the unified kernel builds
//! exactly the components the legacy runtime would have built — including
//! codecs, names, temporary tables, and the WAL-backed input wrapper.

use crate::executor::stream_compiler::CODEC_PAYLOAD_KEY;
use crate::input::{ConcurrentAck, Input, InputConfig};
use crate::job::{JobComponentAdapter, OperatorSpec, SinkSpec, SourceSpec};
use crate::wal::{Wal, WalAck, WalConfig};
use crate::Error;
use crate::{
    codec::CodecConfig,
    output::{Output, OutputConfig},
    processor::{Processor, ProcessorConfig},
    Resource,
};
use datafusion::arrow::array::{
    Array, Int32Array, Int64Array, MapArray, StringArray, UInt32Array, UInt64Array,
};
use std::sync::Arc;

/// Adapter for one compiled stream: rebuilds components from the original
/// `StreamConfig` pieces embedded in the JobSpec, and attaches the stream's
/// WAL to the built input (the WAL is an input durability property, not an
/// execution-model property).
pub struct StreamJobAdapter {
    wal: Option<Arc<Wal>>,
    /// Temporary-table configs from the stream; `build_resource` constructs
    /// them so SQL processors find their tables in the shared Resource.
    temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
}

impl StreamJobAdapter {
    /// Build the adapter for a compiled stream, opening the WAL when the
    /// stream declares enabled durability.
    pub fn new(durability: Option<&WalConfig>) -> Result<Self, Error> {
        Self::with_temporary(durability, None)
    }

    /// Adapter carrying the stream's temporary-table configs.
    pub fn with_temporary(
        durability: Option<&WalConfig>,
        temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
    ) -> Result<Self, Error> {
        let wal = match durability {
            Some(config) if config.enabled => Some(Wal::open(config)?),
            _ => None,
        };
        Ok(Self { wal, temporary })
    }

    /// Construct the shared Resource with temporary tables built (SQL
    /// processors resolve `temporary_list` entries from it). Also serves as
    /// the synchronous dry-run: unknown inputs/outputs/temporaries fail
    /// here, before the kernel starts.
    pub fn build_resource(&self) -> Result<Resource, Error> {
        let mut resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        if let Some(temporary_configs) = &self.temporary {
            for temporary_config in temporary_configs {
                resource.temporary.insert(
                    temporary_config.name.clone(),
                    temporary_config.build(&resource)?,
                );
            }
        }
        Ok(resource)
    }

    pub fn wal(&self) -> Option<&Arc<Wal>> {
        self.wal.as_ref()
    }

    /// Close the adapter's WAL: stop the background flusher, flush pending
    /// appends, and release the store handle's exclusive lock. Dry-run and
    /// deep-validation paths must call this — including on their error paths
    /// — before the real runtime rebuilds the adapter and reopens the same
    /// WAL path.
    pub async fn close(&self) -> Result<(), Error> {
        match &self.wal {
            Some(wal) => wal.close().await,
            None => Ok(()),
        }
    }
}

fn decode_codec(payload: &serde_json::Value) -> Result<Option<CodecConfig>, Error> {
    match payload.get(CODEC_PAYLOAD_KEY) {
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|error| {
                Error::Config(format!("compiled stream codec payload is invalid: {error}"))
            }),
        None => Ok(None),
    }
}

fn decode_name(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// Wrap an input with the adapter's WAL: replays unacknowledged entries
/// before new reads (crash recovery), appends each batch, and gates the ack
/// on the WAL commit — exactly like the legacy `Stream::do_input` path.
pub struct WalInput {
    inner: Arc<dyn Input>,
    wal: Arc<Wal>,
    /// Unacked-entry replay queue, initialized on the first read (the
    /// component-builder trait is sync; WAL reads are async).
    replay: tokio::sync::OnceCell<
        tokio::sync::Mutex<std::collections::VecDeque<(u64, crate::MessageBatchRef)>>,
    >,
    /// Checkpoint positions are installed before the first read.  WAL
    /// entries are durable independently of the connector cursor, so the
    /// replay queue must discard entries already covered by a checkpoint
    /// while retaining entries that were read but not acknowledged.
    checkpoint_positions: tokio::sync::RwLock<Option<Vec<crate::checkpoint::SourcePosition>>>,
}

impl WalInput {
    pub fn new(inner: Arc<dyn Input>, wal: Arc<Wal>) -> Self {
        Self {
            inner,
            wal,
            replay: tokio::sync::OnceCell::new(),
            checkpoint_positions: tokio::sync::RwLock::new(None),
        }
    }

    fn batch_is_covered_by_checkpoint(
        seq: u64,
        batch: &crate::MessageBatch,
        positions: &[crate::checkpoint::SourcePosition],
    ) -> bool {
        if positions.is_empty() || batch.is_empty() {
            return false;
        }

        let partition = batch
            .record_batch()
            .column_by_name(crate::meta_columns::PARTITION)
            .and_then(|column| {
                if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
                    Some(
                        (0..array.len())
                            .map(|row| array.value(row) as u64)
                            .collect::<Vec<_>>(),
                    )
                } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                    Some(
                        (0..array.len())
                            .map(|row| (!array.is_null(row)).then_some(array.value(row) as u64))
                            .collect::<Option<Vec<_>>>()?,
                    )
                } else {
                    None
                }
            });
        let offset = batch
            .record_batch()
            .column_by_name(crate::meta_columns::OFFSET)
            .and_then(|column| {
                if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                    Some(
                        (0..array.len())
                            .map(|row| array.value(row))
                            .collect::<Vec<_>>(),
                    )
                } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    Some(
                        (0..array.len())
                            .map(|row| (!array.is_null(row)).then_some(array.value(row) as u64))
                            .collect::<Option<Vec<_>>>()?,
                    )
                } else {
                    None
                }
            });

        // Connector metadata lets us compare a Kafka-style checkpoint's
        // next offset directly.  If a connector did not expose metadata, use
        // the WAL sequence only for the local, topic-less fallback position.
        let Some((partitions, offsets)) = partition.zip(offset) else {
            return positions.iter().any(|position| {
                position.topic.is_none() && position.partition == 0 && seq < position.offset
            });
        };
        if partitions.len() != offsets.len() || partitions.len() != batch.len() {
            return false;
        }
        (0..batch.len()).all(|row| {
            let topic = batch_topic(batch.record_batch(), row);
            positions.iter().any(|position| {
                position.partition as u64 == partitions[row]
                    && offsets[row] < position.offset
                    && position
                        .topic
                        .as_deref()
                        .is_none_or(|expected| topic.as_deref() == Some(expected))
            })
        })
    }

    /// Load the unacknowledged entries once (first read), then replay them
    /// ahead of new input. The OnceCell guarantees recovery runs exactly one
    /// time even with concurrent first reads.
    async fn replay_queue(
        &self,
    ) -> Result<&tokio::sync::Mutex<std::collections::VecDeque<(u64, crate::MessageBatchRef)>>, Error>
    {
        self.replay
            .get_or_try_init(|| async {
                let entries = self.wal.read_after_cursor().await?;
                let positions = self.checkpoint_positions.read().await.clone();
                let replay = if let Some(positions) = positions.as_deref() {
                    let mut next_seq = self.wal.cursor().await?.saturating_add(1);
                    let mut covered = Vec::new();
                    let mut replay = Vec::new();
                    for (seq, batch) in entries {
                        // Only discard the covered prefix that can be
                        // reconciled contiguously. A covered entry after an
                        // uncovered WAL sequence must remain replayable;
                        // otherwise the cursor would never be able to cross
                        // that missing sequence and a later acknowledgement
                        // would wait forever on a filtered-out entry.
                        if seq == next_seq
                            && Self::batch_is_covered_by_checkpoint(seq, &batch, positions)
                        {
                            covered.push(seq);
                            next_seq = next_seq.saturating_add(1);
                        } else {
                            replay.push((seq, batch));
                        }
                    }
                    self.wal.reconcile_covered(&covered).await?;
                    replay
                } else {
                    entries
                };
                if !replay.is_empty() {
                    tracing::info!(count = replay.len(), "WAL recovery: replaying entries");
                }
                Ok::<_, Error>(tokio::sync::Mutex::new(replay.into_iter().collect()))
            })
            .await?;
        // SAFETY-free: OnceCell::get is Some after get_or_init resolves.
        Ok(self.replay.get().expect("replay queue initialized"))
    }
}

#[async_trait::async_trait]
impl Input for WalInput {
    async fn connect(&self) -> Result<(), Error> {
        self.inner.connect().await
    }

    async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
        // Replay unacked WAL entries before reading new input (spec:
        // recovery forwards pending entries before do_input starts).
        {
            let queue = self.replay_queue().await?;
            let mut queue = queue.lock().await;
            if let Some((seq, msg)) = queue.pop_front() {
                drop(queue);
                // A replayed WAL record still represents the original source
                // delivery. Rebuild its source-position acknowledgement when
                // the connector supports it; using NoopAck here would leave
                // Kafka's broker cursor behind and allow the same record to
                // be delivered again after recovery.
                let source_ack = self.replay_source_ack(&msg).await?;
                let ack: Arc<dyn crate::input::Ack> =
                    Arc::new(WalAck::new(self.wal.clone(), seq, source_ack));
                return Ok((msg, ack));
            }
        }
        let (batch, ack) = self.inner.read().await?;
        let seq = self.wal.append(&batch).await?;
        // Group-commit and periodic WALs stage appends asynchronously. The
        // record is now being published to downstream processing, so force the
        // durable hand-off before returning it from read().
        self.wal.flush().await?;
        Ok((batch, Arc::new(WalAck::new(self.wal.clone(), seq, ack))))
    }

    async fn restore_positions(
        &self,
        positions: &[crate::checkpoint::SourcePosition],
    ) -> Result<(), Error> {
        *self.checkpoint_positions.write().await = Some(positions.to_vec());
        self.inner.restore_positions(positions).await
    }

    async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        let positions = self.inner.current_positions().await?;
        if !positions.is_empty() {
            return Ok(positions);
        }
        // Inputs without a native cursor still get a durable local position
        // from the WAL.  The offset is expressed as the next sequence, just
        // like the Kafka connector's checkpoint position.
        let next = self.wal.cursor().await?.saturating_add(1);
        Ok(vec![crate::checkpoint::SourcePosition::for_partition(
            0, next,
        )])
    }

    async fn watermark_partitions(
        &self,
    ) -> Result<Vec<crate::event_time::EventTimePartition>, Error> {
        self.inner.watermark_partitions().await
    }

    async fn ack_for_position(
        &self,
        position: &crate::checkpoint::SourcePosition,
    ) -> Result<Option<Arc<dyn crate::input::Ack>>, Error> {
        self.inner.ack_for_position(position).await
    }

    fn supports_partitioning(&self) -> bool {
        self.inner.supports_partitioning()
    }

    fn assign_partition(&self, partition: u32) -> Result<(), Error> {
        self.inner.assign_partition(partition)
    }

    async fn close(&self) -> Result<(), Error> {
        // Close the wrapped connector first (stop its consumer/subscription),
        // then stop the WAL flusher, flush pending appends, and release the
        // redb handle so a replacement stream can reopen the same path.
        // `Wal::close` is idempotent, so a repeated close stays a no-op.
        let inner_result = self.inner.close().await;
        let wal_result = self.wal.close().await;
        match (inner_result, wal_result) {
            (Err(inner), Err(wal)) => Err(Error::Process(format!(
                "failed to close wrapped input ({inner}); failed to close WAL ({wal})"
            ))),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

impl WalInput {
    /// Reconstruct source-side acknowledgements for all distinct source
    /// positions represented by a replayed batch. A single connector read can
    /// decode several rows, but it must still commit each physical source
    /// position exactly once.
    async fn replay_source_ack(
        &self,
        batch: &crate::MessageBatch,
    ) -> Result<Arc<dyn crate::input::Ack>, Error> {
        let record = batch.record_batch();
        let partitions = record
            .column_by_name(crate::meta_columns::PARTITION)
            .and_then(array_u32_values);
        let offsets = record
            .column_by_name(crate::meta_columns::OFFSET)
            .and_then(array_u64_values);
        let Some((partitions, offsets)) = partitions.zip(offsets) else {
            return Ok(Arc::new(crate::input::NoopAck));
        };
        if partitions.len() != offsets.len() || partitions.len() != batch.len() {
            return Err(Error::Process(
                "WAL replay source metadata has inconsistent lengths".into(),
            ));
        }

        let mut positions = Vec::new();
        for row in 0..batch.len() {
            let topic = batch_topic(record, row);
            let position = crate::checkpoint::SourcePosition {
                topic,
                partition: partitions[row],
                offset: offsets[row].saturating_add(1),
            };
            if !positions.contains(&position) {
                positions.push(position);
            }
        }

        let mut acks = Vec::new();
        for position in &positions {
            if let Some(ack) = self.inner.ack_for_position(position).await? {
                acks.push(ack);
            }
        }
        match acks.len() {
            0 => Ok(Arc::new(crate::input::NoopAck)),
            1 => Ok(acks.remove(0)),
            _ => Ok(Arc::new(ConcurrentAck(acks))),
        }
    }
}

fn array_u32_values(column: &Arc<dyn Array>) -> Option<Vec<u32>> {
    if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        return Some((0..array.len()).map(|row| array.value(row)).collect());
    }
    let array = column.as_any().downcast_ref::<Int32Array>()?;
    (0..array.len())
        .map(|row| (!array.is_null(row)).then_some(array.value(row) as u32))
        .collect()
}

fn array_u64_values(column: &Arc<dyn Array>) -> Option<Vec<u64>> {
    if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        return Some((0..array.len()).map(|row| array.value(row)).collect());
    }
    let array = column.as_any().downcast_ref::<Int64Array>()?;
    (0..array.len())
        .map(|row| (!array.is_null(row)).then_some(array.value(row) as u64))
        .collect()
}

fn batch_topic(batch: &datafusion::arrow::record_batch::RecordBatch, row: usize) -> Option<String> {
    let column = batch.column_by_name(crate::meta_columns::EXT)?;
    let map = column.as_any().downcast_ref::<MapArray>()?;
    let entries = map.entries();
    let keys = entries.column(0).as_any().downcast_ref::<StringArray>()?;
    let values = entries.column(1).as_any().downcast_ref::<StringArray>()?;
    let offsets = map.offsets();
    let start = offsets.get(row).copied()? as usize;
    let end = offsets.get(row + 1).copied()? as usize;
    (start..end)
        .find_map(|index| (keys.value(index) == "topic").then(|| values.value(index).to_owned()))
}

impl JobComponentAdapter for StreamJobAdapter {
    fn build_input(
        &self,
        source: &SourceSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let payload = source.config.clone();
        let config = InputConfig {
            input_type: source.input_type.clone(),
            name: decode_name(&payload),
            codec: decode_codec(&payload)?,
            config: strip_payload_keys(payload),
        };
        let input = config.build(resource)?;
        match &self.wal {
            Some(wal) => Ok(Arc::new(WalInput::new(input, wal.clone()))),
            None => Ok(input),
        }
    }

    fn build_output(&self, sink: &SinkSpec, resource: &Resource) -> Result<Arc<dyn Output>, Error> {
        let payload = sink.config.clone();
        OutputConfig {
            output_type: sink.output_type.clone(),
            name: decode_name(&payload),
            codec: decode_codec(&payload)?,
            config: strip_payload_keys(payload),
        }
        .build(resource)
    }

    fn build_processor(
        &self,
        operator: &OperatorSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let processor_type = operator
            .config
            .get("type")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::Config(format!("operator '{}' requires config.type", operator.id))
            })?;
        let mut config = operator.config.clone();
        if let Some(object) = config.as_object_mut() {
            object.remove("type");
            object.remove("name");
        }
        ProcessorConfig {
            processor_type,
            name: decode_name(&operator.config),
            config: Some(config),
        }
        .build(resource)
    }
}

fn strip_payload_keys(mut payload: serde_json::Value) -> Option<serde_json::Value> {
    if let Some(object) = payload.as_object_mut() {
        object.remove(CODEC_PAYLOAD_KEY);
        object.remove("name");
    }
    // Plugins require a present (non-null) config object; hand them `{}` when
    // nothing user-facing remains after stripping compiler metadata.
    Some(match payload {
        serde_json::Value::Null => serde_json::json!({}),
        value => value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::SourcePosition;
    use crate::input::{Ack, Input};
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::HashMap;

    struct EmptyInput;

    #[async_trait::async_trait]
    impl Input for EmptyInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::EOF)
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn metadata_batch(partition: u32, offset: u64, topic: &str) -> crate::MessageBatchRef {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let batch = crate::metadata::with_partition(batch, partition).unwrap();
        let batch = crate::metadata::with_offset(batch, offset).unwrap();
        let mut extended = HashMap::new();
        extended.insert("topic".to_owned(), topic.to_owned());
        Arc::new(crate::MessageBatch::new_arrow(
            crate::metadata::with_ext_metadata(batch, &extended).unwrap(),
        ))
    }

    #[test]
    fn checkpoint_coverage_requires_matching_topic_partition_and_next_offset() {
        let position = SourcePosition {
            topic: Some("orders".into()),
            partition: 2,
            offset: 11,
        };
        assert!(WalInput::batch_is_covered_by_checkpoint(
            1,
            &metadata_batch(2, 10, "orders"),
            &[position.clone()]
        ));
        assert!(!WalInput::batch_is_covered_by_checkpoint(
            1,
            &metadata_batch(2, 11, "orders"),
            &[position.clone()]
        ));
        assert!(!WalInput::batch_is_covered_by_checkpoint(
            1,
            &metadata_batch(2, 10, "payments"),
            &[position]
        ));
    }

    #[test]
    fn checkpoint_coverage_has_topicless_wal_sequence_fallback() {
        let batch = Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        ));
        let position = SourcePosition::for_partition(0, 3);
        assert!(WalInput::batch_is_covered_by_checkpoint(
            2,
            &batch,
            &[position.clone()]
        ));
        assert!(!WalInput::batch_is_covered_by_checkpoint(
            3,
            &batch,
            &[position]
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replay_queue_skips_wal_entries_already_covered_by_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let config = WalConfig::local(
            true,
            directory.path().to_string_lossy().to_string(),
            crate::wal::SyncPolicy::PerEntry,
        );
        let wal = Wal::open(&config).unwrap();
        wal.append(&metadata_batch(2, 10, "orders")).await.unwrap();
        wal.append(&metadata_batch(2, 11, "orders")).await.unwrap();

        let input = WalInput::new(Arc::new(EmptyInput), wal.clone());
        input
            .restore_positions(&[SourcePosition {
                topic: Some("orders".into()),
                partition: 2,
                offset: 11,
            }])
            .await
            .unwrap();
        let queue = input.replay_queue().await.unwrap();
        let entries = queue.lock().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries.front().unwrap().0, 2);
        assert_eq!(wal.cursor().await.unwrap(), 1);
        drop(entries);

        // The covered prefix is now part of the local cursor. A replay of
        // sequence 2 can complete, and the first newly appended sequence 3
        // must not wait forever for the filtered sequence 1.
        let (_, replay_ack) = input.read().await.unwrap();
        replay_ack.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        let sequence = wal.append(&metadata_batch(2, 12, "orders")).await.unwrap();
        assert_eq!(sequence, 3);
        WalAck::new(wal.clone(), sequence, Arc::new(crate::input::NoopAck))
            .ack()
            .await
            .unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 3);

        wal.close().await.unwrap();
    }
}

#[cfg(test)]
mod wal_lifecycle_tests {
    use super::*;
    use crate::wal::{SyncPolicy, Wal, WalConfig};
    use std::sync::Mutex;

    struct EmptyInput;

    #[async_trait::async_trait]
    impl Input for EmptyInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct OneInput {
        batch: Mutex<Option<crate::MessageBatchRef>>,
    }

    #[async_trait::async_trait]
    impl Input for OneInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            self.batch
                .lock()
                .unwrap()
                .take()
                .map(|batch| {
                    (
                        batch,
                        Arc::new(crate::input::NoopAck) as Arc<dyn crate::input::Ack>,
                    )
                })
                .ok_or(Error::EOF)
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn trivial_batch() -> crate::MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(
            datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                    datafusion::arrow::datatypes::Field::new(
                        "value",
                        datafusion::arrow::datatypes::DataType::Int64,
                        false,
                    ),
                ])),
                vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                    1,
                ]))],
            )
            .unwrap(),
        ))
    }

    /// Task 2.3: a normal shutdown closes the wrapped connector, stops and
    /// flushes the WAL flusher, and releases the redb handle so the same
    /// path reopens without an exclusive-lock failure — including appends
    /// still pending under `group-commit`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_input_close_flushes_pending_and_releases_the_handle() {
        let directory = tempfile::tempdir().unwrap();
        let config = WalConfig::local(
            true,
            directory.path().to_string_lossy().to_string(),
            SyncPolicy::GroupCommit,
        );
        let wal = Wal::open(&config).unwrap();
        // A pending append the background flusher has not committed yet.
        wal.append(&trivial_batch()).await.unwrap();
        let input = WalInput::new(Arc::new(EmptyInput), wal.clone());

        input.close().await.unwrap();
        // Idempotent close.
        input.close().await.unwrap();
        // redb releases its flock when the last handle drops: the closed
        // WalInput and the test's reference both go away before the reopen,
        // mirroring the runtime dropping the finished adapter.
        drop(input);
        drop(wal);

        // The same path reopens (no exclusive lock) and the pending entry
        // survived the close-time flush.
        let reopened = Wal::open(&config).unwrap();
        let pending = reopened.read_after_cursor().await.unwrap();
        assert_eq!(pending.len(), 1);
        reopened.close().await.unwrap();
    }

    /// Task 2.4's companion: a WAL opened during validation or a partial
    /// startup must be closed before another adapter opens the same path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_input_close_after_partial_startup_releases_the_lock() {
        let directory = tempfile::tempdir().unwrap();
        let config = WalConfig::local(
            true,
            directory.path().to_string_lossy().to_string(),
            SyncPolicy::PerEntry,
        );
        {
            let wal = Wal::open(&config).unwrap();
            let input = WalInput::new(Arc::new(EmptyInput), wal);
            // Simulate a partial startup: nothing read, immediate close.
            input.close().await.unwrap();
        }
        // The next adapter can open the same redb path.
        let second = Wal::open(&config).unwrap();
        second.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_input_read_flushes_group_and_periodic_entries_before_publish() {
        for sync in [
            SyncPolicy::GroupCommit,
            SyncPolicy::Periodic(std::time::Duration::from_secs(60)),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let config =
                WalConfig::local(true, directory.path().to_string_lossy().to_string(), sync);
            let wal = Wal::open(&config).unwrap();
            let input = WalInput::new(
                Arc::new(OneInput {
                    batch: Mutex::new(Some(trivial_batch())),
                }),
                wal.clone(),
            );
            input.connect().await.unwrap();
            let _ = input.read().await.unwrap();

            // The read has crossed the publication boundary. It must already
            // be present in storage even though both policies normally stage
            // appends for a later flusher tick.
            assert_eq!(wal.read_after_cursor().await.unwrap().len(), 1);
            input.close().await.unwrap();
            drop(input);
            drop(wal);

            let reopened = Wal::open(&config).unwrap();
            assert_eq!(reopened.read_after_cursor().await.unwrap().len(), 1);
            reopened.close().await.unwrap();
        }
    }
}
