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

//! Batch Processor Components
//!
//! Batch multiple messages into one or more messages

use crate::component::batch_merge::normalize_and_concat;
use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::input::Ack;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Batch processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchProcessorConfig {
    /// Batch size, counted in message rows
    count: usize,
    /// Batch timeout (ms)
    timeout_ms: u64,
}

/// One buffered input delivery: the batch, the acknowledgement that settles
/// its source delivery (absent on the ack-less `process` path), and the row
/// count the delivery contributes to the flush trigger.
struct HeldDelivery {
    batch: MessageBatchRef,
    ack: Option<Arc<dyn Ack>>,
    rows: usize,
}

/// Settlement composite for the acknowledgements of every delivery merged
/// into one flush output. Mirrors the kernel's `ConcurrentAck` semantics
/// (crate-private in `arkflow-core`, hence re-implemented here): children
/// settle concurrently, any failure compensates every child through `undo`,
/// and `undo`/`abort` propagate to all children in reverse delivery order.
/// Settlement methods release the hold taken when the delivery was buffered
/// so barrier draining re-admits the acknowledgements before they settle.
struct HeldAcksAck(Vec<Arc<dyn Ack>>);

#[async_trait]
impl Ack for HeldAcksAck {
    async fn ack(&self) -> Result<(), Error> {
        self.release_held();
        let results = futures::future::join_all(self.0.iter().map(|ack| ack.ack())).await;
        let mut first_error = None;
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if first_error.is_some() {
            // Any child may have advanced its durable source before returning
            // an error. Compensate successful and failed children alike so a
            // composite failure cannot roll back state while one source
            // cursor remains past the input.
            for ack in self.0.iter().rev() {
                if let Err(error) = ack.undo().await {
                    first_error.get_or_insert(Error::Process(format!(
                        "source acknowledgement failed and compensation failed: {error}"
                    )));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn undo(&self) -> Result<(), Error> {
        self.release_held();
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.undo().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn abort(&self) -> Result<(), Error> {
        self.release_held();
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.abort().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn mark_held(&self) {
        for ack in &self.0 {
            ack.mark_held();
        }
    }

    fn release_held(&self) {
        for ack in &self.0 {
            ack.release_held();
        }
    }
}

/// Batch Processor Components
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    held: Arc<RwLock<Vec<HeldDelivery>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

/// Who owns the newest delivery's acknowledgement when a flush's merge
/// fails. An `accept`-triggered flush returns the CURRENT delivery with
/// the error — the executor aborts that delivery's ack on `Err`, so it
/// must not stay buffered (otherwise the same ack is settled twice, and
/// a persistently conflicting batch re-fails every arrival while the
/// buffer grows without bound). `finish`/`on_tick` have no current
/// input, so every delivery stays buffered for a later retry or the
/// close abort path.
#[derive(Clone, Copy, PartialEq)]
enum FlushErrOwnership {
    /// The delivery just pushed by `accept` leaves the buffer with the
    /// error; only previously buffered deliveries are restored.
    ReturnCurrent,
    /// No current input: restore everything (a later flush or the
    /// close abort path still owns the acknowledgements).
    RestoreAll,
}

impl BatchProcessor {
    /// Create a new batch processor component
    fn new(config: BatchProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            held: Arc::new(RwLock::new(Vec::with_capacity(config.count))),
            last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
        })
    }

    /// Total message rows currently buffered.
    fn held_rows(held: &[HeldDelivery]) -> usize {
        held.iter().map(|delivery| delivery.rows).sum()
    }

    /// Check if the batch should be refreshed
    async fn should_flush(&self) -> bool {
        let held = self.held.read().await;
        if Self::held_rows(&held) >= self.config.count {
            return true;
        }
        let last_batch_time = self.last_batch_time.lock().await;
        // 如果超过超时时间且批处理不为空，则刷新
        if !held.is_empty()
            && last_batch_time.elapsed().as_millis() >= self.config.timeout_ms as u128
        {
            return true;
        }

        false
    }

    /// Merge every buffered delivery into one output. The merge is
    /// schema-normalizing: fields are unioned by name, missing columns are
    /// null-filled, and same-name type conflicts fail explicitly. On a
    /// merge failure the buffered deliveries are restored (minus the
    /// current one under [`FlushErrOwnership::ReturnCurrent`]) so a retry
    /// sees the same content without double-owning an aborted ack.
    async fn flush_held(&self, err_ownership: FlushErrOwnership) -> Result<ProcessResult, Error> {
        let mut held = self.held.write().await;
        if held.is_empty() {
            return Ok(ProcessResult::None);
        }

        let mut deliveries = std::mem::take(&mut *held);
        let arrow_batches: Vec<datafusion::arrow::array::RecordBatch> = deliveries
            .iter()
            .map(|delivery| delivery.batch.record_batch().clone())
            .collect();
        let merged = match normalize_and_concat(&arrow_batches) {
            Ok(merged) => merged,
            Err(error) => {
                // Keep the deliveries buffered: a later flush (or the close
                // abort path) still owns their acknowledgements. The
                // current delivery (if any) leaves with the error — its
                // ack is aborted by the executor, not by us.
                if err_ownership == FlushErrOwnership::ReturnCurrent {
                    deliveries.pop();
                }
                *held = deliveries;
                return Err(error);
            }
        };
        let merged = Arc::new(MessageBatch::new_arrow(merged));

        {
            let mut last_batch_time = self.last_batch_time.lock().await;
            *last_batch_time = std::time::Instant::now();
        }

        let acks: Vec<Arc<dyn Ack>> = deliveries.into_iter().filter_map(|d| d.ack).collect();
        if acks.is_empty() {
            // Ack-less buffering path (`process`): nothing to settle.
            Ok(ProcessResult::Single(merged))
        } else {
            // One output emission carries the acknowledgements of every
            // merged delivery: the kernel settles the composite only after
            // the output is written downstream.
            Ok(ProcessResult::SingleWithAck(
                merged,
                Arc::new(HeldAcksAck(acks)),
            ))
        }
    }

    /// Buffer one delivery and flush when a trigger fires. Returns
    /// `ProcessResult::Deferred` while an acknowledged delivery is still
    /// buffered (the kernel must not settle its ack yet) and `None` on the
    /// ack-less path (nothing to retain).
    async fn accept(
        &self,
        msg: MessageBatchRef,
        ack: Option<Arc<dyn Ack>>,
    ) -> Result<ProcessResult, Error> {
        let rows = msg.len();
        if let Some(ack) = &ack {
            // The acknowledgement may complete much later (or never before
            // shutdown); barrier draining must not wait on it while the rows
            // sit in this volatile buffer.
            ack.mark_held();
        }
        {
            let mut held = self.held.write().await;
            // Add messages to a batch
            held.push(HeldDelivery {
                batch: msg,
                ack: ack.clone(),
                rows,
            });
        }

        // Check if the batch should be refreshed
        if self.should_flush().await {
            self.flush_held(FlushErrOwnership::ReturnCurrent).await
        } else if ack.is_some() {
            Ok(ProcessResult::Deferred)
        } else {
            // If it is not refreshed, return None (filtered)
            Ok(ProcessResult::None)
        }
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatchRef) -> Result<ProcessResult, Error> {
        self.accept(msg, None).await
    }

    async fn process_with_ack(
        &self,
        msg: MessageBatchRef,
        ack: Arc<dyn Ack>,
    ) -> Result<ProcessResult, Error> {
        self.accept(msg, Some(ack)).await
    }

    async fn finish(&self) -> Result<ProcessResult, Error> {
        // EOS: emit the partial batch so its acknowledgements settle through
        // the normal output path instead of dying in `close`.
        self.flush_held(FlushErrOwnership::RestoreAll).await
    }

    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        // Idle input: fire the timeout trigger without waiting for the next
        // arrival to run the flush check.
        if !self.should_flush().await {
            return Ok(ProcessResult::None);
        }
        self.flush_held(FlushErrOwnership::RestoreAll).await
    }

    async fn close(&self) -> Result<(), Error> {
        let mut held = self.held.write().await;
        if held.is_empty() {
            return Ok(());
        }
        // Only reachable when the chain exited without an orderly EOS drain
        // (`finish` already emitted on normal shutdown paths). The buffered
        // rows cannot be delivered; abort their acknowledgements so the
        // sources replay them instead of committing past data that was
        // never written downstream.
        let rows = Self::held_rows(&held);
        let batches = held.len();
        let acks: Vec<Arc<dyn Ack>> = held.drain(..).filter_map(|delivery| delivery.ack).collect();
        if acks.is_empty() {
            tracing::warn!(
                batches,
                rows,
                "batch processor closed with retained messages; dropping them"
            );
            return Ok(());
        }
        tracing::warn!(
            batches,
            rows,
            "batch processor closed with retained messages; aborting their acknowledgements for replay"
        );
        HeldAcksAck(acks).abort().await
    }
}

struct BatchProcessorBuilder;
impl ProcessorBuilder for BatchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: BatchProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(BatchProcessor::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder("batch", Arc::new(BatchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "batch",
        "Batches messages by row count with an idle timeout before forwarding.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "count": {"type": "integer", "minimum": 1, "description": "Number of message rows to accumulate before flushing."},
                "timeout_ms": {"type": "integer", "minimum": 1, "description": "Idle timeout that flushes a partial batch (milliseconds)."}
            },
            "required": ["count", "timeout_ms"]
        }),
    ).with_optional().with_example(serde_json::json!({"count": 100, "timeout_ms": 5000})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    /// Acknowledgement double that records every settlement call so tests
    /// can assert exactly-once settlement and compensation propagation.
    #[derive(Default)]
    struct RecordingAck {
        acked: AtomicUsize,
        undone: AtomicUsize,
        aborted: AtomicUsize,
        held: AtomicUsize,
        released: AtomicUsize,
    }

    #[async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn undo(&self) -> Result<(), Error> {
            self.undone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn abort(&self) -> Result<(), Error> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn mark_held(&self) {
            self.held.fetch_add(1, Ordering::SeqCst);
        }

        fn release_held(&self) {
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Acknowledgement double whose source commit always fails, to exercise
    /// the composite failure-compensation path.
    struct FailingAck {
        undone: AtomicUsize,
    }

    #[async_trait]
    impl Ack for FailingAck {
        async fn ack(&self) -> Result<(), Error> {
            Err(Error::Process("source commit failed".to_string()))
        }

        async fn undo(&self) -> Result<(), Error> {
            self.undone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn binary_msg(values: &[&str]) -> MessageBatchRef {
        Arc::new(
            MessageBatch::new_binary(values.iter().map(|v| v.as_bytes().to_vec()).collect())
                .unwrap(),
        )
    }

    fn int64_batch(columns: &[(&str, Vec<i64>)]) -> MessageBatchRef {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, true))
            .collect();
        let arrays = columns
            .iter()
            .map(|(_, values)| {
                Arc::new(Int64Array::from(values.clone()))
                    as Arc<dyn datafusion::arrow::array::Array>
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }

    fn recording_ack() -> (Arc<RecordingAck>, Arc<dyn Ack>) {
        let inner = Arc::new(RecordingAck::default());
        (inner.clone(), inner as Arc<dyn Ack>)
    }

    #[tokio::test]
    async fn test_batch_processor_size() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 1000,
        })
        .unwrap();

        // First message should not trigger flush
        let result = processor.process(binary_msg(&["test1"])).await.unwrap();
        assert!(result.is_empty());

        // Second message should trigger flush due to batch size
        let result = processor.process(binary_msg(&["test2"])).await.unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 2); // 2 messages combined
            }
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test]
    async fn test_batch_processor_timeout() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 100,
        })
        .unwrap();

        // Add one message
        let result = processor.process(binary_msg(&["test1"])).await.unwrap();
        assert!(result.is_empty());

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Next message should trigger flush due to timeout
        let result = processor.process(binary_msg(&["test2"])).await.unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 2); // 2 messages combined
            }
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test]
    async fn test_batch_processor_empty() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 1000,
        })
        .unwrap();

        let result = processor
            .flush_held(FlushErrOwnership::RestoreAll)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_close() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 1000,
        })
        .unwrap();

        // Add a message to the batch
        processor.process(binary_msg(&["test1"])).await.unwrap();

        // Orderly shutdown drains the partial batch through `finish`
        // before `close` releases the processor.
        let drained = processor.finish().await.unwrap();
        assert!(matches!(drained, ProcessResult::Single(ref b) if b.len() == 1));

        // Close the processor
        processor.close().await.unwrap();

        // Verify the batch is empty by checking that flush returns empty
        let result = processor
            .flush_held(FlushErrOwnership::RestoreAll)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_finish_drains_partial() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 60_000,
        })
        .unwrap();

        // Two messages below the count threshold: no flush on process
        processor.process(binary_msg(&["a"])).await.unwrap();
        processor.process(binary_msg(&["b"])).await.unwrap();

        // EOS drains the partial batch instead of dropping it in close
        match processor.finish().await.unwrap() {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 2),
            other => panic!(
                "expected ProcessResult::Single, got empty: {}",
                other.is_empty()
            ),
        }

        // A second finish has nothing left to emit
        assert!(processor.finish().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_batch_processor_on_tick_flushes_timeout() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 5,
            timeout_ms: 100,
        })
        .unwrap();

        processor.process(binary_msg(&["late"])).await.unwrap();

        // Before the timeout elapses the tick is a no-op
        assert!(processor.on_tick().await.unwrap().is_empty());

        sleep(Duration::from_millis(150)).await;

        // The idle tick fires the timeout flush without a new arrival
        match processor.on_tick().await.unwrap() {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 1),
            other => panic!(
                "expected ProcessResult::Single, got empty: {}",
                other.is_empty()
            ),
        }
    }

    #[tokio::test]
    async fn test_batch_processor_flush_failure_retains_buffer() {
        // Same column name with conflicting types must fail the merge.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        processor.process(binary_msg(&["a"])).await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "__value__",
            DataType::Int64,
            false,
        )]));
        let arrow_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(arrow_batch)))
            .await;

        // The merge failure propagates...
        assert!(result.is_err());

        // ...and the conflicting (current) delivery left the buffer with
        // the error: only the previously buffered binary message remains,
        // and flushing it alone succeeds — a single poisoned batch cannot
        // block the whole buffer (CR ownership fix).
        let drained = processor.finish().await.unwrap();
        assert!(
            matches!(drained, ProcessResult::Single(ref b) if b.len() == 1),
            "the retained non-conflicting message must still drain: {drained:?}"
        );
        assert!(processor
            .flush_held(FlushErrOwnership::RestoreAll)
            .await
            .unwrap()
            .is_empty());
    }

    /// CR follow-up (ack path): on a merge failure the CURRENT delivery's
    /// ack returns to the executor (which aborts it) and must not stay
    /// buffered — otherwise the same ack is settled twice (executor abort +
    /// later HeldAcksAck settle) and the poisoned buffer re-fails every
    /// arrival.
    #[tokio::test]
    async fn test_flush_failure_returns_current_ack_to_executor() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        // First (binary) delivery stays buffered under its own ack.
        let (first, first_ack) = recording_ack();
        processor
            .process_with_ack(binary_msg(&["a"]), first_ack)
            .await
            .unwrap();

        // Second (int64) delivery triggers the flush and conflicts.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__value__",
            DataType::Int64,
            false,
        )]));
        let arrow_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let (second, second_ack) = recording_ack();
        let result = processor
            .process_with_ack(Arc::new(MessageBatch::new_arrow(arrow_batch)), second_ack)
            .await;
        assert!(result.is_err(), "conflicting merge must fail");

        // Neither ack settled through the processor yet; the executor owns
        // the second ack (it aborts it on Err) — so closing the processor
        // may only settle the FIRST delivery's ack.
        assert_eq!(first.acked.load(Ordering::SeqCst), 0);
        assert_eq!(second.acked.load(Ordering::SeqCst), 0);
        assert_eq!(first.aborted.load(Ordering::SeqCst), 0);
        assert_eq!(second.aborted.load(Ordering::SeqCst), 0);

        processor.close().await.unwrap();
        assert_eq!(
            first.aborted.load(Ordering::SeqCst),
            1,
            "the retained delivery's ack is aborted by close for replay"
        );
        assert_eq!(
            second.aborted.load(Ordering::SeqCst),
            0,
            "the current delivery's ack belongs to the executor, not the buffer"
        );
        assert_eq!(first.acked.load(Ordering::SeqCst), 0);
        assert_eq!(second.acked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_deferred_while_buffered_ack_not_settled() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 100,
            timeout_ms: 60_000,
        })
        .unwrap();

        let (recording, ack) = recording_ack();
        let result = processor
            .process_with_ack(binary_msg(&["m1"]), ack)
            .await
            .unwrap();

        // The kernel must not settle the ack while the rows are buffered.
        assert!(matches!(result, ProcessResult::Deferred));
        assert_eq!(recording.acked.load(Ordering::SeqCst), 0);
        assert_eq!(recording.undone.load(Ordering::SeqCst), 0);
        assert_eq!(recording.aborted.load(Ordering::SeqCst), 0);
        // The hold keeps barrier draining from waiting on the buffered ack.
        assert_eq!(recording.held.load(Ordering::SeqCst), 1);

        // The ack-less path still filters with None while buffering.
        let result = processor.process(binary_msg(&["m2"])).await.unwrap();
        assert!(matches!(result, ProcessResult::None));
    }

    #[tokio::test]
    async fn test_flush_emission_settles_each_held_ack_exactly_once() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        let (first, first_ack) = recording_ack();
        let result = processor
            .process_with_ack(binary_msg(&["m1"]), first_ack)
            .await
            .unwrap();
        assert!(matches!(result, ProcessResult::Deferred));

        let (second, second_ack) = recording_ack();
        let emission = processor
            .process_with_ack(binary_msg(&["m2"]), second_ack)
            .await
            .unwrap();

        let emission_ack = match emission {
            ProcessResult::SingleWithAck(batch, ack) => {
                assert_eq!(batch.len(), 2);
                ack
            }
            other => panic!("expected SingleWithAck, got {:?}", other),
        };

        // Nothing settles before the downstream write confirms.
        assert_eq!(first.acked.load(Ordering::SeqCst), 0);
        assert_eq!(second.acked.load(Ordering::SeqCst), 0);

        // The kernel settles the composite after the output is written.
        emission_ack.ack().await.unwrap();
        assert_eq!(first.acked.load(Ordering::SeqCst), 1);
        assert_eq!(second.acked.load(Ordering::SeqCst), 1);
        assert_eq!(first.released.load(Ordering::SeqCst), 1);
        assert_eq!(second.released.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_downstream_failure_compensates_every_held_ack() {
        // One child whose source commit fails must compensate the sibling
        // that already committed, not leave its cursor ahead of the input.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        let (sibling, sibling_ack) = recording_ack();
        processor
            .process_with_ack(binary_msg(&["m1"]), sibling_ack)
            .await
            .unwrap();

        let failing = Arc::new(FailingAck {
            undone: AtomicUsize::new(0),
        });
        let emission = processor
            .process_with_ack(binary_msg(&["m2"]), failing.clone() as Arc<dyn Ack>)
            .await
            .unwrap();

        let emission_ack = match emission {
            ProcessResult::SingleWithAck(_, ack) => ack,
            other => panic!("expected SingleWithAck, got {:?}", other),
        };

        let outcome = emission_ack.ack().await;
        assert!(outcome.is_err());
        // Settle-then-compensate is the ConcurrentAck contract: children
        // may have advanced their durable cursors before one failed, so the
        // composite must roll every held ack back exactly once for replay.
        assert_eq!(sibling.acked.load(Ordering::SeqCst), 1);
        assert_eq!(sibling.undone.load(Ordering::SeqCst), 1);
        assert_eq!(failing.undone.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_emission_abort_propagates_to_every_held_ack() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        let (first, first_ack) = recording_ack();
        processor
            .process_with_ack(binary_msg(&["m1"]), first_ack)
            .await
            .unwrap();
        let (second, second_ack) = recording_ack();
        let emission = processor
            .process_with_ack(binary_msg(&["m2"]), second_ack)
            .await
            .unwrap();

        let emission_ack = match emission {
            ProcessResult::SingleWithAck(_, ack) => ack,
            other => panic!("expected SingleWithAck, got {:?}", other),
        };

        // A failed downstream route aborts the composite delivery.
        emission_ack.abort().await.unwrap();
        assert_eq!(first.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(second.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(first.acked.load(Ordering::SeqCst), 0);
        assert_eq!(second.acked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_close_aborts_unemitted_held_acks() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 100,
            timeout_ms: 60_000,
        })
        .unwrap();

        let (first, first_ack) = recording_ack();
        processor
            .process_with_ack(binary_msg(&["m1"]), first_ack)
            .await
            .unwrap();
        let (second, second_ack) = recording_ack();
        processor
            .process_with_ack(binary_msg(&["m2"]), second_ack)
            .await
            .unwrap();

        processor.close().await.unwrap();

        // Cancelled chains abort (not ack) the retained deliveries so the
        // sources replay them.
        assert_eq!(first.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(second.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(first.acked.load(Ordering::SeqCst), 0);
        assert_eq!(second.acked.load(Ordering::SeqCst), 0);

        // The buffer is released.
        assert!(processor
            .flush_held(FlushErrOwnership::RestoreAll)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_heterogeneous_key_order_merges_by_column_name() {
        // {"a":1,"b":2} then {"b":5,"a":6}: the second row must keep a=6,
        // b=5 — a positional concat would swap the values.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        processor
            .process(int64_batch(&[("a", vec![1]), ("b", vec![2])]))
            .await
            .unwrap();
        let merged = processor
            .process(int64_batch(&[("b", vec![5]), ("a", vec![6])]))
            .await
            .unwrap();

        let batch = match merged {
            ProcessResult::Single(batch) => batch,
            other => panic!("expected single result, got {:?}", other),
        };
        assert_eq!(batch.len(), 2);
        let record = batch.record_batch();
        let a = record
            .column(record.schema().index_of("a").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = record
            .column(record.schema().index_of("b").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 1);
        assert_eq!(a.value(1), 6);
        assert_eq!(b.value(0), 2);
        assert_eq!(b.value(1), 5);
    }

    #[tokio::test]
    async fn test_union_merge_null_fills_missing_columns() {
        // {"a":1,"b":2} then {"a":3,"b":4,"c":5}: the union keeps c and
        // null-fills the first row.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        processor
            .process(int64_batch(&[("a", vec![1]), ("b", vec![2])]))
            .await
            .unwrap();
        let merged = processor
            .process(int64_batch(&[
                ("a", vec![3]),
                ("b", vec![4]),
                ("c", vec![5]),
            ]))
            .await
            .unwrap();

        let batch = match merged {
            ProcessResult::Single(batch) => batch,
            other => panic!("expected single result, got {:?}", other),
        };
        let record = batch.record_batch();
        assert_eq!(record.num_columns(), 3);
        let c = record
            .column(record.schema().index_of("c").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(c.is_null(0));
        assert_eq!(c.value(1), 5);
    }

    #[tokio::test]
    async fn test_type_conflict_fails_with_column_and_types() {
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        let int_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let int_batch =
            RecordBatch::try_new(int_schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        processor
            .process(Arc::new(MessageBatch::new_arrow(int_batch)))
            .await
            .unwrap();

        let utf8_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let utf8_batch =
            RecordBatch::try_new(utf8_schema, vec![Arc::new(StringArray::from(vec!["one"]))])
                .unwrap();

        let error = processor
            .process(Arc::new(MessageBatch::new_arrow(utf8_batch)))
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("`v`"),
            "error should name the conflicting column: {message}"
        );
        assert!(
            message.contains("Int64") && message.contains("Utf8"),
            "error should name both types: {message}"
        );
    }

    #[tokio::test]
    async fn test_count_triggers_on_rows_not_batches() {
        // count: 3 with a 2-row batch followed by a 1-row batch must flush
        // on the second batch (3 accumulated rows), not wait for 3 batches.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 3,
            timeout_ms: 60_000,
        })
        .unwrap();

        let two_rows = int64_batch(&[("a", vec![1, 2])]);
        assert_eq!(two_rows.len(), 2);
        let result = processor.process(two_rows).await.unwrap();
        assert!(result.is_empty(), "2 rows below count 3 must buffer");

        let one_row = int64_batch(&[("a", vec![3])]);
        match processor.process(one_row).await.unwrap() {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 3),
            other => panic!("expected a flush on the 3rd row, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_multi_row_batch_exceeding_count_flushes_immediately() {
        // A single batch larger than count flushes on arrival.
        let processor = BatchProcessor::new(BatchProcessorConfig {
            count: 2,
            timeout_ms: 60_000,
        })
        .unwrap();

        let result = processor
            .process(int64_batch(&[("a", vec![1, 2, 3])]))
            .await
            .unwrap();
        match result {
            ProcessResult::Single(batch) => assert_eq!(batch.len(), 3),
            other => panic!("expected an immediate flush, got {:?}", other),
        }
    }
}
