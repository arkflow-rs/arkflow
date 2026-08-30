//! Stateful operator wiring for the unified kernel.
//!
//! Mirrors the legacy `job_runner::StatefulProcessor` semantics: a stateful
//! operator's processor is wrapped so each batch passes a keyed counter
//! (namespaced per task) into the operator before its own processing, and the
//! counter state lives in the Job's `StateBackend` so barrier snapshots
//! capture it. The wrapper is applied at graph-build time — the kernel's
//! event loops stay state-agnostic.
//!
//! When built through [`StatefulOperator::with_journal`], each batch's
//! increments are staged in an execution-local [`StateJournal`] and committed
//! only when the final output acknowledgement fires (see `CommitOnAck`), so a
//! failed sink write cannot durably apply a mutation a replay would repeat.

use crate::processor::Processor;
use crate::state::StateBackend;
use crate::Error;
use crate::MessageBatchRef;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Wrapper that injects per-key state (running count) as a column before the
/// inner processor runs, persisting counts into the task's state namespace.
pub struct StatefulOperator {
    inner: Arc<dyn Processor>,
    /// Increments stage in the journal and apply when the final output
    /// acknowledgement fires. The constructor that accepts a backend creates
    /// a private journal as well, so legacy callers get the same failure-safe
    /// ordering as graph-built operators.
    journal: Arc<super::state_journal::StateJournal>,
    namespace: String,
    key_field: String,
    state_field: String,
    ttl_ms: Option<u64>,
}

impl StatefulOperator {
    pub fn new(
        inner: Arc<dyn Processor>,
        backend: Arc<dyn StateBackend>,
        namespace: String,
        key_field: String,
        ttl_ms: Option<u64>,
        state_field: String,
    ) -> Self {
        Self {
            inner,
            journal: Arc::new(super::state_journal::StateJournal::new(backend)),
            namespace,
            key_field,
            state_field,
            ttl_ms,
        }
    }

    /// Build the operator with output-gated state commits: each batch's
    /// increments stage in `journal` and apply to the backend only when the
    /// batch's final output acknowledgement succeeds.
    pub fn with_journal(
        inner: Arc<dyn Processor>,
        journal: Arc<super::state_journal::StateJournal>,
        namespace: String,
        key_field: String,
        ttl_ms: Option<u64>,
        state_field: String,
    ) -> Self {
        Self {
            inner,
            journal,
            namespace,
            key_field,
            state_field,
            ttl_ms,
        }
    }

    fn keys_for_batch(&self, batch: &crate::MessageBatch) -> Result<Vec<Vec<u8>>, Error> {
        let Some(column) = batch.record_batch().column_by_name(&self.key_field) else {
            return Err(Error::Process(format!(
                "stateful operator key field '{}' is missing from input batch",
                self.key_field
            )));
        };
        if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
            return Ok(values
                .iter()
                .map(|value| {
                    value
                        .map(|value| [b"binary:".as_slice(), value].concat())
                        .unwrap_or_else(|| b"null:binary".to_vec())
                })
                .collect());
        }
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(values
                .iter()
                .map(|value| {
                    value
                        .map(|value| [b"utf8:".as_slice(), value.as_bytes()].concat())
                        .unwrap_or_else(|| b"null:utf8".to_vec())
                })
                .collect());
        }
        macro_rules! encode_integer_keys {
            ($array:ty, $tag:literal) => {
                if let Some(values) = column.as_any().downcast_ref::<$array>() {
                    return Ok(values
                        .iter()
                        .map(|value| match value {
                            Some(value) => [$tag.as_bytes(), &value.to_be_bytes()].concat(),
                            None => concat!("null:", $tag).as_bytes().to_vec(),
                        })
                        .collect());
                }
            };
        }
        encode_integer_keys!(Int8Array, "i8");
        encode_integer_keys!(Int16Array, "i16");
        encode_integer_keys!(Int32Array, "i32");
        encode_integer_keys!(Int64Array, "i64");
        encode_integer_keys!(UInt8Array, "u8");
        encode_integer_keys!(UInt16Array, "u16");
        encode_integer_keys!(UInt32Array, "u32");
        encode_integer_keys!(UInt64Array, "u64");
        Err(Error::Process(format!(
            "stateful operator key field '{}' has unsupported Arrow type {:?}",
            self.key_field,
            column.data_type()
        )))
    }
}

#[async_trait]
impl Processor for StatefulOperator {
    async fn process(&self, batch: MessageBatchRef) -> Result<crate::ProcessResult, Error> {
        // Without an acknowledgement flow, no downstream handle can commit
        // the transaction later. Commit only after the inner processor has
        // accepted the enriched batch, so a processor failure cannot leave a
        // live counter mutation behind for a replay to apply again.
        let journal = self.journal.clone();
        let keys = self.keys_for_batch(&batch)?;
        let txn = journal.begin()?;
        let counts = keys
            .iter()
            .map(|key| journal.update_i64(txn, &self.namespace, key, 1, self.ttl_ms))
            .collect::<Result<Vec<_>, _>>();
        let counts = match counts {
            Ok(counts) => counts,
            Err(error) => {
                journal.rollback(txn);
                return Err(error);
            }
        };
        let enriched = match self.enrich(batch, counts) {
            Ok(enriched) => enriched,
            Err(error) => {
                journal.rollback(txn);
                return Err(error);
            }
        };
        match self.inner.process(enriched).await {
            Ok(result) => {
                journal.commit(txn)?;
                Ok(result)
            }
            Err(error) => {
                journal.rollback(txn);
                Err(error)
            }
        }
    }

    /// Journaled path: stage the increments, run the inner processor, and
    /// return outputs whose acknowledgement applies the staged state only
    /// after the downstream write confirms. A failure anywhere before the
    /// acknowledgement rolls the staged increments back, so an at-least-once
    /// replay cannot double-apply them.
    async fn process_with_ack(
        &self,
        batch: MessageBatchRef,
        ack: Arc<dyn crate::input::Ack>,
    ) -> Result<crate::ProcessResult, Error> {
        let journal = self.journal.clone();
        let keys = self.keys_for_batch(&batch)?;
        let txn = journal.begin()?;
        let counts = keys
            .iter()
            .map(|key| journal.update_i64(txn, &self.namespace, key, 1, self.ttl_ms))
            .collect::<Result<Vec<_>, _>>();
        let counts = match counts {
            Ok(counts) => counts,
            Err(error) => {
                journal.rollback(txn);
                return Err(error);
            }
        };
        let enriched = match self.enrich(batch, counts) {
            Ok(enriched) => enriched,
            Err(error) => {
                journal.rollback(txn);
                return Err(error);
            }
        };
        let result = self.inner.process(enriched).await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                journal.rollback(txn);
                return Err(error);
            }
        };
        let commit_ack: Arc<dyn crate::input::Ack> = Arc::new(
            super::state_journal::CommitOnAck::new(journal.clone(), txn, ack),
        );
        match result {
            crate::ProcessResult::Single(output) => {
                Ok(crate::ProcessResult::SingleWithAck(output, commit_ack))
            }
            crate::ProcessResult::Multiple(outputs) if outputs.is_empty() => {
                commit_ack.ack().await?;
                Ok(crate::ProcessResult::None)
            }
            crate::ProcessResult::Multiple(outputs) => {
                let acks = crate::input::fanout_ack(commit_ack, outputs.len());
                Ok(crate::ProcessResult::MultipleWithAck(
                    outputs.into_iter().zip(acks).collect(),
                ))
            }
            crate::ProcessResult::None => {
                commit_ack.ack().await?;
                Ok(crate::ProcessResult::None)
            }
            crate::ProcessResult::SingleWithAck(output, replacement) => {
                Ok(crate::ProcessResult::SingleWithAck(
                    output,
                    Arc::new(super::state_journal::CommitOnAck::new(
                        journal.clone(),
                        txn,
                        replacement,
                    )),
                ))
            }
            crate::ProcessResult::MultipleWithAck(_) | crate::ProcessResult::Deferred => {
                journal.rollback(txn);
                Err(Error::Process(
                    "stateful operator's inner processor returned an unsupported ack variant"
                        .into(),
                ))
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

impl StatefulOperator {
    /// Attach the per-key count column to a batch.
    fn enrich(&self, batch: MessageBatchRef, counts: Vec<i64>) -> Result<MessageBatchRef, Error> {
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();
        fields.push(Arc::new(Field::new(
            &self.state_field,
            DataType::Int64,
            false,
        )));
        columns.push(Arc::new(Int64Array::from(counts)) as ArrayRef);
        let enriched = RecordBatch::try_new(
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
            columns,
        )
        .map_err(|error| Error::Process(format!("build stateful batch: {error}")))?;
        let mut enriched = crate::MessageBatch::new_arrow(enriched);
        enriched.set_input_name(batch.get_input_name());
        Ok(Arc::new(enriched))
    }
}

/// Filter helper shared with the gate (kept here for stateful routing use).
pub fn filter_rows(
    batch: &crate::MessageBatch,
    keep: Vec<bool>,
) -> Result<crate::MessageBatchRef, Error> {
    let filtered = filter_record_batch(batch.record_batch(), &BooleanArray::from(keep))
        .map_err(|error| Error::Process(format!("filter stateful batch: {error}")))?;
    let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
    filtered_batch.set_input_name(batch.get_input_name());
    Ok(Arc::new(filtered_batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcessResult;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::Schema;

    struct EchoProcessor;
    #[async_trait]
    impl Processor for EchoProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingProcessor;
    #[async_trait]
    impl Processor for FailingProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Err(Error::Process("processor rejected batch".into()))
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct PendingAck {
        fail: std::sync::atomic::AtomicBool,
    }
    #[async_trait]
    impl crate::input::Ack for PendingAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail.load(std::sync::atomic::Ordering::Acquire) {
                // The downstream sink write failed: the acknowledgement does
                // not complete, exactly like a failed `write_batch`.
                Err(Error::Process("sink write failed".into()))
            } else {
                Ok(())
            }
        }
    }

    fn batch(keys: Vec<&str>) -> MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
                vec![Arc::new(StringArray::from(
                    keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
                ))],
            )
            .unwrap(),
        ))
    }

    /// Task 1.2: a failed sink write must not persist a replayed increment
    /// twice. The first delivery's output acknowledgement fails; its staged
    /// increment never reaches the backend. The replay applies the mutation
    /// exactly once.
    #[tokio::test]
    async fn failed_sink_write_does_not_persist_a_replayed_increment_twice() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let journal = Arc::new(super::super::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let operator = StatefulOperator::with_journal(
            Arc::new(EchoProcessor),
            journal.clone(),
            "job:t:task:m-0".into(),
            "key".into(),
            None,
            "count".into(),
        );

        // First delivery: the sink write fails, so the output acknowledgement
        // never completes. The transaction remains staged for the same ack's
        // retry, but the mutation is not applied to the backend.
        let source_ack = Arc::new(PendingAck {
            fail: std::sync::atomic::AtomicBool::new(true),
        });
        let first = operator
            .process_with_ack(batch(vec!["a"]), source_ack.clone())
            .await
            .unwrap();
        let ProcessResult::SingleWithAck(_, failed_ack) = first else {
            panic!("journaled operator returns an ack-gated output");
        };
        assert!(failed_ack.ack().await.is_err());
        assert!(backend.scan("job:t:task:m-0").unwrap().is_empty());
        assert_eq!(journal.pending_transactions(), 1);

        // Fan-out retry reuses the same acknowledgement and must re-apply the
        // retained staged transaction before acknowledging the source.
        source_ack
            .fail
            .store(false, std::sync::atomic::Ordering::Release);
        failed_ack.ack().await.unwrap();
        let state = backend.scan("job:t:task:m-0").unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(
            crate::state::KeyedCounter::new(backend, "job:t:task:m-0")
                .get(b"utf8:a")
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn injects_keyed_counts_and_persists_per_task_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let operator = StatefulOperator::new(
            Arc::new(EchoProcessor),
            backend.clone(),
            "job:t:task:m-0".into(),
            "key".into(),
            None,
            "count".into(),
        );
        let first = operator.process(batch(vec!["a", "b", "a"])).await.unwrap();
        let ProcessResult::Single(output) = first else {
            panic!("one batch")
        };
        let counts = output
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[1, 1, 2]);
        let second = operator.process(batch(vec!["a"])).await.unwrap();
        let ProcessResult::Single(output) = second else {
            panic!("one batch")
        };
        let counts = output
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[3]);
        assert_eq!(backend.scan("job:t:task:m-0").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn direct_backend_path_does_not_commit_when_processor_fails() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let operator = StatefulOperator::new(
            Arc::new(FailingProcessor),
            backend.clone(),
            "failed-processor".into(),
            "key".into(),
            None,
            "count".into(),
        );

        assert!(operator.process(batch(vec!["a"])).await.is_err());
        assert!(
            backend.scan("failed-processor").unwrap().is_empty(),
            "processor failure must not leave a keyed increment behind"
        );
    }

    #[tokio::test]
    async fn non_64_bit_integer_keys_stay_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let operator = StatefulOperator::new(
            Arc::new(EchoProcessor),
            backend.clone(),
            "int-keys".into(),
            "key".into(),
            None,
            "count".into(),
        );
        for keys in [vec!["1", "2"], vec!["3"]] {
            let record = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(
                    keys.iter()
                        .map(|k| k.parse::<i32>().unwrap())
                        .collect::<Vec<_>>(),
                ))],
            )
            .unwrap();
            operator
                .process(Arc::new(crate::MessageBatch::new_arrow(record)))
                .await
                .unwrap();
        }
        assert_eq!(backend.scan("int-keys").unwrap().len(), 3);
    }
}
