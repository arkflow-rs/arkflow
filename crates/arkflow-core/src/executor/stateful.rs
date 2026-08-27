//! Stateful operator wiring for the unified kernel.
//!
//! Mirrors the legacy `job_runner::StatefulProcessor` semantics: a stateful
//! operator's processor is wrapped so each batch passes a keyed counter
//! (namespaced per task) into the operator before its own processing, and the
//! counter state lives in the Job's `StateBackend` so barrier snapshots
//! capture it. The wrapper is applied at graph-build time — the kernel's
//! event loops stay state-agnostic.

use crate::Error;
use crate::MessageBatchRef;
use crate::processor::Processor;
use crate::state::{KeyedCounter, StateBackend};
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
    counter: KeyedCounter,
    key_field: String,
    state_field: String,
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
            counter: KeyedCounter::with_ttl(backend, namespace, ttl_ms),
            key_field,
            state_field,
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
        let counts = self
            .keys_for_batch(&batch)?
            .into_iter()
            .map(|key| self.counter.add(&key, 1))
            .collect::<Result<Vec<_>, _>>()?;
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
        self.inner.process(Arc::new(enriched)).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
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
        async fn close(&self) -> Result<(), Error> { Ok(()) }
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
        let ProcessResult::Single(output) = first else { panic!("one batch") };
        let counts = output
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[1, 1, 2]);
        let second = operator.process(batch(vec!["a"])).await.unwrap();
        let ProcessResult::Single(output) = second else { panic!("one batch") };
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
                    keys.iter().map(|k| k.parse::<i32>().unwrap()).collect::<Vec<_>>(),
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
