//! Single-Compute execution adapter for a distributed Job plan.

use crate::checkpoint::RecoveryPlan;
use crate::event_time::{window_action, FieldTimestampExtractor, WatermarkTracker, WindowAction};
use crate::input::{Ack, Input};
use crate::job::{JobComponentAdapter, JobPlan};
use crate::output::Output;
use crate::processor::Processor;
use crate::state::{KeyedCounter, StateBackend};
use crate::{Error, MessageBatch, ProcessResult, Resource};
use datafusion::arrow::array::{BinaryArray, BooleanArray, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::compute::filter_record_batch;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

type InputRead = Result<(usize, crate::MessageBatchRef, Arc<dyn Ack>), Error>;

struct SourceRuntime {
    partition: u32,
    extractor: FieldTimestampExtractor,
    tracker: WatermarkTracker,
    late_policy: crate::job::LateEventPolicy,
    allowed_lateness_ms: u64,
}

struct StatefulProcessor {
    inner: Arc<dyn Processor>,
    counter: KeyedCounter,
    key_field: String,
}

impl StatefulProcessor {
    fn new(
        inner: Arc<dyn Processor>,
        backend: Arc<dyn StateBackend>,
        namespace: String,
        key_field: String,
    ) -> Self {
        Self {
            inner,
            counter: KeyedCounter::new(backend, namespace),
            key_field,
        }
    }

    fn keys_for_batch(&self, batch: &MessageBatch) -> Vec<Vec<u8>> {
        let row_count = batch.len();
        let Some(column) = batch.record_batch().column_by_name(&self.key_field) else {
            return vec![b"__all__".to_vec(); row_count];
        };
        if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
            return values
                .iter()
                .map(|value| {
                    value
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| b"__null__".to_vec())
                })
                .collect();
        }
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            return values
                .iter()
                .map(|value| {
                    value
                        .map(|value| value.as_bytes().to_vec())
                        .unwrap_or_else(|| b"__null__".to_vec())
                })
                .collect();
        }
        if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            return values
                .iter()
                .map(|value| {
                    value
                        .map(|value| value.to_be_bytes().to_vec())
                        .unwrap_or_else(|| b"__null__".to_vec())
                })
                .collect();
        }
        if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
            return values
                .iter()
                .map(|value| {
                    value
                        .map(|value| value.to_be_bytes().to_vec())
                        .unwrap_or_else(|| b"__null__".to_vec())
                })
                .collect();
        }
        vec![b"__all__".to_vec(); row_count]
    }
}

#[async_trait::async_trait]
impl Processor for StatefulProcessor {
    async fn process(&self, batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
        for key in self.keys_for_batch(&batch) {
            self.counter.add(&key, 1)?;
        }
        self.inner.process(batch).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

pub struct SingleComputeJobRunner {
    inputs: Vec<Arc<dyn Input>>,
    source_task_ids: Vec<String>,
    processors: BTreeMap<String, Arc<dyn Processor>>,
    outputs: BTreeMap<String, Arc<dyn Output>>,
    edges: BTreeMap<String, Vec<String>>,
    source_runtimes: Mutex<BTreeMap<String, SourceRuntime>>,
    window_sizes_ms: BTreeMap<String, i64>,
}

impl SingleComputeJobRunner {
    pub fn build<A: JobComponentAdapter>(
        plan: &JobPlan,
        adapter: &A,
        resource: &Resource,
    ) -> Result<Self, Error> {
        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        Self::build_for_tasks(plan, &task_ids, adapter, resource)
    }

    pub fn build_for_tasks<A: JobComponentAdapter>(
        plan: &JobPlan,
        task_ids: &[String],
        adapter: &A,
        resource: &Resource,
    ) -> Result<Self, Error> {
        Self::build_for_tasks_with_state(plan, task_ids, adapter, resource, None)
    }

    pub fn build_for_tasks_with_state<A: JobComponentAdapter>(
        plan: &JobPlan,
        task_ids: &[String],
        adapter: &A,
        resource: &Resource,
        state_backend: Option<Arc<dyn StateBackend>>,
    ) -> Result<Self, Error> {
        plan.spec.validate()?;
        let assigned_operators = task_ids
            .iter()
            .filter_map(|task_id| plan.task(task_id).map(|task| task.operator_id.clone()))
            .collect::<BTreeSet<_>>();
        if assigned_operators.is_empty() {
            return Err(Error::Config(
                "Job assignment contains no known tasks".into(),
            ));
        }
        let source_specs = plan
            .spec
            .sources
            .iter()
            .map(|source| (source.operator_id.as_str(), source))
            .collect::<BTreeMap<_, _>>();
        let source_tasks = task_ids
            .iter()
            .filter_map(|task_id| {
                let task = plan.task(task_id)?;
                let source = source_specs.get(task.operator_id.as_str())?;
                Some((task, *source))
            })
            .collect::<Vec<_>>();
        let source_task_ids = source_tasks
            .iter()
            .map(|(task, _)| task.id.clone())
            .collect::<Vec<_>>();
        let source_operator_ids = source_tasks
            .iter()
            .map(|(task, _)| task.operator_id.clone())
            .collect::<BTreeSet<_>>();
        let sink_operator_ids = plan
            .spec
            .sinks
            .iter()
            .filter(|sink| assigned_operators.contains(&sink.operator_id))
            .map(|sink| sink.operator_id.clone())
            .collect::<BTreeSet<_>>();
        let assigned_task_by_operator_subtask = task_ids
            .iter()
            .filter_map(|task_id| plan.task(task_id))
            .map(|task| ((task.operator_id.clone(), task.subtask), task.id.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut inputs = Vec::with_capacity(source_tasks.len());
        let mut source_task_counts = BTreeMap::<String, usize>::new();
        for (task, source) in &source_tasks {
            let input = adapter.build_input(source, resource)?;
            let count = source_task_counts
                .entry(task.operator_id.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
            if *count > 1 && !input.supports_partitioning() {
                return Err(Error::Config(format!(
                    "source '{}' does not support partitioned task execution",
                    task.operator_id
                )));
            }
            let partition = task
                .partitions
                .first()
                .map(|partition| partition.id)
                .ok_or_else(|| {
                    Error::Config(format!("task '{}' has no source partition", task.id))
                })?;
            input.assign_partition(partition)?;
            inputs.push(input);
        }
        let outputs = plan
            .spec
            .sinks
            .iter()
            .filter(|sink| assigned_operators.contains(&sink.operator_id))
            .flat_map(|sink| {
                task_ids
                    .iter()
                    .filter_map(|task_id| plan.task(task_id))
                    .filter(|task| task.operator_id == sink.operator_id)
                    .map(|task| Ok((task.id.clone(), adapter.build_output(sink, resource)?)))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let processors = task_ids
            .iter()
            .filter_map(|task_id| plan.task(task_id))
            .filter(|task| {
                !source_operator_ids.contains(&task.operator_id)
                    && !sink_operator_ids.contains(&task.operator_id)
            })
            .map(|task| {
                let operator = plan
                    .spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
                    .ok_or_else(|| {
                        Error::Config(format!("task '{}' references unknown operator", task.id))
                    })?;
                let processor = adapter.build_processor(operator, resource)?;
                let processor: Arc<dyn Processor> = if operator.stateful {
                    let backend = state_backend.clone().ok_or_else(|| {
                        Error::Config(format!(
                            "stateful operator '{}' requires a Job state backend",
                            operator.id
                        ))
                    })?;
                    Arc::new(StatefulProcessor::new(
                        processor,
                        backend,
                        format!("job:{}:task:{}", plan.spec.id, task.id),
                        operator.key_field.clone().ok_or_else(|| {
                            Error::Config(format!(
                                "stateful operator '{}' requires key_field",
                                operator.id
                            ))
                        })?,
                    ))
                } else {
                    processor
                };
                Ok((task.id.clone(), processor))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let mut edges = BTreeMap::<String, Vec<String>>::new();
        for task_id in task_ids {
            let task = plan.task(task_id).ok_or_else(|| {
                Error::Config(format!("Job assignment contains unknown task '{task_id}'"))
            })?;
            for edge in &plan.spec.edges {
                if edge.from != task.operator_id {
                    continue;
                }
                if assigned_operators.contains(&edge.to) != assigned_operators.contains(&edge.from)
                {
                    return Err(Error::Config(format!(
                        "Job assignment splits edge '{}' between nodes; connected tasks must be co-located",
                        edge.id
                    )));
                }
                let downstream_ids = if edge.partitioned {
                    assigned_task_by_operator_subtask
                        .get(&(edge.to.clone(), task.subtask))
                        .cloned()
                        .into_iter()
                        .collect::<Vec<_>>()
                } else {
                    task_ids
                        .iter()
                        .filter_map(|task_id| plan.task(task_id))
                        .filter(|downstream| downstream.operator_id == edge.to)
                        .map(|downstream| downstream.id.clone())
                        .collect::<Vec<_>>()
                };
                edges
                    .entry(task.id.clone())
                    .or_default()
                    .extend(downstream_ids);
            }
        }
        let mut source_runtimes = BTreeMap::new();
        for (task, source) in &source_tasks {
            if source.time.mode == crate::job::TimeMode::EventTime {
                source_runtimes.insert(
                    task.id.clone(),
                    SourceRuntime {
                        partition: task.partitions.first().map(|p| p.id).unwrap_or_default(),
                        extractor: FieldTimestampExtractor {
                            field: source.time.timestamp_field.clone().ok_or_else(|| {
                                Error::Config(format!(
                                    "event-time source '{}' requires timestamp_field",
                                    source.operator_id
                                ))
                            })?,
                        },
                        tracker: WatermarkTracker::from_time_spec(&source.time)?,
                        late_policy: source.time.late_event_policy,
                        allowed_lateness_ms: source.time.allowed_lateness_ms,
                    },
                );
            }
        }
        let window_sizes_ms = plan
            .spec
            .operators
            .iter()
            .filter(|operator| operator.kind == crate::job::OperatorKind::Window)
            .filter_map(|operator| {
                operator
                    .config
                    .get("window_size_ms")
                    .and_then(serde_json::Value::as_i64)
                    .filter(|size| *size > 0)
                    .map(|size| (operator.id.clone(), size))
            })
            .collect();
        Ok(Self {
            inputs,
            source_task_ids,
            processors,
            outputs,
            edges,
            source_runtimes: Mutex::new(source_runtimes),
            window_sizes_ms,
        })
    }

    pub async fn run(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.run_with_recovery(cancellation, None).await
    }

    pub async fn run_with_recovery(
        &self,
        cancellation: CancellationToken,
        recovery: Option<&RecoveryPlan>,
    ) -> Result<(), Error> {
        for input in &self.inputs {
            input.connect().await?;
        }
        if let Some(recovery) = recovery {
            for input in &self.inputs {
                input.restore_positions(&recovery.source_positions).await?;
            }
        }
        for output in self.outputs.values() {
            output.connect().await?;
        }
        let result = self.run_connected(cancellation).await;
        for processor in self.processors.values() {
            if let Err(error) = processor.close().await {
                tracing::warn!(%error, "failed to close Job processor");
            }
        }
        for input in &self.inputs {
            if let Err(error) = input.close().await {
                tracing::warn!(%error, "failed to close Job input");
            }
        }
        for output in self.outputs.values() {
            if let Err(error) = output.close().await {
                tracing::warn!(%error, "failed to close Job output");
            }
        }
        result
    }

    pub async fn current_source_positions(
        &self,
    ) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        let mut positions = Vec::new();
        for input in &self.inputs {
            positions.extend(input.current_positions().await?);
        }
        Ok(positions)
    }

    async fn run_connected(&self, cancellation: CancellationToken) -> Result<(), Error> {
        let mut reads = FuturesUnordered::new();
        for (index, input) in self.inputs.iter().enumerate() {
            reads.push(read_input(index, input.clone()));
        }
        loop {
            let Some(read) = (tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                result = reads.next() => result,
            }) else {
                return Ok(());
            };
            let (index, batch, ack) = read?;
            if let Some(batch) = self.prepare_event_time(&self.source_task_ids[index], batch)? {
                self.dispatch_from_source(&self.source_task_ids[index], batch)
                    .await?;
            }
            ack.ack().await?;
            reads.push(read_input(index, self.inputs[index].clone()));
        }
    }

    fn prepare_event_time(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<Option<crate::MessageBatchRef>, Error> {
        let mut runtimes = self
            .source_runtimes
            .lock()
            .map_err(|_| Error::Process("event-time runtime lock is unavailable".into()))?;
        let Some(runtime) = runtimes.get_mut(source_task_id) else {
            return Ok(Some(batch));
        };
        let event_times_ms = runtime.extractor.extract_timestamps_ms(&batch)?;
        let now_ms = crate::state::now_ms() as i64;
        let mut keep = Vec::with_capacity(event_times_ms.len());
        for event_time_ms in event_times_ms {
            let watermark_ms = runtime
                .tracker
                .observe(runtime.partition, event_time_ms, now_ms);
            let action = self.window_sizes_ms.values().next().map(|window_size| {
                let window_start = event_time_ms.div_euclid(*window_size) * *window_size;
                window_action(
                    window_start + *window_size,
                    event_time_ms,
                    Some(watermark_ms),
                    runtime.allowed_lateness_ms,
                    runtime.late_policy,
                )
            });
            keep.push(!matches!(action, Some(WindowAction::Drop)));
        }
        if keep.iter().all(|value| *value) {
            return Ok(Some(batch));
        }
        if keep.iter().all(|value| !*value) {
            return Ok(None);
        }
        let filtered = filter_record_batch(batch.record_batch(), &BooleanArray::from(keep))
            .map_err(|error| Error::Process(format!("filter late events: {error}")))?;
        let mut filtered_batch = MessageBatch::new_arrow(filtered);
        filtered_batch.set_input_name(batch.get_input_name());
        Ok(Some(Arc::new(filtered_batch)))
    }

    async fn dispatch_from_source(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<(), Error> {
        self.dispatch(source_task_id, batch).await
    }

    fn dispatch<'a>(
        &'a self,
        task_id: &'a str,
        batch: crate::MessageBatchRef,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let results = if let Some(processor) = self.processors.get(task_id) {
                match processor.process(batch).await? {
                    ProcessResult::Single(batch) => vec![batch],
                    ProcessResult::Multiple(batches) => batches,
                    ProcessResult::None => Vec::new(),
                }
            } else {
                vec![batch]
            };
            if let Some(output) = self.outputs.get(task_id) {
                for batch in results {
                    output.write(batch).await?;
                }
                return Ok(());
            }
            let downstream = self.edges.get(task_id).cloned().unwrap_or_default();
            for batch in results {
                for downstream_id in &downstream {
                    self.dispatch(downstream_id, batch.clone()).await?;
                }
            }
            Ok(())
        })
    }
}

fn read_input(index: usize, input: Arc<dyn Input>) -> BoxFuture<'static, InputRead> {
    Box::pin(async move {
        let (batch, ack) = input.read().await?;
        Ok((index, batch, ack))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{
        EdgeSpec, JobId, JobSpec, JobVersion, LateEventPolicy, OperatorKind, OperatorSpec,
        SinkSpec, SourceSpec, TimeMode, TimeSpec,
    };
    use crate::temporary::Temporary;
    use async_trait::async_trait;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;
    use std::collections::HashMap;

    struct TestInput;

    #[async_trait]
    impl Input for TestInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::Process("test input is not readable".into()))
        }

        fn supports_partitioning(&self) -> bool {
            true
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct TestOutput;

    #[async_trait]
    impl Output for TestOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct TestProcessor;

    #[async_trait]
    impl Processor for TestProcessor {
        async fn process(&self, batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct TestAdapter;

    impl JobComponentAdapter for TestAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(TestInput))
        }

        fn build_output(
            &self,
            _sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(TestOutput))
        }

        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(TestProcessor))
        }
    }

    fn plan() -> JobPlan {
        JobPlan::compile(JobSpec {
            id: JobId::new("routing").unwrap(),
            version: JobVersion(1),
            max_parallelism: 2,
            parallelism: 2,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "map".into(),
                    kind: OperatorKind::Map,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
            ],
            edges: vec![
                EdgeSpec {
                    id: "source-map".into(),
                    from: "source".into(),
                    to: "map".into(),
                    partitioned: true,
                },
                EdgeSpec {
                    id: "map-sink-broadcast".into(),
                    from: "map".into(),
                    to: "sink".into(),
                    partitioned: false,
                },
            ],
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "test".into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: LateEventPolicy::Drop,
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "test".into(),
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            recovery: Default::default(),
        })
        .unwrap()
    }

    #[test]
    fn builds_processors_per_task_and_broadcasts_unpartitioned_edges() {
        let plan = plan();
        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let resource = Resource {
            temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
            input_names: RefCell::new(Vec::new()),
        };
        let runner =
            SingleComputeJobRunner::build_for_tasks(&plan, &task_ids, &TestAdapter, &resource)
                .unwrap();
        assert_eq!(runner.processors.len(), 2);
        assert_eq!(runner.edges["map-0"], vec!["sink-0", "sink-1"]);
        assert_eq!(runner.edges["map-1"], vec!["sink-0", "sink-1"]);
    }

    #[tokio::test]
    async fn injects_task_scoped_state_into_stateful_processors() {
        let mut plan = plan();
        let map = plan
            .spec
            .operators
            .iter_mut()
            .find(|operator| operator.id == "map")
            .unwrap();
        map.stateful = true;
        map.key_field = Some("key".into());

        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let resource = Resource {
            temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
            input_names: RefCell::new(Vec::new()),
        };
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let runner = SingleComputeJobRunner::build_for_tasks_with_state(
            &plan,
            &task_ids,
            &TestAdapter,
            &resource,
            Some(backend.clone()),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a", "b", "a"]))],
        )
        .unwrap();
        runner.processors["map-0"]
            .process(Arc::new(MessageBatch::new_arrow(batch)))
            .await
            .unwrap();

        let entries = backend.scan("job:routing:task:map-0").unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn applies_event_time_late_event_policy_before_dispatch() {
        let mut plan = plan();
        let source = plan.spec.sources.first_mut().unwrap();
        source.time = TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(crate::job::WatermarkSpec {
                strategy: crate::job::WatermarkStrategy::Monotonous,
                out_of_orderness_ms: 0,
                idle_timeout_ms: None,
            }),
            allowed_lateness_ms: 0,
            late_event_policy: LateEventPolicy::Drop,
        };
        plan.spec.operators[1].kind = OperatorKind::Window;
        plan.spec.operators[1].config = serde_json::json!({"window_size_ms": 1_000});
        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let resource = Resource {
            temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
            input_names: RefCell::new(Vec::new()),
        };
        let runner =
            SingleComputeJobRunner::build_for_tasks(&plan, &task_ids, &TestAdapter, &resource)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
        ]));
        let mixed = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000, 0])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let prepared = runner
            .prepare_event_time("source-0", Arc::new(MessageBatch::new_arrow(mixed)))
            .unwrap()
            .unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared.record_batch().num_rows(), 1);
    }
}
