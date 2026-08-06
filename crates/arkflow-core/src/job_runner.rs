//! Single-Compute execution adapter for a distributed Job plan.

use crate::checkpoint::RecoveryPlan;
use crate::event_time::{window_action, FieldTimestampExtractor, WatermarkTracker, WindowAction};
use crate::input::{Ack, Input};
use crate::job::{JobComponentAdapter, JobPlan};
use crate::output::Output;
use crate::processor::Processor;
use crate::state::{KeyedCounter, StateBackend, StateSnapshot};
use crate::{Error, MessageBatch, ProcessResult, Resource};
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
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
    window_sizes_ms: Vec<i64>,
}

struct StatefulProcessor {
    inner: Arc<dyn Processor>,
    counter: KeyedCounter,
    key_field: String,
    state_field: String,
}

impl StatefulProcessor {
    fn new(
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
        let counts = self
            .keys_for_batch(&batch)
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
        let mut enriched = MessageBatch::new_arrow(enriched);
        enriched.set_input_name(batch.get_input_name());
        self.inner.process(Arc::new(enriched)).await
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
    late_route_tasks: BTreeMap<String, String>,
    source_runtimes: Mutex<BTreeMap<String, SourceRuntime>>,
    checkpoint_gate: Arc<tokio::sync::RwLock<()>>,
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
                        plan.spec.state.as_ref().and_then(|state| state.ttl_ms),
                        operator
                            .config
                            .get("state_output_field")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("__arkflow_state_count")
                            .to_owned(),
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
        let mut late_route_tasks = BTreeMap::new();
        for (task, source) in &source_tasks {
            if let Some(route_operator) = source.time.late_event_route.as_ref() {
                let route_task = task_ids
                    .iter()
                    .filter_map(|task_id| plan.task(task_id))
                    .find(|candidate| {
                        candidate.operator_id == *route_operator
                            && candidate.subtask == task.subtask
                    })
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "late event route '{}' has no assigned task for source '{}'",
                            route_operator, task.id
                        ))
                    })?;
                late_route_tasks.insert(task.id.clone(), route_task.id.clone());
            }
            if source.time.mode == crate::job::TimeMode::EventTime {
                let operator_kinds = plan
                    .spec
                    .operators
                    .iter()
                    .map(|operator| (operator.id.as_str(), operator.kind))
                    .collect::<BTreeMap<_, _>>();
                let window_sizes = plan
                    .spec
                    .edges
                    .iter()
                    .filter(|edge| edge.from == source.operator_id)
                    .map(|edge| edge.to.clone())
                    .collect::<Vec<_>>();
                let mut queue = window_sizes;
                let mut visited = BTreeSet::new();
                let mut applicable_window_sizes_ms = Vec::new();
                while let Some(operator_id) = queue.pop() {
                    if !visited.insert(operator_id.clone()) {
                        continue;
                    }
                    if operator_kinds.get(operator_id.as_str())
                        == Some(&crate::job::OperatorKind::Window)
                    {
                        if let Some(size) = plan
                            .spec
                            .operators
                            .iter()
                            .find(|operator| operator.id == operator_id)
                            .and_then(|operator| operator.config.get("window_size_ms"))
                            .and_then(serde_json::Value::as_i64)
                            .filter(|size| *size > 0)
                        {
                            applicable_window_sizes_ms.push(size);
                        }
                    }
                    queue.extend(
                        plan.spec
                            .edges
                            .iter()
                            .filter(|edge| edge.from == operator_id)
                            .map(|edge| edge.to.clone()),
                    );
                }
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
                        window_sizes_ms: applicable_window_sizes_ms,
                    },
                );
            }
        }
        Ok(Self {
            inputs,
            source_task_ids,
            processors,
            outputs,
            edges,
            late_route_tasks,
            source_runtimes: Mutex::new(source_runtimes),
            checkpoint_gate: Arc::new(tokio::sync::RwLock::new(())),
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
        let _guard = self.checkpoint_gate.read().await;
        self.current_source_positions_unlocked().await
    }

    async fn current_source_positions_unlocked(
        &self,
    ) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        let mut positions = Vec::new();
        for input in &self.inputs {
            positions.extend(input.current_positions().await?);
        }
        Ok(positions)
    }

    pub async fn checkpoint_snapshot(
        &self,
        state: &dyn StateBackend,
    ) -> Result<
        (
            StateSnapshot,
            Vec<crate::checkpoint::SourcePosition>,
            BTreeMap<String, i64>,
        ),
        Error,
    > {
        let _guard = self.checkpoint_gate.write().await;
        let snapshot = state.snapshot()?;
        let positions = self.current_source_positions_unlocked().await?;
        let watermarks = self
            .source_runtimes
            .lock()
            .map_err(|_| Error::Process("event-time runtime lock is unavailable".into()))?
            .iter()
            .filter_map(|(task_id, runtime)| {
                runtime
                    .tracker
                    .watermark()
                    .map(|watermark| (task_id.clone(), watermark))
            })
            .collect();
        Ok((snapshot, positions, watermarks))
    }

    pub fn restore_watermarks(&self, watermarks_ms: &BTreeMap<u32, i64>) -> Result<(), Error> {
        let mut runtimes = self
            .source_runtimes
            .lock()
            .map_err(|_| Error::Process("event-time runtime lock is unavailable".into()))?;
        for runtime in runtimes.values_mut() {
            if let Some(watermark) = watermarks_ms.get(&runtime.partition) {
                runtime
                    .tracker
                    .restore_partition(runtime.partition, *watermark);
            }
        }
        Ok(())
    }

    async fn run_connected(&self, cancellation: CancellationToken) -> Result<(), Error> {
        let mut reads = FuturesUnordered::new();
        let mut idle_tick = tokio::time::interval(std::time::Duration::from_millis(100));
        for (index, input) in self.inputs.iter().enumerate() {
            reads.push(read_input(index, input.clone()));
        }
        loop {
            let Some(read) = (tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = idle_tick.tick() => {
                    self.refresh_idle_partitions()?;
                    continue;
                }
                result = reads.next() => result,
            }) else {
                return Ok(());
            };
            let (index, batch, ack) = read?;
            let _guard = self.checkpoint_gate.read().await;
            for (batch, action) in self.prepare_event_time(&self.source_task_ids[index], batch)? {
                match action {
                    WindowAction::Route => {
                        self.dispatch_late_event(&self.source_task_ids[index], batch)
                            .await?;
                    }
                    WindowAction::Update => {
                        self.dispatch_window_update(&self.source_task_ids[index], batch)
                            .await?;
                    }
                    WindowAction::Hold | WindowAction::Emit => {
                        self.dispatch_from_source(&self.source_task_ids[index], batch)
                            .await?;
                    }
                    WindowAction::Drop => {}
                }
            }
            ack.ack().await?;
            drop(_guard);
            reads.push(read_input(index, self.inputs[index].clone()));
        }
    }

    fn refresh_idle_partitions(&self) -> Result<(), Error> {
        let now_ms = crate::state::now_ms() as i64;
        let mut runtimes = self
            .source_runtimes
            .lock()
            .map_err(|_| Error::Process("event-time runtime lock is unavailable".into()))?;
        for runtime in runtimes.values_mut() {
            runtime.tracker.refresh_idle(now_ms);
        }
        Ok(())
    }

    fn prepare_event_time(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<Vec<(crate::MessageBatchRef, WindowAction)>, Error> {
        let mut runtimes = self
            .source_runtimes
            .lock()
            .map_err(|_| Error::Process("event-time runtime lock is unavailable".into()))?;
        let Some(runtime) = runtimes.get_mut(source_task_id) else {
            return Ok(vec![(batch, WindowAction::Hold)]);
        };
        let event_times_ms = runtime.extractor.extract_timestamps_ms(&batch)?;
        let now_ms = crate::state::now_ms() as i64;
        runtime.tracker.refresh_idle(now_ms);
        let mut actions = Vec::with_capacity(event_times_ms.len());
        for event_time_ms in event_times_ms {
            let Some(event_time_ms) = event_time_ms else {
                actions.push(
                    if matches!(runtime.late_policy, crate::job::LateEventPolicy::Drop) {
                        WindowAction::Drop
                    } else {
                        WindowAction::Hold
                    },
                );
                continue;
            };
            let watermark_ms = runtime
                .tracker
                .observe(runtime.partition, event_time_ms, now_ms);
            let action = runtime.window_sizes_ms.iter().min().map(|window_size| {
                let window_start = event_time_ms.div_euclid(*window_size) * *window_size;
                window_action(
                    window_start + *window_size,
                    event_time_ms,
                    Some(watermark_ms),
                    runtime.allowed_lateness_ms,
                    runtime.late_policy,
                )
            });
            actions.push(action.unwrap_or(WindowAction::Hold));
        }
        let mut groups = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for (index, action) in actions.into_iter().enumerate() {
            let group = match action {
                WindowAction::Hold => 0,
                WindowAction::Emit => 1,
                WindowAction::Update => 2,
                WindowAction::Route => 3,
                WindowAction::Drop => continue,
            };
            groups[group].push(index);
        }
        let group_actions = [
            WindowAction::Hold,
            WindowAction::Emit,
            WindowAction::Update,
            WindowAction::Route,
        ];
        groups
            .into_iter()
            .zip(group_actions)
            .filter(|(indices, _)| !indices.is_empty())
            .map(|(indices, action)| {
                let mut keep = vec![false; batch.len()];
                for index in indices {
                    keep[index] = true;
                }
                let filtered = filter_record_batch(batch.record_batch(), &BooleanArray::from(keep))
                    .map_err(|error| Error::Process(format!("filter late events: {error}")))?;
                let mut filtered_batch = MessageBatch::new_arrow(filtered);
                filtered_batch.set_input_name(batch.get_input_name());
                Ok((Arc::new(filtered_batch), action))
            })
            .collect()
    }

    async fn dispatch_from_source(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<(), Error> {
        self.dispatch(source_task_id, batch).await
    }

    async fn dispatch_late_event(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<(), Error> {
        let target = self
            .late_route_tasks
            .get(source_task_id)
            .map(String::as_str)
            .unwrap_or(source_task_id);
        let marked = self.mark_event_batch(batch, "__arkflow_late_event_route")?;
        self.dispatch(target, marked).await
    }

    async fn dispatch_window_update(
        &self,
        source_task_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<(), Error> {
        let marker = "__arkflow_late_event_update";
        let marked = self.mark_event_batch(batch, marker)?;
        self.dispatch(source_task_id, marked).await
    }

    fn mark_event_batch(
        &self,
        batch: crate::MessageBatchRef,
        marker: &str,
    ) -> Result<crate::MessageBatchRef, Error> {
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();
        if batch.record_batch().column_by_name(marker).is_none() {
            fields.push(Arc::new(Field::new(marker, DataType::Boolean, false)));
            columns.push(Arc::new(BooleanArray::from(vec![true; batch.len()])) as ArrayRef);
        }
        let marked = RecordBatch::try_new(
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
            columns,
        )
        .map_err(|error| Error::Process(format!("mark late event batch: {error}")))?;
        let mut marked = MessageBatch::new_arrow(marked);
        marked.set_input_name(batch.get_input_name());
        Ok(Arc::new(marked))
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
                    late_event_route: None,
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
        let result = runner.processors["map-0"]
            .process(Arc::new(MessageBatch::new_arrow(batch)))
            .await
            .unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("stateful test processor should return one batch");
        };
        let counts = output
            .record_batch()
            .column_by_name("__arkflow_state_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[1, 1, 2]);

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
            late_event_route: None,
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
            .into_iter()
            .next()
            .unwrap()
            .0;
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared.record_batch().num_rows(), 1);
    }

    #[test]
    fn separates_route_and_update_late_event_actions() {
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
            allowed_lateness_ms: 100,
            late_event_policy: LateEventPolicy::Route,
            late_event_route: None,
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
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_000]))],
        )
        .unwrap();
        runner
            .prepare_event_time("source-0", Arc::new(MessageBatch::new_arrow(first)))
            .unwrap();
        let late = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0]))]).unwrap();
        let actions = runner
            .prepare_event_time("source-0", Arc::new(MessageBatch::new_arrow(late)))
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].1, WindowAction::Route);
    }
}
