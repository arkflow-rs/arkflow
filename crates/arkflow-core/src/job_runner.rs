//! Single-Compute execution adapter for a distributed Job plan.

use crate::checkpoint::RecoveryPlan;
use crate::input::{Ack, Input};
use crate::job::{JobComponentAdapter, JobPlan};
use crate::output::Output;
use crate::processor::Processor;
use crate::{Error, ProcessResult, Resource};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type InputRead = Result<(usize, crate::MessageBatchRef, Arc<dyn Ack>), Error>;

pub struct SingleComputeJobRunner {
    inputs: Vec<Arc<dyn Input>>,
    source_task_ids: Vec<String>,
    processors: BTreeMap<String, Arc<dyn Processor>>,
    outputs: BTreeMap<String, Arc<dyn Output>>,
    edges: BTreeMap<String, Vec<String>>,
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
                Ok((
                    task.id.clone(),
                    adapter.build_processor(operator, resource)?,
                ))
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
        Ok(Self {
            inputs,
            source_task_ids,
            processors,
            outputs,
            edges,
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
            self.dispatch_from_source(&self.source_task_ids[index], batch)
                .await?;
            ack.ack().await?;
            reads.push(read_input(index, self.inputs[index].clone()));
        }
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
}
