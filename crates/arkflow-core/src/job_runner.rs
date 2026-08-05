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
    source_ids: Vec<String>,
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
        let source_ids = source_tasks
            .iter()
            .map(|(task, _)| task.operator_id.clone())
            .collect::<Vec<_>>();
        let source_id_set = source_ids.iter().cloned().collect::<BTreeSet<_>>();
        let sink_ids = plan
            .spec
            .sinks
            .iter()
            .filter(|sink| assigned_operators.contains(&sink.operator_id))
            .map(|sink| sink.operator_id.clone())
            .collect::<BTreeSet<_>>();
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
            .map(|sink| {
                Ok((
                    sink.operator_id.clone(),
                    adapter.build_output(sink, resource)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let processors = plan
            .spec
            .operators
            .iter()
            .filter(|operator| assigned_operators.contains(&operator.id))
            .filter(|operator| {
                !source_id_set.contains(&operator.id) && !sink_ids.contains(&operator.id)
            })
            .map(|operator| {
                Ok((
                    operator.id.clone(),
                    adapter.build_processor(operator, resource)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let mut edges = BTreeMap::<String, Vec<String>>::new();
        for edge in &plan.spec.edges {
            if assigned_operators.contains(&edge.from) != assigned_operators.contains(&edge.to) {
                return Err(Error::Config(format!(
                    "Job assignment splits edge '{}' between nodes; connected tasks must be co-located",
                    edge.id
                )));
            }
            if !assigned_operators.contains(&edge.from) || !assigned_operators.contains(&edge.to) {
                continue;
            }
            edges
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }
        Ok(Self {
            inputs,
            source_ids,
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
            self.dispatch_from_source(&self.source_ids[index], batch)
                .await?;
            ack.ack().await?;
            reads.push(read_input(index, self.inputs[index].clone()));
        }
    }

    async fn dispatch_from_source(
        &self,
        source_id: &str,
        batch: crate::MessageBatchRef,
    ) -> Result<(), Error> {
        self.dispatch(source_id, batch).await
    }

    fn dispatch<'a>(
        &'a self,
        operator_id: &'a str,
        batch: crate::MessageBatchRef,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let results = if let Some(processor) = self.processors.get(operator_id) {
                match processor.process(batch).await? {
                    ProcessResult::Single(batch) => vec![batch],
                    ProcessResult::Multiple(batches) => batches,
                    ProcessResult::None => Vec::new(),
                }
            } else {
                vec![batch]
            };
            if let Some(output) = self.outputs.get(operator_id) {
                for batch in results {
                    output.write(batch).await?;
                }
                return Ok(());
            }
            let downstream = self.edges.get(operator_id).cloned().unwrap_or_default();
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
