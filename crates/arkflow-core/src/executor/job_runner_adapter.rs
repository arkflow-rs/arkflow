//! High-level entry: run a JobSpec on the unified kernel.
//!
//! One code path for local mode (engine streams compiled to JobSpecs plus
//! YAML-declared jobs) and Agent mode (a subgraph of assigned tasks): compile
//! the plan, build the execution graph through a component adapter, and drive
//! it with `run_graph`.

use crate::Error;
use crate::executor::graph::ExecutionGraphBuilder;
use crate::executor::task::{run_graph, run_graph_with_hooks, CheckpointHook};
use crate::job::{JobComponentAdapter, JobPlan, JobSpec};
use crate::Resource;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

/// Run a whole JobSpec locally (all tasks, single process).
///
/// The graph build consumes `resource` synchronously (component builders use
/// it during construction only) and releases it before the async run, so the
/// returned future is `Send` even though `Resource` itself is not.
pub async fn run_job<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let graph = ExecutionGraphBuilder::default().build(&plan, adapter, resource)?;
    run_graph(graph, cancellation).await
}

/// Run a JobPlan's assigned task subset (Agent mode). The assignment must not
/// split an edge across the co-location boundary.
pub async fn run_job_tasks<A: JobComponentAdapter>(
    plan: &JobPlan,
    task_ids: &[String],
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    let graph = ExecutionGraphBuilder::default().build_subgraph(plan, task_ids, adapter, resource)?;
    run_graph(graph, cancellation).await
}

/// Run a whole JobSpec with per-chain checkpoint hooks (entry task id →
/// hook). Used by callers that drive a `BarrierCoordinator`.
pub async fn run_job_with_hooks<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let graph = ExecutionGraphBuilder::default().build(&plan, adapter, resource)?;
    run_graph_with_hooks(graph, cancellation, hooks).await
}

/// Convenience for building the shared `Resource` outside the executor.
pub fn shared_resource(resource: Resource) -> std::sync::Arc<Resource> {
    std::sync::Arc::new(resource)
}
