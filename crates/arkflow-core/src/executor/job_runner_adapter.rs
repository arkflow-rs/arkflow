//! High-level entry: run a JobSpec on the unified kernel.
//!
//! One code path for local mode (engine streams compiled to JobSpecs plus
//! YAML-declared jobs) and Agent mode (a subgraph of assigned tasks): compile
//! the plan, build the execution graph through a component adapter, and drive
//! it with `run_graph`.

use crate::Error;
use crate::executor::graph::ExecutionGraphBuilder;
use crate::executor::kernel_handle::KernelJobRunner;
use crate::executor::task::{run_graph, run_graph_with_hooks, run_graph_with_metrics, CheckpointHook};
use crate::job::{JobComponentAdapter, JobPlan, JobSpec};
use crate::Resource;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
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
    run_job_with_metrics(spec, adapter, resource, cancellation, None).await
}

/// Run a local Job through the same command-driven kernel runner used by an
/// Agent.  Jobs with a checkpoint section get an embedded interval driver;
/// jobs without one still use the same graph, state, and event-time setup but
/// do not create a checkpoint task.
pub async fn run_job_with_checkpoints<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let checkpoint_root = spec
        .checkpoint
        .as_ref()
        .map(|checkpoint| local_checkpoint_root(&checkpoint.object_store_uri))
        .transpose()?;
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let graph = builder.build(&plan, adapter, resource)?;
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    let participants = graph
        .chains
        .iter()
        .map(|chain| chain.entry_task_id().to_owned())
        .collect::<Vec<_>>();
    let states = state_map(&plan, state.clone());
    let mut watermark_gates = event_time_gates(&graph)?;
    let mut prepared_inputs = false;
    if let Some(root) = checkpoint_root.as_deref() {
        if !matches!(plan.spec.recovery, crate::job::RecoveryPolicy::Fail) {
            if let Some(manifest) = latest_local_checkpoint(root, &plan)? {
                let state = state.as_ref().ok_or_else(|| {
                    Error::Config("local checkpoint recovery requires a state backend".into())
                })?;
                let repository = crate::checkpoint::CheckpointRepository::new(
                    crate::checkpoint::FileCheckpointStore::new(root)?,
                );
                restore_local_snapshot(&repository, &manifest, state)?;
                for input in &inputs {
                    input.connect().await?;
                }
                for input in &inputs {
                    input.restore_positions(&manifest.source_positions).await?;
                }
                restore_event_time_watermarks(&graph, &watermark_gates, &manifest.watermarks_ms)
                    .await;
                prepared_inputs = true;
            }
        }
    }
    let handle = Arc::new(if prepared_inputs {
        KernelJobRunner::spawn_prepared_with_cancellation(
            graph,
            inputs,
            states,
            watermark_gates,
            cancellation.clone(),
        )
        .await?
    } else {
        KernelJobRunner::spawn_with_cancellation(
            graph,
            inputs,
            states,
            std::mem::take(&mut watermark_gates),
            false,
            cancellation.clone(),
        )
        .await?
    });

    let checkpoint_stop = CancellationToken::new();
    let checkpoint_task = spec.checkpoint.as_ref().map(|checkpoint| {
        let handle = handle.clone();
        let plan = plan.clone();
        let participants = participants.clone();
        let stop = checkpoint_stop.clone();
        let checkpoint = checkpoint.clone();
        tokio::spawn(async move {
            run_local_checkpoint_loop(handle, plan, participants, checkpoint, stop).await;
        })
    });

    let result = handle
        .watcher()
        .await
        .map_err(|error| Error::Process(format!("local Job runner task failed: {error}")))?;
    checkpoint_stop.cancel();
    if let Some(task) = checkpoint_task {
        let _ = task.await;
    }
    if let Some(state) = state {
        state.close()?;
    }
    result
}

/// Run a JobSpec with runtime metrics: per-batch counters update the shared
/// `RuntimeMetrics` (input/processing/output/errors) so control-plane
/// snapshots observe kernel activity.
pub async fn run_job_with_metrics<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    metrics: Option<std::sync::Arc<crate::runtime::RuntimeMetrics>>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let result = run_graph_with_metrics(
        builder.build(&plan, adapter, resource)?,
        cancellation,
        metrics,
    )
    .await;
    if let Some(state) = state {
        state.close()?;
    }
    result
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
    let state = local_state_backend(&plan)?;
    let builder = match state {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let graph = builder.build(&plan, adapter, resource)?;
    run_graph_with_hooks(graph, cancellation, hooks).await
}

/// Build the durable local state backend described by a Job.  Streams that
/// only use the compatibility window operator are allowed to omit a Job state
/// section; the graph builder supplies an in-memory backend for that case.
fn local_state_backend(
    plan: &JobPlan,
) -> Result<Option<std::sync::Arc<dyn crate::state::StateBackend>>, Error> {
    let Some(state) = &plan.spec.state else {
        return Ok(None);
    };
    if state.backend != "embedded_kv" && state.backend != "redb" {
        return Err(Error::Config(format!(
            "local Job state backend '{}' is not supported; use embedded_kv",
            state.backend
        )));
    }
    let root = PathBuf::from(std::env::temp_dir())
        .join("arkflow-local-job-state")
        .join(plan.spec.id.as_str())
        .join(format!("version-{}", plan.spec.version.0));
    let backend = crate::state::RedbStateBackend::open(root, state.format_version)?;
    Ok(Some(std::sync::Arc::new(backend)))
}

fn state_map(
    plan: &JobPlan,
    state: Option<Arc<dyn crate::state::StateBackend>>,
) -> BTreeMap<String, Arc<dyn crate::state::StateBackend>> {
    let Some(state) = state else {
        return BTreeMap::new();
    };
    plan.tasks
        .iter()
        .filter(|task| {
            plan.spec
                .operators
                .iter()
                .find(|operator| operator.id == task.operator_id)
                .is_some_and(|operator| {
                    operator.stateful || operator.kind == crate::job::OperatorKind::Window
                })
        })
        .map(|task| (task.id.clone(), state.clone()))
        .collect()
}

fn event_time_gates(
    graph: &crate::executor::graph::ExecutionGraph,
) -> Result<
    BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
    Error,
> {
    let mut gates = BTreeMap::new();
    for chain in &graph.chains {
        let Some(source_time) = chain.source_time.as_ref().filter(|time| {
            time.mode == crate::job::TimeMode::EventTime
        }) else {
            continue;
        };
        let gate = crate::executor::event_time_gate::EventTimeGate::new(
            source_time,
            chain.window_timings.clone(),
        )?;
        gates.insert(
            chain.entry_task_id().to_owned(),
            Arc::new(tokio::sync::Mutex::new(Some(gate))),
        );
    }
    Ok(gates)
}

async fn restore_event_time_watermarks(
    graph: &crate::executor::graph::ExecutionGraph,
    gates: &BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
    watermarks_ms: &BTreeMap<String, i64>,
) {
    for chain in &graph.chains {
        let Some(watermark) = watermarks_ms.get(chain.entry_task_id()) else {
            continue;
        };
        if let Some(gate) = gates.get(chain.entry_task_id()) {
            gate.lock().await.as_mut().map(|gate| {
                gate.restore_partition(chain.source_partition.unwrap_or(0), *watermark)
            });
        }
    }
}

fn latest_local_checkpoint(
    root: &std::path::Path,
    plan: &JobPlan,
) -> Result<Option<crate::checkpoint::CheckpointManifest>, Error> {
    let (directory, prefix, kind) = match plan.spec.recovery {
        crate::job::RecoveryPolicy::LatestSavepoint => (
            root.join("savepoints"),
            "savepoints",
            crate::checkpoint::RecoveryArtifactKind::Savepoint,
        ),
        _ => (
            root.join("checkpoints"),
            "checkpoints",
            crate::checkpoint::RecoveryArtifactKind::Checkpoint,
        ),
    };
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::Process(format!(
                "scan local recovery directory '{}': {error}",
                directory.display()
            )))
        }
    };
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            Error::Process(format!("scan local recovery directory: {error}"))
        })?;
        let path = entry.path().join("manifest.json");
        if !path.is_file() {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        candidates.push((modified, path));
    }
    candidates.sort_by_key(|(modified, _)| *modified);
    let repository = crate::checkpoint::CheckpointRepository::new(
        crate::checkpoint::FileCheckpointStore::new(root)?,
    );
    let state_format = plan
        .spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    for (_, path) in candidates.into_iter().rev() {
        let Some(id) = path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
        else {
            continue;
        };
        let artifact = crate::checkpoint::RecoveryArtifact {
            id: id.to_owned(),
            kind,
            manifest_key: format!("{prefix}/{id}/manifest.json"),
            job_version: plan.spec.version,
            format_version: state_format,
            created_at_ms: 0,
            status: crate::checkpoint::CheckpointStatus::Completed,
        };
        let Ok(manifest) = repository.read_manifest(&artifact) else {
            continue;
        };
        if manifest.job_id != plan.spec.id
            || manifest.job_version != plan.spec.version
            || manifest.format_version != state_format
            || manifest.state_snapshots.is_empty()
        {
            continue;
        }
        if manifest
            .state_snapshots
            .iter()
            .all(|snapshot| repository.read_state_snapshot(snapshot).is_ok())
        {
            return Ok(Some(manifest));
        }
    }
    Ok(None)
}

fn restore_local_snapshot<S: crate::checkpoint::CheckpointStore>(
    repository: &crate::checkpoint::CheckpointRepository<S>,
    manifest: &crate::checkpoint::CheckpointManifest,
    state: &Arc<dyn crate::state::StateBackend>,
) -> Result<(), Error> {
    let mut entries = BTreeMap::<(String, Vec<u8>), crate::state::StateEntry>::new();
    for snapshot_ref in &manifest.state_snapshots {
        let snapshot = repository.read_state_snapshot(snapshot_ref)?;
        if snapshot.format_version != state.format_version() {
            return Err(Error::Config(format!(
                "checkpoint state format {} is incompatible with local backend format {}",
                snapshot.format_version,
                state.format_version()
            )));
        }
        for entry in snapshot.entries {
            entries.insert((entry.namespace.clone(), entry.key.clone()), entry);
        }
    }
    let snapshot = crate::state::StateSnapshot::new(
        state.format_version(),
        entries.into_values().collect(),
    );
    state.restore(&snapshot)
}

fn local_checkpoint_catalog(
    root: &std::path::Path,
    store: &crate::checkpoint::FileCheckpointStore,
    plan: &JobPlan,
) -> crate::checkpoint::CheckpointCatalog {
    let mut catalog = crate::checkpoint::CheckpointCatalog::default();
    let directory = root.join("checkpoints");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return catalog;
    };
    let repository = crate::checkpoint::CheckpointRepository::new(store.clone());
    let format_version = plan
        .spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    for entry in entries.flatten() {
        let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let path = entry.path().join("manifest.json");
        if !path.is_file() {
            continue;
        }
        let artifact = crate::checkpoint::RecoveryArtifact {
            id: id.clone(),
            kind: crate::checkpoint::RecoveryArtifactKind::Checkpoint,
            manifest_key: format!("checkpoints/{id}/manifest.json"),
            job_version: plan.spec.version,
            format_version,
            created_at_ms: entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| modified.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default(),
            status: crate::checkpoint::CheckpointStatus::Completed,
        };
        if let Ok(manifest) = repository.read_manifest(&artifact) {
            if manifest.job_id == plan.spec.id
                && manifest.job_version == plan.spec.version
                && manifest.format_version == format_version
            {
                catalog.record(artifact);
            }
        }
    }
    catalog
}

async fn run_local_checkpoint_loop(
    handle: Arc<crate::executor::kernel_handle::KernelJobHandle>,
    plan: JobPlan,
    participants: Vec<String>,
    checkpoint: crate::job::CheckpointSpec,
    stop: CancellationToken,
) {
    let root = match local_checkpoint_root(&checkpoint.object_store_uri) {
        Ok(root) => root,
        Err(error) => {
            tracing::error!(job_id = %plan.spec.id, %error, "local Job checkpointing disabled");
            return;
        }
    };
    let store = match crate::checkpoint::FileCheckpointStore::new(&root) {
        Ok(store) => store,
        Err(error) => {
            tracing::error!(job_id = %plan.spec.id, %error, "local Job checkpoint store could not be opened");
            return;
        }
    };
    let mut catalog = local_checkpoint_catalog(&root, &store, &plan);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(checkpoint.interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or(sequence as u128);
                let checkpoint_id = format!("local-{}-{timestamp}-{sequence}", plan.spec.id);
                sequence = sequence.saturating_add(1);
                match handle.checkpoint_barrier(checkpoint_id.clone(), 0).await {
                    Ok((snapshot, source_positions, watermarks_ms)) => {
                        if let Err(error) = persist_local_checkpoint(
                            &store,
                            &mut catalog,
                            &plan,
                            &participants,
                            &checkpoint,
                            snapshot,
                            source_positions,
                            watermarks_ms,
                            &checkpoint_id,
                        ) {
                            tracing::warn!(job_id = %plan.spec.id, %error, "local Job checkpoint failed; data processing continues");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(job_id = %plan.spec.id, %error, "local Job barrier failed; data processing continues");
                    }
                }
            }
        }
    }
}

fn persist_local_checkpoint(
    store: &crate::checkpoint::FileCheckpointStore,
    catalog: &mut crate::checkpoint::CheckpointCatalog,
    plan: &JobPlan,
    participants: &[String],
    checkpoint: &crate::job::CheckpointSpec,
    snapshot: crate::state::StateSnapshot,
    source_positions: Vec<crate::checkpoint::SourcePosition>,
    watermarks_ms: BTreeMap<String, i64>,
    checkpoint_id: &str,
) -> Result<(), Error> {
    let repository = crate::checkpoint::CheckpointRepository::new(store.clone());
    let state_ref = repository.write_state_snapshot(checkpoint_id, &snapshot)?;
    let state_refs = participants
        .iter()
        .map(|task_id| crate::checkpoint::StateSnapshotRef {
            task_id: task_id.clone(),
            ..state_ref.clone()
        })
        .collect::<Vec<_>>();
    let mut coordinator = crate::checkpoint::CheckpointCoordinator::new(
        plan.spec.id.clone(),
        plan.spec.version,
        0,
        snapshot.format_version,
        participants.iter().cloned(),
    );
    let barrier = coordinator.start(checkpoint_id.to_owned())?;
    let mut positions_pending = Some(source_positions);
    for task_id in participants {
        let is_source = plan
            .task(task_id)
            .and_then(|task| {
                plan.spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
            })
            .is_some_and(|operator| operator.kind == crate::job::OperatorKind::Source);
        coordinator.acknowledge(crate::checkpoint::TaskCheckpointAck {
            task_id: task_id.clone(),
            attempt_id: format!("{task_id}:local:0"),
            partition: plan
                .task(task_id)
                .and_then(|task| task.partitions.first())
                .map(|partition| partition.id)
                .unwrap_or_default(),
            checkpoint_id: barrier.checkpoint_id.clone(),
            generation: barrier.generation,
            state: snapshot.clone(),
            source_positions: if is_source {
                positions_pending.take().unwrap_or_default()
            } else {
                Vec::new()
            },
            watermark_ms: watermarks_ms.get(task_id).copied(),
        })?;
    }
    let attempts = participants
        .iter()
        .map(|task_id| crate::checkpoint::TaskAttemptSnapshot {
            task_id: task_id.clone(),
            attempt_id: format!("{task_id}:local:0"),
            node_id: "local".into(),
        })
        .collect::<Vec<_>>();
    let manifest = coordinator.complete(attempts, state_refs)?;
    let artifact = repository.write_checkpoint(&manifest)?;
    catalog.record(artifact);
    for removed in catalog.retain_checkpoints(checkpoint.retention as usize) {
        repository.delete(&removed)?;
    }
    Ok(())
}

fn local_checkpoint_root(uri: &str) -> Result<PathBuf, Error> {
    if let Some(path) = uri.strip_prefix("file://") {
        let path = PathBuf::from(path);
        if path.as_os_str().is_empty() {
            return Err(Error::Config("file checkpoint URI has an empty path".into()));
        }
        return Ok(path);
    }
    if uri.contains("://") {
        return Err(Error::Config(format!(
            "local Job checkpoint store '{}' is not local; use file:///path",
            uri
        )));
    }
    Ok(PathBuf::from(uri))
}

/// Convenience for building the shared `Resource` outside the executor.
pub fn shared_resource(resource: Resource) -> std::sync::Arc<Resource> {
    std::sync::Arc::new(resource)
}
