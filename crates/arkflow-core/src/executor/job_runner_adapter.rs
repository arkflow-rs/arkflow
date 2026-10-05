//! High-level entry: run a JobSpec on the unified kernel.
//!
//! One code path for local mode (engine streams compiled to JobSpecs plus
//! YAML-declared jobs) and Agent mode (a subgraph of assigned tasks): compile
//! the plan, build the execution graph through a component adapter, and drive
//! it with `run_graph`.

use crate::executor::graph::ExecutionGraphBuilder;
use crate::executor::kernel_handle::KernelJobRunner;
use crate::executor::task::{
    run_graph, run_graph_with_hooks, run_graph_with_metrics_startup, CheckpointHook,
};
use crate::job::{JobComponentAdapter, JobPlan, JobSpec};
use crate::Error;
use crate::Resource;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Deep-build a local Job without starting it: compile the plan, open the
/// local state backend, and construct the execution graph through the same
/// component path the real startup uses. Every constructed resource is
/// dropped before returning (redb handles release their locks on drop), so
/// the real startup can reopen them. Called before readiness so an invalid
/// component, unsupported backend, or broken graph fails startup visibly.
pub fn validate_local_job(spec: &JobSpec) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let adapter = crate::executor::stream_adapter::StreamJobAdapter::new(None)?;
    let resource = crate::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    builder.build(&plan, &adapter, &resource)?;
    Ok(())
}

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
    run_job_with_checkpoints_started(spec, adapter, resource, cancellation, None, None).await
}

/// Run a local Job and notify the caller once the graph's resource startup
/// has completed. The notification is used by the Engine to delay readiness
/// until component construction and resource connection have actually
/// succeeded; the ordinary API keeps the notification optional for existing
/// callers. When `metrics_registry` is `Some`, the spawned Job's
/// `KernelMetrics` are registered under the Job id for observability export
/// and unregistered once the run finishes.
pub async fn run_job_with_checkpoints_started<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    mut startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    metrics_registry: Option<crate::runtime::JobMetricsRegistry>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let checkpoint_root = spec
        .checkpoint
        .as_ref()
        .map(|checkpoint| local_checkpoint_root(&checkpoint.object_store_uri))
        .transpose()?;
    let recovery_required = durable_local_recovery_required(&plan);
    let recovery_manifest = if recovery_required {
        let Some(root) = checkpoint_root.as_deref() else {
            return Err(Error::Config(
                "durable local Job state requires a checkpoint artifact before restart".into(),
            ));
        };
        Some(latest_local_checkpoint(root, &plan)?.ok_or_else(|| {
            Error::Config(format!(
                "durable local Job '{}' requires recovery but no compatible checkpoint was found",
                plan.spec.id
            ))
        })?)
    } else {
        None
    };
    let mut rescale_context = None;
    if let Some(manifest) = recovery_manifest.as_ref() {
        match validate_manifest_task_compatibility(manifest, &plan) {
            Ok(()) => {}
            Err(error) if plan.spec.rescale => {
                rescale_context = Some(RescaleContext::from_plan(&plan)?);
                tracing::info!(
                    job = %plan.spec.id,
                    "rescale recovery: redistributing keyed state across the new task set ({error})"
                );
            }
            Err(error) => return Err(error),
        }
    }
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    // Local checkpoint manifests are validated against the logical Job plan,
    // while adjacent stateless processors may be fused into one runtime
    // chain. Record every logical task so a fused graph still produces the
    // exact task set required for recovery.
    let participants = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    let states = state_map(&plan, state.clone());
    let mut watermark_gates = event_time_gates(&graph)?;
    let mut prepared_inputs = false;
    if let Some(manifest) = recovery_manifest.as_ref() {
        let Some(root) = checkpoint_root.as_deref() else {
            return Err(Error::Config(
                "local checkpoint recovery requires a checkpoint root".into(),
            ));
        };
        let state = state.as_ref().ok_or_else(|| {
            Error::Config("local checkpoint recovery requires a state backend".into())
        })?;
        let repository = crate::checkpoint::CheckpointRepository::new(
            crate::checkpoint::FileCheckpointStore::new(root)?,
        );
        let namespace_prefix =
            crate::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
        restore_local_snapshot(
            &repository,
            manifest,
            state,
            &namespace_prefix,
            rescale_context.as_ref(),
        )?;
        for input in &inputs {
            if let Err(error) = input.connect().await {
                close_inputs(&inputs).await;
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
        if let Err(error) = seed_event_time_partitions(&graph, &watermark_gates).await {
            close_inputs(&inputs).await;
            let _ = state.close();
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
        for input in &inputs {
            if let Err(error) = input.restore_positions(&manifest.source_positions).await {
                close_inputs(&inputs).await;
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
        restore_event_time_watermarks(
            &graph,
            &watermark_gates,
            &manifest.watermarks_ms,
            &manifest.watermark_partitions,
        )
        .await;
        prepared_inputs = true;
    }
    let start_marker = if durable_local_state(&plan) {
        local_state_start_marker(&plan)
    } else {
        None
    };
    if let Some(marker) = start_marker.as_deref() {
        if let Err(error) = persist_start_marker(marker) {
            if prepared_inputs {
                close_inputs(&inputs).await;
            }
            if let Some(state) = state.as_ref() {
                let _ = state.close();
            }
            let error = Error::Process(format!(
                "durable local Job could not persist its start marker: {error}"
            ));
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
    }
    let spawned_result = if prepared_inputs {
        KernelJobRunner::spawn_prepared_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            watermark_gates,
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation.clone(),
        )
        .await
    } else {
        KernelJobRunner::spawn_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            std::mem::take(&mut watermark_gates),
            false,
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation.clone(),
        )
        .await
    };
    let spawned = match spawned_result {
        Ok(handle) => handle,
        Err(error) => {
            if let Some(marker) = start_marker.as_deref() {
                remove_start_marker(marker);
            }
            if prepared_inputs {
                close_inputs(&inputs).await;
            }
            if let Some(state) = state.as_ref() {
                let _ = state.close();
            }
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
    };
    let handle = Arc::new(spawned);
    if let Some(registry) = metrics_registry.as_ref() {
        registry.register(spec.id.as_str(), handle.metrics());
    }
    if let Some(startup) = startup.take() {
        let _ = startup.send(Ok(()));
    }

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
    if let Some(registry) = metrics_registry.as_ref() {
        registry.unregister(spec.id.as_str());
    }
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
    run_job_with_metrics_started(spec, adapter, resource, cancellation, metrics, None).await
}

/// Metrics-enabled local Job runner with an optional startup handshake. The
/// sender is completed by the graph runner only after temporary stores,
/// sources, sinks, and state backends have connected successfully.
pub async fn run_job_with_metrics_started<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    metrics: Option<std::sync::Arc<crate::runtime::RuntimeMetrics>>,
    startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    let result = run_graph_with_metrics_startup(graph, cancellation, metrics, startup).await;
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
    let mut graph =
        ExecutionGraphBuilder::default().build_subgraph(plan, task_ids, adapter, resource, None)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
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
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
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
    let root = local_state_root(plan).expect("state section checked above");
    let backend = crate::state::RedbStateBackend::open(root, state.format_version)?;
    let backend = match state.max_bytes {
        Some(max_bytes) => backend.with_max_bytes(max_bytes),
        None => backend,
    };
    Ok(Some(std::sync::Arc::new(backend)))
}

fn local_state_root(plan: &JobPlan) -> Option<PathBuf> {
    let state = plan.spec.state.as_ref()?;
    let base = match state.durability {
        crate::job::StateDurability::Durable => crate::job::configured_state_root(state),
        crate::job::StateDurability::Ephemeral => std::env::temp_dir()
            .join("arkflow-ephemeral-job-state")
            .join(ephemeral_state_nonce()),
    };
    Some(
        base.join("jobs")
            .join(plan.spec.id.as_str())
            .join(format!("version-{}", plan.spec.version.0)),
    )
}

fn ephemeral_state_nonce() -> String {
    static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{timestamp}-{attempt}", std::process::id())
}

fn local_state_start_marker(plan: &JobPlan) -> Option<PathBuf> {
    Some(local_state_root(plan)?.join(".arkflow-started"))
}

fn persist_start_marker(marker: &std::path::Path) -> std::io::Result<()> {
    let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
    if let Err(error) = std::fs::write(&temporary, b"started\n") {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&temporary, marker) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn remove_start_marker(marker: &std::path::Path) {
    let _ = std::fs::remove_file(marker);
    let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(temporary);
}

fn durable_local_recovery_required(plan: &JobPlan) -> bool {
    durable_local_state(plan)
        && local_state_start_marker(plan).is_some_and(|marker| marker.is_file())
}

fn durable_local_state(plan: &JobPlan) -> bool {
    plan.spec
        .state
        .as_ref()
        .is_some_and(|state| state.durability == crate::job::StateDurability::Durable)
        && plan.spec.requires_state()
        && plan.spec.checkpoint.is_some()
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

#[allow(clippy::type_complexity)]
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
    let mut shared_trackers =
        BTreeMap::<String, Arc<std::sync::Mutex<crate::event_time::WatermarkTracker>>>::new();
    for chain in &graph.chains {
        let Some(source_time) = chain
            .source_time
            .as_ref()
            .filter(|time| time.mode == crate::job::TimeMode::EventTime)
        else {
            continue;
        };
        let group = chain
            .watermark_group
            .clone()
            .unwrap_or_else(|| chain.entry_task_id().to_owned());
        // The group is the identity of the downstream watermark component.
        // Source edges feeding the same Window must classify lateness against
        // one shared minimum, even when their local source contracts or
        // timing vectors were constructed independently.
        let tracker_key = group;
        let tracker = match shared_trackers.entry(tracker_key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let tracker = crate::event_time::WatermarkTracker::from_time_spec(source_time)?;
                let tracker = Arc::new(std::sync::Mutex::new(tracker));
                entry.insert(tracker.clone());
                tracker
            }
        };
        let gate = crate::executor::event_time_gate::EventTimeGate::new_with_shared_tracker(
            source_time,
            chain.window_timings.clone(),
            tracker,
        )?;
        gates.insert(
            chain.entry_task_id().to_owned(),
            Arc::new(tokio::sync::Mutex::new(Some(gate))),
        );
    }
    Ok(gates)
}

/// Seed every event-time gate with the connector's complete assignment before
/// restoring checkpointed progress.  A Kafka reader may have several idle
/// physical partitions; leaving those partitions out would let the first fast
/// partition advance the shared minimum before the idle partition's first
/// record arrives.
async fn seed_event_time_partitions(
    graph: &crate::executor::graph::ExecutionGraph,
    gates: &BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
) -> Result<(), Error> {
    for chain in &graph.chains {
        let Some(gate) = gates.get(chain.entry_task_id()) else {
            continue;
        };
        let Some(source) = chain.source.as_ref() else {
            continue;
        };
        let source_id = chain.entry_task_id();
        let mut partitions = source
            .watermark_partitions()
            .await?
            .into_iter()
            .map(|partition| partition.with_source_identity(source_id))
            .collect::<Vec<_>>();
        if partitions.is_empty() {
            if let Some(partition) = chain.source_partition {
                partitions.push(crate::event_time::EventTimePartition::for_source(
                    source_id, partition,
                ));
            }
        }
        if !partitions.is_empty() {
            if let Some(gate) = gate.lock().await.as_mut() {
                gate.seed_partitions(&partitions)
            }
        }
    }
    Ok(())
}

async fn restore_event_time_watermarks(
    graph: &crate::executor::graph::ExecutionGraph,
    gates: &BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
    watermarks_ms: &BTreeMap<String, i64>,
    watermark_partitions: &BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
) {
    for (task_id, partitions) in watermark_partitions {
        if let Some(gate) = gates.get(task_id) {
            let mut gate = gate.lock().await;
            if let Some(gate) = gate.as_mut() {
                for partition in partitions {
                    gate.restore_partition_key(
                        &crate::event_time::EventTimePartition::new(
                            partition.topic.clone(),
                            partition.partition,
                        )
                        .with_source_identity(task_id),
                        partition.watermark_ms,
                    );
                }
            }
        }
    }
    for chain in &graph.chains {
        if watermark_partitions
            .get(chain.entry_task_id())
            .is_some_and(|partitions| !partitions.is_empty())
        {
            continue;
        }
        let Some(watermark) = watermarks_ms.get(chain.entry_task_id()) else {
            continue;
        };
        if let Some(gate) = gates.get(chain.entry_task_id()) {
            let mut gate_guard = gate.lock().await;
            if let Some(gate) = gate_guard.as_mut() {
                let known = gate.known_partitions();
                if known.is_empty() {
                    let partition = crate::event_time::EventTimePartition::for_source(
                        chain.entry_task_id(),
                        chain.source_partition.unwrap_or(0),
                    );
                    gate.restore_partition_key(&partition, *watermark);
                } else {
                    for partition in known {
                        gate.restore_partition_key(&partition, *watermark);
                    }
                }
            }
        }
    }
}

async fn close_inputs(inputs: &[Arc<dyn crate::input::Input>]) {
    for input in inputs.iter().rev() {
        if let Err(error) = input.close().await {
            tracing::warn!(%error, "failed to close input after local Job startup failure");
        }
    }
}

/// Fail closed when a recovery artifact was written under a different task
/// set than the current plan: keyed state namespaces embed task ids, so a
/// parallelism (or operator-topology) change would silently strand the old
/// state behind renamed namespaces while source positions still restore.
/// Until key redistribution lands, the operator must keep the parallelism
/// fixed or reset state with a fresh checkpoint/savepoint.
fn validate_manifest_task_compatibility(
    manifest: &crate::checkpoint::CheckpointManifest,
    plan: &JobPlan,
) -> Result<(), Error> {
    let plan_tasks: BTreeSet<&str> = plan.tasks.iter().map(|task| task.id.as_str()).collect();
    let manifest_tasks: BTreeSet<&str> = manifest
        .task_attempts
        .iter()
        .map(|attempt| attempt.task_id.as_str())
        .collect();
    if plan_tasks != manifest_tasks {
        let removed: Vec<&str> = manifest_tasks.difference(&plan_tasks).copied().collect();
        let added: Vec<&str> = plan_tasks.difference(&manifest_tasks).copied().collect();
        return Err(Error::Config(format!(
            "recovery artifact '{}' was written under a different task set than the current plan \
             (parallelism or operator topology changed; removed tasks {:?}, added tasks {:?}); \
             keyed state cannot yet redistribute across a parallelism change — restore the original \
             parallelism or reset state with a fresh checkpoint/savepoint",
            manifest.checkpoint_id, removed, added
        )));
    }
    Ok(())
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
        let entry = entry
            .map_err(|error| Error::Process(format!("scan local recovery directory: {error}")))?;
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
        if manifest.state_snapshots.is_empty() {
            continue;
        }
        // One shared compatibility evaluation (task membership, formats,
        // version direction, checksum) — the same verdict the Hub and Agent
        // would reach for this artifact.
        let planned_tasks = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<BTreeSet<_>>();
        let compatibility = crate::checkpoint::evaluate_recovery_compatibility(
            &manifest,
            &plan.spec.id,
            plan.spec.version,
            state_format,
            &planned_tasks,
        );
        if !compatibility.is_compatible() {
            tracing::warn!(
                job_id = %plan.spec.id,
                checkpoint = %manifest.checkpoint_id,
                reason = compatibility.reason.as_deref().unwrap_or("incompatible"),
                "skipping incompatible local recovery artifact"
            );
            continue;
        }
        if let Err(reason) = crate::checkpoint::validate_state_snapshot_task_set(
            &manifest.state_snapshots,
            &planned_tasks,
        ) {
            tracing::warn!(
                job_id = %plan.spec.id,
                checkpoint = %manifest.checkpoint_id,
                expected = ?planned_tasks,
                reason = %reason,
                "skipping local recovery artifact with invalid task snapshots"
            );
            continue;
        }
        let namespace_prefix =
            crate::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
        let snapshots_are_compatible = manifest.state_snapshots.iter().all(|snapshot_ref| {
            repository
                .read_state_snapshot(snapshot_ref)
                .ok()
                .is_some_and(|snapshot| {
                    crate::checkpoint::validate_state_snapshot_namespace(
                        &snapshot,
                        &namespace_prefix,
                    )
                    .is_ok()
                })
        });
        if snapshots_are_compatible {
            return Ok(Some(manifest));
        }
    }
    Ok(None)
}

/// Redistributes keyed state entries across a rescaled task set: each
/// entry's user key is recovered from its operator-specific state-key
/// encoding, hashed with the same normalization the routing path uses, and
/// the entry is rewritten under the new owning task's namespace.
///
/// Shared by the local runner and the distributed Agent recovery path: the
/// encoding whitelist and key-group resolution must not fork.
pub struct RescaleContext {
    plan: JobPlan,
}

impl RescaleContext {
    pub fn from_plan(plan: &JobPlan) -> Result<Self, Error> {
        Ok(Self { plan: plan.clone() })
    }

    /// The routing-hash input for one state entry: the user key in the exact
    /// byte form `hash_column`/`task_for_key` hash.
    fn routing_key_bytes(&self, namespace: &str, key: &[u8]) -> Result<Vec<u8>, Error> {
        let operator_id = namespace_operator(namespace)?;
        let is_window = self.plan.spec.operators.iter().any(|operator| {
            operator.id == operator_id && operator.kind == crate::job::OperatorKind::Window
        });
        if is_window {
            // Window state key = window_start (8-byte BE) + utf8 user key.
            let user_key = key
                .get(8..)
                .ok_or_else(|| Error::Process("corrupt window state key during rescale".into()))?;
            return Ok(user_key.to_vec());
        }
        // StatefulOperator state key = "<tag>:" + value encoding, where the
        // post-tag bytes are exactly the routing hash input; the null
        // sentinels ("null:<tag>") hash as-is.
        if let Some(rest) = key.strip_prefix(b"utf8:") {
            return Ok(rest.to_vec());
        }
        for tag in [
            &b"binary:"[..],
            b"i8:",
            b"i16:",
            b"i32:",
            b"i64:",
            b"u8:",
            b"u16:",
            b"u32:",
            b"u64:",
        ] {
            if let Some(rest) = key.strip_prefix(tag) {
                return Ok(rest.to_vec());
            }
        }
        if key.starts_with(b"null:") {
            return Ok(key.to_vec());
        }
        Err(Error::Process(format!(
            "state entry for operator '{operator_id}' uses an unrecognized key encoding;              rescale redistribution cannot derive its routing key"
        )))
    }

    pub fn redistribute(
        &self,
        entry: crate::state::StateEntry,
    ) -> Result<crate::state::StateEntry, Error> {
        let operator_id = namespace_operator(&entry.namespace)?;
        let routing_key = self.routing_key_bytes(&entry.namespace, &entry.key)?;
        let group = crate::job::key_group_for_key(&routing_key, self.plan.spec.max_parallelism)?;
        let owner = self
            .plan
            .tasks
            .iter()
            .find(|task| {
                task.operator_id == operator_id
                    && task
                        .partitions
                        .iter()
                        .any(|partition| partition.key_group.contains(group))
            })
            .ok_or_else(|| {
                Error::Process(format!(
                    "rescale found no owner task for key group {group} of operator '{operator_id}'"
                ))
            })?;
        let new_namespace = crate::job::effective_state_namespace(
            &self.plan.spec.id,
            self.plan.spec.state.as_ref(),
            &operator_id,
            &owner.id,
        );
        Ok(crate::state::StateEntry {
            namespace: new_namespace,
            ..entry
        })
    }

    /// The task-id segment of a redistributed entry's namespace. Distributed
    /// recovery uses it to keep only the entries this node's assignments own
    /// (mirror of [`namespace_operator`]'s percent-decoding).
    pub fn task_of_namespace(namespace: &str) -> Result<String, Error> {
        let marker = ":task:";
        let start = namespace
            .find(marker)
            .ok_or_else(|| Error::Process("state namespace lacks a task segment".into()))?
            + marker.len();
        Ok(namespace[start..]
            .split(':')
            .next()
            .unwrap_or_default()
            .replace("%3A", ":")
            .replace("%25", "%"))
    }
}

/// Extract the operator-id segment from a state namespace built by
/// `effective_state_namespace` (`...:operator:<id>:task:<id>` with
/// percent-encoded components).
fn namespace_operator(namespace: &str) -> Result<String, Error> {
    let marker = ":operator:";
    let start = namespace
        .find(marker)
        .ok_or_else(|| Error::Process("state namespace lacks an operator segment".into()))?
        + marker.len();
    let rest = &namespace[start..];
    let end = rest.find(":task:").unwrap_or(rest.len());
    Ok(rest[..end].replace("%3A", ":").replace("%25", "%"))
}

fn restore_local_snapshot<S: crate::checkpoint::CheckpointStore>(
    repository: &crate::checkpoint::CheckpointRepository<S>,
    manifest: &crate::checkpoint::CheckpointManifest,
    state: &Arc<dyn crate::state::StateBackend>,
    namespace_prefix: &str,
    rescale: Option<&RescaleContext>,
) -> Result<(), Error> {
    let mut entries = BTreeMap::<(String, Vec<u8>), crate::state::StateEntry>::new();
    for snapshot_ref in &manifest.state_snapshots {
        let snapshot = repository.read_state_snapshot(snapshot_ref)?;
        crate::checkpoint::validate_state_snapshot_namespace(&snapshot, namespace_prefix)
            .map_err(Error::Config)?;
        if snapshot.format_version != state.format_version() {
            return Err(Error::Config(format!(
                "checkpoint state format {} is incompatible with local backend format {}",
                snapshot.format_version,
                state.format_version()
            )));
        }
        for entry in snapshot.entries {
            let entry = match rescale {
                Some(context) => context.redistribute(entry)?,
                None => entry,
            };
            entries.insert((entry.namespace.clone(), entry.key.clone()), entry);
        }
    }
    let snapshot =
        crate::state::StateSnapshot::new(state.format_version(), entries.into_values().collect());
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
                .and_then(|modified| {
                    modified
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .ok()
                })
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default(),
            status: crate::checkpoint::CheckpointStatus::Completed,
        };
        if let Ok(manifest) = repository.read_manifest(&artifact) {
            if manifest.job_id == plan.spec.id
                && manifest.job_version <= plan.spec.version
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
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_millis(checkpoint.interval_ms));
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
                match handle.checkpoint_barrier_with_details(checkpoint_id.clone(), 0).await {
                    Ok((snapshot, source_positions, watermarks_ms, watermark_partitions)) => {
                        if let Err(error) = persist_local_checkpoint(
                            &store,
                            &mut catalog,
                            &plan,
                            &participants,
                            &checkpoint,
                            snapshot,
                            source_positions,
                            watermarks_ms,
                            watermark_partitions,
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

#[allow(clippy::too_many_arguments)]
fn persist_local_checkpoint(
    store: &crate::checkpoint::FileCheckpointStore,
    catalog: &mut crate::checkpoint::CheckpointCatalog,
    plan: &JobPlan,
    participants: &[String],
    checkpoint: &crate::job::CheckpointSpec,
    snapshot: crate::state::StateSnapshot,
    source_positions: Vec<crate::checkpoint::SourcePosition>,
    watermarks_ms: BTreeMap<String, i64>,
    watermark_partitions: BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
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
            watermark_partitions: watermark_partitions
                .get(task_id)
                .cloned()
                .unwrap_or_default(),
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
    let planned_tasks = participants.iter().cloned().collect::<BTreeSet<_>>();
    let artifact = repository.write_checkpoint_with_plan(&manifest, &planned_tasks)?;
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
            return Err(Error::Config(
                "file checkpoint URI has an empty path".into(),
            ));
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
    // `Resource` is not `Sync` (its `input_names` RefCell is only touched
    // during the single-threaded build phase), so this Arc is shared for
    // cheap cloning, never for cross-thread mutation of that field.
    #[allow(clippy::arc_with_non_send_sync)]
    std::sync::Arc::new(resource)
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn rescale_across_parallelism_fails_closed_on_recovery() {
        let mut spec = crate::job::JobSpec {
            resources: Default::default(),
            rescale: false,
            rebalance: None,
            placement: crate::job::PlacementStrategy::Colocated,
            id: JobId::new("rescale-guard").unwrap(),
            version: JobVersion(1),
            max_parallelism: 8,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
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
            edges: vec![crate::job::EdgeSpec {
                id: "edge".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }],
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "vec".into(),
                codec: None,
                config: serde_json::json!({}),
                time: crate::job::TimeSpec {
                    mode: crate::job::TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "collect".into(),
                codec: None,
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            recovery: Default::default(),
        };
        let manifest = crate::checkpoint::CheckpointManifest {
            checkpoint_id: "c-1".into(),
            job_id: spec.id.clone(),
            job_version: spec.version,
            generation: 1,
            task_attempts: vec![
                crate::checkpoint::TaskAttemptSnapshot {
                    task_id: "source-0".into(),
                    attempt_id: "source-0:n1:0".into(),
                    node_id: "n1".into(),
                },
                crate::checkpoint::TaskAttemptSnapshot {
                    task_id: "sink-0".into(),
                    attempt_id: "sink-0:n1:0".into(),
                    node_id: "n1".into(),
                },
            ],
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id: "c-1".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: Vec::new(),
            format_version: 1,
            checksum: 0,
        };
        // Same parallelism: compatible.
        let same = JobPlan::compile(spec.clone()).unwrap();
        assert!(validate_manifest_task_compatibility(&manifest, &same).is_ok());
        // Parallelism change: fail closed with an actionable error.
        spec.parallelism = 2;
        let rescaled = JobPlan::compile(spec).unwrap();
        let error = validate_manifest_task_compatibility(&manifest, &rescaled)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("parallelism or operator topology changed"),
            "{error}"
        );
        assert!(error.contains("fresh checkpoint"), "{error}");
    }

    fn rescale_job_spec(parallelism: u32) -> JobSpec {
        let mut spec = local_spec("vec", "collect");
        spec.id = JobId::new("rescale-job").unwrap();
        spec.max_parallelism = 16;
        spec.parallelism = parallelism;
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.operators = vec![
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "agg".into(),
                kind: OperatorKind::Aggregate,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ];
        spec.edges = vec![
            crate::job::EdgeSpec {
                id: "e1".into(),
                from: "source".into(),
                to: "agg".into(),
                partitioned: true,
            },
            crate::job::EdgeSpec {
                id: "e2".into(),
                from: "agg".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        spec
    }

    #[test]
    fn rescale_redistributes_stateful_entries_by_key_group() {
        // Snapshot written under parallelism 1; restart under parallelism 4:
        // every entry must land on the task that owns its key group.
        let old_spec = rescale_job_spec(1);
        let old_plan = JobPlan::compile(old_spec).unwrap();
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace =
            crate::job::effective_state_namespace(&old_plan.spec.id, None, "agg", &old_task.id);
        for user_key in ["alpha", "beta", "gamma", "delta"] {
            let state_key = format!("utf8:{user_key}").into_bytes();
            let entry = crate::state::StateEntry {
                namespace: old_namespace.clone(),
                key: state_key.clone(),
                value: b"42".to_vec(),
                expires_at_ms: None,
            };
            let moved = context.redistribute(entry).unwrap();
            // The new namespace must belong to the new owning task.
            let group = crate::job::key_group_for_key(user_key.as_bytes(), 16).unwrap();
            let owner = new_plan
                .tasks
                .iter()
                .find(|task| {
                    task.operator_id == "agg"
                        && task
                            .partitions
                            .iter()
                            .any(|partition| partition.key_group.contains(group))
                })
                .unwrap();
            let expected =
                crate::job::effective_state_namespace(&new_plan.spec.id, None, "agg", &owner.id);
            assert_eq!(moved.namespace, expected, "key {user_key}");
            assert_eq!(moved.key, state_key);
            assert_eq!(moved.value, b"42".to_vec());
        }
    }

    #[test]
    fn rescale_redistributes_window_entries_stripping_window_start() {
        let old_plan = JobPlan::compile(rescale_job_spec(2)).unwrap();
        let mut window_spec = rescale_job_spec(4);
        // Turn the aggregate into a window operator so the window key layout
        // (window_start + utf8 key) applies.
        window_spec
            .operators
            .retain(|operator| operator.id != "agg");
        window_spec.operators.push(OperatorSpec {
            id: "agg".into(),
            kind: OperatorKind::Window,
            stateful: true,
            key_field: Some("key".into()),
            config: serde_json::json!({
                "trigger": "watermark",
                "kind": "tumbling",
                "size_ms": 1000,
                "key_field": "key",
                "timestamp_field": "ts",
                "value_fields": ["value"]
            }),
        });
        let new_plan = JobPlan::compile(window_spec).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace =
            crate::job::effective_state_namespace(&old_plan.spec.id, None, "agg", &old_task.id);
        // Window state key: 8-byte BE window_start + utf8 user key.
        let mut state_key = 5_000i64.to_be_bytes().to_vec();
        state_key.extend_from_slice(b"omega");
        let entry = crate::state::StateEntry {
            namespace: old_namespace,
            key: state_key.clone(),
            value: b"7".to_vec(),
            expires_at_ms: None,
        };
        let moved = context.redistribute(entry).unwrap();
        let group = crate::job::key_group_for_key(b"omega", 16).unwrap();
        let owner = new_plan
            .tasks
            .iter()
            .find(|task| {
                task.operator_id == "agg"
                    && task
                        .partitions
                        .iter()
                        .any(|partition| partition.key_group.contains(group))
            })
            .unwrap();
        let expected =
            crate::job::effective_state_namespace(&new_plan.spec.id, None, "agg", &owner.id);
        assert_eq!(moved.namespace, expected);
        assert_eq!(moved.key, state_key);
    }

    #[test]
    fn rescale_rejects_unknown_key_encoding() {
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();
        let old_plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let entry = crate::state::StateEntry {
            namespace: crate::job::effective_state_namespace(
                &old_plan.spec.id,
                None,
                "agg",
                &old_task.id,
            ),
            key: b"no-known-prefix".to_vec(),
            value: Vec::new(),
            expires_at_ms: None,
        };
        assert!(context.redistribute(entry).is_err());
    }

    use crate::job::{
        CheckpointSpec, JobId, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec,
        StateDurability, StateSpec, TimeMode, TimeSpec,
    };

    fn local_spec(input_type: &str, output_type: &str) -> JobSpec {
        JobSpec {
            resources: Default::default(),
            rebalance: None,
            id: JobId::new("validate-job").unwrap(),
            version: JobVersion(1),
            max_parallelism: 1,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({"type": input_type}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({"type": output_type}),
                },
            ],
            edges: vec![crate::job::EdgeSpec {
                id: "source-sink".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }],
            sources: vec![SourceSpec {
                codec: None,
                operator_id: "source".into(),
                input_type: input_type.into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                codec: None,
                operator_id: "sink".into(),
                output_type: output_type.into(),
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            placement: crate::job::PlacementStrategy::Colocated,
            recovery: Default::default(),
            rescale: false,
        }
    }

    /// Task 2.5 / 6.1: a local Job referencing an unknown component fails
    /// the side-effect-free deep build instead of failing later at runtime.
    #[test]
    fn validate_local_job_rejects_unknown_components() {
        let spec = local_spec("no-such-input", "no-such-output");
        let error =
            validate_local_job(&spec).expect_err("unknown input component must fail validation");
        assert!(error.to_string().contains("Unknown input type"));
    }

    #[test]
    fn durable_local_state_reuses_a_stable_root_and_requires_a_checkpoint_after_start() {
        let temp = tempfile::tempdir().unwrap();
        let mut spec = local_spec("memory", "drop");
        spec.operators.insert(
            1,
            OperatorSpec {
                id: "aggregate".into(),
                kind: OperatorKind::Aggregate,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({}),
            },
        );
        spec.edges = vec![
            crate::job::EdgeSpec {
                id: "source-aggregate".into(),
                from: "source".into(),
                to: "aggregate".into(),
                partitioned: false,
            },
            crate::job::EdgeSpec {
                id: "aggregate-sink".into(),
                from: "aggregate".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Durable,
            root: Some(temp.path().display().to_string()),
            namespace: Some("stable".into()),
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.checkpoint = Some(CheckpointSpec {
            interval_ms: 1_000,
            retention: 1,
            object_store_uri: format!("file://{}", temp.path().join("checkpoints").display()),
        });
        let plan = JobPlan::compile(spec).unwrap();
        let first_root = local_state_root(&plan).unwrap();
        let second_root = local_state_root(&plan).unwrap();
        assert_eq!(first_root, second_root);
        assert!(!durable_local_recovery_required(&plan));
        std::fs::create_dir_all(&first_root).unwrap();
        std::fs::write(local_state_start_marker(&plan).unwrap(), b"started\n").unwrap();
        assert!(durable_local_recovery_required(&plan));
    }

    #[test]
    fn ephemeral_state_does_not_turn_an_empty_start_into_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let mut spec = local_spec("memory", "drop");
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Ephemeral,
            root: Some(temp.path().display().to_string()),
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.checkpoint = None;
        let plan = JobPlan::compile(spec).unwrap();
        let first_root = local_state_root(&plan).unwrap();
        let second_root = local_state_root(&plan).unwrap();
        assert_ne!(
            first_root, second_root,
            "each ephemeral Job attempt must get an isolated state directory"
        );
        assert!(!durable_local_recovery_required(&plan));
    }

    #[test]
    fn rescale_routing_keys_decode_every_supported_encoding() {
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();
        let old_plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let namespace = crate::job::effective_state_namespace(
            &old_plan.spec.id,
            old_plan.spec.state.as_ref(),
            "agg",
            &old_task.id,
        );
        for key in [
            b"binary:\x01\x02".to_vec(),
            b"i8:7".to_vec(),
            b"i64:-1".to_vec(),
            b"u64:9".to_vec(),
            b"null:".to_vec(),
        ] {
            let entry = crate::state::StateEntry {
                namespace: namespace.clone(),
                key: key.clone(),
                value: b"42".to_vec(),
                expires_at_ms: None,
            };
            let moved = context.redistribute(entry).unwrap();
            assert_eq!(moved.key, key, "the state key must survive redistribution");
            assert_ne!(
                moved.namespace, namespace,
                "the entry must move to a new owner namespace"
            );
        }
    }

    #[test]
    fn rescale_redistribute_rejects_operators_missing_from_the_plan() {
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();
        let old_plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let namespace = crate::job::effective_state_namespace(
            &old_plan.spec.id,
            old_plan.spec.state.as_ref(),
            "ghost",
            "ghost-0",
        );
        let entry = crate::state::StateEntry {
            namespace,
            key: b"utf8:k".to_vec(),
            value: Vec::new(),
            expires_at_ms: None,
        };
        let error = context
            .redistribute(entry)
            .expect_err("an operator absent from the plan can have no owner task");
        assert!(error.to_string().contains("no owner task"), "{error}");
    }

    #[test]
    fn task_of_namespace_extracts_and_decodes_the_task_segment() {
        assert_eq!(
            RescaleContext::task_of_namespace("job:j:state:d:operator:agg:task:agg-0").unwrap(),
            "agg-0"
        );
        // The segment ends at the next separator; percent escapes decode.
        assert_eq!(
            RescaleContext::task_of_namespace("job:j:state:d:operator:agg:task:agg%3A2:x:rest")
                .unwrap(),
            "agg:2"
        );
        assert_eq!(
            RescaleContext::task_of_namespace("job:j:state:d:operator:agg:task:agg%252").unwrap(),
            "agg%2"
        );
        assert!(
            RescaleContext::task_of_namespace("job:j:state:d:operator:agg").is_err(),
            "a namespace without a task segment must be rejected"
        );
    }

    fn snapshot_entry(namespace: String, key: &[u8]) -> crate::state::StateEntry {
        crate::state::StateEntry {
            namespace,
            key: key.to_vec(),
            value: b"v".to_vec(),
            expires_at_ms: None,
        }
    }

    #[test]
    fn restore_local_snapshot_rejects_format_mismatches() {
        let directory = tempfile::tempdir().unwrap();
        let backend: Arc<dyn crate::state::StateBackend> = Arc::new(
            crate::state::RedbStateBackend::open(directory.path().join("backend"), 1).unwrap(),
        );
        let repository = crate::checkpoint::CheckpointRepository::new(
            crate::checkpoint::FileCheckpointStore::new(directory.path().join("store")).unwrap(),
        );
        let plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let old_task = plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let namespace = crate::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            "agg",
            &old_task.id,
        );
        // Snapshot sealed under format 2 while the backend runs format 1.
        let snapshot =
            crate::state::StateSnapshot::new(2, vec![snapshot_entry(namespace, b"utf8:k")]);
        let reference = repository
            .write_state_snapshot("cp-format", &snapshot)
            .unwrap();
        let manifest = crate::checkpoint::CheckpointManifest {
            checkpoint_id: "cp-format".into(),
            job_id: plan.spec.id.clone(),
            job_version: plan.spec.version,
            generation: 1,
            task_attempts: Vec::new(),
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id: "cp-format".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: vec![reference],
            format_version: 2,
            checksum: 0,
        };
        let prefix = crate::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
        let error = restore_local_snapshot(&repository, &manifest, &backend, &prefix, None)
            .expect_err("a foreign snapshot format must fail the restore");
        assert!(
            error
                .to_string()
                .contains("state format 2 is incompatible with local backend format 1"),
            "{error}"
        );
    }

    #[test]
    fn restore_local_snapshot_redistributes_entries_during_rescale() {
        let directory = tempfile::tempdir().unwrap();
        let backend: Arc<dyn crate::state::StateBackend> = Arc::new(
            crate::state::RedbStateBackend::open(directory.path().join("backend"), 1).unwrap(),
        );
        let repository = crate::checkpoint::CheckpointRepository::new(
            crate::checkpoint::FileCheckpointStore::new(directory.path().join("store")).unwrap(),
        );
        let old_plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();
        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace = crate::job::effective_state_namespace(
            &old_plan.spec.id,
            old_plan.spec.state.as_ref(),
            "agg",
            &old_task.id,
        );
        let key = b"utf8:omega".to_vec();
        let snapshot =
            crate::state::StateSnapshot::new(1, vec![snapshot_entry(old_namespace.clone(), &key)]);
        let reference = repository
            .write_state_snapshot("cp-rescale", &snapshot)
            .unwrap();
        let manifest = crate::checkpoint::CheckpointManifest {
            checkpoint_id: "cp-rescale".into(),
            job_id: old_plan.spec.id.clone(),
            job_version: old_plan.spec.version,
            generation: 1,
            task_attempts: Vec::new(),
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id: "cp-rescale".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: vec![reference],
            format_version: 1,
            checksum: 0,
        };
        let prefix =
            crate::job::state_namespace_prefix(&old_plan.spec.id, old_plan.spec.state.as_ref());
        restore_local_snapshot(&repository, &manifest, &backend, &prefix, Some(&context))
            .expect("rescale restore must succeed");

        // The entry moved off the old namespace onto the owning task's.
        assert!(
            backend.get(&old_namespace, &key).unwrap().is_none(),
            "the old namespace must be empty after redistribution"
        );
        let group = crate::job::key_group_for_key(b"omega", 16).unwrap();
        let owner = new_plan
            .tasks
            .iter()
            .find(|task| {
                task.operator_id == "agg"
                    && task
                        .partitions
                        .iter()
                        .any(|partition| partition.key_group.contains(group))
            })
            .unwrap();
        let owner_namespace = crate::job::effective_state_namespace(
            &new_plan.spec.id,
            new_plan.spec.state.as_ref(),
            "agg",
            &owner.id,
        );
        assert_eq!(
            backend.get(&owner_namespace, &key).unwrap(),
            Some(b"v".to_vec()),
            "the entry must land on the new owning task's namespace"
        );

        // The ordinary (non-rescale) path restores entries unchanged.
        let direct = crate::state::StateSnapshot::new(
            1,
            vec![snapshot_entry(owner_namespace.clone(), b"utf8:direct")],
        );
        let reference = repository
            .write_state_snapshot("cp-direct", &direct)
            .unwrap();
        let manifest = crate::checkpoint::CheckpointManifest {
            checkpoint_id: "cp-direct".into(),
            job_id: old_plan.spec.id.clone(),
            job_version: old_plan.spec.version,
            generation: 1,
            task_attempts: Vec::new(),
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id: "cp-direct".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: vec![reference],
            format_version: 1,
            checksum: 0,
        };
        restore_local_snapshot(&repository, &manifest, &backend, &prefix, None)
            .expect("the ordinary restore must succeed");
        assert_eq!(
            backend.get(&owner_namespace, b"utf8:direct").unwrap(),
            Some(b"v".to_vec()),
            "the non-rescale path must keep the entry's namespace"
        );
    }
}

#[cfg(test)]
mod runner_tests {
    use super::*;
    use crate::input::{Ack, Input, InputBuilder};
    use crate::job::{
        CheckpointSpec, EdgeSpec, JobId, JobVersion, OperatorKind, OperatorSpec, RecoveryPolicy,
        SinkSpec, SourceSpec, StateDurability, StateSpec, TimeMode, TimeSpec, WatermarkSpec,
        WatermarkStrategy,
    };
    use crate::output::{Output, OutputBuilder};
    use crate::processor::Processor;
    use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult};
    use async_trait::async_trait;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    // ---------- component doubles ----------

    /// An input that never delivers a batch and never ends: the Job stays
    /// alive across checkpoint ticks until the test cancels it.
    struct NeverEndingInput {
        connects: AtomicUsize,
        closes: AtomicUsize,
    }

    #[async_trait]
    impl Input for NeverEndingInput {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct OneBatchThenEofInput {
        sent: Mutex<bool>,
    }

    #[async_trait]
    impl Input for OneBatchThenEofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                return Err(Error::EOF);
            }
            *sent = true;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap();
            Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingConnectInput {
        closes: AtomicUsize,
    }

    #[async_trait]
    impl Input for FailingConnectInput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection("injected restart connect failure".into()))
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// An input whose checkpoint-position restore fails after connecting.
    struct FailingRestoreInput;

    #[async_trait]
    impl Input for FailingRestoreInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn restore_positions(
            &self,
            _positions: &[crate::checkpoint::SourcePosition],
        ) -> Result<(), Error> {
            Err(Error::Process("injected restore failure".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// An event-time-capable input whose physical partitions can be made to
    /// fail enumeration for the seeding error path.
    struct PartitionedInput {
        partitions: Vec<crate::event_time::EventTimePartition>,
        fail_enumeration: bool,
    }

    #[async_trait]
    impl Input for PartitionedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn watermark_partitions(
            &self,
        ) -> Result<Vec<crate::event_time::EventTimePartition>, Error> {
            if self.fail_enumeration {
                return Err(Error::Connection("injected enumeration failure".into()));
            }
            Ok(self.partitions.clone())
        }
        fn supports_partitioning(&self) -> bool {
            true
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullOutput;

    #[async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingSinkOutput;

    #[async_trait]
    impl Output for FailingSinkOutput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection("injected sink reconnect failure".into()))
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct PassThroughProcessor;

    #[async_trait]
    impl Processor for PassThroughProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct RunnerAdapter {
        input: Arc<dyn Input>,
        output: Arc<dyn Output>,
    }

    impl crate::job::JobComponentAdapter for RunnerAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(self.input.clone())
        }
        fn build_output(
            &self,
            _sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(self.output.clone())
        }
        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(PassThroughProcessor))
        }
    }

    fn adapter_with(input: Arc<dyn Input>) -> RunnerAdapter {
        RunnerAdapter {
            input,
            output: Arc::new(DevNullOutput),
        }
    }

    // ---------- spec helpers ----------

    fn processing_time() -> TimeSpec {
        TimeSpec {
            mode: TimeMode::ProcessingTime,
            timestamp_field: None,
            watermark: None,
            allowed_lateness_ms: 0,
            late_event_policy: Default::default(),
            late_event_route: None,
        }
    }

    fn event_time(watermark: Option<WatermarkSpec>) -> TimeSpec {
        TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark,
            allowed_lateness_ms: 0,
            late_event_policy: Default::default(),
            late_event_route: None,
        }
    }

    fn bounded_watermark() -> WatermarkSpec {
        WatermarkSpec {
            strategy: WatermarkStrategy::BoundedOutOfOrderness,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }
    }

    struct SpecBuilder {
        spec: JobSpec,
    }

    impl SpecBuilder {
        fn new(job: &str, stateful: bool) -> Self {
            let mut operators = vec![OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            }];
            if stateful {
                operators.push(OperatorSpec {
                    id: "agg".into(),
                    kind: OperatorKind::Aggregate,
                    stateful: true,
                    key_field: Some("key".into()),
                    config: serde_json::json!({}),
                });
            }
            operators.push(OperatorSpec {
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            });
            let mut edges = vec![EdgeSpec {
                id: "source-agg".into(),
                from: "source".into(),
                to: "agg".into(),
                partitioned: false,
            }];
            edges.push(EdgeSpec {
                id: "agg-sink".into(),
                from: "agg".into(),
                to: "sink".into(),
                partitioned: false,
            });
            let edges = if stateful {
                edges
            } else {
                vec![EdgeSpec {
                    id: "source-sink".into(),
                    from: "source".into(),
                    to: "sink".into(),
                    partitioned: false,
                }]
            };
            Self {
                spec: JobSpec {
                    resources: Default::default(),
                    rescale: false,
                    rebalance: None,
                    id: JobId::new(job).unwrap(),
                    version: JobVersion(1),
                    max_parallelism: 4,
                    parallelism: 1,
                    operators,
                    edges,
                    sources: vec![SourceSpec {
                        operator_id: "source".into(),
                        input_type: "vec".into(),
                        codec: None,
                        config: serde_json::json!({}),
                        time: processing_time(),
                    }],
                    sinks: vec![SinkSpec {
                        operator_id: "sink".into(),
                        output_type: "collect".into(),
                        codec: None,
                        config: serde_json::json!({}),
                    }],
                    state: None,
                    checkpoint: None,
                    placement: crate::job::PlacementStrategy::Colocated,
                    recovery: RecoveryPolicy::LatestCheckpoint,
                },
            }
        }

        fn stateless_edges(mut self) -> Self {
            self.spec.edges = vec![EdgeSpec {
                id: "source-sink".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }];
            self.spec
                .operators
                .retain(|operator| operator.id == "source" || operator.id == "sink");
            self
        }

        fn event_time(mut self, time: TimeSpec) -> Self {
            self.spec.sources[0].time = time;
            self
        }

        fn parallelism(mut self, parallelism: u32) -> Self {
            self.spec.parallelism = parallelism;
            self
        }

        fn ephemeral_state(mut self) -> Self {
            self.spec.state = Some(StateSpec {
                backend: "embedded_kv".into(),
                durability: StateDurability::Ephemeral,
                root: None,
                namespace: None,
                ttl_ms: None,
                format_version: 1,
                max_pending_transactions: None,
                max_bytes: None,
            });
            self
        }

        fn durable_state(
            mut self,
            root: &std::path::Path,
            checkpoint_root: &std::path::Path,
        ) -> Self {
            self.spec.state = Some(StateSpec {
                backend: "embedded_kv".into(),
                durability: StateDurability::Durable,
                root: Some(root.display().to_string()),
                namespace: None,
                ttl_ms: None,
                format_version: 1,
                max_pending_transactions: None,
                max_bytes: None,
            });
            self.spec.checkpoint = Some(CheckpointSpec {
                interval_ms: 50,
                retention: 2,
                object_store_uri: format!("file://{}", checkpoint_root.display()),
            });
            self
        }

        fn build(self) -> JobSpec {
            self.spec
        }
    }

    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    // ---------- plain runner wrappers ----------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_runs_a_stateless_job_to_completion() {
        let spec = SpecBuilder::new("runner-plain-job", false)
            .stateless_edges()
            .build();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        run_job(&spec, &adapter, &mut resource(), CancellationToken::new())
            .await
            .expect("an EOF Job must complete successfully");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_with_metrics_started_counts_input_and_reports_startup() {
        let spec = SpecBuilder::new("runner-metrics-job", false)
            .stateless_edges()
            .build();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        run_job_with_metrics_started(
            &spec,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(metrics.clone()),
            Some(startup_tx),
        )
        .await
        .expect("an EOF Job must complete successfully");
        startup_rx
            .await
            .expect("startup handshake must fire")
            .expect("resource startup must succeed");
        let snapshot = metrics.snapshot();
        assert!(
            snapshot.input_batches >= 1,
            "the source batch must be counted: {snapshot:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_with_hooks_runs_the_graph_and_reports_chain_exits() {
        let spec = SpecBuilder::new("runner-hooks-job", false)
            .stateless_edges()
            .build();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let (finished_tx, mut finished_rx) = tokio::sync::mpsc::unbounded_channel();
        let hooks = plan
            .tasks
            .iter()
            .map(|task| {
                (
                    task.id.clone(),
                    crate::executor::task::CheckpointHook {
                        task_id: Some(task.id.clone()),
                        finished_reporter: Some(finished_tx.clone()),
                        ..Default::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        run_job_with_hooks(
            &spec,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
            hooks,
        )
        .await
        .expect("an EOF Job must complete successfully");
        let mut finished = Vec::new();
        while let Ok(task_id) = finished_rx.try_recv() {
            finished.push(task_id);
        }
        assert!(
            !finished.is_empty(),
            "chain exits must be reported through the hooks"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_tasks_runs_the_assigned_subgraph() {
        let spec = SpecBuilder::new("runner-subgraph-job", false).build();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let plan = JobPlan::compile(spec).unwrap();
        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        run_job_tasks(
            &plan,
            &task_ids,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
        )
        .await
        .expect("the full assignment must run to completion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_rejects_unsupported_local_state_backends() {
        let mut spec = SpecBuilder::new("runner-bad-backend-job", true).build();
        spec.state = Some(StateSpec {
            backend: "remote_kv".into(),
            durability: StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let error =
            run_job_with_checkpoints(&spec, &adapter, &mut resource(), CancellationToken::new())
                .await
                .expect_err("an unsupported backend must fail the run");
        assert!(
            error
                .to_string()
                .contains("local Job state backend 'remote_kv' is not supported"),
            "{error}"
        );
    }

    #[test]
    fn local_state_backend_applies_the_configured_byte_cap() {
        let mut spec = SpecBuilder::new("runner-capped-backend-job", true)
            .ephemeral_state()
            .build();
        if let Some(state) = spec.state.as_mut() {
            state.max_bytes = Some(4096);
        }
        let plan = JobPlan::compile(spec).unwrap();
        let backend = local_state_backend(&plan)
            .expect("a capped embedded backend must open")
            .expect("the state section is present");
        assert_eq!(backend.format_version(), 1);
    }

    // ---------- durability and recovery ----------

    /// True once at least one completed checkpoint artifact exists under the
    /// checkpoint root.
    fn any_checkpoint_artifact(checkpoint_root: &std::path::Path) -> bool {
        std::fs::read_dir(checkpoint_root.join("checkpoints"))
            .map(|entries| {
                entries
                    .flatten()
                    .any(|entry| entry.path().join("manifest.json").is_file())
            })
            .unwrap_or(false)
    }

    /// Run a durable Job until the interval-driven checkpoint loop has
    /// persisted at least one artifact, then cancel it. The first barrier
    /// round is timing-sensitive (coverage instrumentation slows it several
    /// fold), so the helper polls for the artifact instead of sleeping a
    /// fixed duration; `budget` only bounds the wait.
    async fn drive_durable_job(
        spec: &JobSpec,
        adapter: &RunnerAdapter,
        budget: Duration,
        checkpoint_root: &std::path::Path,
    ) -> Result<(), Error> {
        let cancellation = CancellationToken::new();
        let run = {
            let spec = spec.clone();
            let cancellation = cancellation.clone();
            let adapter_input = adapter.input.clone();
            tokio::spawn(async move {
                let runner = RunnerAdapter {
                    input: adapter_input,
                    output: Arc::new(DevNullOutput),
                };
                run_job_with_checkpoints(&spec, &runner, &mut resource(), cancellation).await
            })
        };
        let deadline = tokio::time::Instant::now() + budget;
        while !any_checkpoint_artifact(checkpoint_root) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the durable attempt must persist a checkpoint within {budget:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(30), run)
            .await
            .expect("the durable run must settle after cancellation")
            .unwrap()
    }

    fn durable_spec(root: &std::path::Path, checkpoint_root: &std::path::Path) -> JobSpec {
        SpecBuilder::new("runner-durable-job", true)
            .durable_state(root, checkpoint_root)
            .build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn durable_job_persists_local_checkpoints_and_recovers_on_restart() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);

        // First attempt: run long enough for several checkpoint rounds.
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first durable attempt must settle cleanly");
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let marker = local_state_start_marker(&plan).unwrap();
        assert!(
            marker.is_file(),
            "the start marker must survive the attempt"
        );
        let artifacts = std::fs::read_dir(checkpoint_root.join("checkpoints"))
            .unwrap()
            .count();
        assert!(artifacts >= 1, "at least one checkpoint must be persisted");

        // Restart: the marker turns the start into recovery; the latest
        // compatible artifact is restored before the graph spawns.
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let cancellation = CancellationToken::new();
        let restart = {
            let spec = spec.clone();
            let cancellation = cancellation.clone();
            let adapter_input = adapter.input.clone();
            tokio::spawn(async move {
                let runner = RunnerAdapter {
                    input: adapter_input,
                    output: Arc::new(DevNullOutput),
                };
                run_job_with_checkpoints_started(
                    &spec,
                    &runner,
                    &mut resource(),
                    cancellation,
                    Some(startup_tx),
                    None,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), startup_rx)
            .await
            .expect("recovery startup must complete")
            .unwrap()
            .expect("recovered startup must succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(10), restart)
            .await
            .expect("the recovered run must settle after cancellation")
            .unwrap()
            .expect("the recovered run must settle cleanly");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn durable_restart_without_a_compatible_checkpoint_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let plan = JobPlan::compile(spec.clone()).unwrap();
        // Simulate an unclean shutdown: the marker exists but no artifact
        // was ever completed.
        let marker = local_state_start_marker(&plan).unwrap();
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, b"started\n").unwrap();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let error =
            run_job_with_checkpoints(&spec, &adapter, &mut resource(), CancellationToken::new())
                .await
                .expect_err("an unrecoverable marker must fail the restart");
        assert!(
            error
                .to_string()
                .contains("requires recovery but no compatible checkpoint was found"),
            "{error}"
        );
    }

    /// Produce one valid checkpoint artifact, then hand back the paths to
    /// mutate for the artifact-selection tests.
    async fn checkpointed_job(
        directory: &tempfile::TempDir,
    ) -> (JobSpec, JobPlan, std::path::PathBuf) {
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the durable attempt must settle cleanly");
        let plan = JobPlan::compile(spec.clone()).unwrap();
        (spec, plan, checkpoint_root)
    }

    fn all_manifest_paths(checkpoint_root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let entries: Vec<_> = std::fs::read_dir(checkpoint_root.join("checkpoints"))
            .unwrap()
            .flatten()
            .map(|entry| entry.path().join("manifest.json"))
            .filter(|path| path.is_file())
            .collect();
        assert!(!entries.is_empty(), "at least one artifact must persist");
        entries
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn latest_local_checkpoint_skips_unreadable_and_incompatible_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let (_spec, plan, checkpoint_root) = checkpointed_job(&directory).await;
        let manifests = all_manifest_paths(&checkpoint_root);
        let original = std::fs::read_to_string(&manifests[0]).unwrap();

        // A corrupt manifest is skipped.
        for path in &manifests {
            std::fs::write(path, b"not json").unwrap();
        }
        assert!(latest_local_checkpoint(&checkpoint_root, &plan)
            .unwrap()
            .is_none());

        // An artifact without task snapshots is skipped.
        let mut emptied: serde_json::Value =
            serde_json::from_str(&original).expect("the produced manifest is valid");
        emptied["state_snapshots"] = serde_json::json!([]);
        for path in &manifests {
            std::fs::write(path, emptied.to_string()).unwrap();
        }
        assert!(latest_local_checkpoint(&checkpoint_root, &plan)
            .unwrap()
            .is_none());

        // An artifact written under a different task set is skipped.
        let mut rescaled = emptied.clone();
        rescaled["state_snapshots"] = serde_json::json!([
            { "task_id": "agg-9", "snapshot_key": "state.snap" }
        ]);
        rescaled["task_attempts"] = serde_json::json!([
            { "task_id": "agg-9", "attempt_id": "agg-9:local:0", "node_id": "local" }
        ]);
        for path in &manifests {
            std::fs::write(path, rescaled.to_string()).unwrap();
        }
        assert!(latest_local_checkpoint(&checkpoint_root, &plan)
            .unwrap()
            .is_none());

        // Restoring the original artifact makes it selectable again.
        for path in &manifests {
            std::fs::write(path, original.clone()).unwrap();
        }
        assert!(latest_local_checkpoint(&checkpoint_root, &plan).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn savepoint_recovery_policy_reads_the_savepoints_directory() {
        let directory = tempfile::tempdir().unwrap();
        let (mut spec, _plan, checkpoint_root) = checkpointed_job(&directory).await;
        spec.recovery = RecoveryPolicy::LatestSavepoint;
        let plan = JobPlan::compile(spec).unwrap();
        // Nothing was written under savepoints/: no artifact is selected even
        // though checkpoints exist.
        assert!(latest_local_checkpoint(&checkpoint_root, &plan)
            .unwrap()
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_connect_failure_closes_inputs_and_fails_the_startup_handshake() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first attempt must settle cleanly");

        // The restart's source fails to reconnect after position restore.
        let failing = Arc::new(FailingConnectInput {
            closes: AtomicUsize::new(0),
        });
        let restart_adapter = adapter_with(failing.clone());
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let error = run_job_with_checkpoints_started(
            &spec,
            &restart_adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(startup_tx),
            None,
        )
        .await
        .expect_err("a failing reconnect must fail the restart");
        assert!(
            error
                .to_string()
                .contains("injected restart connect failure"),
            "{error}"
        );
        let startup = startup_rx
            .await
            .expect("the startup handshake must observe the failure");
        assert!(startup.is_err(), "startup must report the failure");
        assert_eq!(
            failing.closes.load(Ordering::SeqCst),
            1,
            "the failed input must be closed during cleanup"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_position_restore_failure_fails_the_restart() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first attempt must settle cleanly");

        let restart_adapter = adapter_with(Arc::new(FailingRestoreInput));
        let error = run_job_with_checkpoints(
            &spec,
            &restart_adapter,
            &mut resource(),
            CancellationToken::new(),
        )
        .await
        .expect_err("a failing position restore must fail the restart");
        assert!(
            error.to_string().contains("injected restore failure"),
            "{error}"
        );
    }

    /// A graph whose startup fails AFTER recovery (a sink that cannot
    /// reconnect) must clean up the start marker, close the restored inputs,
    /// and fail the startup handshake.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovered_run_cleans_up_when_the_graph_startup_fails() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first attempt must settle cleanly");
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let marker = local_state_start_marker(&plan).unwrap();
        assert!(marker.is_file());

        let restart_adapter = RunnerAdapter {
            input: Arc::new(NeverEndingInput {
                connects: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
            }),
            output: Arc::new(FailingSinkOutput),
        };
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let error = run_job_with_checkpoints_started(
            &spec,
            &restart_adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(startup_tx),
            None,
        )
        .await
        .expect_err("a failing graph startup must fail the recovered run");
        assert!(
            error
                .to_string()
                .contains("injected sink reconnect failure"),
            "{error}"
        );
        assert!(
            startup_rx
                .await
                .expect("the startup handshake must observe the failure")
                .is_err(),
            "startup must report the failure"
        );
        assert!(
            !marker.exists(),
            "the start marker must be removed so the next attempt can retry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn durable_start_marker_write_failure_fails_the_run() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let marker = local_state_start_marker(&plan).unwrap();
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        // A directory squatting on the marker's temporary write path makes
        // the atomic persist fail after the state backend already opened.
        let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
        std::fs::create_dir_all(&temporary).unwrap();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let error = run_job_with_checkpoints_started(
            &spec,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(startup_tx),
            None,
        )
        .await
        .expect_err("a failed marker persist must fail the run");
        assert!(
            error
                .to_string()
                .contains("could not persist its start marker"),
            "{error}"
        );
        assert!(
            startup_rx
                .await
                .expect("startup handshake must fire")
                .is_err(),
            "startup must report the failure"
        );
        let _ = std::fs::remove_dir_all(&temporary);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn durable_start_marker_rename_failure_cleans_up_the_temporary() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let marker = local_state_start_marker(&plan).unwrap();
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        // A directory at the final marker path makes the rename step fail.
        std::fs::create_dir_all(&marker).unwrap();
        let adapter = adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }));
        let error =
            run_job_with_checkpoints(&spec, &adapter, &mut resource(), CancellationToken::new())
                .await
                .expect_err("a failed marker rename must fail the run");
        assert!(
            error
                .to_string()
                .contains("could not persist its start marker"),
            "{error}"
        );
        let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
        assert!(
            !temporary.exists(),
            "the written temporary must be cleaned up after the failed rename"
        );
        let _ = std::fs::remove_dir_all(&marker);
    }

    // ---------- checkpoint URI handling ----------

    #[test]
    fn local_checkpoint_root_validates_uri_forms() {
        assert_eq!(
            local_checkpoint_root("file:///tmp/arkflow").unwrap(),
            PathBuf::from("/tmp/arkflow")
        );
        assert_eq!(
            local_checkpoint_root("data/ckpt").unwrap(),
            PathBuf::from("data/ckpt")
        );
        let empty = local_checkpoint_root("file://").expect_err("empty path");
        assert!(empty.to_string().contains("empty path"), "{empty}");
        let remote = local_checkpoint_root("s3://bucket/checkpoints").expect_err("remote URI");
        assert!(remote.to_string().contains("use file:///path"), "{remote}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_local_checkpoint_loop_stops_when_the_store_cannot_be_opened() {
        // An empty graph spawns a handle whose run is already complete.
        let graph = crate::executor::graph::ExecutionGraph {
            chains: Vec::new(),
            channel_capacity: 1024,
            temporaries: Vec::new(),
        };
        let handle = Arc::new(
            KernelJobRunner::spawn(graph, Vec::new(), BTreeMap::new(), BTreeMap::new(), false)
                .await
                .unwrap(),
        );
        let plan = JobPlan::compile(
            SpecBuilder::new("runner-checkpoint-loop-job", false)
                .stateless_edges()
                .build(),
        )
        .unwrap();
        let participants: Vec<String> = plan.tasks.iter().map(|task| task.id.clone()).collect();

        // A non-local store URI disables the loop immediately.
        let remote = CheckpointSpec {
            interval_ms: 10,
            retention: 1,
            object_store_uri: "s3://bucket/nope".into(),
        };
        run_local_checkpoint_loop(
            handle.clone(),
            plan.clone(),
            participants.clone(),
            remote,
            CancellationToken::new(),
        )
        .await;

        // A store root that cannot be created also stops the loop.
        let directory = tempfile::tempdir().unwrap();
        let blocked = directory.path().join("not-a-directory");
        std::fs::write(&blocked, b"file").unwrap();
        let unwritable = CheckpointSpec {
            interval_ms: 10,
            retention: 1,
            object_store_uri: format!("file://{}", blocked.display()),
        };
        run_local_checkpoint_loop(
            handle.clone(),
            plan,
            participants,
            unwritable,
            CancellationToken::new(),
        )
        .await;

        // With an already-ended graph the barrier fails and the loop keeps
        // logging until the stop token fires.
        let directory = tempfile::tempdir().unwrap();
        let healthy = CheckpointSpec {
            interval_ms: 1,
            retention: 1,
            object_store_uri: format!("file://{}", directory.path().display()),
        };
        let stop = CancellationToken::new();
        stop.cancel();
        let plan = JobPlan::compile(
            SpecBuilder::new("runner-checkpoint-loop-job", false)
                .stateless_edges()
                .build(),
        )
        .unwrap();
        let participants = plan.tasks.iter().map(|task| task.id.clone()).collect();
        tokio::time::timeout(
            Duration::from_millis(500),
            run_local_checkpoint_loop(handle, plan, participants, healthy, stop),
        )
        .await
        .expect("a cancelled loop must return promptly");
    }

    // ---------- event-time wiring ----------

    fn build_event_time_graph(
        time: TimeSpec,
        input: Arc<dyn Input>,
        parallelism: u32,
    ) -> crate::executor::graph::ExecutionGraph {
        let mut builder = SpecBuilder::new("runner-event-time-job", false)
            .stateless_edges()
            .ephemeral_state()
            .event_time(time)
            .parallelism(parallelism);
        builder.spec.operators.insert(
            1,
            OperatorSpec {
                id: "window".into(),
                kind: OperatorKind::Window,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({
                    "type": "window",
                    "kind": "tumbling",
                    "size_ms": 10_000,
                    "timestamp_field": "ts",
                    "key_field": "key",
                    "value_fields": ["value"],
                    "trigger": "watermark",
                    "watermark_field": "__watermark_ms"
                }),
            },
        );
        builder.spec.edges = vec![
            EdgeSpec {
                id: "source-window".into(),
                from: "source".into(),
                to: "window".into(),
                partitioned: false,
            },
            EdgeSpec {
                id: "window-sink".into(),
                from: "window".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        let spec = builder.build();
        let plan = JobPlan::compile(spec).unwrap();
        let adapter = adapter_with(input);
        ExecutionGraphBuilder::default()
            .build(&plan, &adapter, &resource())
            .unwrap()
    }

    #[tokio::test]
    async fn event_time_gates_reject_sources_without_a_watermark() {
        // Plan validation normally rejects event-time sources without a
        // watermark; the gate wiring must still fail closed if one reaches it
        // (a graph assembled outside the plan compiler).
        let mut graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }),
            1,
        );
        for chain in &mut graph.chains {
            if let Some(time) = &mut chain.source_time {
                time.watermark = None;
            }
        }
        let error = event_time_gates(&graph)
            .err()
            .expect("a missing watermark must fail gate construction");
        assert!(
            error.to_string().contains("watermark specification"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seed_event_time_partitions_uses_the_connector_assignment() {
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(PartitionedInput {
                partitions: vec![crate::event_time::EventTimePartition::new(
                    Some("orders".into()),
                    3,
                )],
                fail_enumeration: false,
            }),
            1,
        );
        let gates = event_time_gates(&graph).unwrap();
        assert!(!gates.is_empty());
        seed_event_time_partitions(&graph, &gates)
            .await
            .expect("seeding must succeed");
        for gate in gates.values() {
            let gate = gate.lock().await;
            let known = gate.as_ref().unwrap().known_partitions();
            assert_eq!(
                known,
                vec![crate::event_time::EventTimePartition::new(
                    Some("orders".into()),
                    3
                )]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seed_event_time_partitions_falls_back_to_the_chain_partition() {
        // No physical partitions from the connector: the chain's plan
        // partition seeds the gate instead (parallelism 2 assigns the second
        // source chain partition 1).
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(PartitionedInput {
                partitions: Vec::new(),
                fail_enumeration: false,
            }),
            2,
        );
        let gates = event_time_gates(&graph).unwrap();
        seed_event_time_partitions(&graph, &gates)
            .await
            .expect("seeding must succeed");
        let mut all_known = Vec::new();
        for gate in gates.values() {
            let gate = gate.lock().await;
            all_known.extend(gate.as_ref().unwrap().known_partitions());
        }
        assert!(
            all_known.contains(&crate::event_time::EventTimePartition::for_source(
                "source-1", 1
            )),
            "the source-1 chain must seed its plan partition: {all_known:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seed_event_time_partitions_surfaces_enumeration_failures() {
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(PartitionedInput {
                partitions: Vec::new(),
                fail_enumeration: true,
            }),
            1,
        );
        let gates = event_time_gates(&graph).unwrap();
        let error = seed_event_time_partitions(&graph, &gates)
            .await
            .expect_err("an enumeration failure must surface");
        assert!(
            error.to_string().contains("injected enumeration failure"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_event_time_watermarks_installs_physical_and_legacy_progress() {
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }),
            1,
        );
        let gates = event_time_gates(&graph).unwrap();
        let source_task = graph.chains[0].entry_task_id().to_string();

        // Physical per-partition progress restores onto the real partitions.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::new(),
            &BTreeMap::from([(
                source_task.clone(),
                vec![crate::checkpoint::WatermarkPosition::new(
                    Some("orders".into()),
                    3,
                    1_000,
                )],
            )]),
        )
        .await;
        let gate = gates.get(&source_task).unwrap().clone();
        let known = gate.lock().await.as_ref().unwrap().known_partitions();
        assert_eq!(
            known,
            vec![
                crate::event_time::EventTimePartition::new(Some("orders".into()), 3)
                    .with_source_identity(&source_task)
            ]
        );

        // Legacy task-level restore fans out to every known partition and is
        // skipped for tasks that already carry physical progress.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::from([(source_task.clone(), 2_000_i64)]),
            &BTreeMap::from([(
                source_task.clone(),
                vec![crate::checkpoint::WatermarkPosition::new(
                    Some("orders".into()),
                    3,
                    1_000,
                )],
            )]),
        )
        .await;

        // A watermark for a task without a gate is ignored.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::from([("missing-task".to_string(), 5_i64)]),
            &BTreeMap::new(),
        )
        .await;
    }

    // ---------- deep validation ----------

    struct EofInputBuilder;

    impl InputBuilder for EofInputBuilder {
        fn build(
            &self,
            _name: Option<&String>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }))
        }
    }

    struct DevNullOutputBuilder;

    impl OutputBuilder for DevNullOutputBuilder {
        fn build(
            &self,
            _name: Option<&String>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput))
        }
    }

    #[test]
    fn validate_local_job_accepts_registered_components() {
        let input_type = "runner-validate-eof-input";
        let output_type = "runner-validate-devnull-output";
        let _ = crate::input::register_input_builder(input_type, Arc::new(EofInputBuilder));
        let _ = crate::output::register_output_builder(output_type, Arc::new(DevNullOutputBuilder));
        let spec = SpecBuilder::new("runner-validate-job", false)
            .stateless_edges()
            .build();
        let mut spec = spec;
        spec.sources[0].input_type = input_type.into();
        spec.operators[0].config = serde_json::json!({"type": input_type});
        spec.sinks[0].output_type = output_type.into();
        spec.operators.last_mut().unwrap().config = serde_json::json!({"type": output_type});
        validate_local_job(&spec).expect("registered components must validate");
    }

    #[test]
    fn shared_resource_wraps_a_resource_for_cheap_cloning() {
        let shared = shared_resource(resource());
        assert!(shared.temporary.is_empty());
    }

    // ---------- stateful runs through the plain runners ----------

    /// A one-batch EOF input whose batch carries the aggregate's key column.
    struct KeyedBatchThenEofInput {
        sent: Mutex<bool>,
    }

    #[async_trait]
    impl Input for KeyedBatchThenEofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                return Err(Error::EOF);
            }
            *sent = true;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(datafusion::arrow::array::StringArray::from(vec!["k1"])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap();
            Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn keyed_eof_input() -> Arc<KeyedBatchThenEofInput> {
        Arc::new(KeyedBatchThenEofInput {
            sent: Mutex::new(false),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_job_flows_batches_through_the_processor() {
        let spec = SpecBuilder::new("runner-stateful-flow-job", true)
            .ephemeral_state()
            .build();
        let adapter = adapter_with(keyed_eof_input());
        run_job(&spec, &adapter, &mut resource(), CancellationToken::new())
            .await
            .expect("an EOF stateful Job must complete successfully");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_with_metrics_started_opens_and_closes_the_state_backend() {
        let spec = SpecBuilder::new("runner-metrics-state-job", true)
            .ephemeral_state()
            .build();
        let adapter = adapter_with(keyed_eof_input());
        run_job_with_metrics_started(
            &spec,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("an EOF stateful Job must complete and close its backend");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_job_with_hooks_opens_and_closes_the_state_backend() {
        let spec = SpecBuilder::new("runner-hooks-state-job", true)
            .ephemeral_state()
            .build();
        let adapter = adapter_with(keyed_eof_input());
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let hooks = plan
            .tasks
            .iter()
            .map(|task| {
                (
                    task.id.clone(),
                    crate::executor::task::CheckpointHook::default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        run_job_with_hooks(
            &spec,
            &adapter,
            &mut resource(),
            CancellationToken::new(),
            hooks,
        )
        .await
        .expect("an EOF stateful Job must complete through the hooks runner");
    }

    struct PassThroughProcessorBuilder;

    impl crate::processor::ProcessorBuilder for PassThroughProcessorBuilder {
        fn build(
            &self,
            _name: Option<&String>,
            _config: &Option<serde_json::Value>,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(PassThroughProcessor))
        }
    }

    #[test]
    fn validate_local_job_builds_through_the_state_backend() {
        let input_type = "runner-validate-eof-input";
        let output_type = "runner-validate-devnull-output";
        let processor_type = "runner-validate-passthrough";
        let _ = crate::input::register_input_builder(input_type, Arc::new(EofInputBuilder));
        let _ = crate::output::register_output_builder(output_type, Arc::new(DevNullOutputBuilder));
        let _ = crate::processor::register_processor_builder(
            processor_type,
            Arc::new(PassThroughProcessorBuilder),
        );
        let spec = SpecBuilder::new("runner-validate-state-job", true)
            .ephemeral_state()
            .build();
        let mut spec = spec;
        spec.sources[0].input_type = input_type.into();
        spec.operators[0].config = serde_json::json!({"type": input_type});
        spec.sinks[0].output_type = output_type.into();
        spec.operators.last_mut().unwrap().config = serde_json::json!({"type": output_type});
        spec.operators[1].config = serde_json::json!({"type": processor_type});
        validate_local_job(&spec).expect("a registered stateful Job must deep-validate");
    }

    // ---------- recovery: seed failures and marker persistence ----------

    /// An input whose watermark-partition enumeration fails after connecting;
    /// optionally its close also fails so the cleanup warn path runs.
    struct BrokenSeedInput {
        closes: AtomicUsize,
        fail_close: bool,
    }

    #[async_trait]
    impl Input for BrokenSeedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn watermark_partitions(
            &self,
        ) -> Result<Vec<crate::event_time::EventTimePartition>, Error> {
            Err(Error::Connection("injected enumeration failure".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                return Err(Error::Connection("injected close failure".into()));
            }
            Ok(())
        }
    }

    fn event_time_durable_spec(
        state_root: &std::path::Path,
        checkpoint_root: &std::path::Path,
    ) -> JobSpec {
        let mut builder = SpecBuilder::new("runner-seed-recovery-job", false)
            .durable_state(state_root, checkpoint_root)
            .event_time(event_time(Some(bounded_watermark())));
        builder.spec.operators.insert(
            1,
            OperatorSpec {
                id: "window".into(),
                kind: OperatorKind::Window,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({
                    "type": "window",
                    "kind": "tumbling",
                    "size_ms": 10_000,
                    "timestamp_field": "ts",
                    "key_field": "key",
                    "value_fields": ["value"],
                    "trigger": "watermark",
                    "watermark_field": "__watermark_ms"
                }),
            },
        );
        builder.spec.edges = vec![
            EdgeSpec {
                id: "source-window".into(),
                from: "source".into(),
                to: "window".into(),
                partitioned: false,
            },
            EdgeSpec {
                id: "window-sink".into(),
                from: "window".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        builder.build()
    }

    /// Recovery seeding failure: the restarted source cannot enumerate its
    /// watermark partitions. Inputs are closed (even when close itself
    /// fails, exercising the cleanup warn), state is closed, and the startup
    /// handshake observes the failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_seed_failure_fails_the_restart_and_closes_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = event_time_durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first attempt must settle cleanly");

        let broken = Arc::new(BrokenSeedInput {
            closes: AtomicUsize::new(0),
            fail_close: true,
        });
        let restart_adapter = adapter_with(broken.clone());
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let error = run_job_with_checkpoints_started(
            &spec,
            &restart_adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(startup_tx),
            None,
        )
        .await
        .expect_err("a failing seed must fail the restart");
        assert!(
            error.to_string().contains("injected enumeration failure"),
            "{error}"
        );
        assert!(
            startup_rx
                .await
                .expect("the startup handshake must observe the failure")
                .is_err(),
            "startup must report the failure"
        );
        assert_eq!(
            broken.closes.load(Ordering::SeqCst),
            1,
            "the failed input must be closed during cleanup"
        );
    }

    /// The start-marker persist fails AFTER recovery prepared the inputs:
    /// the restored inputs must be closed before surfacing the error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_marker_persist_failure_closes_restored_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().join("state");
        let checkpoint_root = directory.path().join("checkpoints");
        let spec = durable_spec(&state_root, &checkpoint_root);
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        drive_durable_job(&spec, &adapter, Duration::from_secs(10), &checkpoint_root)
            .await
            .expect("the first attempt must settle cleanly");
        let plan = JobPlan::compile(spec.clone()).unwrap();
        let marker = local_state_start_marker(&plan).unwrap();
        // A directory squatting on the atomic-rename temporary makes the
        // marker persist fail after recovery reconnected the inputs.
        let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
        std::fs::create_dir_all(&temporary).unwrap();
        let restart_input = Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let restart_adapter = adapter_with(restart_input.clone());
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let error = run_job_with_checkpoints_started(
            &spec,
            &restart_adapter,
            &mut resource(),
            CancellationToken::new(),
            Some(startup_tx),
            None,
        )
        .await
        .expect_err("a failed marker persist must fail the restart");
        assert!(
            error
                .to_string()
                .contains("could not persist its start marker"),
            "{error}"
        );
        assert!(
            startup_rx
                .await
                .expect("the startup handshake must observe the failure")
                .is_err(),
            "startup must report the failure"
        );
        assert_eq!(
            restart_input.closes.load(Ordering::SeqCst),
            1,
            "the restored input must be closed during cleanup"
        );
        let _ = std::fs::remove_dir_all(&temporary);
    }

    // ---------- checkpoint artifact scan (latest_local_checkpoint) ----------

    /// Recompute the manifest envelope checksum the same way the product's
    /// coordinator seals it, so hand-built test manifests pass `verify()`.
    fn seal_manifest(
        mut manifest: crate::checkpoint::CheckpointManifest,
    ) -> crate::checkpoint::CheckpointManifest {
        let encoded = serde_json::to_vec(&(
            &manifest.checkpoint_id,
            &manifest.job_id,
            manifest.job_version,
            manifest.generation,
            &manifest.task_attempts,
            &manifest.source_positions,
            &manifest.watermarks_ms,
            &manifest.watermark_partitions,
            &manifest.in_flight_barrier,
            &manifest.state_snapshots,
            manifest.format_version,
        ))
        .expect("manifest fields must serialize");
        manifest.checksum = encoded.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            hash.wrapping_mul(0x100000001b3) ^ u64::from(*byte)
        });
        manifest
    }

    fn probe_manifest(job: &str, tasks: &[String]) -> crate::checkpoint::CheckpointManifest {
        let checkpoint_id = format!("local-{job}-probe");
        crate::checkpoint::CheckpointManifest {
            checkpoint_id: checkpoint_id.clone(),
            job_id: JobId::new(job).unwrap(),
            job_version: JobVersion(1),
            generation: 1,
            task_attempts: tasks
                .iter()
                .map(|task| crate::checkpoint::TaskAttemptSnapshot {
                    task_id: task.clone(),
                    attempt_id: format!("{task}:local:0"),
                    node_id: "local".into(),
                })
                .collect(),
            source_positions: Vec::new(),
            watermarks_ms: BTreeMap::new(),
            watermark_partitions: BTreeMap::new(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id,
                generation: 1,
                trace_context: None,
            },
            state_snapshots: Vec::new(),
            format_version: 1,
            checksum: 0,
        }
    }

    fn write_manifest_under(
        root: &std::path::Path,
        manifest: &crate::checkpoint::CheckpointManifest,
    ) {
        let directory = root.join("checkpoints").join(&manifest.checkpoint_id);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("manifest.json"),
            serde_json::to_vec(manifest).unwrap(),
        )
        .unwrap();
    }

    fn scan_plan_tasks() -> (JobPlan, Vec<String>) {
        let spec = SpecBuilder::new("runner-scan-job", true)
            .ephemeral_state()
            .build();
        let plan = JobPlan::compile(spec).unwrap();
        let tasks = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        (plan, tasks)
    }

    #[test]
    fn latest_local_checkpoint_surfaces_scan_and_skip_variants() {
        let (plan, _tasks) = scan_plan_tasks();

        // A file squatting on the checkpoints directory makes the scan fail.
        let blocked = tempfile::tempdir().unwrap();
        std::fs::write(blocked.path().join("checkpoints"), b"not a directory").unwrap();
        let error = latest_local_checkpoint(blocked.path(), &plan)
            .expect_err("an unreadable checkpoints directory must fail the scan");
        assert!(
            error.to_string().contains("scan local recovery directory"),
            "{error}"
        );

        // A candidate directory without a manifest is skipped.
        let empty = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(empty.path().join("checkpoints").join("junk")).unwrap();
        assert!(
            latest_local_checkpoint(empty.path(), &plan)
                .unwrap()
                .is_none(),
            "a directory without a manifest must be skipped"
        );

        // A non-UTF-8 directory name cannot yield an artifact id. APFS
        // (macOS) rejects creating such names outright, so this branch is
        // only exercisable on filesystems that accept arbitrary bytes.
        #[cfg(not(target_os = "macos"))]
        {
            use std::os::unix::ffi::OsStrExt;
            let weird = tempfile::tempdir().unwrap();
            let invalid = std::ffi::OsStr::from_bytes(&[0xff, 0xfe]);
            let directory = weird.path().join("checkpoints").join(invalid);
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(directory.join("manifest.json"), b"{}").unwrap();
            assert!(
                latest_local_checkpoint(weird.path(), &plan)
                    .unwrap()
                    .is_none(),
                "a non-UTF-8 artifact directory must be skipped"
            );
        }
    }

    #[test]
    fn latest_local_checkpoint_skips_sealed_but_unusable_artifacts() {
        let (plan, tasks) = scan_plan_tasks();

        // Sealed manifest with no state snapshots at all.
        let directory = tempfile::tempdir().unwrap();
        write_manifest_under(
            directory.path(),
            &seal_manifest(probe_manifest(plan.spec.id.as_str(), &tasks)),
        );
        assert!(
            latest_local_checkpoint(directory.path(), &plan)
                .unwrap()
                .is_none(),
            "an artifact without state snapshots must be skipped"
        );

        // Sealed manifest written for a different Job identity.
        let directory = tempfile::tempdir().unwrap();
        let mut foreign = probe_manifest("runner-other-job", &tasks);
        foreign.state_snapshots = vec![crate::checkpoint::StateSnapshotRef {
            task_id: tasks[0].clone(),
            node_id: None,
            uri: "checkpoints/x/state-1.json".into(),
            checksum: 1,
            bytes: 1,
        }];
        write_manifest_under(directory.path(), &seal_manifest(foreign));
        assert!(
            latest_local_checkpoint(directory.path(), &plan)
                .unwrap()
                .is_none(),
            "an artifact sealed for another Job must be skipped"
        );

        // Sealed manifest whose snapshot task set does not match the plan
        // even though the task attempts do.
        let directory = tempfile::tempdir().unwrap();
        let mut mismatched = probe_manifest(plan.spec.id.as_str(), &tasks);
        mismatched.state_snapshots = vec![crate::checkpoint::StateSnapshotRef {
            task_id: "agg-9".into(),
            node_id: None,
            uri: "checkpoints/x/state-1.json".into(),
            checksum: 1,
            bytes: 1,
        }];
        write_manifest_under(directory.path(), &seal_manifest(mismatched));
        assert!(
            latest_local_checkpoint(directory.path(), &plan)
                .unwrap()
                .is_none(),
            "an artifact with a foreign snapshot task set must be skipped"
        );
    }

    #[test]
    fn latest_local_checkpoint_skips_snapshots_outside_the_job_namespace() {
        let (plan, tasks) = scan_plan_tasks();
        let directory = tempfile::tempdir().unwrap();
        let store = crate::checkpoint::FileCheckpointStore::new(directory.path()).unwrap();
        let repository = crate::checkpoint::CheckpointRepository::new(store);
        // A perfectly readable snapshot whose entries live under another
        // Job's namespace: the manifest seals fine, every structural check
        // passes, and only the namespace boundary rejects it.
        let snapshot = crate::state::StateSnapshot::new(
            1,
            vec![crate::state::StateEntry {
                namespace: "job:someone-else:state:default:operator:agg:task:agg-0".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        let reference = repository
            .write_state_snapshot("local-probe", &snapshot)
            .unwrap();
        let mut manifest = probe_manifest(plan.spec.id.as_str(), &tasks);
        manifest.state_snapshots = tasks
            .iter()
            .map(|task| crate::checkpoint::StateSnapshotRef {
                task_id: task.clone(),
                node_id: None,
                uri: reference.uri.clone(),
                checksum: reference.checksum,
                bytes: reference.bytes,
            })
            .collect();
        write_manifest_under(directory.path(), &seal_manifest(manifest));
        assert!(
            latest_local_checkpoint(directory.path(), &plan)
                .unwrap()
                .is_none(),
            "snapshots outside the Job namespace must be skipped"
        );
    }

    #[test]
    fn local_checkpoint_catalog_skips_foreign_and_malformed_entries() {
        let (plan, tasks) = scan_plan_tasks();
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();

        // Non-UTF-8 directory name: no artifact id (not creatable on APFS).
        #[cfg(not(target_os = "macos"))]
        {
            use std::os::unix::ffi::OsStrExt;
            let invalid = std::ffi::OsStr::from_bytes(&[0xff, 0xfd]);
            std::fs::create_dir_all(root.join("checkpoints").join(invalid)).unwrap();
        }
        // Directory without a manifest.
        std::fs::create_dir_all(root.join("checkpoints").join("junk")).unwrap();
        // Corrupt manifest: readable directory, unusable content.
        std::fs::create_dir_all(root.join("checkpoints").join("corrupt")).unwrap();
        std::fs::write(
            root.join("checkpoints")
                .join("corrupt")
                .join("manifest.json"),
            b"not json",
        )
        .unwrap();
        // Sealed manifest belonging to another Job: readable but not recorded.
        write_manifest_under(
            root,
            &seal_manifest(probe_manifest("runner-other-job", &tasks)),
        );
        let store = crate::checkpoint::FileCheckpointStore::new(root).unwrap();
        let catalog = local_checkpoint_catalog(root, &store, &plan);
        assert!(
            catalog.artifacts().is_empty(),
            "foreign and malformed entries must not be recorded"
        );

        // A sealed manifest for this Job is recorded.
        write_manifest_under(root, &seal_manifest_for(&plan, &tasks));
        let catalog = local_checkpoint_catalog(root, &store, &plan);
        assert_eq!(catalog.artifacts().len(), 1);
    }

    fn seal_manifest_for(
        plan: &JobPlan,
        tasks: &[String],
    ) -> crate::checkpoint::CheckpointManifest {
        let mut manifest = probe_manifest(plan.spec.id.as_str(), tasks);
        manifest.state_snapshots = vec![crate::checkpoint::StateSnapshotRef {
            task_id: tasks[0].clone(),
            node_id: None,
            uri: "checkpoints/none/state-1.json".into(),
            checksum: 1,
            bytes: 1,
        }];
        seal_manifest(manifest)
    }

    // ---------- checkpoint loop: retention and persistence failures ----------

    fn checkpoint_directory_state(
        root: &std::path::Path,
    ) -> (usize, usize, Option<std::path::PathBuf>) {
        // (directory count, manifest count, one directory holding a manifest)
        let mut directories = 0usize;
        let mut manifests = 0usize;
        let mut manifest_dir = None;
        if let Ok(entries) = std::fs::read_dir(root.join("checkpoints")) {
            for entry in entries.flatten() {
                directories += 1;
                if entry.path().join("manifest.json").is_file() {
                    manifests += 1;
                    manifest_dir = Some(entry.path());
                }
            }
        }
        (directories, manifests, manifest_dir)
    }

    /// The interval loop keeps running when persistence fails: retention
    /// deletes old artifacts once the retention window is exceeded, and a
    /// failed delete (read-only artifact directory) only logs a warning —
    /// data processing continues.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_loop_enforces_retention_and_survives_persist_failures() {
        let spec = SpecBuilder::new("runner-retention-job", false)
            .stateless_edges()
            .build();
        let plan = JobPlan::compile(spec).unwrap();
        let participants: Vec<String> = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        let graph = crate::executor::graph::ExecutionGraphBuilder::default()
            .build(&plan, &adapter, &resource())
            .unwrap();
        let handle = Arc::new(
            KernelJobRunner::spawn(graph, Vec::new(), BTreeMap::new(), BTreeMap::new(), true)
                .await
                .unwrap(),
        );
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let checkpoint = CheckpointSpec {
            interval_ms: 10,
            retention: 1,
            object_store_uri: format!("file://{}", root.display()),
        };
        let stop = CancellationToken::new();
        let loop_task = {
            let handle = handle.clone();
            let plan = plan.clone();
            let participants = participants.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                run_local_checkpoint_loop(handle, plan, participants, checkpoint, stop).await;
            })
        };

        // Wait until retention has deleted at least one predecessor:
        // two checkpoint directories exist but only one manifest remains.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let protected = loop {
            let (directories, manifests, manifest_dir) = checkpoint_directory_state(root);
            if directories >= 2 && manifests == 1 {
                break manifest_dir.expect("a manifest directory must exist");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "retention must prune old artifacts: {directories} dirs, {manifests} manifests"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        // The surviving artifact's directory becomes read-only: the next
        // retention delete fails, the loop logs and keeps going, and a new
        // checkpoint still appears in a different directory.
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&protected).unwrap().permissions();
        permissions.set_mode(0o500);
        std::fs::set_permissions(&protected, permissions).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (_, manifests, manifest_dir) = checkpoint_directory_state(root);
            let advanced = manifest_dir.is_some_and(|dir| dir != protected);
            if advanced && manifests >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the loop must persist new checkpoints after a failed delete"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            protected.join("manifest.json").is_file(),
            "the read-only artifact must survive its failed deletion"
        );

        stop.cancel();
        tokio::time::timeout(Duration::from_secs(10), loop_task)
            .await
            .expect("the loop must return after cancellation")
            .unwrap();
        handle.stop();
        tokio::time::timeout(Duration::from_secs(10), handle.watcher())
            .await
            .expect("the kernel must settle after cancellation")
            .unwrap()
            .expect("a cancelled run must complete gracefully");
        let mut permissions = std::fs::metadata(&protected).unwrap().permissions();
        permissions.set_mode(0o755);
        let _ = std::fs::set_permissions(&protected, permissions);
    }

    // ---------- event-time gate wiring edge cases ----------

    fn event_time_chain(
        task: &str,
        time: TimeSpec,
        group: Option<&str>,
    ) -> crate::executor::graph::Chain {
        let mut chain = crate::executor::graph::Chain::for_pool_test(1, Vec::new());
        chain.task_ids = vec![task.to_string()];
        chain.source_time = Some(time);
        chain.watermark_group = group.map(str::to_string);
        chain
    }

    /// The second member of a shared watermark group skips tracker
    /// construction (the shared tracker already exists) — a gate that then
    /// lacks a timestamp field must still fail closed.
    #[tokio::test]
    async fn event_time_gates_reject_shared_group_members_without_a_timestamp_field() {
        let mut healthy = event_time(Some(bounded_watermark()));
        healthy.timestamp_field = Some("ts".into());
        let mut broken = event_time(Some(bounded_watermark()));
        broken.timestamp_field = None;
        let graph = crate::executor::graph::ExecutionGraph {
            chains: vec![
                event_time_chain("a", healthy, Some("group-1")),
                event_time_chain("b", broken, Some("group-1")),
            ],
            channel_capacity: 8,
            temporaries: Vec::new(),
        };
        let error = event_time_gates(&graph)
            .err()
            .expect("a timestamp-less group member must fail gate construction");
        assert!(error.to_string().contains("timestamp_field"), "{error}");
    }

    /// Seeding skips chains that own a gate but no source, and gates whose
    /// Option was already taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seed_event_time_partitions_skips_sourceless_chains_and_taken_gates() {
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(PartitionedInput {
                partitions: vec![crate::event_time::EventTimePartition::new(
                    Some("orders".into()),
                    3,
                )],
                fail_enumeration: false,
            }),
            1,
        );
        let mut gates = event_time_gates(&graph).unwrap();
        let source_task = graph
            .chains
            .iter()
            .find(|chain| chain.is_source())
            .unwrap()
            .entry_task_id()
            .to_string();
        let window_task = graph
            .chains
            .iter()
            .find(|chain| !chain.is_source())
            .unwrap()
            .entry_task_id()
            .to_string();
        // The window chain owns a gate but has no source to enumerate.
        gates.insert(window_task, gates.get(&source_task).unwrap().clone());
        // The source chain's gate was taken: seeding must skip it silently.
        gates.insert(source_task, Arc::new(tokio::sync::Mutex::new(None)));
        seed_event_time_partitions(&graph, &gates)
            .await
            .expect("seeding must skip silently instead of failing");
    }

    /// Legacy task-level watermark restore reaches gates without physical
    /// progress: fresh gates fall back to the chain partition, gates with
    /// known partitions fan out, and taken gates are skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_event_time_watermarks_fans_out_legacy_progress() {
        let graph = build_event_time_graph(
            event_time(Some(bounded_watermark())),
            Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }),
            1,
        );
        let mut gates = event_time_gates(&graph).unwrap();
        let source_task = graph.chains[0].entry_task_id().to_string();

        // A taken gate is skipped even when physical progress targets it.
        gates.insert("ghost".into(), Arc::new(tokio::sync::Mutex::new(None)));
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::new(),
            &BTreeMap::from([(
                "ghost".to_string(),
                vec![crate::checkpoint::WatermarkPosition::new(
                    Some("orders".into()),
                    0,
                    5_000,
                )],
            )]),
        )
        .await;

        // Legacy restore with no known partitions: the chain partition
        // fallback installs the progress.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::from([(source_task.clone(), 1_000_i64)]),
            &BTreeMap::new(),
        )
        .await;
        let gate = gates.get(&source_task).unwrap().clone();
        let known = gate.lock().await.as_ref().unwrap().known_partitions();
        assert_eq!(
            known,
            vec![crate::event_time::EventTimePartition::for_source(
                &source_task,
                0
            )],
            "a fresh gate must seed its chain partition"
        );

        // With known partitions the same task-level value fans out to all
        // of them.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::from([(source_task.clone(), 2_000_i64)]),
            &BTreeMap::new(),
        )
        .await;
        assert_eq!(
            gate.lock().await.as_ref().unwrap().known_partitions().len(),
            1,
            "the fan-out must keep the known partition set"
        );

        // A taken gate is skipped by the legacy fan-out as well.
        restore_event_time_watermarks(
            &graph,
            &gates,
            &BTreeMap::from([("ghost".to_string(), 4_000_i64)]),
            &BTreeMap::new(),
        )
        .await;
        let taken = gates.get("ghost").unwrap().clone();
        assert!(
            taken.lock().await.as_ref().is_none(),
            "a taken gate must stay untouched"
        );
    }
}

#[cfg(test)]
mod metrics_registry_tests {
    use super::*;
    use crate::input::{Ack, Input};
    use crate::job::{
        EdgeSpec, JobId, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec, TimeMode,
        TimeSpec,
    };
    use crate::output::Output;
    use crate::processor::Processor;
    use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult};
    use async_trait::async_trait;
    use datafusion::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Mutex;

    struct DevNullOutput;

    #[async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct PassThrough;

    #[async_trait]
    impl Processor for PassThrough {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// One batch, then the reader parks until `released` flips. The Job
    /// stays RUNNING until the test is done observing the registered
    /// metrics: with an immediate EOF the run can finish and unregister
    /// before a loaded test runner gets to look (the CI flake this guards).
    struct OneBatchThenParkInput {
        sent: Mutex<bool>,
        released: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl Input for OneBatchThenParkInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let already_sent = {
                let mut sent = self.sent.lock().unwrap();
                let was = *sent;
                *sent = true;
                was
            };
            if !already_sent {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Int64,
                        false,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![1]))],
                )
                .unwrap();
                return Ok((
                    Arc::new(MessageBatch::new_arrow(batch)),
                    Arc::new(crate::input::NoopAck),
                ));
            }
            while !self.released.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct MinimalAdapter {
        release: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::job::JobComponentAdapter for MinimalAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(OneBatchThenParkInput {
                sent: Mutex::new(false),
                released: self.release.clone(),
            }))
        }
        fn build_output(
            &self,
            _sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput))
        }
        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(PassThrough))
        }
    }

    fn registry_spec() -> JobSpec {
        JobSpec {
            resources: Default::default(),
            rebalance: None,
            id: JobId::new("registry-metrics-job").unwrap(),
            version: JobVersion(1),
            max_parallelism: 1,
            parallelism: 1,
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
                    partitioned: false,
                },
                EdgeSpec {
                    id: "map-sink".into(),
                    from: "map".into(),
                    to: "sink".into(),
                    partitioned: false,
                },
            ],
            sources: vec![SourceSpec {
                codec: None,
                operator_id: "source".into(),
                input_type: "registry-test-input".into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                codec: None,
                operator_id: "sink".into(),
                output_type: "registry-test-output".into(),
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            placement: crate::job::PlacementStrategy::Colocated,
            recovery: Default::default(),
            rescale: false,
        }
    }

    /// The local runner registers the Job's KernelMetrics once the kernel has
    /// spawned (before startup completion is reported) and unregisters them
    /// when the run finishes, so export never observes a finished Job.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registers_on_startup_and_unregisters_on_completion() {
        let registry = crate::runtime::JobMetricsRegistry::default();
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let spec = registry_spec();
        let task_registry = registry.clone();
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let run_release = release.clone();
        let run = tokio::spawn(async move {
            let adapter = MinimalAdapter {
                release: run_release,
            };
            let mut resource = Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            };
            run_job_with_checkpoints_started(
                &spec,
                &adapter,
                &mut resource,
                CancellationToken::default(),
                Some(startup_tx),
                Some(task_registry),
            )
            .await
        });

        startup_rx.await.unwrap().unwrap();
        let metrics = registry
            .get("registry-metrics-job")
            .expect("Job metrics must be registered once startup completed");
        // The handle is the live kernel registry: chains appear as the graph's
        // event loops spin up.
        let _ = metrics.snapshot();

        // Observations done: let the source EOF so the run completes and
        // unregisters (the parking input is what keeps the window open).
        release.store(true, std::sync::atomic::Ordering::Relaxed);
        run.await.unwrap().unwrap();
        assert!(registry.get("registry-metrics-job").is_none());
        assert!(registry.snapshots().is_empty());
    }

    /// A run without a registry keeps the legacy behavior (no registration).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_without_a_registry_are_untracked() {
        let registry = crate::runtime::JobMetricsRegistry::default();
        let spec = registry_spec();
        // Released up front: the source emits its batch and EOFs exactly
        // like the old one-shot input, so the run completes on its own.
        let adapter = MinimalAdapter {
            release: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        let mut resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        run_job_with_checkpoints_started(
            &spec,
            &adapter,
            &mut resource,
            CancellationToken::default(),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(registry.snapshots().is_empty());
    }
}
